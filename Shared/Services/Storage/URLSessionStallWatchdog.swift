import Foundation

nonisolated final class URLSessionTaskRegistry: @unchecked Sendable {
    private let lock = NSLock()
    private var tasks: [ObjectIdentifier: URLSessionTask] = [:]

    func register(_ task: URLSessionTask) {
        lock.withLock { tasks[ObjectIdentifier(task)] = task }
    }

    func unregister(_ task: URLSessionTask) {
        _ = lock.withLock { tasks.removeValue(forKey: ObjectIdentifier(task)) }
    }

    func cancelAll() {
        let active = lock.withLock {
            let active = Array(tasks.values)
            tasks.removeAll()
            return active
        }
        active.forEach { $0.cancel() }
    }

    func data(for request: URLRequest, in session: URLSession) async throws -> (Data, URLResponse) {
        let taskBox = RegisteredURLSessionTaskBox()
        return try await withTaskCancellationHandler(operation: {
            try await withCheckedThrowingContinuation { continuation in
                let task = session.dataTask(with: request) { data, response, error in
                    if let task = taskBox.value() { self.unregister(task) }
                    if let error {
                        continuation.resume(throwing: error)
                    } else if let response {
                        continuation.resume(returning: (data ?? Data(), response))
                    } else {
                        continuation.resume(throwing: URLError(.badServerResponse))
                    }
                }
                taskBox.set(task)
                register(task)
                task.resume()
                if Task.isCancelled { task.cancel() }
            }
        }, onCancel: {
            taskBox.cancel()
        })
    }

    func upload(for request: URLRequest, from body: Data, in session: URLSession) async throws -> (Data, URLResponse) {
        let taskBox = RegisteredURLSessionTaskBox()
        return try await withTaskCancellationHandler(operation: {
            try await withCheckedThrowingContinuation { continuation in
                let task = session.uploadTask(with: request, from: body) { data, response, error in
                    if let task = taskBox.value() { self.unregister(task) }
                    if let error {
                        continuation.resume(throwing: error)
                    } else if let response {
                        continuation.resume(returning: (data ?? Data(), response))
                    } else {
                        continuation.resume(throwing: URLError(.badServerResponse))
                    }
                }
                taskBox.set(task)
                register(task)
                task.resume()
                if Task.isCancelled { task.cancel() }
            }
        }, onCancel: {
            taskBox.cancel()
        })
    }

}

private final class RegisteredURLSessionTaskBox: @unchecked Sendable {
    private let lock = NSLock()
    private var task: URLSessionTask?

    func set(_ task: URLSessionTask) {
        lock.withLock { self.task = task }
    }

    func cancel() {
        lock.withLock { task }?.cancel()
    }

    func value() -> URLSessionTask? {
        lock.withLock { task }
    }
}

// No-progress stall watchdog for URLSession transfers. A half-open socket — bytes stop flowing but the
// connection never resets — would otherwise hang for the transfer session's multi-day timeout, stalling the
// worker and its lease. The watchdog cancels the task after a no-progress window and surfaces a per-client stall
// error the fault classifier treats as retryable, so the caller reconnects instead of hanging the run.
nonisolated enum URLSessionStallWatchdog {

    enum Stall {
        case uploadBody       // request body bytes stopped flowing
        case uploadResponse   // body fully sent, server response never arrived
        case download         // response bytes stalled, or the first byte never arrived
    }

    struct Timeouts {
        let uploadBodyStall: TimeInterval
        let uploadResponseStall: TimeInterval
        let downloadFirstByte: TimeInterval
        let downloadStall: TimeInterval
        let pollInterval: TimeInterval
    }

    enum Body {
        case file(URL)
        case data(Data)
    }

    typealias StallErrorFactory = @Sendable (_ stall: Stall, _ timeout: TimeInterval, _ bytesTransferred: Int64, _ expectedBytes: Int64?) -> Error

    static func runUpload(
        session: URLSession,
        delegate: Delegate,
        registry: URLSessionTaskRegistry? = nil,
        request: URLRequest,
        body: Body,
        onProgress: ((Double) -> Void)?,
        timeouts: Timeouts,
        makeStallError: @escaping StallErrorFactory
    ) async throws -> (Data, HTTPURLResponse) {
        let progress = UploadProgress(onProgress: onProgress)
        let taskBox = TaskBox()
        let watchdog = Task { await watchUpload(progress: progress, taskBox: taskBox, timeouts: timeouts, makeStallError: makeStallError) }
        defer {
            watchdog.cancel()
            if let task = taskBox.value() {
                registry?.unregister(task)
                delegate.unregister(task)
            }
        }

        return try await withTaskCancellationHandler(operation: {
            try await withCheckedThrowingContinuation { continuation in
                let completion: @Sendable (Data?, URLResponse?, Error?) -> Void = { data, response, error in
                    progress.finish()
                    if let task = taskBox.value() {
                        registry?.unregister(task)
                        delegate.unregister(task)
                    }
                    if let error {
                        continuation.resume(throwing: progress.resolvedTimeoutError() ?? error)
                        return
                    }
                    guard let http = response as? HTTPURLResponse else {
                        continuation.resume(throwing: unexpectedResponseError())
                        return
                    }
                    continuation.resume(returning: (data ?? Data(), http))
                }
                let task: URLSessionUploadTask
                switch body {
                case .file(let url): task = session.uploadTask(with: request, fromFile: url, completionHandler: completion)
                case .data(let data): task = session.uploadTask(with: request, from: data, completionHandler: completion)
                }
                taskBox.set(task)
                registry?.register(task)
                delegate.register(progress, for: task)
                task.resume()
                progress.resetProgressClock()
                if Task.isCancelled { task.cancel() }
            }
        }, onCancel: {
            taskBox.cancel()
        })
    }

    static func runDownload(
        session: URLSession,
        registry: URLSessionTaskRegistry? = nil,
        request: URLRequest,
        onProgress: ((Double) -> Void)? = nil,
        timeouts: Timeouts,
        makeStallError: @escaping StallErrorFactory
    ) async throws -> (URL, HTTPURLResponse) {
        let progress = DownloadProgress(onProgress: onProgress)
        let taskBox = TaskBox()
        let watchdog = Task { await watchDownload(progress: progress, taskBox: taskBox, timeouts: timeouts, makeStallError: makeStallError) }
        defer {
            watchdog.cancel()
            if let task = taskBox.value() { registry?.unregister(task) }
        }

        return try await withTaskCancellationHandler(operation: {
            try await withCheckedThrowingContinuation { continuation in
                let task = session.downloadTask(with: request) { temporaryURL, response, error in
                    if let task = taskBox.value() { registry?.unregister(task) }
                    if let task = taskBox.value() {
                        progress.recordProgress(
                            bytesWritten: task.countOfBytesReceived,
                            totalBytesExpectedToWrite: task.countOfBytesExpectedToReceive
                        )
                    }
                    progress.finish()
                    if let error {
                        continuation.resume(throwing: progress.resolvedTimeoutError() ?? error)
                        return
                    }
                    guard let temporaryURL, let http = response as? HTTPURLResponse else {
                        continuation.resume(throwing: unexpectedResponseError())
                        return
                    }
                    do {
                        // The completion-handler temp file is deleted when this returns; move it somewhere stable.
                        let stable = FileManager.default.temporaryDirectory
                            .appendingPathComponent("Watermelon-Transfer-\(UUID().uuidString)", isDirectory: false)
                        try FileManager.default.moveItem(at: temporaryURL, to: stable)
                        continuation.resume(returning: (stable, http))
                    } catch {
                        continuation.resume(throwing: error)
                    }
                }
                taskBox.set(task)
                registry?.register(task)
                task.resume()
                progress.resetProgressClock()
                if Task.isCancelled { task.cancel() }
            }
        }, onCancel: {
            taskBox.cancel()
        })
    }

    // Response-only transfer (no request body to stream, e.g. S3 UploadPartCopy): bounded by the same
    // first-byte / stall windows as a download, fed from the task's received-bytes counter.
    static func runData(
        session: URLSession,
        registry: URLSessionTaskRegistry? = nil,
        request: URLRequest,
        timeouts: Timeouts,
        makeStallError: @escaping StallErrorFactory
    ) async throws -> (Data, HTTPURLResponse) {
        let progress = DownloadProgress(onProgress: nil)
        let taskBox = TaskBox()
        let watchdog = Task { await watchDownload(progress: progress, taskBox: taskBox, timeouts: timeouts, makeStallError: makeStallError) }
        defer {
            watchdog.cancel()
            if let task = taskBox.value() { registry?.unregister(task) }
        }

        return try await withTaskCancellationHandler(operation: {
            try await withCheckedThrowingContinuation { continuation in
                let task = session.dataTask(with: request) { data, response, error in
                    progress.finish()
                    if let task = taskBox.value() { registry?.unregister(task) }
                    if let error {
                        continuation.resume(throwing: progress.resolvedTimeoutError() ?? error)
                        return
                    }
                    guard let http = response as? HTTPURLResponse else {
                        continuation.resume(throwing: unexpectedResponseError())
                        return
                    }
                    continuation.resume(returning: (data ?? Data(), http))
                }
                taskBox.set(task)
                registry?.register(task)
                task.resume()
                progress.resetProgressClock()
                if Task.isCancelled { task.cancel() }
            }
        }, onCancel: {
            taskBox.cancel()
        })
    }

    private static func watchUpload(
        progress: UploadProgress,
        taskBox: TaskBox,
        timeouts: Timeouts,
        makeStallError: @escaping StallErrorFactory
    ) async {
        while !Task.isCancelled {
            do { try await Task.sleep(nanoseconds: nanoseconds(for: timeouts.pollInterval)) } catch { return }

            let snapshot = progress.snapshot()
            if snapshot.isFinished { return }

            let timeout: TimeInterval
            let stall: Stall
            switch snapshot.phase {
            case .sendingBody:
                timeout = timeouts.uploadBodyStall
                stall = .uploadBody
            case .awaitingResponse:
                timeout = timeouts.uploadResponseStall
                stall = .uploadResponse
            }

            guard elapsedSeconds(since: snapshot.lastProgressAtNanos) >= timeout else { continue }
            let error = makeStallError(stall, timeout, snapshot.bytesSent, snapshot.expectedBytes)
            if progress.markTimedOut(error) { taskBox.cancel() }
            return
        }
    }

    private static func watchDownload(
        progress: DownloadProgress,
        taskBox: TaskBox,
        timeouts: Timeouts,
        makeStallError: @escaping StallErrorFactory
    ) async {
        while !Task.isCancelled {
            do { try await Task.sleep(nanoseconds: nanoseconds(for: timeouts.pollInterval)) } catch { return }

            // Completion-handler download/data tasks suppress delegate progress callbacks, so feed the watchdog
            // from URLSession's internal received-bytes counters instead.
            if let task = taskBox.value() {
                progress.recordProgress(
                    bytesWritten: task.countOfBytesReceived,
                    totalBytesExpectedToWrite: task.countOfBytesExpectedToReceive
                )
            }

            let snapshot = progress.snapshot()
            if snapshot.isFinished { return }

            let timeout: TimeInterval
            switch snapshot.phase {
            case .awaitingFirstByte: timeout = timeouts.downloadFirstByte
            case .receivingBody: timeout = timeouts.downloadStall
            }

            guard elapsedSeconds(since: snapshot.lastProgressAtNanos) >= timeout else { continue }
            let error = makeStallError(.download, timeout, snapshot.bytesWritten, snapshot.expectedBytes)
            if progress.markTimedOut(error) { taskBox.cancel() }
            return
        }
    }

    private static func unexpectedResponseError() -> Error {
        NSError(domain: "URLSessionStallWatchdog", code: -1, userInfo: [
            NSLocalizedDescriptionKey: "Unexpected non-HTTP response"
        ])
    }

    private static func nanoseconds(for interval: TimeInterval) -> UInt64 {
        UInt64(max(interval, 0) * 1_000_000_000)
    }

    // DispatchTime uptime pauses during device sleep, matching foreground URLSession transfer behavior.
    private static func elapsedSeconds(since startNanos: UInt64) -> TimeInterval {
        let now = DispatchTime.now().uptimeNanoseconds
        guard now >= startNanos else { return 0 }
        return TimeInterval(now - startNanos) / 1_000_000_000
    }

    // Upload byte-progress source: set as the transfer session's task delegate at construction.
    class Delegate: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
        private let lock = NSLock()
        private var uploadStates: [Int: UploadProgress] = [:]

        func register(_ state: UploadProgress, for task: URLSessionTask) {
            lock.withLock { uploadStates[task.taskIdentifier] = state }
        }

        func unregister(_ task: URLSessionTask) {
            lock.withLock { uploadStates[task.taskIdentifier] = nil }
        }

        func urlSession(
            _: URLSession,
            task: URLSessionTask,
            didSendBodyData _: Int64,
            totalBytesSent: Int64,
            totalBytesExpectedToSend: Int64
        ) {
            let state = lock.withLock { uploadStates[task.taskIdentifier] }
            state?.recordProgress(bytesSent: totalBytesSent, totalBytesExpectedToSend: totalBytesExpectedToSend)
        }
    }

    final class UploadProgress: @unchecked Sendable {
        enum Phase {
            case sendingBody
            case awaitingResponse
        }

        struct Snapshot {
            let phase: Phase
            let bytesSent: Int64
            let expectedBytes: Int64?
            let lastProgressAtNanos: UInt64
            let isFinished: Bool
        }

        private let lock = NSLock()
        private let onProgress: ((Double) -> Void)?
        private var phase: Phase = .sendingBody
        private var bytesSent: Int64 = 0
        private var expectedBytes: Int64?
        private var lastProgressAtNanos = DispatchTime.now().uptimeNanoseconds
        private var timeoutError: Error?
        private var isFinished = false

        init(onProgress: ((Double) -> Void)?) {
            self.onProgress = onProgress
        }

        func resetProgressClock() {
            lock.withLock {
                guard !isFinished else { return }
                lastProgressAtNanos = DispatchTime.now().uptimeNanoseconds
            }
        }

        func recordProgress(bytesSent incomingBytesSent: Int64, totalBytesExpectedToSend: Int64) {
            let progressToEmit: Double? = lock.withLock {
                guard !isFinished else { return nil }
                let now = DispatchTime.now().uptimeNanoseconds
                if incomingBytesSent > bytesSent {
                    bytesSent = incomingBytesSent
                    lastProgressAtNanos = now
                }
                if totalBytesExpectedToSend > 0 {
                    expectedBytes = totalBytesExpectedToSend
                }
                if let expectedBytes, expectedBytes > 0, incomingBytesSent >= expectedBytes, phase == .sendingBody {
                    phase = .awaitingResponse
                    lastProgressAtNanos = now
                }
                guard let expectedBytes, expectedBytes > 0 else { return nil }
                return min(max(Double(bytesSent) / Double(expectedBytes), 0), 1)
            }
            if let progressToEmit { onProgress?(progressToEmit) }
        }

        func snapshot() -> Snapshot {
            lock.withLock {
                Snapshot(phase: phase, bytesSent: bytesSent, expectedBytes: expectedBytes,
                         lastProgressAtNanos: lastProgressAtNanos, isFinished: isFinished)
            }
        }

        func markTimedOut(_ error: Error) -> Bool {
            lock.withLock {
                guard !isFinished, timeoutError == nil else { return false }
                timeoutError = error
                return true
            }
        }

        func finish() { lock.withLock { isFinished = true } }
        func resolvedTimeoutError() -> Error? { lock.withLock { timeoutError } }
    }

    final class DownloadProgress: @unchecked Sendable {
        enum Phase {
            case awaitingFirstByte
            case receivingBody
        }

        struct Snapshot {
            let phase: Phase
            let bytesWritten: Int64
            let expectedBytes: Int64?
            let lastProgressAtNanos: UInt64
            let isFinished: Bool
        }

        private let lock = NSLock()
        private let onProgress: ((Double) -> Void)?
        private var phase: Phase = .awaitingFirstByte
        private var bytesWritten: Int64 = 0
        private var expectedBytes: Int64?
        private var lastProgressAtNanos = DispatchTime.now().uptimeNanoseconds
        private var timeoutError: Error?
        private var isFinished = false

        init(onProgress: ((Double) -> Void)?) {
            self.onProgress = onProgress
        }

        func resetProgressClock() {
            lock.withLock {
                guard !isFinished else { return }
                lastProgressAtNanos = DispatchTime.now().uptimeNanoseconds
            }
        }

        func recordProgress(bytesWritten incomingBytesWritten: Int64, totalBytesExpectedToWrite: Int64) {
            let progressToEmit: Double? = lock.withLock {
                guard !isFinished else { return nil }
                if totalBytesExpectedToWrite > 0 { expectedBytes = totalBytesExpectedToWrite }
                if incomingBytesWritten > bytesWritten {
                    bytesWritten = incomingBytesWritten
                    phase = .receivingBody
                    lastProgressAtNanos = DispatchTime.now().uptimeNanoseconds
                }
                guard let expectedBytes, expectedBytes > 0 else { return nil }
                return min(max(Double(bytesWritten) / Double(expectedBytes), 0), 1)
            }
            if let progressToEmit { onProgress?(progressToEmit) }
        }

        func snapshot() -> Snapshot {
            lock.withLock {
                Snapshot(phase: phase, bytesWritten: bytesWritten, expectedBytes: expectedBytes,
                         lastProgressAtNanos: lastProgressAtNanos, isFinished: isFinished)
            }
        }

        func markTimedOut(_ error: Error) -> Bool {
            lock.withLock {
                guard !isFinished, timeoutError == nil else { return false }
                timeoutError = error
                return true
            }
        }

        func finish() { lock.withLock { isFinished = true } }
        func resolvedTimeoutError() -> Error? { lock.withLock { timeoutError } }
    }

    final class TaskBox: @unchecked Sendable {
        private let lock = NSLock()
        private var task: URLSessionTask?
        func set(_ task: URLSessionTask) { lock.withLock { self.task = task } }
        func cancel() { lock.withLock { task }?.cancel() }
        func value() -> URLSessionTask? { lock.withLock { task } }
    }
}
