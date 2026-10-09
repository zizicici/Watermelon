import Foundation

struct RestoreDownloadPolicy: Sendable {
    var workerCount = 2
    var bufferedByteLimit: Int64 = 256 * 1024 * 1024
    var freeSpaceReserve: Int64 = 64 * 1024 * 1024

    static let serial = RestoreDownloadPolicy(workerCount: 1)

    static func estimatedBytes(for instances: [RemoteAssetResourceInstance]) -> Int64 {
        var hashes = Set<Data>()
        var bytes: Int64 = 0
        for instance in instances {
            if !instance.resourceHash.isEmpty, !hashes.insert(instance.resourceHash).inserted { continue }
            bytes = adding(bytes, max(0, instance.fileSize))
        }
        return bytes
    }

    static func adding(_ lhs: Int64, _ rhs: Int64) -> Int64 {
        let (sum, overflow) = lhs.addingReportingOverflow(rhs)
        return overflow ? .max : sum
    }

    func admits(bytes: Int64, reserved: [Int64], available: Int64?) -> Bool {
        let total = reserved.reduce(bytes, Self.adding)
        if !reserved.isEmpty {
            guard bytes > 0, reserved.allSatisfy({ $0 > 0 }), total <= bufferedByteLimit else { return false }
        }
        guard let available else { return true }
        let largest = max(bytes, reserved.max() ?? 0)
        let required = Self.adding(Self.adding(total, Self.adding(largest, largest)), freeSpaceReserve)
        return required <= available
    }

    func hasImportCapacity(bytes: Int64, available: Int64?) -> Bool {
        guard let available else { return true }
        return Self.adding(Self.adding(bytes, bytes), freeSpaceReserve) <= available
    }
}

final class OrderedRestorePipeline<Value: Sendable>: Sendable {
    private final class Tasks: @unchecked Sendable {
        private let lock = NSLock()
        private var tasks: [Int: Task<Value, Error>] = [:]
        private var bytes: [Int: Int64] = [:]
        private var currentIndex = 0
        private var draining = false
        private var cancelled = false
        private var failed = false

        func begin(_ index: Int, shouldDrain: () -> Bool) throws {
            try lock.withLock {
                guard !cancelled, !draining, !shouldDrain(), !Task.isCancelled else { throw CancellationError() }
                currentIndex = index
            }
        }

        func insert(_ task: Task<Value, Error>, at index: Int, estimatedBytes: Int64) {
            let cancel = lock.withLock {
                tasks[index] = task
                if bytes[index] == nil { bytes[index] = estimatedBytes }
                return cancelled || (draining && index != currentIndex)
            }
            if cancel { task.cancel() }
        }

        func updateBytes(_ actual: Int64, at index: Int) {
            lock.withLock { bytes[index] = actual }
        }

        var reservedBytes: [Int64] { lock.withLock { Array(bytes.values) } }

        var canSchedule: Bool { lock.withLock { !cancelled && !draining && !failed } }

        func recordFailure() { lock.withLock { failed = true } }

        func finish(_ index: Int) {
            lock.withLock {
                tasks[index] = nil
                bytes[index] = nil
            }
        }

        func cancel(prefetchOnly: Bool = false) -> [Task<Value, Error>] {
            let selected = lock.withLock {
                if prefetchOnly { draining = true } else { cancelled = true }
                return tasks.filter { !prefetchOnly || $0.key != currentIndex }.map(\.value)
            }
            selected.forEach { $0.cancel() }
            return selected
        }
    }

    let policy: RestoreDownloadPolicy

    func run(
        estimatedBytes: [Int64],
        shouldDrain: @escaping @Sendable () -> Bool,
        availableCapacity: @escaping @Sendable () -> Int64?,
        byteCount: @escaping @Sendable (Value) -> Int64,
        resolveSize: (@Sendable (_ index: Int) async throws -> Int64?)? = nil,
        prepare: @escaping @Sendable (_ index: Int, _ workerID: Int) async throws -> Value,
        commit: (_ index: Int, _ result: Result<Value, Error>) async throws -> Void
    ) async throws {
        let tasks = Tasks()
        let monitor = Task.detached {
            while !Task.isCancelled {
                if shouldDrain() {
                    _ = tasks.cancel(prefetchOnly: true)
                    return
                }
                do { try await Task.sleep(for: .milliseconds(50)) } catch { return }
            }
        }
        let outcome: Result<Void, Error> = await withTaskCancellationHandler {
            var pending: [Int: (workerID: Int, task: Task<Value, Error>)] = [:]
            var idleWorkers = Array(0..<min(max(1, policy.workerCount), 4))
            var nextIndex = 0
            do {
                for index in estimatedBytes.indices {
                    try tasks.begin(index, shouldDrain: shouldDrain)
                    while nextIndex < estimatedBytes.count, !idleWorkers.isEmpty, tasks.canSchedule, !shouldDrain() {
                        let reserved = tasks.reservedBytes
                        let needsSizeResolution = !policy.admits(
                            bytes: estimatedBytes[nextIndex], reserved: reserved, available: availableCapacity()
                        )
                        if needsSizeResolution {
                            if !pending.isEmpty { break }
                            guard resolveSize != nil else { throw CocoaError(.fileWriteOutOfSpace) }
                        }
                        let scheduledIndex = nextIndex
                        let workerID = idleWorkers.removeFirst()
                        let task = Task.detached(priority: .userInitiated) { [policy] in
                            do {
                                try Task.checkCancellation()
                                if needsSizeResolution, let size = try await resolveSize?(scheduledIndex) {
                                    guard policy.admits(bytes: size, reserved: [], available: availableCapacity()) else {
                                        throw CocoaError(.fileWriteOutOfSpace)
                                    }
                                }
                                try Task.checkCancellation()
                                if needsSizeResolution, shouldDrain() { throw CancellationError() }
                                let value = try await prepare(scheduledIndex, workerID)
                                let actualBytes = byteCount(value)
                                if needsSizeResolution, !policy.hasImportCapacity(bytes: actualBytes, available: availableCapacity()) {
                                    throw CocoaError(.fileWriteOutOfSpace)
                                }
                                tasks.updateBytes(actualBytes, at: scheduledIndex)
                                return value
                            } catch {
                                tasks.recordFailure()
                                throw error
                            }
                        }
                        tasks.insert(task, at: scheduledIndex, estimatedBytes: estimatedBytes[scheduledIndex])
                        pending[scheduledIndex] = (workerID, task)
                        nextIndex += 1
                        if needsSizeResolution { break }
                    }
                    guard let current = pending[index] else { throw CancellationError() }
                    let result = await current.task.result
                    try Task.checkCancellation()
                    try await commit(index, result)
                    tasks.finish(index)
                    pending[index] = nil
                    idleWorkers.append(current.workerID)
                }
                return .success(())
            } catch {
                return .failure(error)
            }
        } onCancel: {
            _ = tasks.cancel()
        }
        monitor.cancel()
        await monitor.value
        for task in tasks.cancel() { _ = await task.result }
        try outcome.get()
    }

    init(policy: RestoreDownloadPolicy) {
        self.policy = policy
    }
}
