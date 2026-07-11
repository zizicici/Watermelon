import XCTest
@testable import Watermelon

// Classification + reconnect-primitive coverage for the network-fluctuation recovery path. End-to-end
// worker recovery (BackupParallelExecutor) is PHAsset-dependent and covered by manual regression.
final class NetworkFaultRecoveryTests: XCTestCase {

    private func profile(_ type: StorageType) -> ServerProfileRecord {
        ServerProfileRecord(
            id: nil,
            name: "p",
            storageType: type.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: "host.local",
            port: 0,
            shareName: "share",
            basePath: "/p",
            username: "u",
            domain: nil,
            credentialRef: "ref",
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date(),
            writerID: nil
        )
    }

    private func s3(_ serverCode: String, status: Int) -> Error {
        RemoteStorageClientError.underlying(NSError(
            domain: S3ErrorClassifier.errorDomain,
            code: status,
            userInfo: [
                S3ErrorClassifier.userInfoServerCodeKey: serverCode,
                S3ErrorClassifier.userInfoStatusCodeKey: status
            ]
        ))
    }

    private func url(_ code: Int) -> Error { NSError(domain: NSURLErrorDomain, code: code) }

    private func webdav(_ status: Int) -> Error {
        RemoteStorageClientError.underlying(NSError(domain: WebDAVClient.errorDomain, code: status))
    }

    // MARK: - isRecoverableNetworkFault

    func testS3SlowDownRecoverableOnNetworkBackend() {
        XCTAssertTrue(AssetProcessor.isRecoverableNetworkFault(s3("SlowDown", status: 503), profile: profile(.s3)))
    }

    func testConnectionLostRecoverableAcrossNetworkBackends() {
        XCTAssertTrue(AssetProcessor.isRecoverableNetworkFault(url(NSURLErrorNetworkConnectionLost), profile: profile(.s3)))
        XCTAssertTrue(AssetProcessor.isRecoverableNetworkFault(url(NSURLErrorTimedOut), profile: profile(.smb)))
        XCTAssertTrue(AssetProcessor.isRecoverableNetworkFault(webdav(503), profile: profile(.webdav)))
    }

    func testExternalVolumeNeverRecoverable() {
        // A local volume disappearing is not a transient network blip; never enter reconnect recovery.
        XCTAssertFalse(AssetProcessor.isRecoverableNetworkFault(url(NSURLErrorNetworkConnectionLost), profile: profile(.externalVolume)))
        XCTAssertFalse(AssetProcessor.isRecoverableNetworkFault(RemoteStorageClientError.unavailable, profile: profile(.externalVolume)))
    }

    func testTerminalAndCancellationNotRecoverable() {
        XCTAssertFalse(AssetProcessor.isRecoverableNetworkFault(s3("AccessDenied", status: 403), profile: profile(.s3)))
        XCTAssertFalse(AssetProcessor.isRecoverableNetworkFault(CancellationError(), profile: profile(.s3)))
    }

    // MARK: - isNetworkUnavailableFatal (skip-flush / pause routing)

    func testSentinelRoutesToNetworkUnavailable() {
        let sentinel = BackupNetworkRecoveryExhausted(underlying: url(NSURLErrorNetworkConnectionLost))
        XCTAssertTrue(BackupParallelExecutor.isNetworkUnavailableFatal(sentinel, profile: profile(.s3)))
    }

    func testEjectedExternalVolumeRoutesToNetworkUnavailable() {
        XCTAssertTrue(BackupParallelExecutor.isNetworkUnavailableFatal(
            RemoteStorageClientError.externalStorageUnavailable,
            profile: profile(.externalVolume)
        ))
    }

    func testPlainTerminalNotNetworkUnavailable() {
        XCTAssertFalse(BackupParallelExecutor.isNetworkUnavailableFatal(s3("AccessDenied", status: 403), profile: profile(.s3)))
    }

    // Pool reserved-slot replacement (retire-before-connect, bounded connect, reaping) is covered by
    // StorageClientPoolReplacementTests.

    // MARK: - Reducer: sentinel -> paused mapping

    private func runErrorState(
        error: Error,
        phaseBeforeFailure: BackupSessionControlPhase,
        intent: BackupTerminationIntent = .none,
        seedResume: Set<String> = ["a", "b"]
    ) -> BackupSessionState {
        var state = BackupSessionState()
        state.completedAssetIDsForResume = seedResume
        state.applyRunError(
            error,
            runMode: .full,
            displayMode: .full,
            externalUnavailable: false,
            intent: intent,
            phaseBeforeFailure: phaseBeforeFailure
        )
        return state
    }

    func testSentinelMapsToPausedNotFailed() {
        let sentinel = BackupNetworkRecoveryExhausted(underlying: url(NSURLErrorNetworkConnectionLost))
        let state = runErrorState(error: sentinel, phaseBeforeFailure: .idle)
        XCTAssertEqual(state.state, .paused)
        XCTAssertNotNil(state.lastPausedRunMode)
        // applyRunError reaches via a THROWN sentinel (load/asset/incremental exhaustion), so the whole
        // resume-complete set is cleared to force replanning of uncommitted months.
        XCTAssertTrue(state.completedAssetIDsForResume.isEmpty)
    }

    // ADVERSARIAL: if the user was STOPPING when the network recovery exhausted, the sentinel must settle
    // as .stopped (stop wins), not .paused — mirroring the cancellation mapping.
    func testSentinelDuringStoppingMapsToStopped() {
        let sentinel = BackupNetworkRecoveryExhausted(underlying: url(NSURLErrorTimedOut))
        let state = runErrorState(error: sentinel, phaseBeforeFailure: .stopping)
        XCTAssertEqual(state.state, .stopped)
        XCTAssertNil(state.lastPausedRunMode)
    }

    // ADVERSARIAL: an explicit stop intent must override the sentinel's pause default.
    func testExplicitStopIntentOverridesSentinel() {
        let sentinel = BackupNetworkRecoveryExhausted(underlying: url(NSURLErrorNetworkConnectionLost))
        let state = runErrorState(error: sentinel, phaseBeforeFailure: .idle, intent: .stop)
        XCTAssertEqual(state.state, .stopped)
    }

    // CONTROL: a plain terminal fault (no intent, not cancelled, not sentinel) must still go to .failed,
    // and must NOT be silently paused. Proves the sentinel broadening didn't swallow real failures.
    func testPlainTerminalStillFails() {
        let state = runErrorState(error: s3("AccessDenied", status: 403), phaseBeforeFailure: .idle)
        XCTAssertEqual(state.state, .failed)
    }

    // CONTROL: the underlying of the sentinel does not matter — even a terminal underlying still pauses,
    // because the reducer keys on the sentinel TYPE, not classify(underlying).
    func testSentinelWithTerminalUnderlyingStillPauses() {
        let sentinel = BackupNetworkRecoveryExhausted(underlying: s3("AccessDenied", status: 403))
        let state = runErrorState(error: sentinel, phaseBeforeFailure: .idle)
        XCTAssertEqual(state.state, .paused)
    }

    // MARK: - Classification precedence edge cases

    // ADVERSARIAL: cancellation must out-rank a coincident retryable signal so a user pause during a blip is
    // never misread as a recoverable network fault (would re-enter recovery instead of pausing).
    func testCancellationOutranksRetryableForRecoverable() {
        let cancelled = CancellationError()
        XCTAssertFalse(AssetProcessor.isRecoverableNetworkFault(cancelled, profile: profile(.s3)))
        XCTAssertEqual(RemoteFaultLite.classify(cancelled), .cancelled)
    }

    // ADVERSARIAL: a chain carrying BOTH a transient transport fault and a not-found token must classify
    // retryable (fail-closed toward transient), so recovery engages rather than treating a blink as absence.
    func testRetryableOutranksNotFoundInChain() {
        let mixed = RemoteStorageClientError.underlying(NSError(
            domain: NSURLErrorDomain,
            code: NSURLErrorNetworkConnectionLost,
            userInfo: [NSUnderlyingErrorKey: NSError(domain: NSPOSIXErrorDomain, code: Int(ENOENT))]
        ))
        XCTAssertEqual(RemoteFaultLite.classify(mixed), .retryable)
        XCTAssertTrue(AssetProcessor.isRecoverableNetworkFault(mixed, profile: profile(.webdav)))
    }

    // ADVERSARIAL: WebDAV 500 stays terminal even wrapped, so it is NOT recoverable on a WebDAV backend.
    func testWebDAV500NotRecoverableOnWebDAV() {
        XCTAssertEqual(RemoteFaultLite.classify(webdav(500)), .terminal)
        XCTAssertFalse(AssetProcessor.isRecoverableNetworkFault(webdav(500), profile: profile(.webdav)))
    }

    // ADVERSARIAL: a WebDAV transient status (503) on an EXTERNAL VOLUME profile must NOT be recoverable —
    // the externalVolume gate fires before classification, keeping the original fail-fast behavior.
    func testWebDAVTransientOnExternalVolumeNotRecoverable() {
        XCTAssertFalse(AssetProcessor.isRecoverableNetworkFault(webdav(503), profile: profile(.externalVolume)))
    }

    // MARK: - boundedConnect (restore / lock-client bare connects)

    func testBoundedConnectSucceeds() async throws {
        let client = ProbeStorageClient(.succeed)
        try await NetworkRecovery.boundedConnect(client, deadline: Date().addingTimeInterval(5))
        let connected = await client.connected
        XCTAssertTrue(connected)
    }

    // An uncooperative connect that never returns must be abandoned at the deadline (mapped to .unavailable,
    // which classifies retryable), not hang the caller for the session's multi-day timeout.
    func testBoundedConnectTimesOutOnHungConnect() async {
        let client = ProbeStorageClient(.delay(60, cancellable: false))
        let start = Date()
        do {
            try await NetworkRecovery.boundedConnect(client, deadline: Date().addingTimeInterval(0.3))
            XCTFail("a hung connect must throw, not return")
        } catch {
            XCTAssertLessThan(Date().timeIntervalSince(start), 5, "boundedConnect overshot its deadline")
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
        }
    }

    // A terminal connect fault surfaces unchanged so the caller fails fast rather than pausing.
    func testBoundedConnectSurfacesConnectError() async {
        let client = ProbeStorageClient(.throwError(RemoteStorageClientError.invalidConfiguration))
        do {
            try await NetworkRecovery.boundedConnect(client, deadline: Date().addingTimeInterval(5))
            XCTFail("a failing connect must throw")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .terminal)
        }
    }

    func testLateReapStartsAfterAbandonmentHook() async {
        let order = AbandonmentOrderRecorder()
        let result = await NetworkRecovery.boundedAttempt(
            deadline: Date().addingTimeInterval(0.01),
            onAbandon: { order.append("abandon") },
            reap: { (_: Int) in order.append("reap") },
            op: {
                await withCheckedContinuation { continuation in
                    DispatchQueue.global().asyncAfter(deadline: .now() + 0.05) {
                        continuation.resume(returning: 1)
                    }
                }
            }
        )

        guard case .timedOut = result else {
            return XCTFail("expected timeout")
        }
        let deadline = Date().addingTimeInterval(1)
        while order.values.count < 2, Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }
        XCTAssertEqual(order.values, ["abandon", "reap"])
    }

    func testOperationWinnerIsAbandonedAndReapedWhenParentWasCancelled() async {
        let operationGate = AsyncOperationGate()
        let mainBlockStarted = LockedFlag()
        let order = AbandonmentOrderRecorder()
        let task = Task { @MainActor in
            await NetworkRecovery.boundedAttempt(
                deadline: Date().addingTimeInterval(5),
                onAbandon: { order.append("abandon") },
                reap: { (_: Int) in order.append("reap") },
                op: {
                    await operationGate.wait()
                    return 1
                }
            )
        }

        await waitUntil { operationGate.entered }
        DispatchQueue.main.async {
            mainBlockStarted.set()
            Thread.sleep(forTimeInterval: 0.2)
        }
        await waitUntil { mainBlockStarted.value }
        operationGate.release()
        try? await Task.sleep(nanoseconds: 30_000_000)
        task.cancel()

        guard case .timedOut = await task.value else {
            return XCTFail("cancelled operation winner must be abandoned")
        }
        await waitUntil { order.values.count == 2 }
        XCTAssertEqual(order.values, ["abandon", "reap"])
    }

    private func waitUntil(
        timeout: TimeInterval = 1,
        _ condition: @escaping @Sendable () -> Bool
    ) async {
        let deadline = Date().addingTimeInterval(timeout)
        while !condition(), Date() < deadline {
            try? await Task.sleep(nanoseconds: 5_000_000)
        }
        XCTAssertTrue(condition())
    }
}

private final class AbandonmentOrderRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [String] = []

    var values: [String] {
        lock.withLock { storage }
    }

    func append(_ value: String) {
        lock.withLock { storage.append(value) }
    }
}

private final class LockedFlag: @unchecked Sendable {
    private let lock = NSLock()
    private var storage = false
    var value: Bool { lock.withLock { storage } }
    func set() { lock.withLock { storage = true } }
}

private final class AsyncOperationGate: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<Void, Never>?
    private var didEnter = false
    private var released = false

    var entered: Bool { lock.withLock { didEnter } }

    func wait() async {
        await withCheckedContinuation { continuation in
            let resumeNow = lock.withLock {
                didEnter = true
                if released { return true }
                self.continuation = continuation
                return false
            }
            if resumeNow { continuation.resume() }
        }
    }

    func release() {
        let continuation = lock.withLock { () -> CheckedContinuation<Void, Never>? in
            released = true
            let continuation = self.continuation
            self.continuation = nil
            return continuation
        }
        continuation?.resume()
    }
}
