import Foundation
import XCTest
@testable import Watermelon

// Bug-IX P01 R01 Codex B Finding 1: a background BGProcessingTask container must see foreground
// execution / verify state via process-shared AppRuntimeFlags so it skips before opening a second
// V2 runtime against the same repo.
final class AppRuntimeFlagsGateTests: XCTestCase {
    override func setUp() {
        super.setUp()
        AppRuntimeFlags.shared.setExecuting(false)
        AppRuntimeFlags.shared.setVerifying(false)
    }

    override func tearDown() {
        AppRuntimeFlags.shared.setExecuting(false)
        AppRuntimeFlags.shared.setVerifying(false)
        super.tearDown()
    }

    func testAppRuntimeFlagsSharedIsSingleton() {
        let a = AppRuntimeFlags.shared
        let b = AppRuntimeFlags.shared
        XCTAssertTrue(a === b, "AppRuntimeFlags.shared must return the same instance across calls")
    }

    func testSharedFlagsObservableAcrossReferences() {
        AppRuntimeFlags.shared.setExecuting(true)
        XCTAssertTrue(AppRuntimeFlags.shared.isExecuting,
                      "setExecuting must surface through subsequent .shared reads")
        AppRuntimeFlags.shared.setExecuting(false)
        XCTAssertFalse(AppRuntimeFlags.shared.isExecuting)

        AppRuntimeFlags.shared.setVerifying(true)
        XCTAssertTrue(AppRuntimeFlags.shared.isVerifying,
                      "setVerifying must surface through subsequent .shared reads")
        AppRuntimeFlags.shared.setVerifying(false)
        XCTAssertFalse(AppRuntimeFlags.shared.isVerifying)
    }

    func testExecutingAndVerifyingAreIndependent() {
        AppRuntimeFlags.shared.setExecuting(true)
        XCTAssertFalse(AppRuntimeFlags.shared.isVerifying,
                       "isExecuting must not implicitly set isVerifying")
        AppRuntimeFlags.shared.setVerifying(true)
        XCTAssertTrue(AppRuntimeFlags.shared.isExecuting,
                      "setVerifying must not clear isExecuting")
        AppRuntimeFlags.shared.setExecuting(false)
        XCTAssertTrue(AppRuntimeFlags.shared.isVerifying,
                      "setExecuting(false) must not clear isVerifying")
    }

    func testBackgroundBackupRunnerGuardsBothExecutingAndVerifying() throws {
        // Source-level pin to keep the gate in BackgroundBackupRunner.run() — running the full
        // runner needs PhotoKit + remote storage which the unit test rig doesn't provide. After
        // the R02 fix the BG gate is the atomic `tryBeginExecution`, which itself rejects when
        // either isExecuting or isVerifying is set (see AppRuntimeFlags.tryBeginExecution).
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let url = root.appendingPathComponent("Watermelon/Services/Backup/BackgroundBackupRunner.swift")
        let source = try String(contentsOf: url, encoding: .utf8)
        XCTAssertTrue(
            source.contains("AppRuntimeFlags.shared"),
            "BackgroundBackupRunner.run() must read the shared AppRuntimeFlags singleton"
        )
        XCTAssertTrue(
            source.contains("runtimeFlags.tryBeginExecution()"),
            "BackgroundBackupRunner.run() must use the atomic tryBeginExecution; it gates on both isExecuting and isVerifying internally"
        )
    }

    func testDependencyContainerWiresSharedAppRuntimeFlags() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let url = root.appendingPathComponent("Watermelon/App/DependencyContainer.swift")
        let source = try String(contentsOf: url, encoding: .utf8)
        XCTAssertTrue(
            source.contains("AppRuntimeFlags.shared"),
            "DependencyContainer must use AppRuntimeFlags.shared so foreground and background containers share execution state"
        )
        XCTAssertFalse(
            source.contains("AppRuntimeFlags()"),
            "DependencyContainer must not construct a fresh AppRuntimeFlags instance per container"
        )
    }

    // MARK: - P01 R02 Codex B / Checker Finding: bidirectional ownership

    func testTryBeginExecution_FailsWhenAlreadyExecuting() {
        XCTAssertTrue(AppRuntimeFlags.shared.tryBeginExecution())
        XCTAssertTrue(AppRuntimeFlags.shared.isExecuting)
        XCTAssertFalse(AppRuntimeFlags.shared.tryBeginExecution(),
                       "second tryBeginExecution must fail while the first owner still holds the lease")
        AppRuntimeFlags.shared.setExecuting(false)
        XCTAssertTrue(AppRuntimeFlags.shared.tryBeginExecution(),
                      "after the previous owner clears the flag, a fresh tryBeginExecution succeeds")
    }

    func testTryBeginExecution_FailsWhileVerifying() {
        XCTAssertTrue(AppRuntimeFlags.shared.tryBeginVerifying())
        XCTAssertFalse(AppRuntimeFlags.shared.tryBeginExecution(),
                       "foreground/background execution must not start while a verify is in flight")
        AppRuntimeFlags.shared.setVerifying(false)
        XCTAssertTrue(AppRuntimeFlags.shared.tryBeginExecution())
    }

    func testTryBeginVerifying_FailsWhileExecuting() {
        XCTAssertTrue(AppRuntimeFlags.shared.tryBeginExecution())
        XCTAssertFalse(AppRuntimeFlags.shared.tryBeginVerifying(),
                       "manual verify must not start while an execution (foreground or background) is in flight")
        AppRuntimeFlags.shared.setExecuting(false)
        XCTAssertTrue(AppRuntimeFlags.shared.tryBeginVerifying())
    }

    func testTryBeginVerifying_FailsWhenAlreadyVerifying() {
        XCTAssertTrue(AppRuntimeFlags.shared.tryBeginVerifying())
        XCTAssertFalse(AppRuntimeFlags.shared.tryBeginVerifying(),
                       "second tryBeginVerifying must fail while the first verify still holds the lease")
    }

    func testBackgroundBackupRunnerClaimsAndReleasesExecutionLease() throws {
        // Source-level pin: running the BG runner needs PhotoKit + remote storage. The contract is
        // that `run()` claims via `tryBeginExecution` and releases via `setExecuting(false)` in a
        // `defer`, so every exit path (early-return / cancellation / throw) clears the lease.
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let url = root.appendingPathComponent("Watermelon/Services/Backup/BackgroundBackupRunner.swift")
        let source = try String(contentsOf: url, encoding: .utf8)
        XCTAssertTrue(
            source.contains("runtimeFlags.tryBeginExecution()"),
            "BackgroundBackupRunner.run() must claim the execution lease atomically via tryBeginExecution"
        )
        XCTAssertTrue(
            source.contains("defer { runtimeFlags.setExecuting(false) }"),
            "BackgroundBackupRunner.run() must release the execution lease via defer so every exit path clears it"
        )
    }

    func testRemoteMaintenanceControllerUsesAtomicVerifyClaim() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let url = root.appendingPathComponent("Watermelon/Services/Backup/RemoteMaintenanceController.swift")
        let source = try String(contentsOf: url, encoding: .utf8)
        XCTAssertTrue(
            source.contains("appRuntimeFlags.tryBeginVerifying()"),
            "RemoteMaintenanceController.startFullVerify must claim the verify lease atomically via tryBeginVerifying"
        )
        XCTAssertFalse(
            source.contains("guard !appRuntimeFlags.isExecuting else"),
            "RemoteMaintenanceController must no longer read-then-set isExecuting separately — the atomic claim subsumes that gate"
        )
    }

    func testHomeExecutionCoordinatorUsesAtomicExecutionClaim() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let url = root.appendingPathComponent("Watermelon/Home/HomeExecutionCoordinator.swift")
        let source = try String(contentsOf: url, encoding: .utf8)
        XCTAssertTrue(
            source.contains("appRuntimeFlags.tryBeginExecution()"),
            "HomeExecutionCoordinator.enter must claim the execution lease atomically via tryBeginExecution so a concurrent BG runner can't race in"
        )
        XCTAssertFalse(
            source.contains("dependencies.appRuntimeFlags.setExecuting(true)"),
            "HomeExecutionCoordinator.enter must no longer use the unsynchronized setExecuting(true) call"
        )
    }

    // MARK: - P01 R03 Codex B Finding: BGTask expiration must wait for runner unwind

    /// AppDelegate's BGProcessingTask `expirationHandler` previously called
    /// `setTaskCompleted(success: false)` immediately after `backupTask.cancel()`. Because the
    /// runner's `defer { setExecuting(false) }` only fires when its async `run()` finishes
    /// unwinding, iOS could suspend the app after the completion signal but before the lease
    /// was released — stranding `AppRuntimeFlags.shared.isExecuting=true` until process relaunch
    /// and blocking foreground execution / manual verify in the meantime.
    func testAppDelegateBackgroundExpirationAwaitsRunnerUnwindBeforeCompletion() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let url = root.appendingPathComponent("Watermelon/App/AppDelegate.swift")
        let source = try String(contentsOf: url, encoding: .utf8)
        XCTAssertTrue(
            source.contains("await backupTask.value"),
            "AppDelegate.expirationHandler must await the cancelled backup task before calling setTaskCompleted so the runner's defer releases AppRuntimeFlags before iOS suspends the app"
        )
        // Belt and suspenders: the completion guard pattern must remain intact so a normal
        // completion that beats the expiration handler still calls setTaskCompleted exactly once.
        XCTAssertTrue(
            source.contains("completionGuard.withLock"),
            "AppDelegate must keep the completionGuard mutex so normal-completion and expiration paths don't both call setTaskCompleted"
        )
    }

    // MARK: - P01 R04 Codex B Finding: foreground exit must await BSC cleanup

    /// `HomeExecutionCoordinator.exit()` must defer `setExecuting(false)` until the cancelled
    /// `BackupSessionAsyncBridge` run finishes unwinding. Without this, a forced-failure +
    /// immediate close (e.g. connection lost mid-run, user taps the failed-state dismiss) drops
    /// the shared execution lease while the cancelled run's V2 runtime / metadata client / data
    /// client cleanup is still in flight. A new foreground run or scheduled BG task could then
    /// claim the lease and open a second V2 runtime against the same repo mid-shutdown.
    func testHomeExecutionCoordinatorExitDefersLeaseUntilBridgeCleanup() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let coordinatorURL = root.appendingPathComponent("Watermelon/Home/HomeExecutionCoordinator.swift")
        let coordinator = try String(contentsOf: coordinatorURL, encoding: .utf8)
        XCTAssertTrue(
            coordinator.contains("await bridgeToAwait.awaitCleanup()"),
            "HomeExecutionCoordinator.exit() must await the bridge's cleanup before clearing the execution lease"
        )
        XCTAssertTrue(
            coordinator.contains("flags.setExecuting(false)"),
            "HomeExecutionCoordinator.exit() must still release the lease — just deferred until cleanup completes"
        )

        let bridgeURL = root.appendingPathComponent("Watermelon/Services/Backup/BackupSessionAsyncBridge.swift")
        let bridge = try String(contentsOf: bridgeURL, encoding: .utf8)
        XCTAssertTrue(
            bridge.contains("func awaitCleanup() async"),
            "BackupSessionAsyncBridge must expose awaitCleanup() so the coordinator can wait on the underlying run's unwind"
        )
        XCTAssertTrue(
            bridge.contains("backupSessionController.awaitRunCleanup()"),
            "awaitCleanup must delegate to the BSC so the bridge stays the single async-boundary owner over the controller"
        )

        let bscURL = root.appendingPathComponent("Watermelon/Services/Backup/BackupSessionController.swift")
        let bsc = try String(contentsOf: bscURL, encoding: .utf8)
        XCTAssertTrue(
            bsc.contains("func awaitRunCleanup() async"),
            "BackupSessionController must expose awaitRunCleanup so callers can wait on the runDriver's runTask"
        )
        XCTAssertTrue(
            bsc.contains("runDriver.awaitRunTaskCompletion()"),
            "awaitRunCleanup must delegate to the runDriver's runTask completion so the V2 runtime cleanup `defer`s have fired"
        )

        let driverURL = root.appendingPathComponent("Watermelon/Services/Backup/BackupRunDriver.swift")
        let driver = try String(contentsOf: driverURL, encoding: .utf8)
        XCTAssertTrue(
            driver.contains("func awaitRunTaskCompletion() async"),
            "BackupRunDriver must expose awaitRunTaskCompletion so cleanup ordering can be enforced from above"
        )
        XCTAssertTrue(
            driver.contains("await runTask?.value"),
            "awaitRunTaskCompletion must await the runTask's value so its `defer` blocks have all fired"
        )
    }

    /// Behavioral pin for the new ordering: a runner-like Task models the cancelled BSC run that
    /// is still unwinding cleanup `defer`s after `cancel()` returns. The shared execution lease
    /// must stay held until that task's `await ... value` resolves; clearing the lease earlier
    /// would let a fresh `tryBeginExecution()` succeed against an in-flight cleanup.
    func testExitDeferredReleaseHoldsLeaseUntilSimulatedBridgeCleanupCompletes() async {
        XCTAssertTrue(AppRuntimeFlags.shared.tryBeginExecution(),
                      "test seam must own the lease before simulating a cancelled run")

        let runnerStarted = XCTestExpectation(description: "simulated bridge run entered cleanup")
        let bridgeRun = Task<Void, Never> {
            runnerStarted.fulfill()
            // Mimic post-cancel cleanup: V2 runtime services shutdown + liveness drain + client
            // disconnect, which takes non-zero time after the upload continuation has resumed.
            try? await Task.sleep(nanoseconds: 5_000_000)
        }
        await fulfillment(of: [runnerStarted], timeout: 2.0)

        // Buggy ordering: release the lease while bridge cleanup is still in flight — a fresh
        // `tryBeginExecution()` would succeed and a new V2 runtime could open mid-shutdown.
        XCTAssertTrue(AppRuntimeFlags.shared.isExecuting,
                      "lease must still be held — bridge cleanup has not finished yet")

        // Correct ordering (matches the new exit() shape): await the bridge before releasing.
        await bridgeRun.value
        AppRuntimeFlags.shared.setExecuting(false)
        XCTAssertFalse(AppRuntimeFlags.shared.isExecuting,
                       "after bridge cleanup the lease must release for the next claimant")
        XCTAssertTrue(AppRuntimeFlags.shared.tryBeginExecution(),
                      "with the lease released, a fresh foreground claim succeeds")
        AppRuntimeFlags.shared.setExecuting(false)
    }

    // MARK: - P01 R05 Codex B / Checker Finding 1: zero-asset branch must shutdown before emit

    /// `BackupParallelExecutor.execute()`'s zero-asset early-return must run V2 runtime shutdown
    /// and data-client disconnect BEFORE emitting `.finished`. `.finished` is what triggers
    /// `BackupSessionController.handleEvent` → `runDriver.clearActiveRunState()` which nils
    /// `runTask`; if shutdown lagged the emit, `HomeExecutionCoordinator.exit()`'s deferred
    /// cleanup-await chain could observe `runTask == nil` (because clearActiveRunState already
    /// ran) and release the shared execution lease while V2 runtime services / metadata client
    /// / data client cleanup is still in flight. A subsequent foreground run, manual verify, or
    /// scheduled BG task could then claim the lease and open a second V2 runtime against the
    /// same repo mid-shutdown.
    func testBackupParallelExecutor_ZeroAssetBranch_ShutsDownBeforeFinishedEmit() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let url = root.appendingPathComponent("Watermelon/Services/Backup/BackupParallelExecutor.swift")
        let source = try String(contentsOf: url, encoding: .utf8)

        guard let guardRange = source.range(of: "guard preparedRun.totalAssetCount > 0 else {") else {
            XCTFail("zero-asset guard must remain in BackupParallelExecutor.execute()")
            return
        }
        let bodyStart = guardRange.upperBound
        let windowEnd = source.index(bodyStart, offsetBy: 2000, limitedBy: source.endIndex) ?? source.endIndex
        let body = String(source[bodyStart..<windowEnd])

        guard let shutdownRange = body.range(of: "v2Services?.shutdown()") else {
            XCTFail("zero-asset branch must call v2Services?.shutdown()")
            return
        }
        guard let disconnectRange = body.range(of: "initialClient.disconnectSafely()") else {
            XCTFail("zero-asset branch must call initialClient.disconnectSafely()")
            return
        }
        guard let emitRange = body.range(of: "eventStream.emit(.finished(result))") else {
            XCTFail("zero-asset branch must still emit .finished(result)")
            return
        }

        XCTAssertLessThan(
            shutdownRange.lowerBound, emitRange.lowerBound,
            "V2 runtime shutdown must complete BEFORE .finished emit so BSC.clearActiveRunState cannot release the lease mid-shutdown"
        )
        XCTAssertLessThan(
            disconnectRange.lowerBound, emitRange.lowerBound,
            "data-client disconnect must complete BEFORE .finished emit so the next runtime cannot reuse the connection mid-disconnect"
        )
    }

    // MARK: - P01 R06 Codex B Finding: Manage Profiles delete must block on any execution

    /// `ManageStorageProfilesViewController.deleteProfile` previously narrowed the
    /// `isExecuting` block to the active profile (`isActiveProfile && isExecuting`). A background
    /// V2 run owns `AppRuntimeFlags.shared.isExecuting` for the profile it is uploading to —
    /// which may NOT be the active Home profile — so deletion of that non-active background
    /// profile passed the gate while the V2 runtime still owned its `repo_state`. Subsequent
    /// `SeqAllocator.allocate()` reads would then throw `missingRepoState`, or the background
    /// runtime would continue mutating a remote repo whose local binding the user just deleted.
    /// The block must mirror `StorageProfileDetailViewController.isProfileMutationBlocked` and
    /// `BackgroundBackupNodesViewController.isProfileMutationBlocked`: any `isExecuting` blocks,
    /// regardless of whether the deleted profile is the active one.
    func testManageProfilesDeleteBlocksOnExecutingRegardlessOfActiveProfile() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let url = root.appendingPathComponent("Watermelon/UI/Auth/ManageStorageProfilesViewController.swift")
        let source = try String(contentsOf: url, encoding: .utf8)

        XCTAssertTrue(
            source.contains("|| dependencies.appRuntimeFlags.isExecuting"),
            "deleteProfile must block on appRuntimeFlags.isExecuting unconditionally so a background V2 run on a non-active profile cannot have its repo_state deleted mid-run"
        )
        XCTAssertFalse(
            source.contains("isActiveProfile && dependencies.appRuntimeFlags.isExecuting"),
            "deleteProfile must NOT narrow the isExecuting block to the active profile — background runs hold the shared lease for any background-enabled profile"
        )
    }

    // MARK: - P01 R07 Codex B Finding 3: Storage Profile Detail delete confirmation must re-check

    /// `StorageProfileDetailViewController.deleteProfile()` was reachable from a confirmation
    /// alert action; rendering the alert went through `rejectIfProfileMutationBlocked()` but the
    /// destructive method itself did not re-check, leaving a window where execution/verify could
    /// start between alert presentation and tap. The re-check at the destructive call site closes
    /// that window without depending on lifecycle observers redrawing the row beneath the alert.
    func testStorageProfileDetailDeleteRechecksMutationGateAtDestructiveCallSite() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let url = root.appendingPathComponent("Watermelon/UI/Auth/StorageProfileDetailViewController.swift")
        let source = try String(contentsOf: url, encoding: .utf8)

        guard let deleteRange = source.range(of: "private func deleteProfile() {") else {
            XCTFail("deleteProfile() must remain in StorageProfileDetailViewController")
            return
        }
        let bodyStart = deleteRange.upperBound
        let windowEnd = source.index(bodyStart, offsetBy: 600, limitedBy: source.endIndex) ?? source.endIndex
        let body = String(source[bodyStart..<windowEnd])

        guard let rejectRange = body.range(of: "rejectIfProfileMutationBlocked()") else {
            XCTFail("deleteProfile() must re-call rejectIfProfileMutationBlocked at the destructive site")
            return
        }
        guard let deleteCallRange = body.range(of: "deleteServerProfile(id:") else {
            XCTFail("deleteProfile() must still call deleteServerProfile")
            return
        }
        XCTAssertLessThan(
            rejectRange.lowerBound, deleteCallRange.lowerBound,
            "the mutation-gate re-check must run BEFORE deleteServerProfile so execution/verify starting between alert presentation and tap cannot cascade repo_state under an in-flight V2 runtime"
        )
    }

    // MARK: - P01 R07 Codex B Finding 1: endpoint edit must drop the stale local repo binding

    /// Editing an existing profile's endpoint kept the local `repo_state` row bound to the
    /// profileID. The next open against the new remote then tripped `BackupV2RepoOpenService`'s
    /// `.bootstrapFresh` regression guard (or a canonical-identity mismatch), stranding the
    /// profile until the user deleted and recreated it. `handleConnectionEdited()` must drop
    /// the binding so the fresh open adopts the new remote's canonical identity instead.
    func testStorageProfileDetailHandleConnectionEditedClearsRepoState() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let url = root.appendingPathComponent("Watermelon/UI/Auth/StorageProfileDetailViewController.swift")
        let source = try String(contentsOf: url, encoding: .utf8)

        guard let editedRange = source.range(of: "private func handleConnectionEdited() {") else {
            XCTFail("handleConnectionEdited must remain in StorageProfileDetailViewController")
            return
        }
        let bodyStart = editedRange.upperBound
        let windowEnd = source.index(bodyStart, offsetBy: 1000, limitedBy: source.endIndex) ?? source.endIndex
        let body = String(source[bodyStart..<windowEnd])

        XCTAssertTrue(
            body.contains("clearRepoState(profileID:"),
            "handleConnectionEdited must clear repo_state for the profile so .bootstrapFresh does not throw repoFormatRegression against a stale repoID"
        )
    }

    func testDatabaseManagerExposesClearRepoState() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let url = root.appendingPathComponent("Shared/Data/Database/DatabaseManager.swift")
        let source = try String(contentsOf: url, encoding: .utf8)
        XCTAssertTrue(
            source.contains("func clearRepoState(profileID: Int64) throws"),
            "DatabaseManager must expose clearRepoState so the connection-edited flow can drop the stale binding"
        )
    }

    // MARK: - P01 R07 Codex B Finding 2: editor commit paths must gate on execution/verify

    /// Editors that mutate an existing profile (`editingProfile != nil`) must re-check the
    /// shared execution/verify state at commit time, not just at navigation push. A background
    /// V2 run can claim the lease after the editor was pushed; without the commit-time guard,
    /// saving would mutate `server_profiles` / keychain under the in-flight runtime.
    func testStorageEditorsGateSaveOnMutationBlockWhenEditingExistingProfile() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        for path in [
            "Watermelon/UI/Auth/AddSMBServerViewController.swift",
            "Watermelon/UI/Auth/AddWebDAVStorageViewController.swift",
            "Watermelon/UI/Auth/AddS3StorageViewController.swift",
            "Watermelon/UI/Auth/AddSFTPStorageViewController.swift",
            "Watermelon/UI/Auth/AddExternalStorageViewController.swift",
        ] {
            let url = root.appendingPathComponent(path)
            let source = try String(contentsOf: url, encoding: .utf8)
            XCTAssertTrue(
                source.contains("ProfileEditorMutationGate.isBlocked(dependencies: dependencies)"),
                "\(path) must check ProfileEditorMutationGate before committing an edit"
            )
            XCTAssertTrue(
                source.contains("editingProfile != nil, ProfileEditorMutationGate"),
                "\(path) must scope the gate to editingProfile != nil — adding a new profile cannot collide with an in-flight runtime"
            )
        }
    }

    // Bug-IX P01 R08 Codex B F1: WebDAV / S3 / SFTP editors await network I/O between the initial
    // gate check and the final commit block; a background V2 run can claim the shared execution
    // lease in that window. The final MainActor commit block must re-check the mutation gate so
    // saveServerProfile / keychain.save / keychain.delete do not mutate credentials under an
    // in-flight runtime. SMB and external-volume editors commit synchronously after the gate.
    func testAsyncStorageEditorsRecheckMutationGateAtCommitBoundary() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        for path in [
            "Watermelon/UI/Auth/AddWebDAVStorageViewController.swift",
            "Watermelon/UI/Auth/AddS3StorageViewController.swift",
            "Watermelon/UI/Auth/AddSFTPStorageViewController.swift",
        ] {
            let url = root.appendingPathComponent(path)
            let source = try String(contentsOf: url, encoding: .utf8)
            let gateOccurrences = source.components(separatedBy: "ProfileEditorMutationGate.isBlocked").count - 1
            XCTAssertGreaterThanOrEqual(
                gateOccurrences, 2,
                "\(path) must call ProfileEditorMutationGate.isBlocked at least twice — once at saveTapped entry and once inside the final MainActor commit block after async validation"
            )
        }
    }

    func testProfileEditorMutationGateMatchesDetailVCInvariant() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let url = root.appendingPathComponent("Watermelon/UI/Auth/ProfileEditorMutationGate.swift")
        let source = try String(contentsOf: url, encoding: .utf8)
        XCTAssertTrue(
            source.contains("appRuntimeFlags.isExecuting"),
            "ProfileEditorMutationGate must observe the shared execution lease"
        )
        XCTAssertTrue(
            source.contains("remoteMaintenanceController.isVerifying"),
            "ProfileEditorMutationGate must observe the remote verify state"
        )
    }

    // MARK: - P01 R09 Codex B Finding 2: scene teardown must release the execution lease

    /// `HomeExecutionCoordinator.deinit` must release the shared execution lease when the scene
    /// is destroyed mid-run (system scene disconnect, HomeScreenStore dropped without exit()).
    /// Without this, the lease leaks for the rest of the process lifetime and every later
    /// foreground tap / manual verify / scheduled BG task is locked out until app relaunch.
    /// Mirrors exit()'s deferred-release pattern: cancel the bridge, await cleanup, then clear.
    func testHomeExecutionCoordinatorDeinitReleasesExecutionLease() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let url = root.appendingPathComponent("Watermelon/Home/HomeExecutionCoordinator.swift")
        let source = try String(contentsOf: url, encoding: .utf8)

        guard let deinitRange = source.range(of: "deinit {") else {
            XCTFail("HomeExecutionCoordinator must declare a deinit to release the execution lease on scene teardown")
            return
        }
        let bodyStart = deinitRange.upperBound
        let windowEnd = source.index(bodyStart, offsetBy: 1200, limitedBy: source.endIndex) ?? source.endIndex
        let body = String(source[bodyStart..<windowEnd])

        XCTAssertTrue(
            body.contains("holdsExecutionLease"),
            "deinit must guard on the same lease-ownership flag enter()/exit() use, so we only release when we still owe the lease"
        )
        XCTAssertTrue(
            body.contains("flags.setExecuting(false)"),
            "deinit must release the shared execution lease via setExecuting(false)"
        )
        XCTAssertTrue(
            body.contains("awaitCleanup()"),
            "deinit must await the bridge's cleanup before releasing the lease — same ordering as exit()"
        )
    }

    /// Behavioural pin for the scene-teardown lease-release pattern: a coordinator-like owner
    /// acquires the shared lease and is dropped without an explicit release. The released lease
    /// must become observable only after the cleanup task finishes, not on the deinit itself.
    func testCoordinatorLikeOwnerReleasesLeaseAfterDeferredCleanup() async {
        XCTAssertTrue(AppRuntimeFlags.shared.tryBeginExecution(),
                      "test seam must own the lease before simulating scene teardown")

        final class FakeBridgeLike {
            let cleanupReady = XCTestExpectation(description: "bridge cleanup completed")
            func awaitCleanup() async {
                try? await Task.sleep(nanoseconds: 3_000_000)
                cleanupReady.fulfill()
            }
        }

        let bridge = FakeBridgeLike()
        // Mirror the deinit body shape: capture local references and schedule the deferred cleanup
        // → release sequence. The MainActor hop matches HomeExecutionCoordinator.deinit's pattern.
        let releaseTask = Task<Void, Never> {
            await bridge.awaitCleanup()
            AppRuntimeFlags.shared.setExecuting(false)
        }

        // Immediately after the "deinit" returns, the lease must still be held — the cleanup task
        // has not finished yet. Any concurrent foreground/verify claim must still fail.
        XCTAssertTrue(AppRuntimeFlags.shared.isExecuting,
                      "lease must still be held immediately after scene teardown — cleanup is in flight")
        XCTAssertFalse(AppRuntimeFlags.shared.tryBeginExecution(),
                       "new claims must fail while the cleanup-await is still draining")

        await fulfillment(of: [bridge.cleanupReady], timeout: 2.0)
        _ = await releaseTask.value
        XCTAssertFalse(AppRuntimeFlags.shared.isExecuting,
                       "after cleanup completes, the deferred release must have cleared the lease")
        XCTAssertTrue(AppRuntimeFlags.shared.tryBeginExecution(),
                      "next claimant must be able to begin once the lease has been released")
        AppRuntimeFlags.shared.setExecuting(false)
    }

    // MARK: - P01 R09 Codex B Finding 3: keychain-save failure must drop stale repo_state

    /// All four password-backed storage editors (SMB, WebDAV, S3, SFTP) write the DB row first
    /// and then save the keychain entry. If the keychain save throws after the DB row was
    /// committed, the row already points at the edited endpoint while the keychain entry the
    /// user expected to save is missing, and `handleConnectionEdited()` never runs (commit
    /// threw). The catch path must mirror `handleConnectionEdited()`'s clearing of the local
    /// V2 binding so the partial save cannot strand a stale `repo_state` against the new remote.
    func testPasswordEditorsClearRepoStateOnKeychainFailureAfterDBSave() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        for path in [
            "Watermelon/UI/Auth/AddSMBServerViewController.swift",
            "Watermelon/UI/Auth/AddWebDAVStorageViewController.swift",
            "Watermelon/UI/Auth/AddS3StorageViewController.swift",
            "Watermelon/UI/Auth/AddSFTPStorageViewController.swift",
        ] {
            let url = root.appendingPathComponent(path)
            let source = try String(contentsOf: url, encoding: .utf8)
            XCTAssertTrue(
                source.contains("clearRepoState(profileID: existingProfileID)"),
                "\(path) must clear repo_state for the existing profile when keychain.save throws after saveServerProfile committed the DB row"
            )
            XCTAssertTrue(
                source.contains("clearRemoteVerifiedAt(profileID: existingProfileID)"),
                "\(path) must also clear the remote verify timestamp on a partial-save rollback so the user does not see a stale 'verified' badge"
            )
        }
    }

    // MARK: - P01 R10 Codex B Finding 3: process-wide verify lease must be observed by the mutation gate

    /// `ProfileEditorMutationGate` must consult `appRuntimeFlags.isVerifying` so a verify task
    /// that survived scene disconnect/reconnect (its `verifyTask` retains the controller via
    /// `guard let self`; the new container's controller is idle but the shared verify lease is
    /// still held in `AppRuntimeFlags.shared`) still blocks new-scene mutations. Source pin
    /// rather than behavioural: constructing a fresh `DependencyContainer` mid-test would touch
    /// real DB / keychain services and slow down focused runs unnecessarily; the source pin
    /// guarantees the predicate stays present across refactors.
    func testProfileEditorMutationGateSourceReadsProcessWideIsVerifying() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let url = root.appendingPathComponent("Watermelon/UI/Auth/ProfileEditorMutationGate.swift")
        let source = try String(contentsOf: url, encoding: .utf8)
        XCTAssertTrue(
            source.contains("appRuntimeFlags.isVerifying"),
            "ProfileEditorMutationGate must read appRuntimeFlags.isVerifying so a verify task that survived scene disconnect blocks new-scene mutations"
        )
    }

    // MARK: - P01 R10 Codex Checker Finding 2: Add-as-duplicate must be gated like an explicit Edit

    /// All five storage editors compute `baseProfile = editingProfile ?? existing` so an Add flow
    /// that adopts an existing duplicate row updates `server_profiles` for `baseProfile.id`. The
    /// commit method must re-check the mutation gate on `baseProfile?.id != nil`, not on
    /// `editingProfile != nil`, so duplicate-adoption is gated the same as an explicit Edit.
    func testEditorsGateAddAsDuplicateAtCommitTime() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let helpers = [
            "Watermelon/UI/Auth/AddSMBServerViewController.swift",
            "Watermelon/UI/Auth/AddWebDAVStorageViewController.swift",
            "Watermelon/UI/Auth/AddS3StorageViewController.swift",
            "Watermelon/UI/Auth/AddSFTPStorageViewController.swift",
            "Watermelon/UI/Auth/AddExternalStorageViewController.swift",
        ]
        for path in helpers {
            let url = root.appendingPathComponent(path)
            let source = try String(contentsOf: url, encoding: .utf8)
            XCTAssertTrue(
                source.contains("ProfileEditorMutationGate.throwIfBlocked(dependencies: dependencies)"),
                "\(path) must call ProfileEditorMutationGate.throwIfBlocked inside the commit method so the gate fires on the actual mutation target rather than the UI editingProfile flag"
            )
        }
    }

    func testAsyncEditorsPostValidationGateUsesBaseProfileID() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        for path in [
            "Watermelon/UI/Auth/AddWebDAVStorageViewController.swift",
            "Watermelon/UI/Auth/AddS3StorageViewController.swift",
            "Watermelon/UI/Auth/AddSFTPStorageViewController.swift",
        ] {
            let url = root.appendingPathComponent(path)
            let source = try String(contentsOf: url, encoding: .utf8)
            XCTAssertTrue(
                source.contains("draft.baseProfile?.id != nil"),
                "\(path) must gate the post-validation MainActor commit block on draft.baseProfile?.id so Add-as-duplicate is treated like Edit"
            )
        }
    }

    // MARK: - P01 R10 Codex B Finding 1 / Checker Finding 1: keychain-save failure must also clear active session

    /// The R09 catch path clears `repo_state` + `remoteVerifiedAt` but missed the active
    /// `AppSession` cleanup that `StorageProfileDetailViewController.handleConnectionEdited()`
    /// performs on the success path. Without it, `BackupSessionController.resolveActiveConnection()`
    /// can keep reading the cached pre-edit profile/password and run against the old remote even
    /// though the DB row has been repointed.
    func testPasswordEditorsClearActiveSessionOnKeychainFailureAfterDBSave() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        for path in [
            "Watermelon/UI/Auth/AddSMBServerViewController.swift",
            "Watermelon/UI/Auth/AddWebDAVStorageViewController.swift",
            "Watermelon/UI/Auth/AddS3StorageViewController.swift",
            "Watermelon/UI/Auth/AddSFTPStorageViewController.swift",
        ] {
            let url = root.appendingPathComponent(path)
            let source = try String(contentsOf: url, encoding: .utf8)
            XCTAssertTrue(
                source.contains("clearActiveSessionIfMatches(profileID: existingProfileID)"),
                "\(path) must clear the active AppSession on the keychain-failure rollback if the existing row is the active profile, matching handleConnectionEdited()'s success-path cleanup"
            )
            XCTAssertTrue(
                source.contains("setActiveServerProfileID(nil)") &&
                source.contains("appSession.clear()"),
                "\(path) clearActiveSessionIfMatches helper must mirror the DB + in-memory clear that handleConnectionEdited uses"
            )
        }
    }

    // MARK: - P01 R10 Codex B Finding 2: explicit scene-disconnect teardown breaks the task-retain cycle

    /// `executionTask = Task { [weak self] in guard let self else { return } ... }` retains
    /// `HomeExecutionCoordinator` strongly while the body runs, and the coordinator holds the
    /// task back via the stored property. R09's deinit cleanup is unreachable until the body
    /// returns. The fix routes scene-disconnect through SceneDelegate → AppCoordinator →
    /// HomeViewController → HomeScreenStore so cancellation can be driven before the scene's
    /// container is released.
    func testSceneDisconnectRoutesThroughHomeScreenStoreTeardown() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()

        let scene = try String(contentsOf: root.appendingPathComponent("Watermelon/App/SceneDelegate.swift"), encoding: .utf8)
        XCTAssertTrue(
            scene.contains("appCoordinator?.handleSceneDisconnect()"),
            "SceneDelegate.sceneDidDisconnect must forward to AppCoordinator.handleSceneDisconnect so cleanup can run before the scene's container is reclaimed"
        )

        let coord = try String(contentsOf: root.appendingPathComponent("Watermelon/App/AppCoordinator.swift"), encoding: .utf8)
        XCTAssertTrue(
            coord.contains("func handleSceneDisconnect()"),
            "AppCoordinator must expose handleSceneDisconnect so SceneDelegate can drive cleanup"
        )
        XCTAssertTrue(
            coord.contains("homeViewController?.handleSceneDisconnect()"),
            "AppCoordinator.handleSceneDisconnect must forward to HomeViewController so the store can cancel in-flight execution / verify"
        )

        let vc = try String(contentsOf: root.appendingPathComponent("Watermelon/Home/HomeViewController.swift"), encoding: .utf8)
        XCTAssertTrue(
            vc.contains("func handleSceneDisconnect()") &&
            vc.contains("store.handleSceneDisconnect()"),
            "HomeViewController must expose handleSceneDisconnect that delegates to the store"
        )

        let store = try String(contentsOf: root.appendingPathComponent("Watermelon/Home/HomeScreenStore.swift"), encoding: .utf8)
        XCTAssertTrue(
            store.contains("func handleSceneDisconnect()"),
            "HomeScreenStore must expose handleSceneDisconnect so the scene-teardown path has a single entry point"
        )
        XCTAssertTrue(
            store.contains("executionCoordinator.exit()"),
            "HomeScreenStore.handleSceneDisconnect must call executionCoordinator.exit() so the task gets cancelled and the lease release ordering kicks in"
        )
        XCTAssertTrue(
            store.contains("remoteMaintenanceController.cancel()"),
            "HomeScreenStore.handleSceneDisconnect must cancel verify too — its verifyTask has the same `guard let self` retain pattern as executionTask"
        )
    }

    // MARK: - P01 R11 Claude A Finding 1: profile editors must preserve writerID across save

    /// `ServerProfileRecord.writerID` is the per-profile installation identity used by identity
    /// claims, liveness heartbeats, migration markers, commit/snapshot filenames, and retention
    /// attribution. Each editor must pass `writerID: baseProfile?.writerID` to its
    /// `ServerProfileRecord` constructor so saving an edited row does NOT overwrite the column
    /// with nil (Swift memberwise init defaults Optional-without-explicit-default to nil; GRDB
    /// `save(db)` UPDATEs all columns including writerID).
    func testEditorsPreserveWriterIDAcrossSave() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let dbURL = dir.appendingPathComponent("test.sqlite")
        let dm = try DatabaseManager(databaseURL: dbURL)

        var seed = ServerProfileRecord(
            id: nil,
            name: "seed",
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: "h", port: 445, shareName: "s",
            basePath: "/p",
            username: "u", domain: nil,
            credentialRef: "ref",
            backgroundBackupEnabled: false,
            createdAt: Date(), updatedAt: Date(),
            writerID: "11111111-1111-1111-1111-111111111111"
        )
        try dm.saveServerProfile(&seed)
        guard let profileID = seed.id else { return XCTFail("expected saved profile to receive an id") }

        // Mirror the editor commit shape: rebuild from baseProfile (which has writerID set) and
        // pass writerID through. Without F28's `writerID: baseProfile?.writerID`, GRDB's UPDATE
        // would wipe the column to NULL and the next V2 open would mint a fresh writerID.
        let baseProfile = seed
        var edited = ServerProfileRecord(
            id: baseProfile.id,
            name: "seed-renamed",
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: baseProfile.sortOrder,
            host: "h", port: 445, shareName: "s",
            basePath: "/p",
            username: "u", domain: nil,
            credentialRef: "ref",
            backgroundBackupEnabled: false,
            createdAt: baseProfile.createdAt, updatedAt: Date(),
            writerID: baseProfile.writerID
        )
        try dm.saveServerProfile(&edited)

        let fetched = try dm.fetchServerProfiles().first(where: { $0.id == profileID })
        XCTAssertEqual(
            fetched?.writerID,
            "11111111-1111-1111-1111-111111111111",
            "writerID must survive editor save — see Bug-IX P01 R11 F28"
        )
    }

    /// Source pin: each of the five editors' `ServerProfileRecord(...)` constructor must include
    /// `writerID:` populated from `baseProfile?.writerID` (or `draft.baseProfile?.writerID` for
    /// the async editors that use the validated-draft pattern).
    func testEditorsPassWriterIDInServerProfileRecordConstructor() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let editorWriterIDExpectations: [(path: String, snippet: String)] = [
            ("Watermelon/UI/Auth/AddSMBServerViewController.swift", "writerID: baseProfile?.writerID"),
            ("Watermelon/UI/Auth/AddWebDAVStorageViewController.swift", "writerID: draft.baseProfile?.writerID"),
            ("Watermelon/UI/Auth/AddS3StorageViewController.swift", "writerID: draft.baseProfile?.writerID"),
            ("Watermelon/UI/Auth/AddSFTPStorageViewController.swift", "writerID: draft.baseProfile?.writerID"),
            ("Watermelon/UI/Auth/AddExternalStorageViewController.swift", "writerID: baseProfile?.writerID"),
        ]
        for (path, snippet) in editorWriterIDExpectations {
            let url = root.appendingPathComponent(path)
            let source = try String(contentsOf: url, encoding: .utf8)
            XCTAssertTrue(
                source.contains(snippet),
                "\(path) must pass `\(snippet)` to ServerProfileRecord(...) so editor save does not wipe the column to NULL"
            )
        }
    }

    // MARK: - P01 R12 Claude A+B / Checker: writerID column preserved across any saveServerProfile UPDATE

    /// `DatabaseManager.saveServerProfile` must preserve the live `writerID` column when
    /// updating an existing row, regardless of what the caller passed. The writerID is owned
    /// by `RepoIdentity.lazyEnsureWriterID` (generate once, reuse forever); editor commits,
    /// `StorageClientFactory.onBookmarkRefreshed`, and any other caller that builds a
    /// `ServerProfileRecord` from a snapshot would otherwise silently wipe the column when
    /// a `lazyEnsureWriterID` mint happened between snapshot capture and save.
    func testSaveServerProfilePreservesDBWriterIDAgainstStaleNilSnapshot() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let dbURL = dir.appendingPathComponent("test.sqlite")
        let dm = try DatabaseManager(databaseURL: dbURL)

        // Seed a row with no writerID (the pre-`lazyEnsureWriterID` state).
        var seed = ServerProfileRecord(
            id: nil,
            name: "seed",
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: "h", port: 445, shareName: "s",
            basePath: "/p",
            username: "u", domain: nil,
            credentialRef: "ref",
            backgroundBackupEnabled: false,
            createdAt: Date(), updatedAt: Date(),
            writerID: nil
        )
        try dm.saveServerProfile(&seed)
        guard let profileID = seed.id else { return XCTFail("expected saved profile to receive an id") }

        // Mint a writerID via the durable path (matches what `lazyEnsureWriterID` would do).
        try dm.write { db in
            try db.execute(
                sql: "UPDATE server_profiles SET writerID = ? WHERE id = ?",
                arguments: ["concurrent-mint-uuid", profileID]
            )
        }

        // Editor commit / StorageClientFactory callback shape: build a new record from the
        // stale snapshot (writerID nil) and save. The DB column must survive the UPDATE.
        var staleSnapshotSave = ServerProfileRecord(
            id: profileID,
            name: "seed-renamed",
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: "h", port: 445, shareName: "s",
            basePath: "/p2",
            username: "u", domain: nil,
            credentialRef: "ref",
            backgroundBackupEnabled: false,
            createdAt: seed.createdAt, updatedAt: Date(),
            writerID: nil
        )
        try dm.saveServerProfile(&staleSnapshotSave)

        let fetched = try dm.fetchServerProfiles().first(where: { $0.id == profileID })
        XCTAssertEqual(
            fetched?.writerID,
            "concurrent-mint-uuid",
            "saveServerProfile must preserve the live writerID column against a stale nil snapshot — see Bug-IX P01 R12 F30"
        )
        // Other column edits should still land — only writerID is locked.
        XCTAssertEqual(fetched?.basePath, "/p2", "non-writerID columns must still be updated by saveServerProfile")
        XCTAssertEqual(fetched?.name, "seed-renamed", "non-writerID columns must still be updated by saveServerProfile")
        // The inout writerID is rewritten to match the DB so callers see the live value.
        XCTAssertEqual(staleSnapshotSave.writerID, "concurrent-mint-uuid")
    }

    /// Source pin for the DatabaseManager-side fix so a future refactor that drops the
    /// preservation guard re-triggers F30.
    func testDatabaseManagerPreservesWriterIDOnUpdate() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let source = try String(
            contentsOf: root.appendingPathComponent("Shared/Data/Database/DatabaseManager.swift"),
            encoding: .utf8
        )
        guard let saveRange = source.range(of: "func saveServerProfile(_ profile: inout ServerProfileRecord) throws {") else {
            XCTFail("DatabaseManager.saveServerProfile must remain present")
            return
        }
        let bodyStart = saveRange.upperBound
        let bodyEnd = source.index(bodyStart, offsetBy: 1400, limitedBy: source.endIndex) ?? source.endIndex
        let body = String(source[bodyStart..<bodyEnd])
        XCTAssertTrue(
            body.contains("profile.writerID = existing.writerID"),
            "saveServerProfile must overwrite the caller's writerID with the live DB value when updating an existing row — see Bug-IX P01 R12 F30"
        )
    }

    // MARK: - P01 R12 Codex Checker Finding 1: password-prompt completion must re-check the gate

    /// The password prompt is a UI await: a background V2 runner, a verify task from another
    /// scene, or a foreground execution that started while the prompt was open would otherwise
    /// race `connect()` → `reloadRemoteIndex` against the same remote. `HomeScreenStore` wraps
    /// the prompt completion with a live process-wide execution / verify re-check before
    /// forwarding to `HomeConnectionController`.
    func testHomeScreenStoreWrapsPasswordPromptCompletionWithMaintenanceRecheck() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let source = try String(
            contentsOf: root.appendingPathComponent("Watermelon/Home/HomeScreenStore.swift"),
            encoding: .utf8
        )
        XCTAssertTrue(
            source.contains("isConnectMaintenanceOrExecutionBlocked()"),
            "HomeScreenStore must factor the connect-time predicate into a helper that consults isExecuting AND isVerifying (process-wide) AND the local verify controller"
        )

        guard let bindRange = source.range(of: "connectionController.onNeedsPasswordPrompt = ") else {
            XCTFail("HomeScreenStore must bind connectionController.onNeedsPasswordPrompt so the prompt completion can be wrapped")
            return
        }
        let bodyStart = bindRange.upperBound
        let bodyEnd = source.index(bodyStart, offsetBy: 1200, limitedBy: source.endIndex) ?? source.endIndex
        let body = String(source[bodyStart..<bodyEnd])
        XCTAssertTrue(
            body.contains("isConnectMaintenanceOrExecutionBlocked"),
            "The HomeScreenStore binding must re-check the gate inside the password prompt completion before forwarding password to HomeConnectionController"
        )
        XCTAssertTrue(
            body.contains("home.alert.maintenanceInProgress"),
            "The re-check must surface the maintenance-in-progress alert when the gate blocks at completion time"
        )
    }

    // MARK: - P01 R13 Codex A / Codex B / Codex Checker: every connect entrypoint must observe isExecuting

    /// R12 F31 wrapped only the password-prompt completion path. The saved-password,
    /// passwordless, and auto-connect paths still call `HomeConnectionController.connect()`
    /// → `reloadRemoteIndex` directly, racing a BG runner that holds `isExecuting`.
    /// `connectProfile(_:)` must use the wider predicate, not the verify-only one.
    func testConnectProfileGatesOnConnectBlockedNotJustMaintaining() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let source = try String(
            contentsOf: root.appendingPathComponent("Watermelon/Home/HomeScreenStore.swift"),
            encoding: .utf8
        )
        guard let connectRange = source.range(of: "func connectProfile(_ profile: ServerProfileRecord) {") else {
            XCTFail("HomeScreenStore.connectProfile must remain present")
            return
        }
        // Narrow the body window to JUST connectProfile — the adjacent `disconnect()` legitimately
        // still uses `rejectIfMaintaining()` because disconnect doesn't open a V2 runtime, and we
        // don't want the negative assertion below to bleed across the function boundary.
        let bodyStart = connectRange.upperBound
        guard let closingBraceRange = source.range(of: "    }", range: bodyStart..<source.endIndex) else {
            XCTFail("HomeScreenStore.connectProfile must close with `    }`")
            return
        }
        let body = String(source[bodyStart..<closingBraceRange.lowerBound])
        XCTAssertTrue(
            body.contains("rejectIfConnectBlocked()"),
            "connectProfile must gate via the wider connect-blocked predicate so saved-password / passwordless connects don't race a process-wide execution lease"
        )
        XCTAssertFalse(
            body.contains("rejectIfMaintaining()"),
            "connectProfile must NOT use the verify-only `rejectIfMaintaining` — that predicate ignores `isExecuting` which is held by a BG runner the connect would race"
        )
        XCTAssertTrue(
            source.contains("private func rejectIfConnectBlocked() -> Bool"),
            "HomeScreenStore must expose a rejectIfConnectBlocked helper that surfaces the maintenance-in-progress alert on a positive check"
        )
    }

    /// `HomeScreenStore.load()` must gate the bootstrap auto-connect on the wider predicate too,
    /// not just on `isMaintenanceBlocked`. A BG `BGProcessingTask` running at app launch sets
    /// `isExecuting` but not `isVerifying`; the verify-only `isMaintenanceBlocked` would let
    /// `attemptAutoConnect` fire and open `reloadRemoteIndex` against the same remote.
    func testLoadAutoConnectGatesOnConnectBlocked() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let source = try String(
            contentsOf: root.appendingPathComponent("Watermelon/Home/HomeScreenStore.swift"),
            encoding: .utf8
        )
        guard let loadRange = source.range(of: "func load() {") else {
            XCTFail("HomeScreenStore.load() must remain present")
            return
        }
        let bodyEnd = source.index(loadRange.upperBound, offsetBy: 1400, limitedBy: source.endIndex) ?? source.endIndex
        let body = String(source[loadRange.upperBound..<bodyEnd])
        XCTAssertTrue(
            body.contains("isConnectMaintenanceOrExecutionBlocked()"),
            "load() must gate attemptAutoConnect on the wider predicate so a BG runner holding the execution lease at app launch defers the bootstrap connect"
        )
        XCTAssertFalse(
            body.contains("if !self.isMaintenanceBlocked"),
            "load() must NOT use the verify-only `isMaintenanceBlocked` — that predicate ignores `isExecuting`"
        )
    }

    /// `observeMaintenance` must retry auto-connect on the connect-blocked → idle transition,
    /// which fires when EITHER the verify lease OR the execution lease clears. The earlier
    /// verify-only retry path missed `isExecuting → idle` transitions because
    /// `isRemoteMaintenanceActive` never flipped on `isExecuting` changes.
    func testObserveMaintenanceRetriesAutoConnectOnConnectBlockedIdleTransition() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let source = try String(
            contentsOf: root.appendingPathComponent("Watermelon/Home/HomeScreenStore.swift"),
            encoding: .utf8
        )
        XCTAssertTrue(
            source.contains("private var wasConnectBlocked"),
            "HomeScreenStore must persist the previous connect-blocked value so observeMaintenance can detect the blocked → idle transition"
        )
        guard let observeRange = source.range(of: "private func observeMaintenance() {") else {
            XCTFail("HomeScreenStore.observeMaintenance must remain present")
            return
        }
        let bodyEnd = source.index(observeRange.upperBound, offsetBy: 2400, limitedBy: source.endIndex) ?? source.endIndex
        let body = String(source[observeRange.upperBound..<bodyEnd])
        XCTAssertTrue(
            body.contains("self.wasConnectBlocked && !connectBlocked"),
            "observeMaintenance must retry attemptAutoConnect when isConnectMaintenanceOrExecutionBlocked transitions from true to false"
        )
        XCTAssertTrue(
            body.contains("attemptAutoConnect()"),
            "observeMaintenance must call attemptAutoConnect() on the connect-blocked → idle transition"
        )
    }

    // MARK: - P01 R14 Codex B Finding 1: stale background-refresh reload must not corrupt active profile

    /// `refreshAfterBackgroundBackupIfRan()` starts an untracked async Task for
    /// `reloadRemoteIndex`. If the user switches to a different profile before the Task
    /// completes, the stale reload can reset `RemoteIndexSyncService`'s committed view to
    /// the old profile's data. The fix adds staleness guards before and after the reload,
    /// and cancels the Task on profile switch.
    func testRefreshAfterBackgroundBackupChecksProfileStillActive() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let source = try String(
            contentsOf: root.appendingPathComponent("Watermelon/Home/HomeScreenStore.swift"),
            encoding: .utf8
        )
        guard let refreshRange = source.range(of: "private func refreshAfterBackgroundBackupIfRan() {") else {
            XCTFail("HomeScreenStore.refreshAfterBackgroundBackupIfRan must remain present")
            return
        }
        let bodyEnd = source.index(refreshRange.upperBound, offsetBy: 1600, limitedBy: source.endIndex) ?? source.endIndex
        let body = String(source[refreshRange.upperBound..<bodyEnd])
        let stalenessChecks = body.components(separatedBy: "activeProfile?.id == profile.id").count - 1
        XCTAssertGreaterThanOrEqual(
            stalenessChecks, 2,
            "refreshAfterBackgroundBackupIfRan must check activeProfile?.id == profile.id both before and after reloadRemoteIndex so a stale reload does not corrupt a different profile's snapshot"
        )
        XCTAssertTrue(
            body.contains("backgroundRefreshTask?.cancel()"),
            "refreshAfterBackgroundBackupIfRan must cancel any previous background refresh Task before starting a new one"
        )
    }

    /// `connectProfile(_:)` and `disconnect()` must cancel the background refresh Task so a
    /// stale reload doesn't arrive after the user has already switched profiles.
    func testConnectProfileAndDisconnectCancelBackgroundRefreshTask() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let source = try String(
            contentsOf: root.appendingPathComponent("Watermelon/Home/HomeScreenStore.swift"),
            encoding: .utf8
        )
        guard let connectRange = source.range(of: "func connectProfile(_ profile: ServerProfileRecord) {") else {
            XCTFail("HomeScreenStore.connectProfile must remain present")
            return
        }
        let connectBodyEnd = source.index(connectRange.upperBound, offsetBy: 800, limitedBy: source.endIndex) ?? source.endIndex
        let connectBody = String(source[connectRange.upperBound..<connectBodyEnd])
        XCTAssertTrue(
            connectBody.contains("backgroundRefreshTask?.cancel()"),
            "connectProfile must cancel the background refresh Task before starting a new connect so the stale reload does not race"
        )

        guard let disconnectRange = source.range(of: "func disconnect() {") else {
            XCTFail("HomeScreenStore.disconnect must remain present")
            return
        }
        let disconnectBodyEnd = source.index(disconnectRange.upperBound, offsetBy: 400, limitedBy: source.endIndex) ?? source.endIndex
        let disconnectBody = String(source[disconnectRange.upperBound..<disconnectBodyEnd])
        XCTAssertTrue(
            disconnectBody.contains("backgroundRefreshTask?.cancel()"),
            "disconnect must cancel the background refresh Task so a stale reload does not arrive after disconnection"
        )
    }

    func testHomeScreenStoreTracksBackgroundRefreshTask() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let source = try String(
            contentsOf: root.appendingPathComponent("Watermelon/Home/HomeScreenStore.swift"),
            encoding: .utf8
        )
        XCTAssertTrue(
            source.contains("private var backgroundRefreshTask: Task<Void, Never>?"),
            "HomeScreenStore must track the background refresh Task so it can be cancelled on profile switch"
        )
    }

    // MARK: - P01 R14 Codex Checker Finding 1: auto-connect one-shot must not fire before profiles loaded

    /// `attemptAutoConnect()` previously set `didAttemptAutoConnect = true` before checking
    /// `savedProfiles`, so an observer retry between `init` and `load()` consumed the one-shot
    /// without connecting, stranding the user. The fix moves the flag set after the profile guard.
    func testAttemptAutoConnectDoesNotConsumeOneShotWithoutProfile() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let source = try String(
            contentsOf: root.appendingPathComponent("Watermelon/Home/HomeConnectionController.swift"),
            encoding: .utf8
        )
        guard let autoRange = source.range(of: "func attemptAutoConnect() {") else {
            XCTFail("HomeConnectionController.attemptAutoConnect must remain present")
            return
        }
        let bodyEnd = source.index(autoRange.upperBound, offsetBy: 800, limitedBy: source.endIndex) ?? source.endIndex
        let body = String(source[autoRange.upperBound..<bodyEnd])

        guard let flagSet = body.range(of: "didAttemptAutoConnect = true") else {
            XCTFail("attemptAutoConnect must still set didAttemptAutoConnect")
            return
        }
        guard let profileGuard = body.range(of: "savedProfiles.first(where:") else {
            XCTFail("attemptAutoConnect must still check savedProfiles")
            return
        }
        XCTAssertGreaterThan(
            flagSet.lowerBound, profileGuard.lowerBound,
            "didAttemptAutoConnect = true must appear AFTER the savedProfiles check so an observer retry before load() doesn't consume the one-shot without connecting"
        )
    }

    // MARK: - P01 R11 Codex Checker Finding 1: Home maintenance state must observe process-wide isVerifying

    /// `HomeScreenStore.isMaintenanceBlocked` and the maintenance-active derivation must consult
    /// the process-wide `appRuntimeFlags.isVerifying` flag so a verify task that survived scene
    /// disconnect/reconnect still blocks Home selection, auto-connect, and connect/disconnect
    /// affordances. The local controller alone is insufficient because the new container's
    /// controller is idle while the old verify task still owns the shared lease.
    func testHomeScreenStoreObservesProcessWideVerifyLease() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let source = try String(
            contentsOf: root.appendingPathComponent("Watermelon/Home/HomeScreenStore.swift"),
            encoding: .utf8
        )

        // isMaintenanceBlocked must consult both halves of the shared verify ownership boundary.
        guard let blockedRange = source.range(of: "var isMaintenanceBlocked: Bool {") else {
            XCTFail("HomeScreenStore.isMaintenanceBlocked must remain present")
            return
        }
        let blockedEnd = source.index(blockedRange.upperBound, offsetBy: 400, limitedBy: source.endIndex) ?? source.endIndex
        let blockedBody = String(source[blockedRange.upperBound..<blockedEnd])
        XCTAssertTrue(
            blockedBody.contains("appRuntimeFlags.isVerifying"),
            "isMaintenanceBlocked must check appRuntimeFlags.isVerifying so scene-reconnect with a leftover verify lease still blocks Home actions"
        )
        XCTAssertTrue(
            blockedBody.contains("remoteMaintenanceController.isVerifying"),
            "isMaintenanceBlocked must still check the local controller as the fast path"
        )

        // The active-derivation closure must combine both flags.
        XCTAssertTrue(
            source.contains("computedRemoteMaintenanceActive()"),
            "HomeScreenStore must factor maintenance-active derivation into a helper that consults both flags"
        )

        // observeMaintenance must register an ExecutionLifecycleDidChange observer so process-wide
        // flag changes (not just the local controller's RemoteMaintenanceDidChange) wake the store.
        XCTAssertTrue(
            source.contains("ExecutionLifecycleDidChange"),
            "HomeScreenStore.observeMaintenance must observe ExecutionLifecycleDidChange so process-wide verify flag transitions wake the store"
        )
    }

    /// `HomeScreenStore.load()` must skip auto-connect while ANY process-wide V2 owner (BG
    /// runner via `isExecuting`, verify task via `isVerifying`, local verify) is active.
    /// Superseded R11's verify-only pin — see R13 F32 (Codex A/B/Checker findings).
    func testHomeScreenStoreLoadGatesAutoConnectOnMaintenance() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let source = try String(
            contentsOf: root.appendingPathComponent("Watermelon/Home/HomeScreenStore.swift"),
            encoding: .utf8
        )
        guard let loadRange = source.range(of: "func load() {") else {
            XCTFail("HomeScreenStore.load() must remain present")
            return
        }
        let loadEnd = source.index(loadRange.upperBound, offsetBy: 1400, limitedBy: source.endIndex) ?? source.endIndex
        let loadBody = String(source[loadRange.upperBound..<loadEnd])
        XCTAssertTrue(
            loadBody.contains("isConnectMaintenanceOrExecutionBlocked()"),
            "load() must gate attemptAutoConnect on the connect-blocked predicate so a BG runner holding the execution lease at app launch defers the bootstrap connect"
        )

        // observeMaintenance must retry attemptAutoConnect on the connect-blocked → idle transition.
        guard let observeRange = source.range(of: "private func observeMaintenance() {") else {
            XCTFail("HomeScreenStore.observeMaintenance must remain present")
            return
        }
        let observeEnd = source.index(observeRange.upperBound, offsetBy: 2400, limitedBy: source.endIndex) ?? source.endIndex
        let observeBody = String(source[observeRange.upperBound..<observeEnd])
        XCTAssertTrue(
            observeBody.contains("attemptAutoConnect()"),
            "observeMaintenance must retry attemptAutoConnect on the connect-blocked → idle transition so a load-time skip is recoverable"
        )
    }

    /// Documents the ordering invariant that the fix enforces: a runner-like task that holds the
    /// shared execution lease via `tryBeginExecution` and releases it via `defer` only clears the
    /// lease after the task's async work finishes unwinding. Signaling system completion before
    /// awaiting that unwind would let iOS suspend with the lease still held.
    func testExecutionLeaseStaysHeldUntilRunnerLikeTaskUnwinds() async {
        XCTAssertTrue(AppRuntimeFlags.shared.tryBeginExecution())

        let runnerStarted = XCTestExpectation(description: "runner-like task entered its work loop")
        let backupTask = Task<Bool, Never> {
            defer { AppRuntimeFlags.shared.setExecuting(false) }
            runnerStarted.fulfill()
            // Simulate a long await inside the runner that responds to cancellation.
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 1_000_000)
            }
            // Mimic post-cancel cleanup work (runtime shutdown, client disconnect) between
            // the cancellation point and the `defer` firing.
            try? await Task.sleep(nanoseconds: 5_000_000)
            return false
        }
        await fulfillment(of: [runnerStarted], timeout: 2.0)

        // Buggy ordering: cancel and immediately observe — the lease is still held because the
        // runner's defer has not fired yet. If the AppDelegate signaled system completion at
        // this point, iOS could suspend the app with `isExecuting == true`.
        backupTask.cancel()
        XCTAssertTrue(AppRuntimeFlags.shared.isExecuting,
                      "lease must still be held immediately after cancel; defer has not fired yet")
        XCTAssertFalse(AppRuntimeFlags.shared.tryBeginExecution(),
                       "foreground/verify claim must fail while the BG runner is unwinding")

        // Correct ordering: await the task's value (runs the defer). Only now should the lease
        // be released and future claims succeed — matching the fixed expirationHandler shape.
        _ = await backupTask.value
        XCTAssertFalse(AppRuntimeFlags.shared.isExecuting,
                       "after awaiting unwind, the BG runner's defer must have released the lease")
        XCTAssertTrue(AppRuntimeFlags.shared.tryBeginExecution(),
                      "with the lease released, a fresh foreground claim succeeds")
        AppRuntimeFlags.shared.setExecuting(false)
    }
}
