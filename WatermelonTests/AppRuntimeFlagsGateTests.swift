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
