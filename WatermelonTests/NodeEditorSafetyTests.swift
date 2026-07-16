import NIOCore
import NIOPosix
import Security
import XCTest
import UIKit
@testable import Watermelon

final class NodeEditorSafetyTests: XCTestCase {
    override func setUp() {
        super.setUp()
        AppRuntimeFlags._testReset()
    }

    override func tearDown() {
        AppRuntimeFlags._testReset()
        super.tearDown()
    }

    @MainActor
    func testScreenBoundRunnerStartEndRejectsReentryAndNotifiesState() async {
        var continuation: CheckedContinuation<Int, Never>?
        var states: [Bool] = []
        var completions: [Int] = []
        var runner: ScreenBoundAsyncRunner<Int>!
        runner = ScreenBoundAsyncRunner(
            isScreenActive: { true },
            onStateChanged: { states.append(runner.isRunning) }
        )

        XCTAssertTrue(runner.start(
            operation: {
                await withCheckedContinuation { continuation = $0 }
            },
            completion: { result in completions.append(try! result.get()) }
        ))
        XCTAssertTrue(runner.isRunning)
        XCTAssertFalse(runner.start(operation: { 99 }, completion: { _ in }))
        await Task.yield()
        continuation?.resume(returning: 42)
        while runner.isRunning { await Task.yield() }

        XCTAssertEqual(states, [true, false])
        XCTAssertEqual(completions, [42])
    }

    @MainActor
    func testScreenBoundRunnerCancelDiscardsLateOperationBeforeNewCompletion() async {
        var firstContinuation: CheckedContinuation<Int, Never>?
        var secondContinuation: CheckedContinuation<Int, Never>?
        var completions: [Int] = []
        let runner = ScreenBoundAsyncRunner<Int>(
            isScreenActive: { true },
            onStateChanged: {}
        )

        XCTAssertTrue(runner.start(
            operation: { await withCheckedContinuation { firstContinuation = $0 } },
            completion: { result in completions.append(try! result.get()) }
        ))
        await Task.yield()
        runner.cancel()
        XCTAssertFalse(runner.isRunning)
        XCTAssertTrue(runner.start(
            operation: { await withCheckedContinuation { secondContinuation = $0 } },
            completion: { result in completions.append(try! result.get()) }
        ))
        await Task.yield()
        firstContinuation?.resume(returning: 1)
        await Task.yield()
        XCTAssertTrue(runner.isRunning)
        XCTAssertTrue(completions.isEmpty)
        secondContinuation?.resume(returning: 2)
        while runner.isRunning { await Task.yield() }

        XCTAssertEqual(completions, [2])
    }

    @MainActor
    func testSFTPNoncooperativeFingerprintCaptureDoesNotRetainOwnerOrApplyLateResult() async {
        var captureContinuation: CheckedContinuation<String, Never>?
        var completionCount = 0
        var owner: WeakOwnerSFTPConnectionHarness? = WeakOwnerSFTPConnectionHarness {
            completionCount += 1
        }
        weak var weakOwner = owner

        owner?.startFingerprintCapture {
            await withCheckedContinuation { captureContinuation = $0 }
        }
        while captureContinuation == nil { await Task.yield() }

        owner = nil
        XCTAssertNil(weakOwner)

        captureContinuation?.resume(returning: "late-fingerprint")
        for _ in 0 ..< 10 { await Task.yield() }
        XCTAssertEqual(completionCount, 0)
    }

    @MainActor
    func testScreenBoundRunnerAppliesSuccessAndFailureOnlyToActiveScreen() async {
        var isActive = false
        var completionCount = 0
        let runner = ScreenBoundAsyncRunner<Int>(
            isScreenActive: { isActive },
            onStateChanged: {}
        )

        XCTAssertTrue(runner.start(
            operation: { 1 },
            completion: { _ in completionCount += 1 }
        ))
        while runner.isRunning { await Task.yield() }
        XCTAssertTrue(runner.start(
            operation: { throw RemoteStorageClientError.unavailable },
            completion: { _ in completionCount += 1 }
        ))
        while runner.isRunning { await Task.yield() }
        XCTAssertEqual(completionCount, 0)

        isActive = true
        XCTAssertTrue(runner.start(
            operation: { 2 },
            completion: { _ in completionCount += 1 }
        ))
        while runner.isRunning { await Task.yield() }
        XCTAssertTrue(runner.start(
            operation: { throw RemoteStorageClientError.unavailable },
            completion: { _ in completionCount += 1 }
        ))
        while runner.isRunning { await Task.yield() }
        XCTAssertEqual(completionCount, 2)
    }

    @MainActor
    func testPresentationDismissalSequencerRunsActionOnlyAfterPresentedStateClears() async {
        var checks = 0
        var actionCheckCount: Int?
        await PresentationDismissalSequencer.performAfterDismissal(
            isPresented: {
                checks += 1
                return checks < 3
            },
            action: { actionCheckCount = checks }
        )
        XCTAssertEqual(checks, 3)
        XCTAssertEqual(actionCheckCount, 3)
    }

    @MainActor
    func testPresentationDismissalSequencerDoesNotRetainTornDownOwner() async {
        var actionCount = 0
        var owner: DismissalSequencingOwner? = DismissalSequencingOwner {
            actionCount += 1
        }
        weak var weakOwner = owner
        owner?.start()
        await Task.yield()

        owner = nil
        for _ in 0 ..< 10 { await Task.yield() }

        XCTAssertNil(weakOwner)
        XCTAssertEqual(actionCount, 0)
    }

    func testMutationServiceRejectsCreateDuplicateBeforeCredentialWrite() throws {
        for (backend, fixture) in try makeRemoteProfileFixtures() {
            let directory = FileManager.default.temporaryDirectory
                .appendingPathComponent("WatermelonMutationTests-\(backend)-\(UUID().uuidString)", isDirectory: true)
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            defer { try? FileManager.default.removeItem(at: directory) }
            let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

            var existing = fixture
            existing.credentialRef = StorageProfilePersistence.credentialRef(for: try XCTUnwrap(existing.duplicateIdentity))
            try database.saveConnectionProfile(&existing, editingProfileID: nil)
            let credentialStore = TestStorageProfileCredentialStore(values: [
                existing.credentialRef: "existing-secret"
            ])
            let service = StorageProfileMutationService(
                databaseManager: database,
                credentialStore: credentialStore,
                runtimeFlags: AppRuntimeFlags()
            )
            var duplicate = existing
            duplicate.id = nil

            XCTAssertThrowsError(try service.saveRemoteProfile(
                editingProfile: nil,
                credential: "replacement-secret",
                makeProfile: { _ in duplicate }
            ), backend)
            XCTAssertEqual(credentialStore.values[existing.credentialRef], "existing-secret", backend)
            XCTAssertEqual(credentialStore.saveCount, 0, backend)
            XCTAssertEqual(try database.fetchServerProfiles().count, 1, backend)
        }
    }

    func testMutationServiceCreatesAllRemoteBackendsWithoutConnectionDependencies() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonMutationTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        let credentialStore = TestStorageProfileCredentialStore()
        let service = StorageProfileMutationService(
            databaseManager: database,
            credentialStore: credentialStore,
            runtimeFlags: AppRuntimeFlags()
        )

        for (backend, fixture) in try makeRemoteProfileFixtures() {
            var candidate = fixture
            candidate.credentialRef = StorageProfilePersistence.credentialRef(for: try XCTUnwrap(candidate.duplicateIdentity))
            let saved = try XCTUnwrap(service.saveRemoteProfile(
                editingProfile: nil,
                credential: "\(backend)-secret",
                makeProfile: { _ in candidate }
            ))
            XCTAssertEqual(saved.resolvedStorageType.rawValue, backend)
            XCTAssertEqual(credentialStore.values[candidate.credentialRef], "\(backend)-secret")
        }
        XCTAssertEqual(try database.fetchServerProfiles().count, 4)
        XCTAssertEqual(credentialStore.saveCount, 4)
    }

    func testMutationServiceRejectsCreateCandidateWithExistingIDBeforeCredentialWrite() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonMutationTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        var existing = makeSMBProfile(basePath: "/original", credentialRef: "existing-ref", thumbnails: false)
        try database.saveConnectionProfile(&existing, editingProfileID: nil)
        let profileID = try XCTUnwrap(existing.id)
        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)
        let credentialStore = TestStorageProfileCredentialStore(values: ["existing-ref": "existing-secret"])
        let service = StorageProfileMutationService(
            databaseManager: database,
            credentialStore: credentialStore,
            runtimeFlags: AppRuntimeFlags()
        )
        var candidate = existing
        candidate.basePath = "/replacement"
        candidate.credentialRef = "replacement-ref"

        XCTAssertThrowsError(try service.saveRemoteProfile(
            editingProfile: nil,
            credential: "replacement-secret",
            makeProfile: { _ in candidate }
        ))

        let live = try XCTUnwrap(database.fetchServerProfile(id: profileID))
        XCTAssertEqual(live.basePath, "/original")
        XCTAssertEqual(live.credentialRef, "existing-ref")
        XCTAssertEqual(credentialStore.values, ["existing-ref": "existing-secret"])
        XCTAssertEqual(credentialStore.saveCount, 0)
        XCTAssertEqual(try database.activeServerProfileID(), profileID)
        XCTAssertNotNil(try database.remoteVerifiedAt(profileID: profileID))
    }

    func testDatabaseCreateRejectsProfileWithExistingIDWithoutMutatingLiveRowOrState() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonMutationTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        var existing = makeSMBProfile(basePath: "/original", credentialRef: "existing-ref", thumbnails: false)
        try database.saveConnectionProfile(&existing, editingProfileID: nil)
        let profileID = try XCTUnwrap(existing.id)
        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)
        var candidate = existing
        candidate.basePath = "/replacement"
        candidate.credentialRef = "replacement-ref"

        XCTAssertThrowsError(try database.saveConnectionProfile(&candidate, editingProfileID: nil))

        let live = try XCTUnwrap(database.fetchServerProfile(id: profileID))
        XCTAssertEqual(live.basePath, "/original")
        XCTAssertEqual(live.credentialRef, "existing-ref")
        XCTAssertEqual(try database.fetchServerProfiles().count, 1)
        XCTAssertEqual(try database.activeServerProfileID(), profileID)
        XCTAssertNotNil(try database.remoteVerifiedAt(profileID: profileID))
    }

    func testMutationServiceRejectsEditCollisionBeforeCredentialWrite() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonMutationTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        var first = makeSMBProfile(basePath: "/A", credentialRef: "", thumbnails: false)
        first.credentialRef = StorageProfilePersistence.credentialRef(for: try XCTUnwrap(first.duplicateIdentity))
        try database.saveConnectionProfile(&first, editingProfileID: nil)
        var second = makeSMBProfile(basePath: "/B", credentialRef: "", thumbnails: false)
        second.credentialRef = StorageProfilePersistence.credentialRef(for: try XCTUnwrap(second.duplicateIdentity))
        try database.saveConnectionProfile(&second, editingProfileID: nil)
        let credentialStore = TestStorageProfileCredentialStore(values: [
            first.credentialRef: "first-secret",
            second.credentialRef: "second-secret"
        ])
        let service = StorageProfileMutationService(
            databaseManager: database,
            credentialStore: credentialStore,
            runtimeFlags: AppRuntimeFlags()
        )

        XCTAssertThrowsError(try service.saveRemoteProfile(
            editingProfile: first,
            credential: "replacement-secret",
            makeProfile: { liveProfile in
                var collision = second
                collision.id = liveProfile?.id
                return collision
            }
        ))
        XCTAssertEqual(credentialStore.values[first.credentialRef], "first-secret")
        XCTAssertEqual(credentialStore.values[second.credentialRef], "second-secret")
        XCTAssertEqual(credentialStore.saveCount, 0)
    }

    func testMutationServiceFinalDatabaseCheckRejectsRaceAndRollsBackCredential() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonMutationTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        var candidate = makeSMBProfile(basePath: "/A", credentialRef: "", thumbnails: false)
        candidate.credentialRef = StorageProfilePersistence.credentialRef(for: try XCTUnwrap(candidate.duplicateIdentity))
        let credentialStore = TestStorageProfileCredentialStore(values: [
            candidate.credentialRef: "previous-secret"
        ])
        credentialStore.onFirstSave = { _, _ in
            var racer = candidate
            try database.saveConnectionProfile(&racer, editingProfileID: nil)
        }
        let service = StorageProfileMutationService(
            databaseManager: database,
            credentialStore: credentialStore,
            runtimeFlags: AppRuntimeFlags()
        )

        XCTAssertThrowsError(try service.saveRemoteProfile(
            editingProfile: nil,
            credential: "new-secret",
            makeProfile: { _ in candidate }
        ))
        XCTAssertEqual(credentialStore.values[candidate.credentialRef], "previous-secret")
        XCTAssertEqual(credentialStore.saveCount, 2)
        XCTAssertEqual(try database.fetchServerProfiles().count, 1)
    }

    func testMutationServiceRejectsDeletedEditingRowBeforeCredentialWrite() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonMutationTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        var editing = makeSMBProfile(basePath: "/A", credentialRef: "editing-ref", thumbnails: false)
        try database.saveConnectionProfile(&editing, editingProfileID: nil)
        try database.deleteServerProfile(id: try XCTUnwrap(editing.id))
        let credentialStore = TestStorageProfileCredentialStore(values: ["editing-ref": "existing-secret"])
        let service = StorageProfileMutationService(
            databaseManager: database,
            credentialStore: credentialStore,
            runtimeFlags: AppRuntimeFlags()
        )
        var didBuildProfile = false

        XCTAssertThrowsError(try service.saveRemoteProfile(
            editingProfile: editing,
            credential: "replacement-secret",
            makeProfile: { _ in
                didBuildProfile = true
                return editing
            }
        ))
        XCTAssertFalse(didBuildProfile)
        XCTAssertEqual(credentialStore.values["editing-ref"], "existing-secret")
        XCTAssertEqual(credentialStore.saveCount, 0)
    }

    func testMutationServiceBuildsEditFromLiveProfileAndCleansOldCredential() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonMutationTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        var snapshot = makeSMBProfile(basePath: "/A", credentialRef: "", thumbnails: false)
        snapshot.credentialRef = StorageProfilePersistence.credentialRef(for: try XCTUnwrap(snapshot.duplicateIdentity))
        try database.saveConnectionProfile(&snapshot, editingProfileID: nil)
        try database.setServerProfileName("Live Name", profileID: try XCTUnwrap(snapshot.id))
        var newIdentityShape = snapshot
        newIdentityShape.basePath = "/B"
        let newCredentialRef = StorageProfilePersistence.credentialRef(
            for: try XCTUnwrap(newIdentityShape.duplicateIdentity)
        )
        let credentialStore = TestStorageProfileCredentialStore(values: [
            snapshot.credentialRef: "old-secret"
        ])
        let service = StorageProfileMutationService(
            databaseManager: database,
            credentialStore: credentialStore,
            runtimeFlags: AppRuntimeFlags()
        )

        let saved = try XCTUnwrap(service.saveRemoteProfile(
            editingProfile: snapshot,
            credential: "new-secret",
            makeProfile: { liveProfile in
                var candidate = try XCTUnwrap(liveProfile)
                XCTAssertEqual(candidate.name, "Live Name")
                candidate.basePath = "/B"
                candidate.credentialRef = newCredentialRef
                return candidate
            }
        ))
        XCTAssertEqual(saved.name, "Live Name")
        XCTAssertEqual(saved.basePath, "/B")
        XCTAssertEqual(credentialStore.values[newCredentialRef], "new-secret")
        XCTAssertNil(credentialStore.values[snapshot.credentialRef])
    }

    func testProfileCommitGateRejectsReentryAndAllowsRetryOnlyAfterFailure() {
        var gate = StorageProfileCommitGate()

        XCTAssertTrue(gate.begin())
        XCTAssertTrue(gate.isCommitting)
        XCTAssertFalse(gate.begin())

        gate.releaseAfterFailure()
        XCTAssertFalse(gate.isCommitting)
        XCTAssertTrue(gate.begin())
        XCTAssertTrue(gate.isCommitting)
    }

    func testExternalSaveCompletionRoutesByScreenPhase() {
        XCTAssertEqual(
            ExternalStorageSaveCompletionPolicy.mode(
                commitSucceeded: false,
                operationIsCurrent: true,
                screenPhase: .active,
                isScreenActive: true
            ),
            .none
        )
        XCTAssertEqual(
            ExternalStorageSaveCompletionPolicy.mode(
                commitSucceeded: true,
                operationIsCurrent: false,
                screenPhase: .active,
                isScreenActive: true
            ),
            .refreshOnly
        )
        XCTAssertEqual(
            ExternalStorageSaveCompletionPolicy.mode(
                commitSucceeded: true,
                operationIsCurrent: true,
                screenPhase: .active,
                isScreenActive: true
            ),
            .normal
        )
        XCTAssertEqual(
            ExternalStorageSaveCompletionPolicy.mode(
                commitSucceeded: true,
                operationIsCurrent: true,
                screenPhase: .active,
                isScreenActive: false
            ),
            .refreshOnly
        )
        XCTAssertEqual(
            ExternalStorageSaveCompletionPolicy.mode(
                commitSucceeded: true,
                operationIsCurrent: true,
                screenPhase: .departing,
                isScreenActive: false
            ),
            .deferred
        )
        XCTAssertEqual(
            ExternalStorageSaveCompletionPolicy.mode(
                commitSucceeded: true,
                operationIsCurrent: true,
                screenPhase: .inactive,
                isScreenActive: false
            ),
            .refreshOnly
        )
    }

    func testExternalRefreshOnlyEndsOnlyTheCurrentOperationCommitGate() {
        XCTAssertTrue(ExternalStorageSaveCompletionPolicy.shouldEndCommitGate(
            mode: .refreshOnly,
            operationIsCurrent: true
        ))
        XCTAssertFalse(ExternalStorageSaveCompletionPolicy.shouldEndCommitGate(
            mode: .normal,
            operationIsCurrent: true
        ))
        XCTAssertFalse(ExternalStorageSaveCompletionPolicy.shouldEndCommitGate(
            mode: .deferred,
            operationIsCurrent: true
        ))
        XCTAssertFalse(ExternalStorageSaveCompletionPolicy.shouldEndCommitGate(
            mode: .refreshOnly,
            operationIsCurrent: false
        ))
    }

    @MainActor
    func testExternalRefreshOnlyUpdatesOrInvalidatesActiveSessionWithoutConnecting() throws {
        var original = makeSMBProfile(basePath: "/", credentialRef: "external", thumbnails: false)
        original.id = 41
        original.storageType = StorageType.externalVolume.rawValue
        original.shareName = "external-location"
        original.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(rootBookmarkData: Data([1]), displayPath: "/Volumes/Photos")
        )
        var refreshed = original
        refreshed.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(rootBookmarkData: Data([2]), displayPath: "/Volumes/Photos")
        )

        let session = AppSession()
        session.activate(profile: original, password: "preserved-password")
        ExternalStoragePersistedProfileRefresh.applyToActiveSession(
            appSession: session,
            originalProfile: original,
            savedProfile: refreshed
        )
        XCTAssertEqual(session.activeProfile?.connectionParams, refreshed.connectionParams)
        XCTAssertEqual(session.activePassword, "preserved-password")

        refreshed.shareName = "external-new-location"
        ExternalStoragePersistedProfileRefresh.applyToActiveSession(
            appSession: session,
            originalProfile: original,
            savedProfile: refreshed
        )
        XCTAssertNil(session.activeProfile)
        XCTAssertNil(session.activePassword)
    }

    func testSMBSelectionBindingTracksCanonicalConnectionContext() {
        let original = SMBSelectionContextSignature(auth: SMBServerAuthContext(
            name: "Original Name",
            host: "SMB://NAS.Local/",
            port: 0,
            username: " user ",
            password: "secret ",
            domain: " WORKGROUP "
        ))
        let equivalent = SMBSelectionContextSignature(auth: SMBServerAuthContext(
            name: "Renamed",
            host: "nas.local",
            port: 445,
            username: "user",
            password: "secret ",
            domain: "workgroup"
        ))
        let changedPassword = SMBSelectionContextSignature(auth: SMBServerAuthContext(
            name: "Original Name",
            host: "nas.local",
            port: 445,
            username: "user",
            password: "secret",
            domain: "workgroup"
        ))
        let changedEndpoint = SMBSelectionContextSignature(auth: SMBServerAuthContext(
            name: "Original Name",
            host: "other.local",
            port: 445,
            username: "user",
            password: "secret ",
            domain: "workgroup"
        ))

        XCTAssertEqual(original, equivalent)
        XCTAssertNotEqual(original, changedPassword)
        XCTAssertNotEqual(original, changedEndpoint)

        var binding = SMBSelectionContextBinding()
        binding.bind(to: original)
        XCTAssertFalse(binding.invalidateIfMismatched(equivalent))
        XCTAssertTrue(binding.matches(original))
        XCTAssertTrue(binding.invalidateIfMismatched(changedPassword))
        XCTAssertFalse(binding.isBound)
        XCTAssertFalse(binding.matches(original))
        XCTAssertFalse(binding.invalidateIfMismatched(original))
    }

    func testProfileMutationLeaseBlocksExecutionStart() throws {
        let flags = AppRuntimeFlags()

        let result = flags.withProfileMutationLease(profileID: 7) {
            XCTAssertFalse(flags.tryEnterExecution())
            return 42
        }

        XCTAssertEqual(result, 42)
        XCTAssertTrue(flags.tryEnterExecution())
        flags.exitExecution()
    }

    func testExecutionBlocksProfileMutationLease() {
        let flags = AppRuntimeFlags()
        XCTAssertTrue(flags.tryEnterExecution())

        XCTAssertNil(flags.withProfileMutationLease(profileID: 7) { true })

        flags.exitExecution()
        XCTAssertEqual(flags.withProfileMutationLease(profileID: 7) { true }, true)
    }

    func testAsyncProfileMutationLeaseBlocksExecutionAndAllowsNestedCommit() async {
        let flags = AppRuntimeFlags()
        let otherFlags = AppRuntimeFlags()

        let result = await flags.withAsyncProfileMutationLease(profileID: 7) {
            XCTAssertFalse(otherFlags.tryEnterExecution())
            XCTAssertEqual(flags.withProfileMutationLease(profileID: 7) { 42 }, 42)
            await Task.yield()
            XCTAssertFalse(otherFlags.tryEnterExecution())
            return 7
        }

        XCTAssertEqual(result, 7)
        XCTAssertTrue(otherFlags.tryEnterExecution())
        otherFlags.exitExecution()
    }

    func testConnectingProfileBlocksItsMutationButNotAnotherProfile() throws {
        let flags = AppRuntimeFlags()
        let otherFlags = AppRuntimeFlags()
        XCTAssertTrue(flags.tryBeginConnecting(profileID: 7))
        XCTAssertFalse(otherFlags.tryBeginConnecting(profileID: 8))

        let blocked = flags.withProfileMutationLease(profileID: 7) { true }
        let allowed = flags.withProfileMutationLease(profileID: 8) { true }

        XCTAssertNil(blocked)
        XCTAssertEqual(allowed, true)
        flags.endConnecting(profileID: 7)
        XCTAssertTrue(otherFlags.tryBeginConnecting(profileID: 8))
        otherFlags.endConnecting(profileID: 8)
        XCTAssertEqual(flags.withProfileMutationLease(profileID: 7) { true }, true)
    }

    func testConnectingBlocksExecutionStart() {
        let flags = AppRuntimeFlags()
        XCTAssertTrue(flags.tryBeginConnecting(profileID: 7))
        XCTAssertFalse(flags.tryEnterExecution())
        flags.endConnecting(profileID: 7)
        XCTAssertTrue(flags.tryEnterExecution())
        flags.exitExecution()
    }

    func testConnectingOwnershipIsReleasedOnDeinit() {
        var owner: AppRuntimeFlags? = AppRuntimeFlags()
        XCTAssertTrue(owner?.tryBeginConnecting(profileID: 7) == true)

        owner = nil

        let next = AppRuntimeFlags()
        XCTAssertTrue(next.tryBeginConnecting(profileID: 8))
        next.endConnecting(profileID: 8)
    }

    func testRemoteDestinationComparisonIgnoresCredentialAndSettings() {
        let original = makeSMBProfile(basePath: "/A", credentialRef: "legacy", thumbnails: true)
        var edited = original
        edited.credentialRef = "path-scoped"
        edited.backgroundBackupEnabled = false
        edited.generateRemoteThumbnails = false
        edited.updatedAt = Date()

        XCTAssertTrue(original.hasSameRemoteDestination(as: edited))

        edited.basePath = "/B"
        XCTAssertFalse(original.hasSameRemoteDestination(as: edited))
    }

    func testRemoteHostIdentityIsCaseInsensitiveForExistingProfiles() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        var original = makeSMBProfile(basePath: "/A", credentialRef: "legacy", thumbnails: false)
        original.host = "SMB://NAS.Local/"
        original.domain = "WORKGROUP"
        try database.saveServerProfile(&original)

        var sameDestination = original
        sameDestination.host = "nas.local"
        sameDestination.shareName = "photos"
        sameDestination.basePath = "/A/"
        sameDestination.domain = "workgroup"
        XCTAssertTrue(original.hasSameRemoteDestination(as: sameDestination))
        XCTAssertEqual(RemoteHostIdentity.canonicalSMB(original.host), "nas.local")
        XCTAssertEqual(original.storageProfile.displaySubtitle, "SMB://nas.local/Photos/A")
        XCTAssertEqual(
            try database.findServerProfile(
                host: "nas.local",
                port: original.port,
                shareName: "photos",
                basePath: "/A/",
                username: original.username,
                domain: "workgroup"
            )?.id,
            original.id
        )

        var duplicate = sameDestination
        duplicate.id = nil
        XCTAssertThrowsError(try database.saveConnectionProfile(&duplicate, editingProfileID: nil))
        XCTAssertEqual(try database.fetchServerProfiles().count, 1)
    }

    func testRemoteStorageWriteVerifierRemovesProbeDirectory() async throws {
        let client = InMemoryRemoteStorageClient()
        try await RemoteStorageWriteVerifier.verify(
            client: client,
            cleanupClientFactory: { ForwardingProbeCleanupClient(target: client) },
            basePath: "/target"
        )

        let entries = try await client.list(path: "/target")
        let uploadedCount = await client.uploadedPaths.count
        let createdDirectoryCount = await client.createdDirectories.count
        let deletedCount = await client.deletedPaths.count
        XCTAssertTrue(entries.isEmpty)
        XCTAssertEqual(uploadedCount, 1)
        XCTAssertEqual(createdDirectoryCount, 2)
        XCTAssertEqual(deletedCount, 2)
    }

    func testRemoteStorageWriteVerifierRejectsCorruptReadBackAndCleansProbe() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueDownloadData(Data("corrupt".utf8))

        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { ForwardingProbeCleanupClient(target: client) },
                basePath: "/target"
            )
            XCTFail("Expected read-back mismatch")
        } catch {
            XCTAssertTrue(error is RemoteStorageClientError)
        }

        try await waitForProbeCleanup(client)
        let entries = try await client.list(path: "/target")
        let deletedCount = await client.deletedPaths.count
        XCTAssertTrue(entries.isEmpty)
        XCTAssertEqual(deletedCount, 2)
    }

    func testRemoteStorageWriteVerifierRejectsBackendThatOverwritesConditionalCreate() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setIgnoreCreateIfAbsent(true)

        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { ForwardingProbeCleanupClient(target: client) },
                basePath: "/target",
                timeout: 5
            )
            XCTFail("Expected conditional-create verification to fail")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .terminal)
            guard let storageError = error as? RemoteStorageClientError,
                  case .unsafeConditionalCreateUnsupported = storageError else {
                return XCTFail("Expected explicit unsafe conditional-create error, got \(error)")
            }
            XCTAssertEqual(
                UserFacingErrorLocalizer.message(for: error, storageType: .s3),
                String(localized: "storage.client.unsafeConditionalCreateUnsupported")
            )
        }

        try await waitForProbeCleanup(client)
        let entries = try await client.list(path: "/target")
        XCTAssertTrue(entries.isEmpty)
    }

    func testRemoteStorageWriteVerifierDeadlineDoesNotWaitForUncooperativeUpload() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setOnUpload {
            await withCheckedContinuation { continuation in
                DispatchQueue.global().asyncAfter(deadline: .now() + 0.3) {
                    continuation.resume()
                }
            }
        }

        let start = Date()
        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { ForwardingProbeCleanupClient(target: client) },
                basePath: "/target",
                timeout: 0.03,
                cleanupRetryDelays: [0, 0.1, 0.1]
            )
            XCTFail("Expected verifier deadline")
        } catch {
            XCTAssertLessThan(Date().timeIntervalSince(start), 0.2)
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
        }

        try await Task.sleep(nanoseconds: 500_000_000)
        let entries = try await client.list(path: "/target")
        let deletedCount = await client.deletedPaths.count
        XCTAssertTrue(entries.isEmpty)
        XCTAssertGreaterThanOrEqual(deletedCount, 2)
    }

    func testRemoteStorageWriteVerifierConfirmsCleanupAfterFailureAndLateWrite() async throws {
        let client = InMemoryRemoteStorageClient()
        let factory = NotFoundThenForwardingProbeCleanupFactory(target: client)
        await client.failUpload(
            forPathSuffix: "write-test",
            error: RemoteErrorFixtures.retryable
        )
        await client.setOnUpload {
            Task {
                while factory.count < 1 {
                    try? await Task.sleep(nanoseconds: 1_000_000)
                }
                try? await Task.sleep(nanoseconds: 20_000_000)
                guard let probeDirectory = await client.createdDirectories.last else { return }
                await client.seedFile(path: probeDirectory + "/write-test")
            }
        }

        let start = Date()
        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { factory.makeClient() },
                basePath: "/target",
                timeout: 5,
                cleanupRetryDelays: [0, 0.1]
            )
            XCTFail("Expected upload failure")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
            XCTAssertLessThan(Date().timeIntervalSince(start), 0.5)
        }

        try await waitForProbeCleanup(client, minimumFactoryCount: 0)
        let deadline = Date().addingTimeInterval(1)
        while factory.count < 2, Date() < deadline {
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        let entries = try await client.list(path: "/target")
        let deletedLateWrites = await client.deletedPaths.filter { $0.hasSuffix("/write-test") }
        XCTAssertTrue(entries.isEmpty)
        XCTAssertEqual(factory.count, 2)
        XCTAssertEqual(deletedLateWrites.count, 1)
    }

    func testRemoteStorageWriteVerifierDoesNotCleanupWhenConnectFails() async throws {
        let client = ProbeStorageClient(.throwError(RemoteStorageClientError.invalidConfiguration))
        let cleanupTarget = InMemoryRemoteStorageClient()
        let factory = ProbeCleanupFactoryRecorder(target: cleanupTarget)

        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { factory.makeClient() },
                basePath: "/target",
                timeout: 1,
                cleanupRetryDelays: [0, 0.01]
            )
            XCTFail("Expected connect failure")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .terminal)
        }

        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertEqual(factory.count, 0)
    }

    func testRemoteStorageWriteVerifierRetriesFreshCleanupAfterTransientFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        let factory = ProbeCleanupFactoryRecorder(target: client)
        await client.failUploadAfterWrite(
            forPathSuffix: "write-test",
            error: RemoteErrorFixtures.retryable
        )
        await client.enqueueDeleteError(RemoteErrorFixtures.retryable)

        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { factory.makeClient() },
                basePath: "/target",
                timeout: 5,
                cleanupRetryDelays: [0, 0.01, 0.02]
            )
            XCTFail("Expected upload failure")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
        }

        try await waitForProbeCleanup(client, factory: factory, minimumFactoryCount: 3)
        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertEqual(factory.count, 3)
    }

    func testRemoteStorageWriteVerifierCanWaitForCleanupBeforeReturningFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        let factory = ProbeCleanupFactoryRecorder(target: client)
        await client.failUploadAfterWrite(
            forPathSuffix: "write-test",
            error: RemoteErrorFixtures.retryable
        )
        await client.enqueueDeleteError(RemoteErrorFixtures.retryable)

        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { factory.makeClient() },
                basePath: "/target",
                timeout: 5,
                cleanupRetryDelays: [0, 0.01],
                failureCleanupPolicy: .waitForCompletion
            )
            XCTFail("Expected upload failure")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
        }

        let entries = try await client.list(path: "/target")
        XCTAssertTrue(entries.isEmpty)
        XCTAssertEqual(factory.count, 2)
    }

    func testRemoteStorageWriteVerifierExposesLateReapCompletion() async throws {
        let client = InMemoryRemoteStorageClient()
        let factory = ProbeCleanupFactoryRecorder(target: client)
        let gate = DeferredCleanupTestGate()
        await client.setOnUpload {
            await gate.wait()
            await gate.markHookCompleted()
        }

        let deferredCleanup: RemoteProbeDeferredCleanupError
        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { factory.makeClient() },
                basePath: "/target",
                timeout: 0.03,
                cleanupRetryDelays: [0],
                failureCleanupPolicy: .waitForCompletion
            )
            return XCTFail("Expected verifier deadline")
        } catch let error as RemoteProbeDeferredCleanupError {
            deferredCleanup = error
            XCTAssertEqual(RemoteFaultLite.classify(error.underlyingError), .retryable)
        } catch {
            return XCTFail("Expected deferred cleanup error, got \(error)")
        }

        let hookCompletedBeforeRelease = await gate.hookCompleted
        XCTAssertFalse(hookCompletedBeforeRelease)
        await gate.release()
        await deferredCleanup.waitUntilCleanupCompletes()
        let hookCompletedAfterCleanup = await gate.hookCompleted
        XCTAssertTrue(hookCompletedAfterCleanup)
        let entries = try await client.list(path: "/target")
        XCTAssertTrue(entries.isEmpty)
        XCTAssertGreaterThanOrEqual(factory.count, 2)
    }

    func testRemoteStorageWriteVerifierDoesNotWaitForeverForDeferredCleanup() async {
        let client = InMemoryRemoteStorageClient()
        await client.setOnUpload {
            await withUnsafeContinuation { (_: UnsafeContinuation<Void, Never>) in }
        }
        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { ForwardingProbeCleanupClient(target: client) },
                basePath: "/target",
                timeout: 0.03,
                cleanupRetryDelays: [0],
                failureCleanupPolicy: .waitForCompletion
            )
            XCTFail("Expected verifier deadline")
        } catch let error as RemoteProbeDeferredCleanupError {
            XCTAssertEqual(RemoteFaultLite.classify(error.underlyingError), .retryable)
        } catch {
            XCTFail("Expected deferred cleanup error, got \(error)")
        }

    }

    func testRemoteStorageWriteVerifierReclaimsTemporaryFilesWhenOperationNeverReturns() async throws {
        let artifactsBefore = try verifierTemporaryArtifacts()
        let client = InMemoryRemoteStorageClient()
        await client.setOnUpload {
            await withUnsafeContinuation { (_: UnsafeContinuation<Void, Never>) in }
        }

        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { ForwardingProbeCleanupClient(target: client) },
                basePath: "/target",
                timeout: 0.03,
                cleanupRetryDelays: [0, 0.01, 0.01]
            )
            XCTFail("Expected verifier deadline")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
        }

        XCTAssertEqual(try verifierTemporaryArtifacts(), artifactsBefore)
    }

    func testRemoteStorageWriteVerifierUsesIndependentCleanupWhenWrittenUploadNeverReturns() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setOnUploadAfterWrite {
            await withUnsafeContinuation { (_: UnsafeContinuation<Void, Never>) in }
        }

        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { ForwardingProbeCleanupClient(target: client) },
                basePath: "/target",
                timeout: 0.03,
                cleanupRetryDelays: [0, 0.05, 0.05]
            )
            XCTFail("Expected verifier deadline")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
        }

        try await Task.sleep(nanoseconds: 200_000_000)
        let entries = try await client.list(path: "/target")
        let deletedCount = await client.deletedPaths.count
        XCTAssertTrue(entries.isEmpty)
        XCTAssertGreaterThanOrEqual(deletedCount, 2)
    }

    func testRemoteStorageWriteVerifierDelayedCleanupRemovesLateProbeWrite() async throws {
        let client = InMemoryRemoteStorageClient()
        let factory = ProbeCleanupFactoryRecorder(target: client)
        await client.setOnUpload {
            await withCheckedContinuation { continuation in
                DispatchQueue.global().asyncAfter(deadline: .now() + 0.08) {
                    continuation.resume()
                }
            }
        }
        await client.setOnUploadAfterWrite {
            await withUnsafeContinuation { (_: UnsafeContinuation<Void, Never>) in }
        }

        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { factory.makeClient() },
                basePath: "/target",
                timeout: 0.03,
                cleanupRetryDelays: [0, 0.12, 0.12]
            )
            XCTFail("Expected verifier deadline")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
        }

        try await Task.sleep(nanoseconds: 230_000_000)
        let entries = try await client.list(path: "/target")
        XCTAssertTrue(entries.isEmpty)
        XCTAssertGreaterThanOrEqual(factory.count, 2)
    }

    func testCredentialRefV2IsDeterministicAndUnambiguous() {
        let first = StorageProfilePersistence.credentialRef(
            storageType: .webdav,
            identityFields: ["a|b", "c"]
        )
        let same = StorageProfilePersistence.credentialRef(
            storageType: .webdav,
            identityFields: ["a|b", "c"]
        )
        let formerlyColliding = StorageProfilePersistence.credentialRef(
            storageType: .webdav,
            identityFields: ["a", "b|c"]
        )

        XCTAssertEqual(first, same)
        XCTAssertNotEqual(first, formerlyColliding)
        XCTAssertTrue(first.hasPrefix("v2|webdav|"))
    }

    func testRemoteHostCanonicalizationIsSharedAcrossBackendsAndCredentialRefs() throws {
        XCTAssertEqual(RemoteHostIdentity.canonical(" MÜNICH.Example. "), "xn--mnich-kva.example")
        XCTAssertEqual(RemoteHostIdentity.canonical("nas.local."), "nas.local")
        XCTAssertEqual(
            RemoteHostIdentity.canonical("[2001:0DB8:0:0:0:0:0:1]"),
            RemoteHostIdentity.canonical("2001:db8::1")
        )
        XCTAssertNotEqual(
            RemoteHostIdentity.canonical("[fe80::1%en0]"),
            RemoteHostIdentity.canonical("[fe80::1%en1]")
        )
        XCTAssertNotEqual(RemoteHostIdentity.canonical("nas-a.local"), RemoteHostIdentity.canonical("nas-b.local"))

        var smbUnicode = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        smbUnicode.host = "SMB://MÜNICH.Example./"
        var smbASCII = smbUnicode
        smbASCII.host = "xn--mnich-kva.example"

        var webDAVUnicode = smbUnicode
        webDAVUnicode.storageType = StorageType.webdav.rawValue
        webDAVUnicode.host = "MÜNICH.Example."
        webDAVUnicode.connectionParams = try ServerProfileRecord.encodedConnectionParams(WebDAVConnectionParams(scheme: "https"))
        var webDAVASCII = webDAVUnicode
        webDAVASCII.host = "xn--mnich-kva.example"

        var s3Unicode = smbUnicode
        s3Unicode.storageType = StorageType.s3.rawValue
        s3Unicode.host = "MÜNICH.Example."
        s3Unicode.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "us-east-1", usePathStyle: true)
        )
        var s3ASCII = s3Unicode
        s3ASCII.host = "xn--mnich-kva.example"

        var sftpExpanded = smbUnicode
        sftpExpanded.storageType = StorageType.sftp.rawValue
        sftpExpanded.host = "[2001:0db8:0:0:0:0:0:1]"
        sftpExpanded.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "host-key")
        )
        var sftpCompressed = sftpExpanded
        sftpCompressed.host = "2001:db8::1"

        for (first, second) in [
            (smbUnicode, smbASCII),
            (webDAVUnicode, webDAVASCII),
            (s3Unicode, s3ASCII),
            (sftpExpanded, sftpCompressed)
        ] {
            let firstDuplicate = try XCTUnwrap(first.duplicateIdentity)
            let secondDuplicate = try XCTUnwrap(second.duplicateIdentity)
            XCTAssertEqual(firstDuplicate, secondDuplicate)
            XCTAssertEqual(first.remoteDestinationIdentity, second.remoteDestinationIdentity)
            XCTAssertEqual(
                StorageProfilePersistence.credentialRef(for: firstDuplicate),
                StorageProfilePersistence.credentialRef(for: secondDuplicate)
            )
        }

        var differentHost = webDAVASCII
        differentHost.host = "other.example"
        XCTAssertNotEqual(webDAVASCII.duplicateIdentity, differentHost.duplicateIdentity)
        XCTAssertNotEqual(webDAVASCII.remoteDestinationIdentity, differentHost.remoteDestinationIdentity)

        var r2RootDot = s3ASCII
        r2RootDot.host = "account.r2.cloudflarestorage.com."
        r2RootDot.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "", usePathStyle: false)
        )
        var r2Canonical = r2RootDot
        r2Canonical.host = "account.r2.cloudflarestorage.com"
        r2Canonical.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "auto", usePathStyle: false)
        )
        XCTAssertEqual(r2RootDot.duplicateIdentity, r2Canonical.duplicateIdentity)
        XCTAssertEqual(r2RootDot.remoteDestinationIdentity, r2Canonical.remoteDestinationIdentity)

        var anotherR2 = r2Canonical
        anotherR2.host = "another.r2.cloudflarestorage.com"
        XCTAssertNotEqual(r2Canonical.duplicateIdentity, anotherR2.duplicateIdentity)
        XCTAssertNotEqual(r2Canonical.remoteDestinationIdentity, anotherR2.remoteDestinationIdentity)
    }

    func testIPv4LeadingZeroCanonicalizationIsSharedAcrossBackendsAndEndpoints() throws {
        for (input, expected) in [
            ("192.168.001.1", "192.168.1.1"),
            ("0177.0.0.1", "177.0.0.1"),
            ("192.168.01.1", "192.168.1.1")
        ] {
            XCTAssertEqual(RemoteHostIdentity.canonical(input), expected)
            let endpoint = try XCTUnwrap(RemoteHostEndpoint.representation(input))
            XCTAssertEqual(endpoint.socketHost, expected)
            XCTAssertEqual(endpoint.urlAuthority, expected)
            XCTAssertTrue(endpoint.isIPLiteral)
        }

        var smbPadded = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        smbPadded.host = "192.168.001.1"
        var smbCanonical = smbPadded
        smbCanonical.host = "192.168.1.1"

        var webDAVPadded = smbPadded
        webDAVPadded.storageType = StorageType.webdav.rawValue
        webDAVPadded.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            WebDAVConnectionParams(scheme: "https")
        )
        var webDAVCanonical = webDAVPadded
        webDAVCanonical.host = "192.168.1.1"

        var s3Padded = smbPadded
        s3Padded.storageType = StorageType.s3.rawValue
        s3Padded.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "us-east-1", usePathStyle: true)
        )
        var s3Canonical = s3Padded
        s3Canonical.host = "192.168.1.1"

        var sftpPadded = smbPadded
        sftpPadded.storageType = StorageType.sftp.rawValue
        sftpPadded.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "host-key")
        )
        var sftpCanonical = sftpPadded
        sftpCanonical.host = "192.168.1.1"

        for (padded, canonical) in [
            (smbPadded, smbCanonical),
            (webDAVPadded, webDAVCanonical),
            (s3Padded, s3Canonical),
            (sftpPadded, sftpCanonical)
        ] {
            let paddedDuplicate = try XCTUnwrap(padded.duplicateIdentity)
            let canonicalDuplicate = try XCTUnwrap(canonical.duplicateIdentity)
            XCTAssertEqual(paddedDuplicate, canonicalDuplicate)
            XCTAssertEqual(padded.remoteDestinationIdentity, canonical.remoteDestinationIdentity)
            XCTAssertTrue(padded.hasSameRemoteDestination(as: canonical))
            XCTAssertEqual(
                StorageProfilePersistence.credentialRef(for: paddedDuplicate),
                StorageProfilePersistence.credentialRef(for: canonicalDuplicate)
            )
        }

        XCTAssertEqual(ProfileReachabilityService.operationalProbeHost(for: smbPadded), "192.168.1.1")
        XCTAssertTrue(smbPadded.storageProfile.displaySubtitle.contains("192.168.1.1"))
    }

    func testLegacyPaddedIPv4DuplicateIsRejected() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        var legacy = makeSMBProfile(basePath: "/A", credentialRef: "legacy", thumbnails: false)
        legacy.host = "192.168.001.1"
        try database.saveServerProfile(&legacy)

        var duplicate = legacy
        duplicate.id = nil
        duplicate.host = "192.168.1.1"
        duplicate.credentialRef = "canonical"
        XCTAssertThrowsError(try database.saveConnectionProfile(&duplicate, editingProfileID: nil))
    }

    func testLegacyPaddedIPv4EditPreservesDestinationState() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        var legacy = makeSMBProfile(basePath: "/A", credentialRef: "legacy", thumbnails: false)
        legacy.host = "192.168.001.1"
        try database.saveServerProfile(&legacy)
        let profileID = try XCTUnwrap(legacy.id)
        let profileKey = RemoteIndexSyncService.remoteProfileKey(legacy)
        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastCompletedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastRanAt(Date(), profileID: profileID)

        var edited = legacy
        edited.host = "192.168.1.1"
        XCTAssertTrue(legacy.hasSameRemoteDestination(as: edited))
        XCTAssertEqual(RemoteIndexSyncService.remoteProfileKey(edited), profileKey)
        try database.saveConnectionProfile(&edited, editingProfileID: profileID)

        XCTAssertEqual(try database.fetchServerProfile(id: profileID)?.host, "192.168.1.1")
        XCTAssertEqual(try database.activeServerProfileID(), profileID)
        XCTAssertNotNil(try database.remoteVerifiedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastCompletedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastRanAt(profileID: profileID))
    }

    func testRemoteHostEndpointSeparatesURLAuthorityFromSocketHost() throws {
        let expanded = try XCTUnwrap(RemoteHostEndpoint.representation("[2001:0DB8:0:0:0:0:0:1]"))
        XCTAssertEqual(expanded.socketHost, "2001:db8::1")
        XCTAssertEqual(expanded.urlAuthority, "[2001:db8::1]")
        XCTAssertTrue(expanded.isIPLiteral)

        let zoned = try XCTUnwrap(RemoteHostEndpoint.representation("[fe80::1%25en0]"))
        XCTAssertEqual(zoned.socketHost, "fe80::1%en0")
        XCTAssertEqual(zoned.urlAuthority, "[fe80::1%25en0]")
        XCTAssertNotEqual(
            zoned.socketHost,
            RemoteHostEndpoint.socketHost("[fe80::1%25en1]")
        )

        let smbURL = try XCTUnwrap(RemoteHostEndpoint.url(
            scheme: "smb",
            host: "smb://[2001:db8::1]/",
            port: 445,
            strippingSMBScheme: true
        ))
        XCTAssertEqual(smbURL.absoluteString, "smb://[2001:db8::1]:445")

        for host in ["nas/", "//nas/", "smb://nas/"] {
            XCTAssertEqual(
                RemoteHostEndpoint.socketHost(host, strippingSMBScheme: true),
                "nas"
            )
            XCTAssertEqual(
                RemoteHostEndpoint.url(
                    scheme: "smb",
                    host: host,
                    port: 445,
                    strippingSMBScheme: true
                )?.absoluteString,
                "smb://nas:445"
            )
        }

        let webDAVURL = try XCTUnwrap(ServerProfileRecord.buildWebDAVEndpointURL(
            scheme: "https",
            host: "2001:db8::1",
            port: 8443,
            mountPath: "/dav"
        ))
        XCTAssertEqual(webDAVURL.absoluteString, "https://[2001:db8::1]:8443/dav")

        let rootedWebDAV = ServerProfileRecord.buildWebDAVEndpointURL(
            scheme: "https",
            host: "MÜNICH.Example.",
            port: 443,
            mountPath: "/dav"
        )
        let canonicalWebDAV = ServerProfileRecord.buildWebDAVEndpointURL(
            scheme: "https",
            host: "xn--mnich-kva.example",
            port: 443,
            mountPath: "/dav"
        )
        XCTAssertEqual(rootedWebDAV, canonicalWebDAV)
        XCTAssertEqual(rootedWebDAV?.absoluteString, "https://xn--mnich-kva.example/dav")
        XCTAssertNotEqual(
            rootedWebDAV,
            ServerProfileRecord.buildWebDAVEndpointURL(
                scheme: "https",
                host: "other.example",
                port: 443,
                mountPath: "/dav"
            )
        )
        XCTAssertEqual(RemoteHostEndpoint.socketHost("nas.local."), "nas.local")
    }

    func testReachabilityProbeSignatureUsesOperationalSocketHost() throws {
        var smb = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        smb.port = 0
        let smbSignatures = ["nas/", "//nas/", "smb://nas/"].map { host -> ProfileReachabilityService.ProbeSignature in
            var profile = smb
            profile.host = host
            return ProfileReachabilityService.probeSignature(of: profile)
        }
        XCTAssertEqual(Set(smbSignatures).count, 1)
        XCTAssertEqual(smbSignatures.first?.host, "nas")
        XCTAssertEqual(smbSignatures.first?.port, 445)

        var differentSMB = smb
        differentSMB.host = "other-nas"
        XCTAssertNotEqual(
            smbSignatures.first,
            ProfileReachabilityService.probeSignature(of: differentSMB)
        )

        var sftp = smb
        sftp.storageType = StorageType.sftp.rawValue
        sftp.host = "[fe80:0:0:0:0:0:0:1%25en0]"
        sftp.port = 0
        sftp.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "host-key")
        )
        let sftpSignature = ProfileReachabilityService.probeSignature(of: sftp)
        XCTAssertEqual(sftpSignature.host, "fe80::1%en0")
        XCTAssertEqual(sftpSignature.port, 22)
        XCTAssertEqual(ProfileReachabilityService.operationalProbeHost(for: sftp), "fe80::1%en0")

        var invalidSMB = smb
        invalidSMB.host = "///"
        XCTAssertNil(ProfileReachabilityService.operationalProbeHost(for: invalidSMB))
        XCTAssertEqual(ProfileReachabilityService.probeSignature(of: invalidSMB).host, "")
    }

    func testSMBZeroPortIsOperationallyEquivalentToDefaultPort() throws {
        XCTAssertEqual(SMBEndpoint.effectivePort(0), SMBEndpoint.defaultPort)
        XCTAssertEqual(
            SMBEndpoint.url(host: "nas.local", port: 0),
            SMBEndpoint.url(host: "nas.local", port: SMBEndpoint.defaultPort)
        )
        XCTAssertEqual(SMBEndpoint.url(host: "nas.local", port: 0)?.absoluteString, "smb://nas.local:445")
        XCTAssertEqual(
            SMBServerLoginDraft(name: "NAS", host: "nas.local", port: 0, username: "alice", domain: nil).effectivePort,
            SMBEndpoint.defaultPort
        )

        var legacy = makeSMBProfile(basePath: "/A", credentialRef: "legacy", thumbnails: false)
        legacy.port = 0
        var canonical = legacy
        canonical.port = SMBEndpoint.defaultPort
        XCTAssertEqual(legacy.duplicateIdentity, canonical.duplicateIdentity)
        XCTAssertEqual(legacy.remoteDestinationIdentity, canonical.remoteDestinationIdentity)
        let legacyIdentity = try XCTUnwrap(legacy.duplicateIdentity)
        let canonicalIdentity = try XCTUnwrap(canonical.duplicateIdentity)
        XCTAssertEqual(
            StorageProfilePersistence.credentialRef(for: legacyIdentity),
            StorageProfilePersistence.credentialRef(for: canonicalIdentity)
        )

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        try database.saveConnectionProfile(&legacy, editingProfileID: nil)
        let profileID = try XCTUnwrap(legacy.id)

        XCTAssertEqual(
            try database.findServerProfile(
                host: legacy.host,
                port: SMBEndpoint.defaultPort,
                shareName: legacy.shareName,
                basePath: legacy.basePath,
                username: legacy.username,
                domain: legacy.domain
            )?.id,
            profileID
        )

        var duplicate = canonical
        duplicate.id = nil
        XCTAssertThrowsError(try database.saveConnectionProfile(&duplicate, editingProfileID: nil))

        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastCompletedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastRanAt(Date(), profileID: profileID)
        canonical.id = profileID
        try database.saveConnectionProfile(&canonical, editingProfileID: profileID)

        XCTAssertEqual(try database.fetchServerProfile(id: profileID)?.port, SMBEndpoint.defaultPort)
        XCTAssertEqual(try database.activeServerProfileID(), profileID)
        XCTAssertNotNil(try database.remoteVerifiedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastCompletedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastRanAt(profileID: profileID))
    }

    func testSMBPathsCanonicalizeOperationallyAndRejectParents() throws {
        XCTAssertEqual(try SMBPathCanonicalizer.canonicalRawPath("/A//./B/"), "/A/B")
        XCTAssertEqual(try SMBPathCanonicalizer.canonicalRawPath("\\\\A\\B\\"), "/A/B")
        XCTAssertEqual(try SMBPathCanonicalizer.canonicalRawPath("/A\\//.\\B"), "/A/B")
        XCTAssertEqual(try SMBPathCanonicalizer.canonicalRawPath("/%2F/相册/file..name"), "/%2F/相册/file..name")
        XCTAssertThrowsError(try SMBPathCanonicalizer.canonicalRawPath("/A/../B"))
        XCTAssertThrowsError(try SMBPathCanonicalizer.canonicalRawPath("/A\\..\\B"))
        XCTAssertThrowsError(try SMBPathCanonicalizer.canonicalRawPath("/A\\../B"))

        let repeated = makeSMBProfile(basePath: "/A\\//./B", credentialRef: "a", thumbnails: false)
        var canonical = repeated
        canonical.basePath = "/A/B"
        XCTAssertNotEqual(repeated.duplicateIdentity, canonical.duplicateIdentity)
        XCTAssertNotEqual(repeated.remoteDestinationIdentity, canonical.remoteDestinationIdentity)
        XCTAssertEqual(
            repeated.canonicalConnection?.canonicalComparisonKey,
            canonical.canonicalConnection?.canonicalComparisonKey
        )
        XCTAssertNoThrow(try StorageClientFactory().makeClient(profile: repeated, credentialPayload: "secret"))
        XCTAssertNoThrow(try StorageClientFactory().makeClient(profile: canonical, credentialPayload: "secret"))

        var invalid = repeated
        invalid.id = 999
        invalid.basePath = "/A\\..\\B"
        XCTAssertNil(invalid.duplicateIdentity)
        XCTAssertNotEqual(invalid.remoteDestinationIdentity, canonical.remoteDestinationIdentity)
        XCTAssertThrowsError(try StorageClientFactory().makeClient(profile: invalid, credentialPayload: "secret"))
    }

    func testReachabilityRefreshSchedulerRunsOnlyInForeground() {
        let harness = ReachabilityRefreshSchedulerHarness()
        let scheduler = ProfileReachabilityRefreshScheduler(
            interval: 45,
            hooks: .init(
                scheduleRepeating: { interval, action in
                    harness.schedule(interval: interval, action: action)
                },
                refreshImmediately: { harness.recordImmediateRefresh() },
                refreshPeriodically: { harness.recordPeriodicRefresh() }
            )
        )

        scheduler.enterForeground()
        XCTAssertEqual(harness.immediateRefreshCount, 1)
        XCTAssertEqual(harness.scheduledIntervals, [45])
        harness.fire()
        XCTAssertEqual(harness.periodicRefreshCount, 1)

        scheduler.enterBackground()
        XCTAssertEqual(harness.cancellationCount, 1)
        harness.fire()
        XCTAssertEqual(harness.periodicRefreshCount, 1)

        scheduler.enterForeground()
        scheduler.enterForeground()
        XCTAssertEqual(harness.immediateRefreshCount, 3)
        XCTAssertEqual(harness.scheduledIntervals, [45, 45, 45])
        XCTAssertEqual(harness.cancellationCount, 2)
        harness.fire()
        XCTAssertEqual(harness.periodicRefreshCount, 2)

        scheduler.stop()
        XCTAssertEqual(harness.cancellationCount, 3)
        harness.fire()
        XCTAssertEqual(harness.periodicRefreshCount, 2)
    }

    func testReachabilityPendingForceSweepReplaysExactlyOnce() async throws {
        let harness = ManualReachabilityProbeHarness()
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        let service = ProfileReachabilityService(hooks: .init(
            now: { now },
            probe: { profile, _ in await harness.probe(profile) }
        ))
        var profile = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        profile.id = 1
        service.setProfiles([profile], activeProfileID: nil)
        service.resumeForeground()
        await harness.waitForInvocationCount(1)

        service.sweep(force: false)
        service.sweep(force: true)
        service.sweep(force: false)
        _ = service.reachability(for: 1)
        let countBeforeCompletion = await harness.invocationCount
        XCTAssertEqual(countBeforeCompletion, 1)

        await harness.completeInvocation(at: 0, with: .unreachable)
        await harness.waitForInvocationCount(2)
        XCTAssertEqual(service.reachability(for: 1), .unreachable)
        await harness.completeInvocation(at: 1, with: .reachable)
        await waitForReachability(service, profileID: 1, expected: .reachable)
        let finalCount = await harness.invocationCount
        XCTAssertEqual(finalCount, 2)
    }

    func testReachabilityPendingNonforceSweepRespectsThrottle() async throws {
        let harness = ManualReachabilityProbeHarness()
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        let service = ProfileReachabilityService(hooks: .init(
            now: { now },
            probe: { profile, _ in await harness.probe(profile) }
        ))
        var profile = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        profile.id = 1
        service.setProfiles([profile], activeProfileID: nil)
        service.resumeForeground()
        await harness.waitForInvocationCount(1)
        service.sweep(force: false)
        _ = service.reachability(for: 1)
        await harness.completeInvocation(at: 0, with: .unreachable)
        await waitForReachability(service, profileID: 1, expected: .unreachable)
        _ = service.reachability(for: 1)
        let finalCount = await harness.invocationCount
        XCTAssertEqual(finalCount, 1)
    }

    func testReachabilityBackgroundAndStopClearPendingSweep() async throws {
        for stopInsteadOfBackground in [false, true] {
            let harness = ManualReachabilityProbeHarness()
            let service = ProfileReachabilityService(hooks: .init(
                now: { Date(timeIntervalSince1970: 1_700_000_000) },
                probe: { profile, _ in await harness.probe(profile) }
            ))
            var profile = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
            profile.id = 1
            service.setProfiles([profile], activeProfileID: nil)
            service.resumeForeground()
            await harness.waitForInvocationCount(1)
            service.sweep(force: true)
            _ = service.reachability(for: 1)
            if stopInsteadOfBackground {
                service.stop()
            } else {
                service.pauseForBackground()
            }
            await harness.completeInvocation(at: 0, with: .unreachable)
            for _ in 0 ..< 20 { await Task.yield() }
            let finalCount = await harness.invocationCount
            XCTAssertEqual(finalCount, 1)
        }
    }

    func testReachabilityProfileChangeDoesNotReplayOldPendingSweep() async throws {
        let harness = ManualReachabilityProbeHarness()
        let service = ProfileReachabilityService(hooks: .init(
            now: { Date(timeIntervalSince1970: 1_700_000_000) },
            probe: { profile, _ in await harness.probe(profile) }
        ))
        var profile = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        profile.id = 1
        service.setProfiles([profile], activeProfileID: nil)
        service.resumeForeground()
        await harness.waitForInvocationCount(1)
        service.sweep(force: true)
        _ = service.reachability(for: 1)

        profile.host = "replacement.local"
        service.setProfiles([profile], activeProfileID: nil)
        await harness.waitForInvocationCount(2)
        let replacementHost = await harness.host(at: 1)
        XCTAssertEqual(replacementHost, "replacement.local")
        await harness.completeInvocation(at: 0, with: .unreachable)
        await harness.completeInvocation(at: 1, with: .reachable)
        await waitForReachability(service, profileID: 1, expected: .reachable)
        for _ in 0 ..< 20 { await Task.yield() }
        let finalCount = await harness.invocationCount
        XCTAssertEqual(finalCount, 2)
    }

    func testWebDAVRemoteDestinationIncludesSchemeAndPaths() throws {
        var original = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        original.storageType = StorageType.webdav.rawValue
        original.connectionParams = try ServerProfileRecord.encodedConnectionParams(WebDAVConnectionParams(scheme: "https"))
        original.shareName = "/dav"

        var edited = original
        edited.credentialRef = "new-ref"
        XCTAssertTrue(original.hasSameRemoteDestination(as: edited))

        edited.connectionParams = try ServerProfileRecord.encodedConnectionParams(WebDAVConnectionParams(scheme: "http"))
        XCTAssertFalse(original.hasSameRemoteDestination(as: edited))
    }

    func testWebDAVBasePathCanonicalizationMatchesIdentityCredentialAndRequestURL() throws {
        var repeated = makeSMBProfile(basePath: "/photos//library/", credentialRef: "first", thumbnails: false)
        repeated.storageType = StorageType.webdav.rawValue
        repeated.shareName = "/dav//mount"
        repeated.connectionParams = try ServerProfileRecord.encodedConnectionParams(WebDAVConnectionParams(scheme: "https"))
        var canonical = repeated
        canonical.basePath = "/photos/library"
        canonical.shareName = "/dav/mount"

        let repeatedIdentity = try XCTUnwrap(repeated.duplicateIdentity)
        let canonicalIdentity = try XCTUnwrap(canonical.duplicateIdentity)
        XCTAssertNotEqual(repeatedIdentity, canonicalIdentity)
        XCTAssertNotEqual(repeated.remoteDestinationIdentity, canonical.remoteDestinationIdentity)
        XCTAssertNotEqual(
            StorageProfilePersistence.credentialRef(for: repeatedIdentity),
            StorageProfilePersistence.credentialRef(for: canonicalIdentity)
        )
        XCTAssertEqual(
            repeated.canonicalConnection?.canonicalComparisonKey,
            canonical.canonicalConnection?.canonicalComparisonKey
        )

        let endpoint = try XCTUnwrap(repeated.webDAVEndpointURL)
        let repeatedURL = try WebDAVClient.operationalRequestURL(endpointURL: endpoint, remotePath: repeated.basePath)
        let canonicalURL = try WebDAVClient.operationalRequestURL(endpointURL: endpoint, remotePath: canonical.basePath)
        XCTAssertEqual(repeatedURL, canonicalURL)
        XCTAssertEqual(endpoint.path, "/dav/mount")
        XCTAssertEqual(repeatedURL.path, "/dav/mount/photos/library")

        XCTAssertEqual(try WebDAVPathCanonicalizer.canonicalRawPath("/photos/%2fslot"), "/photos/%2fslot")
        var lowercaseEscape = canonical
        lowercaseEscape.basePath = "/photos/%2fslot"
        var uppercaseEscape = canonical
        uppercaseEscape.basePath = "/photos/%2Fslot"
        let lowercaseIdentity = try XCTUnwrap(lowercaseEscape.duplicateIdentity)
        let uppercaseIdentity = try XCTUnwrap(uppercaseEscape.duplicateIdentity)
        XCTAssertNotEqual(lowercaseIdentity, uppercaseIdentity)
        XCTAssertNotEqual(
            StorageProfilePersistence.credentialRef(for: lowercaseIdentity),
            StorageProfilePersistence.credentialRef(for: uppercaseIdentity)
        )
        XCTAssertNotEqual(
            try WebDAVClient.operationalRequestURL(endpointURL: endpoint, remotePath: "/photos/%2fslot"),
            try WebDAVClient.operationalRequestURL(endpointURL: endpoint, remotePath: "/photos/%2Fslot")
        )

        var splitAtMount = canonical
        splitAtMount.shareName = "/dav"
        splitAtMount.basePath = "/photos/相册/%literal"
        var splitAtBase = canonical
        splitAtBase.shareName = "/dav/photos"
        splitAtBase.basePath = "/相册/%literal/./"
        let mountIdentity = try XCTUnwrap(splitAtMount.duplicateIdentity)
        let baseIdentity = try XCTUnwrap(splitAtBase.duplicateIdentity)
        XCTAssertNotEqual(mountIdentity, baseIdentity)
        XCTAssertNotEqual(splitAtMount.remoteDestinationIdentity, splitAtBase.remoteDestinationIdentity)
        XCTAssertNotEqual(
            StorageProfilePersistence.credentialRef(for: mountIdentity),
            StorageProfilePersistence.credentialRef(for: baseIdentity)
        )
        XCTAssertEqual(
            splitAtMount.canonicalConnection?.canonicalComparisonKey,
            splitAtBase.canonicalConnection?.canonicalComparisonKey
        )
        guard case .webDAV(let mountConnection) = try StorageClientFactory.canonicalConnection(for: splitAtMount),
              case .webDAV(let baseConnection) = try StorageClientFactory.canonicalConnection(for: splitAtBase) else {
            return XCTFail("Expected WebDAV factory descriptors")
        }
        XCTAssertEqual(mountConnection.effectiveRoot, baseConnection.effectiveRoot)
        XCTAssertEqual(
            try WebDAVClient.operationalRequestURL(
                endpointURL: XCTUnwrap(splitAtMount.webDAVEndpointURL),
                remotePath: splitAtMount.basePath
            ),
            try WebDAVClient.operationalRequestURL(
                endpointURL: XCTUnwrap(splitAtBase.webDAVEndpointURL),
                remotePath: splitAtBase.basePath
            )
        )
        XCTAssertEqual(
            try WebDAVPathCanonicalizer.effectiveRootRawPath(
                mountPath: "/dav/%2F",
                basePath: "/相册"
            ),
            try WebDAVPathCanonicalizer.effectiveRootRawPath(
                mountPath: "/dav",
                basePath: "/%2F/相册"
            )
        )
    }

    func testWebDAVRawPathEncodingAndHrefRoundTrip() throws {
        let cases: [(raw: String, encoded: String)] = [
            ("/archive/%20", "/archive/%2520"),
            ("/archive/%25", "/archive/%2525"),
            ("/archive/%252F", "/archive/%25252F"),
            ("/archive/%2F", "/archive/%252F"),
            ("/archive/literal space", "/archive/literal%20space"),
            ("/archive/相册", "/archive/%E7%9B%B8%E5%86%8C"),
            ("/archive/bad%zz", "/archive/bad%25zz")
        ]
        let endpoint = try XCTUnwrap(URL(string: "https://example.test/dav"))
        for value in cases {
            XCTAssertEqual(
                try WebDAVPathCanonicalizer.percentEncodedRequestPath(fromRawPath: value.raw),
                value.encoded
            )
            XCTAssertEqual(
                try WebDAVPathCanonicalizer.rawPath(fromPercentEncodedHrefPath: value.encoded),
                value.raw
            )
            let url = try WebDAVClient.operationalRequestURL(endpointURL: endpoint, remotePath: value.raw)
            XCTAssertEqual(URLComponents(url: url, resolvingAgainstBaseURL: false)?.percentEncodedPath, "/dav" + value.encoded)
        }
    }

    func testWebDAVPathsCollapseDotSegmentsRejectParentsAndKeepComponentsDistinct() throws {
        XCTAssertEqual(
            try WebDAVPathCanonicalizer.canonicalRawPath("/photos//./library"),
            "/photos/library"
        )
        XCTAssertEqual(
            try WebDAVClient.operationalRequestURL(
                endpointURL: XCTUnwrap(URL(string: "https://example.test/dav")),
                remotePath: "/photos//./library"
            ).path,
            "/dav/photos/library"
        )
        XCTAssertThrowsError(try WebDAVPathCanonicalizer.canonicalRawPath("/photos/../library"))
        XCTAssertThrowsError(
            try WebDAVClient.operationalRequestURL(
                endpointURL: XCTUnwrap(URL(string: "https://example.test/dav")),
                remotePath: "/photos/../library"
            )
        )

        XCTAssertEqual(
            try WebDAVPathCanonicalizer.canonicalRawPath("/photos/%2e/library"),
            "/photos/%2e/library"
        )
        XCTAssertEqual(
            try WebDAVPathCanonicalizer.percentEncodedRequestPath(fromRawPath: "/photos/%2e/library"),
            "/photos/%252e/library"
        )
        XCTAssertThrowsError(
            try WebDAVPathCanonicalizer.rawPath(fromPercentEncodedHrefPath: "/photos/%2e/library")
        )

        var invalid = makeSMBProfile(basePath: "/photos/../library", credentialRef: "invalid", thumbnails: false)
        invalid.id = 7
        invalid.storageType = StorageType.webdav.rawValue
        invalid.connectionParams = try ServerProfileRecord.encodedConnectionParams(WebDAVConnectionParams(scheme: "https"))
        var valid = invalid
        valid.id = 8
        valid.basePath = "/photos/library"
        XCTAssertNil(invalid.duplicateIdentity)
        XCTAssertNotEqual(invalid.remoteDestinationIdentity, valid.remoteDestinationIdentity)

        var encodedSlash = valid
        encodedSlash.basePath = "/photos/a%2Fb"
        var separateComponents = valid
        separateComponents.basePath = "/photos/a/b"
        XCTAssertNotEqual(encodedSlash.duplicateIdentity, separateComponents.duplicateIdentity)
        XCTAssertNotEqual(encodedSlash.remoteDestinationIdentity, separateComponents.remoteDestinationIdentity)

        var canonicalMount = valid
        canonicalMount.shareName = "/dav/相册/%literal"
        var equivalentMount = canonicalMount
        equivalentMount.shareName = "//dav/./相册//%literal/"
        XCTAssertNotEqual(canonicalMount.duplicateIdentity, equivalentMount.duplicateIdentity)
        XCTAssertNotEqual(canonicalMount.remoteDestinationIdentity, equivalentMount.remoteDestinationIdentity)
        XCTAssertEqual(
            canonicalMount.canonicalConnection?.canonicalComparisonKey,
            equivalentMount.canonicalConnection?.canonicalComparisonKey
        )
        XCTAssertEqual(canonicalMount.webDAVEndpointURL, equivalentMount.webDAVEndpointURL)
        XCTAssertEqual(
            canonicalMount.webDAVEndpointURL?.absoluteString,
            "https://nas.local:445/dav/%E7%9B%B8%E5%86%8C/%25literal"
        )

        var invalidMount = valid
        invalidMount.id = 9
        invalidMount.shareName = "/dav/../other"
        XCTAssertNil(invalidMount.duplicateIdentity)
        XCTAssertNil(invalidMount.webDAVEndpointURL)
        XCTAssertNotEqual(invalidMount.remoteDestinationIdentity, valid.remoteDestinationIdentity)
    }

    func testEquivalentWebDAVBasePathEditPersistsCanonicalPathWithoutClearingState() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        var original = makeSMBProfile(basePath: "/photos//library", credentialRef: "webdav", thumbnails: false)
        original.storageType = StorageType.webdav.rawValue
        original.connectionParams = try ServerProfileRecord.encodedConnectionParams(WebDAVConnectionParams(scheme: "https"))
        try database.saveServerProfile(&original)
        let profileID = try XCTUnwrap(original.id)
        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)

        var edited = original
        edited.basePath = try WebDAVPathCanonicalizer.canonicalRawPath(original.basePath)
        try database.saveConnectionProfile(&edited, editingProfileID: profileID)

        XCTAssertEqual(try database.fetchServerProfile(id: profileID)?.basePath, "/photos/library")
        XCTAssertEqual(try database.activeServerProfileID(), profileID)
        XCTAssertNotNil(try database.remoteVerifiedAt(profileID: profileID))
    }

    func testS3RemoteDestinationIncludesSigningConfiguration() throws {
        var original = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        original.storageType = StorageType.s3.rawValue
        original.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "us-east-1", usePathStyle: true)
        )

        var edited = original
        XCTAssertTrue(original.hasSameRemoteDestination(as: edited))

        edited.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "us-east-1", usePathStyle: false)
        )
        XCTAssertFalse(original.hasSameRemoteDestination(as: edited))
    }

    func testS3InvalidLegacyEndpointFailsIdentitiesAndClientClosed() throws {
        var invalid = makeSMBProfile(basePath: "/photos", credentialRef: "legacy", thumbnails: false)
        invalid.id = 41
        invalid.storageType = StorageType.s3.rawValue
        invalid.host = "objects.example.test/path"
        invalid.port = 443
        invalid.shareName = "bucket"
        invalid.username = "access-key"
        invalid.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "us-east-1", usePathStyle: true)
        )

        var sameInvalidShape = invalid
        sameInvalidShape.id = 42
        var invalidPort = invalid
        invalidPort.id = 43
        invalidPort.host = "objects.example.test"
        invalidPort.port = 65536
        var legal = invalidPort
        legal.id = 44
        legal.port = 443

        XCTAssertNil(invalid.duplicateIdentity)
        XCTAssertNil(invalidPort.duplicateIdentity)
        XCTAssertNotEqual(invalid.remoteDestinationIdentity, sameInvalidShape.remoteDestinationIdentity)
        XCTAssertNotEqual(invalid.remoteDestinationIdentity, legal.remoteDestinationIdentity)
        XCTAssertNotEqual(invalidPort.remoteDestinationIdentity, legal.remoteDestinationIdentity)
        XCTAssertNotEqual(
            invalid.remoteDestinationIdentity.cacheKeyComponent,
            sameInvalidShape.remoteDestinationIdentity.cacheKeyComponent
        )
        XCTAssertThrowsError(try StorageClientFactory().makeClient(profile: invalid, credentialPayload: "secret"))
        XCTAssertThrowsError(try StorageClientFactory().makeClient(profile: invalidPort, credentialPayload: "secret"))
    }

    func testDuplicateIdentityMatchesEffectiveBackendRoutes() throws {
        var webDAVLegacy = makeSMBProfile(basePath: "/A", credentialRef: "webdav", thumbnails: false)
        webDAVLegacy.storageType = StorageType.webdav.rawValue
        webDAVLegacy.port = 0
        webDAVLegacy.shareName = "/dav"
        webDAVLegacy.connectionParams = try ServerProfileRecord.encodedConnectionParams(WebDAVConnectionParams(scheme: "https"))
        var webDAVExplicit = webDAVLegacy
        webDAVExplicit.port = 443
        XCTAssertEqual(webDAVLegacy.duplicateIdentity, webDAVExplicit.duplicateIdentity)

        var s3Legacy = makeSMBProfile(basePath: "/photos", credentialRef: "s3", thumbnails: false)
        s3Legacy.storageType = StorageType.s3.rawValue
        s3Legacy.host = "account.r2.cloudflarestorage.com"
        s3Legacy.port = 0
        s3Legacy.shareName = "bucket"
        s3Legacy.username = "access-key"
        s3Legacy.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "", usePathStyle: true)
        )
        var s3Resolved = s3Legacy
        s3Resolved.port = 443
        s3Resolved.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "auto", usePathStyle: true)
        )
        XCTAssertEqual(s3Legacy.duplicateIdentity, s3Resolved.duplicateIdentity)

        var customS3Default = s3Legacy
        customS3Default.host = "objects.example.test"
        customS3Default.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "", usePathStyle: true)
        )
        var customS3Explicit = customS3Default
        customS3Explicit.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "us-east-1", usePathStyle: true)
        )
        XCTAssertEqual(S3Client.effectiveSigningRegion(userInput: "", host: customS3Default.host), "us-east-1")
        XCTAssertEqual(customS3Default.duplicateIdentity, customS3Explicit.duplicateIdentity)
        XCTAssertEqual(customS3Default.remoteDestinationIdentity, customS3Explicit.remoteDestinationIdentity)

        var sftp = makeSMBProfile(basePath: "/photos", credentialRef: "sftp", thumbnails: false)
        sftp.storageType = StorageType.sftp.rawValue
        sftp.port = 0
        sftp.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "old")
        )
        var changedFingerprint = sftp
        changedFingerprint.port = 22
        changedFingerprint.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .privateKey, hostKeyFingerprintSHA256: "new")
        )
        XCTAssertEqual(sftp.duplicateIdentity, changedFingerprint.duplicateIdentity)
    }

    func testCanonicalNetworkDescriptorsPreserveV2IdentityFactoryAndDisplayContracts() throws {
        var smb = makeSMBProfile(basePath: "\\A\\.\\B\\", credentialRef: "smb", thumbnails: false)
        smb.host = "NAS.Local."
        smb.port = 0
        smb.shareName = "/Photos/"
        smb.domain = "WORKGROUP"

        var webDAV = makeSMBProfile(basePath: "/photos/%2fslot", credentialRef: "webdav", thumbnails: false)
        webDAV.storageType = StorageType.webdav.rawValue
        webDAV.host = "NAS.Local."
        webDAV.port = 0
        webDAV.shareName = "/dav"
        webDAV.connectionParams = Data(#"{"scheme":"HTTPS"}"#.utf8)

        var s3 = makeSMBProfile(basePath: "photos//./raw/", credentialRef: "s3", thumbnails: false)
        s3.storageType = StorageType.s3.rawValue
        s3.host = "account.r2.cloudflarestorage.com."
        s3.port = 0
        s3.shareName = "bucket"
        s3.username = "access"
        s3.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "", region: "", usePathStyle: true)
        )

        var sftp = makeSMBProfile(basePath: "/home/u//./photos/", credentialRef: "sftp", thumbnails: false)
        sftp.storageType = StorageType.sftp.rawValue
        sftp.host = "NAS.Local."
        sftp.port = 0
        sftp.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .privateKey, hostKeyFingerprintSHA256: "host-key")
        )

        let fixtures: [(ServerProfileRecord, [String], [String], String, String, String)] = [
            (smb,
             ["nas.local", "445", "photos", "/\\A\\.\\B\\", "alice", "workgroup"],
             ["nas.local", "445", "photos", "/\\A\\.\\B\\", "alice", "workgroup"],
             "v2|smb|ecc9a90dff138e723366c76d6416547db9a82cff92681bec21c3970117ee6a5f",
             "WyJzbWIiLCJuYXMubG9jYWwiLCI0NDUiLCJwaG90b3MiLCJcL1xcQVxcLlxcQlxcIiwiYWxpY2UiLCJ3b3JrZ3JvdXAiXQ==",
             "SMB://nas.local/Photos/A/B"),
            (webDAV,
             ["https", "nas.local", "443", "/dav", "/photos/%2fslot", "alice"],
             ["https", "nas.local", "443", "/dav", "/photos/%2fslot", "alice"],
             "v2|webdav|9db0eff36d18905a23506d8126507a7ed708c8c893b2830e2a32f8c579508314",
             "WyJ3ZWJkYXYiLCJodHRwcyIsIm5hcy5sb2NhbCIsIjQ0MyIsIlwvZGF2IiwiXC9waG90b3NcLyUyZnNsb3QiLCJhbGljZSJd",
             "https://nas.local/dav/photos/%2fslot"),
            (s3,
             ["", "account.r2.cloudflarestorage.com", "443", "auto", "path", "bucket", "/photos//./raw", "access"],
             ["", "account.r2.cloudflarestorage.com", "443", "auto", "path", "bucket", "/photos//./raw", "access"],
             "v2|s3|a0235041f8e9604aafd79c714438869c47d3b90ed10a0de71c392729d2bde305",
             "WyJzMyIsIiIsImFjY291bnQucjIuY2xvdWRmbGFyZXN0b3JhZ2UuY29tIiwiNDQzIiwiYXV0byIsInBhdGgiLCJidWNrZXQiLCJcL3Bob3Rvc1wvXC8uXC9yYXciLCJhY2Nlc3MiXQ==",
             "https://account.r2.cloudflarestorage.com/bucket/photos//./raw"),
            (sftp,
             ["nas.local", "22", "/home/u/photos", "alice"],
             ["nas.local", "22", "/home/u/photos", "alice", "host-key"],
             "v2|sftp|75e42c4cb24b29f29f64238e1d0aa02d358afe0a9dc99d11a514a3e70878378d",
             "WyJzZnRwIiwibmFzLmxvY2FsIiwiMjIiLCJcL2hvbWVcL3VcL3Bob3RvcyIsImFsaWNlIiwiaG9zdC1rZXkiXQ==",
             "sftp://alice@nas.local/home/u/photos")
        ]

        for (profile, duplicateComponents, remoteComponents, credentialRef, cacheKey, display) in fixtures {
            let descriptor = try XCTUnwrap(profile.canonicalConnection)
            XCTAssertEqual(descriptor.publishedV2IdentityComponents, duplicateComponents)
            XCTAssertEqual(descriptor.publishedV2RemoteIdentityComponents, remoteComponents)
            XCTAssertEqual(try XCTUnwrap(profile.duplicateIdentity).components, duplicateComponents)
            XCTAssertEqual(profile.remoteDestinationIdentity.components, remoteComponents)
            XCTAssertEqual(StorageProfilePersistence.credentialRef(for: try XCTUnwrap(profile.duplicateIdentity)), credentialRef)
            XCTAssertEqual(profile.remoteDestinationIdentity.cacheKeyComponent, cacheKey)
            XCTAssertEqual(try StorageClientFactory.canonicalConnection(for: profile), descriptor)
            XCTAssertEqual(profile.storageProfile.displaySubtitle, display)
            let password = profile.resolvedStorageType == .sftp
                ? try SFTPCredentialBlob.privateKey(pem: "key", passphrase: nil).encodedJSONString()
                : "secret"
            XCTAssertNoThrow(try StorageClientFactory().makeClient(profile: profile, credentialPayload: password))
        }
    }

    func testCanonicalNetworkDescriptorBoundaryMatrixFailsClosedWithoutExternalUnification() throws {
        XCTAssertEqual(HTTPTransportScheme.parse(" HTTPS "), .https)
        XCTAssertEqual(HTTPTransportScheme.parse("http"), .http)
        XCTAssertNil(HTTPTransportScheme.parse(""))
        XCTAssertNil(HTTPTransportScheme.parse("ftp"))
        XCTAssertEqual(HTTPTransportScheme.parseS3Compatible(""), .https)

        let smb = try CanonicalSMBConnection(
            host: "NAS.Local.",
            port: 0,
            shareName: "\\Photos\\",
            basePath: "/A\\.//B/",
            username: "user",
            domain: "WORKGROUP"
        )
        XCTAssertEqual(smb.shareName, "Photos")
        XCTAssertEqual(smb.basePath, "/A/B")
        XCTAssertThrowsError(try CanonicalSMBConnection(
            host: "nas.local",
            port: 445,
            shareName: "..",
            basePath: "/",
            username: "user",
            domain: nil
        ))

        for scheme in ["", "ftp", "webdav"] {
            XCTAssertThrowsError(try CanonicalWebDAVConnection(
                scheme: scheme,
                host: "nas.local",
                port: 0,
                mountPath: "/dav/%2fslot",
                basePath: "/photos",
                username: "user"
            ))
        }
        let webDAV = try CanonicalWebDAVConnection(
            scheme: "HtTpS",
            host: "[fe80::1%25en0]",
            port: 0,
            mountPath: "/dav/%2fslot",
            basePath: "/photos/./raw",
            username: "user"
        )
        XCTAssertEqual(webDAV.port.value, 443)
        XCTAssertEqual(webDAV.effectiveRoot, "/dav/%2fslot/photos/raw")
        XCTAssertThrowsError(try CanonicalWebDAVConnection(
            scheme: "https",
            host: "nas.local",
            port: 443,
            mountPath: "/dav/../root",
            basePath: "/photos",
            username: "user"
        ))

        let s3 = try CanonicalS3Connection(
            scheme: "",
            host: "objects.example.com",
            port: 0,
            region: "",
            usePathStyle: true,
            bucket: "bucket",
            basePath: "a//./b",
            accessKeyID: "access"
        )
        XCTAssertEqual(s3.endpoint.scheme, .https)
        XCTAssertEqual(s3.endpoint.port.value, 443)
        XCTAssertEqual(s3.basePrefix, "/a//./b")
        XCTAssertEqual(s3.effectiveSigningRegion, "us-east-1")

        let sftp = try CanonicalSFTPConnection(
            host: "[2001:0db8::1]",
            port: 0,
            basePath: "/home/u//./photos",
            username: "user",
            authMethod: .privateKey,
            hostKeyFingerprintSHA256: "fingerprint"
        )
        XCTAssertEqual(sftp.host.socketHost, "2001:db8::1")
        XCTAssertEqual(sftp.port.value, 22)
        XCTAssertEqual(sftp.basePath, "/home/u/photos")

        var invalidWebDAV = makeSMBProfile(basePath: "/photos", credentialRef: "invalid", thumbnails: false)
        invalidWebDAV.storageType = StorageType.webdav.rawValue
        invalidWebDAV.connectionParams = Data(#"{"scheme":"ftp"}"#.utf8)
        XCTAssertNil(invalidWebDAV.canonicalConnection)
        XCTAssertNil(invalidWebDAV.duplicateIdentity)
        XCTAssertThrowsError(try StorageClientFactory().makeClient(profile: invalidWebDAV, credentialPayload: "secret"))
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonCanonicalCommitTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        XCTAssertThrowsError(try database.saveConnectionProfile(&invalidWebDAV, editingProfileID: nil))

        var external = invalidWebDAV
        external.storageType = StorageType.externalVolume.rawValue
        external.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(rootBookmarkData: Data([1]), displayPath: "/Volumes/Photos")
        )
        XCTAssertNil(external.canonicalConnection)
        XCTAssertNil(external.duplicateIdentity)
    }

    func testDatabaseRejectsCanonicalDuplicatesForStructuredNetworkBackends() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        var webDAV = makeSMBProfile(basePath: "/A//library/", credentialRef: "webdav-a", thumbnails: false)
        webDAV.storageType = StorageType.webdav.rawValue
        webDAV.port = 0
        webDAV.connectionParams = try ServerProfileRecord.encodedConnectionParams(WebDAVConnectionParams(scheme: "https"))
        try database.saveConnectionProfile(&webDAV, editingProfileID: nil)
        var webDAVDuplicate = webDAV
        webDAVDuplicate.id = nil
        webDAVDuplicate.port = 443
        webDAVDuplicate.basePath = "/A/library"
        webDAVDuplicate.credentialRef = "webdav-b"
        XCTAssertThrowsError(try database.saveConnectionProfile(&webDAVDuplicate, editingProfileID: nil))

        var s3 = makeSMBProfile(basePath: "/photos", credentialRef: "s3-a", thumbnails: false)
        s3.storageType = StorageType.s3.rawValue
        s3.host = "objects.example.test"
        s3.port = 0
        s3.shareName = "bucket"
        s3.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "", usePathStyle: true)
        )
        try database.saveConnectionProfile(&s3, editingProfileID: nil)
        var s3Duplicate = s3
        s3Duplicate.id = nil
        s3Duplicate.port = 443
        s3Duplicate.credentialRef = "s3-b"
        s3Duplicate.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "us-east-1", usePathStyle: true)
        )
        XCTAssertThrowsError(try database.saveConnectionProfile(&s3Duplicate, editingProfileID: nil))

        var sftp = makeSMBProfile(basePath: "/home/u//./photos", credentialRef: "sftp-a", thumbnails: false)
        sftp.storageType = StorageType.sftp.rawValue
        sftp.port = 0
        sftp.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "old")
        )
        try database.saveConnectionProfile(&sftp, editingProfileID: nil)
        var sftpDuplicate = sftp
        sftpDuplicate.id = nil
        sftpDuplicate.port = 22
        sftpDuplicate.basePath = "/home/u/photos"
        sftpDuplicate.credentialRef = "sftp-b"
        sftpDuplicate.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .privateKey, hostKeyFingerprintSHA256: "new")
        )
        XCTAssertThrowsError(try database.saveConnectionProfile(&sftpDuplicate, editingProfileID: nil))

    }

    func testSFTPPathCanonicalizerUsesPOSIXRawSemantics() throws {
        XCTAssertEqual(try SFTPPathCanonicalizer.canonicalRawPath("/"), "/")
        XCTAssertEqual(try SFTPPathCanonicalizer.canonicalRawPath("///./"), "/")
        XCTAssertEqual(try SFTPPathCanonicalizer.canonicalRawPath("/home/u//photos/"), "/home/u/photos")
        XCTAssertEqual(try SFTPPathCanonicalizer.canonicalRawPath("/home/u/./photos"), "/home/u/photos")
        XCTAssertEqual(try SFTPPathCanonicalizer.canonicalRawPath("/家/%2F/相册"), "/家/%2F/相册")
        XCTAssertNotEqual(
            try SFTPPathCanonicalizer.canonicalRawPath("/home/u/photos"),
            try SFTPPathCanonicalizer.canonicalRawPath("/home/user/photos")
        )
        for invalid in ["/..", "/home/../photos", "/home/u/../../photos"] {
            XCTAssertThrowsError(try SFTPPathCanonicalizer.canonicalRawPath(invalid))
            XCTAssertThrowsError(try SFTPClient.operationalPath(invalid))
        }
    }

    func testSFTPEventLoopTaskExecutorKeepsAsyncWorkOnBoundEventLoop() async {
        let eventLoop = MultiThreadedEventLoopGroup.singleton.next()
        let executor = SFTPEventLoopTaskExecutor(eventLoop: eventLoop)
        let ranOnEventLoop = await Task(executorPreference: executor) {
            await Task.yield()
            return eventLoop.inEventLoop
        }.value

        XCTAssertTrue(ranOnEventLoop)
    }

    func testSFTPConnectAttachesCitadelHandlersOnChannelEventLoop() async throws {
        let server = try await ServerBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .childChannelInitializer { channel in channel.close() }
            .bind(host: "127.0.0.1", port: 0)
            .get()
        defer { server.close(promise: nil) }
        let port = try XCTUnwrap(server.localAddress?.port)
        let client = SFTPClient(config: .init(
            host: "127.0.0.1",
            port: port,
            username: "user",
            credential: .password("secret"),
            expectedHostKeyFingerprintSHA256: "host-key"
        ))

        do {
            try await client.connect()
            XCTFail("Expected the test server to close the connection")
        } catch {
            await client.disconnect()
        }
    }

    func testSFTPLegacyEquivalentPathsShareIdentityCredentialDestinationAndCache() throws {
        var repeated = makeSMBProfile(basePath: "/home/u//photos", credentialRef: "legacy", thumbnails: false)
        repeated.id = 7
        repeated.storageType = StorageType.sftp.rawValue
        repeated.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "host-key")
        )
        var dotted = repeated
        dotted.basePath = "/home/u/./photos"
        var canonical = repeated
        canonical.basePath = "/home/u/photos"

        let repeatedIdentity = try XCTUnwrap(repeated.duplicateIdentity)
        let dottedIdentity = try XCTUnwrap(dotted.duplicateIdentity)
        let canonicalIdentity = try XCTUnwrap(canonical.duplicateIdentity)
        XCTAssertEqual(repeatedIdentity, dottedIdentity)
        XCTAssertEqual(dottedIdentity, canonicalIdentity)
        XCTAssertEqual(repeated.remoteDestinationIdentity, dotted.remoteDestinationIdentity)
        XCTAssertEqual(dotted.remoteDestinationIdentity, canonical.remoteDestinationIdentity)
        XCTAssertEqual(
            StorageProfilePersistence.credentialRef(for: repeatedIdentity),
            StorageProfilePersistence.credentialRef(for: canonicalIdentity)
        )
        XCTAssertEqual(
            RemoteIndexSyncService.remoteProfileKey(repeated),
            RemoteIndexSyncService.remoteProfileKey(canonical)
        )
        XCTAssertEqual(repeated.sftpDisplayURLString, canonical.sftpDisplayURLString)
        XCTAssertEqual(try SFTPClient.operationalPath(repeated.basePath), canonical.basePath)
    }

    func testSFTPInvalidLegacyParentPathFailsClosed() throws {
        var invalid = makeSMBProfile(basePath: "/home/u/../photos", credentialRef: "legacy", thumbnails: false)
        invalid.id = 7
        invalid.storageType = StorageType.sftp.rawValue
        invalid.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "host-key")
        )
        var legal = invalid
        legal.id = 8
        legal.basePath = "/home/photos"

        XCTAssertNil(invalid.duplicateIdentity)
        XCTAssertNil(invalid.sftpDisplayURLString)
        XCTAssertNotEqual(invalid.remoteDestinationIdentity, legal.remoteDestinationIdentity)
        XCTAssertThrowsError(try SFTPClient.operationalPath(invalid.basePath))
        let password = try SFTPCredentialBlob.password("secret").encodedJSONString()
        XCTAssertThrowsError(try StorageClientFactory().makeClient(profile: invalid, credentialPayload: password))
    }

    func testSFTPRemoteDestinationIgnoresCredentialModeButIncludesHostKey() throws {
        var original = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        original.storageType = StorageType.sftp.rawValue
        original.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "old")
        )

        var edited = original
        edited.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .privateKey, hostKeyFingerprintSHA256: "old")
        )
        XCTAssertTrue(original.hasSameRemoteDestination(as: edited))

        edited.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .privateKey, hostKeyFingerprintSHA256: "new")
        )
        XCTAssertFalse(original.hasSameRemoteDestination(as: edited))

        edited.connectionParams = original.connectionParams
        edited.port = 2222
        XCTAssertFalse(original.hasSameRemoteDestination(as: edited))
    }

    func testSFTPLegacyDefaultPortUsesChangedKeyWarningPolicy() {
        var legacy = makeSMBProfile(basePath: "/photos", credentialRef: "legacy", thumbnails: false)
        legacy.storageType = StorageType.sftp.rawValue
        legacy.host = "NAS.Local."
        legacy.port = 0
        legacy.connectionParams = try? ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "old-key")
        )
        XCTAssertEqual(
            SFTPHostKeyPromptPolicy.retainedFingerprint(
                existingProfile: legacy,
                proposedHost: "nas.local",
                proposedPort: 22
            ),
            "old-key"
        )
        XCTAssertEqual(
            SFTPHostKeyPromptPolicy.retainedFingerprint(
                existingProfile: legacy,
                proposedHost: "nas.local",
                proposedPort: 2222
            ),
            ""
        )
        XCTAssertEqual(
            SFTPHostKeyPromptPolicy.retainedFingerprint(
                existingProfile: legacy,
                proposedHost: "other.local",
                proposedPort: 22
            ),
            ""
        )
        XCTAssertEqual(
            SFTPHostKeyPromptPolicy.decision(
                existingHost: "NAS.Local.",
                existingPort: 0,
                expectedFingerprint: "old-key",
                proposedHost: "nas.local",
                proposedPort: 22,
                actualFingerprint: "new-key"
            ),
            .changedKey(expected: "old-key")
        )
        XCTAssertEqual(
            SFTPHostKeyPromptPolicy.decision(
                existingHost: "nas.local",
                existingPort: 0,
                expectedFingerprint: "old-key",
                proposedHost: "nas.local",
                proposedPort: 2222,
                actualFingerprint: "new-key"
            ),
            .firstTrust
        )
        XCTAssertEqual(
            SFTPHostKeyPromptPolicy.decision(
                existingHost: "nas.local",
                existingPort: 0,
                expectedFingerprint: "same-key",
                proposedHost: "nas.local",
                proposedPort: 22,
                actualFingerprint: "same-key"
            ),
            .none
        )
    }

    func testSFTPHostKeySavePolicyUsesBoundTestThenLiveFingerprint() throws {
        var live = makeSMBProfile(basePath: "/photos", credentialRef: "sftp", thumbnails: false)
        live.storageType = StorageType.sftp.rawValue
        live.host = "NAS.Local."
        live.port = 0
        live.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "live-key")
        )

        XCTAssertEqual(
            SFTPHostKeyPromptPolicy.fingerprintForSave(
                liveProfile: live,
                proposedHost: "nas.local",
                proposedPort: 22,
                testedHost: nil,
                testedPort: nil,
                testedFingerprint: nil
            ),
            "live-key"
        )
        XCTAssertEqual(
            SFTPHostKeyPromptPolicy.fingerprintForSave(
                liveProfile: live,
                proposedHost: "nas.local",
                proposedPort: 22,
                testedHost: "NAS.LOCAL",
                testedPort: 22,
                testedFingerprint: "tested-key"
            ),
            "tested-key"
        )
        XCTAssertEqual(
            SFTPHostKeyPromptPolicy.fingerprintForSave(
                liveProfile: live,
                proposedHost: "nas.local",
                proposedPort: 22,
                testedHost: "nas.local",
                testedPort: 2222,
                testedFingerprint: "wrong-endpoint-key"
            ),
            "live-key"
        )
        XCTAssertEqual(
            SFTPHostKeyPromptPolicy.fingerprintForSave(
                liveProfile: live,
                proposedHost: "nas.local",
                proposedPort: 2222,
                testedHost: nil,
                testedPort: nil,
                testedFingerprint: nil
            ),
            ""
        )
    }

    func testLiveEditingProfileRefreshesSnapshotAndFailsAfterDeletion() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        var snapshot = makeSMBProfile(basePath: "/photos", credentialRef: "sftp", thumbnails: false)
        snapshot.storageType = StorageType.sftp.rawValue
        snapshot.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "old-key")
        )
        try database.saveServerProfile(&snapshot)
        let profileID = try XCTUnwrap(snapshot.id)

        try database.setServerProfileName("Live Name", profileID: profileID)
        var updated = snapshot
        updated.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "new-key")
        )
        try database.saveConnectionProfile(&updated, editingProfileID: profileID)

        let live = try XCTUnwrap(StorageProfilePersistence.liveEditingProfile(
            databaseManager: database,
            snapshot: snapshot
        ))
        XCTAssertEqual(live.name, "Live Name")
        XCTAssertEqual(live.sftpParams?.hostKeyFingerprintSHA256, "new-key")

        try database.deleteServerProfile(id: profileID)
        XCTAssertThrowsError(try StorageProfilePersistence.liveEditingProfile(
            databaseManager: database,
            snapshot: snapshot
        ))
    }

    func testSFTPConnectionPreparationPersistsAcceptedHostKey() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        var profile = makeSMBProfile(basePath: "/photos", credentialRef: "sftp", thumbnails: false)
        profile.storageType = StorageType.sftp.rawValue
        profile.port = 22
        profile.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "")
        )
        try database.saveConnectionProfile(&profile, editingProfileID: nil)
        let profileID = try XCTUnwrap(profile.id)
        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)

        let service = StorageProfileConnectionService(
            databaseManager: database,
            hooks: .init(captureSFTPHostKey: { host, port in
                XCTAssertEqual(host, "nas.local")
                XCTAssertEqual(port, 22)
                return "new-key"
            })
        )
        let prepared = try await service.prepareForConnection(profile: profile) { decision, actual in
            XCTAssertEqual(decision, .firstTrust)
            XCTAssertEqual(actual, "new-key")
            try? database.setServerProfileName("Renamed During Prompt", profileID: profileID)
            return true
        }

        XCTAssertEqual(prepared.sftpParams?.hostKeyFingerprintSHA256, "new-key")
        XCTAssertEqual(prepared.name, "Renamed During Prompt")
        XCTAssertEqual(
            try database.fetchServerProfile(id: profileID)?.sftpParams?.hostKeyFingerprintSHA256,
            "new-key"
        )
        XCTAssertNil(try database.activeServerProfileID())
        XCTAssertNil(try database.remoteVerifiedAt(profileID: profileID))

        var changedConnection = prepared
        changedConnection.basePath = "/other"
        try database.saveConnectionProfile(&changedConnection, editingProfileID: profileID)
        XCTAssertThrowsError(try database.updateSFTPHostKeyFingerprint(
            profileID: profileID,
            expected: prepared,
            fingerprint: "stale-overwrite"
        ))
        let live = try XCTUnwrap(database.fetchServerProfile(id: profileID))
        XCTAssertEqual(live.basePath, "/other")
        XCTAssertEqual(live.sftpParams?.hostKeyFingerprintSHA256, "new-key")
    }

    func testSFTPHostKeyFingerprintUpdateRejectsStaleExpectedPinWithoutStateChurn() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        var original = makeSMBProfile(basePath: "/photos", credentialRef: "sftp-ref", thumbnails: false)
        original.storageType = StorageType.sftp.rawValue
        original.port = 22
        original.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "old-key")
        )
        try database.saveConnectionProfile(&original, editingProfileID: nil)
        let profileID = try XCTUnwrap(original.id)

        let newer = try database.updateSFTPHostKeyFingerprint(
            profileID: profileID,
            expected: original,
            fingerprint: "newer-key"
        )
        XCTAssertEqual(newer.sftpParams?.hostKeyFingerprintSHA256, "newer-key")
        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastCompletedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastRanAt(Date(), profileID: profileID)

        XCTAssertThrowsError(try database.updateSFTPHostKeyFingerprint(
            profileID: profileID,
            expected: original,
            fingerprint: "stale-key"
        ))

        XCTAssertEqual(
            try database.fetchServerProfile(id: profileID)?.sftpParams?.hostKeyFingerprintSHA256,
            "newer-key"
        )
        XCTAssertEqual(try database.activeServerProfileID(), profileID)
        XCTAssertNotNil(try database.remoteVerifiedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastCompletedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastRanAt(profileID: profileID))
    }

    func testSFTPEffectivePortIsSharedByIdentityClientReachabilityAndDisplay() throws {
        var legacy = makeSMBProfile(basePath: "/photos", credentialRef: "legacy", thumbnails: false)
        legacy.storageType = StorageType.sftp.rawValue
        legacy.host = "NAS.Local."
        legacy.port = 0
        legacy.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "host-key")
        )
        var explicit = legacy
        explicit.host = "nas.local"
        explicit.port = 22

        let legacyIdentity = try XCTUnwrap(legacy.duplicateIdentity)
        let explicitIdentity = try XCTUnwrap(explicit.duplicateIdentity)
        XCTAssertEqual(SFTPEndpoint.effectivePort(legacy.port), SFTPEndpoint.defaultPort)
        XCTAssertEqual(legacyIdentity, explicitIdentity)
        XCTAssertEqual(legacy.remoteDestinationIdentity, explicit.remoteDestinationIdentity)
        XCTAssertEqual(
            StorageProfilePersistence.credentialRef(for: legacyIdentity),
            StorageProfilePersistence.credentialRef(for: explicitIdentity)
        )
        XCTAssertEqual(legacy.sftpDisplayURLString, explicit.sftpDisplayURLString)

        let legacySignature = ProfileReachabilityService.probeSignature(of: legacy)
        let explicitSignature = ProfileReachabilityService.probeSignature(of: explicit)
        XCTAssertEqual(legacySignature, explicitSignature)
        XCTAssertEqual(legacySignature.port, SFTPEndpoint.defaultPort)

        let credential = SFTPCredentialBlob.password("secret")
        let legacyConfig = SFTPClient.Config(
            host: legacy.host,
            port: legacy.port,
            username: legacy.username,
            credential: credential,
            expectedHostKeyFingerprintSHA256: "host-key"
        )
        let explicitConfig = SFTPClient.Config(
            host: explicit.host,
            port: explicit.port,
            username: explicit.username,
            credential: credential,
            expectedHostKeyFingerprintSHA256: "host-key"
        )
        XCTAssertEqual(legacyConfig.effectivePort, explicitConfig.effectivePort)
    }

    func testSFTPLegacyDefaultPortEditCanonicalizesWithoutClearingState() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        var legacy = makeSMBProfile(basePath: "/home/u//./photos", credentialRef: "", thumbnails: false)
        legacy.storageType = StorageType.sftp.rawValue
        legacy.port = 0
        legacy.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "host-key")
        )
        legacy.credentialRef = StorageProfilePersistence.credentialRef(for: try XCTUnwrap(legacy.duplicateIdentity))
        try database.saveServerProfile(&legacy)
        let profileID = try XCTUnwrap(legacy.id)
        let cacheKey = RemoteIndexSyncService.remoteProfileKey(legacy)
        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastCompletedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastRanAt(Date(), profileID: profileID)

        var edited = legacy
        edited.port = SFTPEndpoint.defaultPort
        edited.basePath = try SFTPPathCanonicalizer.canonicalRawPath(legacy.basePath)
        edited.credentialRef = StorageProfilePersistence.credentialRef(for: try XCTUnwrap(edited.duplicateIdentity))
        XCTAssertEqual(legacy.credentialRef, edited.credentialRef)
        XCTAssertTrue(legacy.hasSameRemoteDestination(as: edited))
        XCTAssertEqual(RemoteIndexSyncService.remoteProfileKey(edited), cacheKey)
        try database.saveConnectionProfile(&edited, editingProfileID: profileID)

        XCTAssertEqual(try database.fetchServerProfile(id: profileID)?.port, SFTPEndpoint.defaultPort)
        XCTAssertEqual(try database.fetchServerProfile(id: profileID)?.basePath, "/home/u/photos")
        XCTAssertEqual(try database.activeServerProfileID(), profileID)
        XCTAssertNotNil(try database.remoteVerifiedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastCompletedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastRanAt(profileID: profileID))
    }

    func testExternalRemoteDestinationUsesStableLocationToken() throws {
        var original = makeSMBProfile(basePath: "/", credentialRef: "ref", thumbnails: false)
        original.storageType = StorageType.externalVolume.rawValue
        original.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(rootBookmarkData: Data([1]), displayPath: "/Volumes/Photos")
        )

        var edited = original
        edited.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(rootBookmarkData: Data([2]), displayPath: "/Volumes/Photos")
        )
        XCTAssertTrue(original.hasSameRemoteDestination(as: edited))

        edited.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(rootBookmarkData: Data([2]), displayPath: "/Volumes/Archive")
        )
        XCTAssertTrue(original.hasSameRemoteDestination(as: edited))

        edited.shareName = "external-new-location"
        XCTAssertFalse(original.hasSameRemoteDestination(as: edited))
    }

    func testExternalSaveRevalidationTracksCompleteRelevantProfilesOnly() throws {
        var external = makeSMBProfile(basePath: "/", credentialRef: "external", thumbnails: false)
        external.id = 1
        external.storageType = StorageType.externalVolume.rawValue
        external.shareName = "external-location"
        external.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(rootBookmarkData: Data([1]), displayPath: "/Volumes/Photos")
        )
        var unrelated = makeSMBProfile(basePath: "/photos", credentialRef: "smb", thumbnails: false)
        unrelated.id = 2
        let snapshot = ExternalStorageProfileSaveWorker.RelevantSnapshot(
            allProfiles: [external, unrelated],
            editingProfileID: external.id
        )

        unrelated.updatedAt = Date(timeIntervalSince1970: 123)
        XCTAssertTrue(snapshot.matches([external, unrelated], editingProfileID: external.id))

        var settingsOnlyChange = external
        settingsOnlyChange.name = "Renamed Concurrently"
        settingsOnlyChange.sortOrder += 1
        settingsOnlyChange.backgroundBackupEnabled.toggle()
        settingsOnlyChange.generateRemoteThumbnails.toggle()
        settingsOnlyChange.updatedAt = Date(timeIntervalSince1970: 456)
        settingsOnlyChange.credentialRef = "concurrent-credential-refresh"
        XCTAssertTrue(snapshot.matches([settingsOnlyChange, unrelated], editingProfileID: external.id))

        var changedConnection = external
        changedConnection.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(rootBookmarkData: Data([2]), displayPath: "/Volumes/Archive")
        )
        XCTAssertFalse(snapshot.matches([changedConnection, unrelated], editingProfileID: external.id))

        var changedToken = external
        changedToken.shareName = "external-new-token"
        XCTAssertFalse(snapshot.matches([changedToken, unrelated], editingProfileID: external.id))

        var newExternal = external
        newExternal.id = 3
        XCTAssertFalse(snapshot.matches([external, unrelated, newExternal], editingProfileID: external.id))
        XCTAssertFalse(snapshot.matches([unrelated], editingProfileID: external.id))

        var changedType = external
        changedType.storageType = StorageType.smb.rawValue
        XCTAssertFalse(snapshot.matches([changedType, unrelated], editingProfileID: external.id))
    }

    func testExternalSaveWithoutRepickPreservesLiveTokenAndOpaqueBookmark() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        let opaqueBookmark = Data("not-a-resolvable-bookmark".utf8)
        var profile = makeSMBProfile(basePath: "/", credentialRef: "external-ref", thumbnails: false)
        profile.storageType = StorageType.externalVolume.rawValue
        profile.shareName = "external-live-token"
        profile.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(
                rootBookmarkData: opaqueBookmark,
                displayPath: "/Volumes/Unavailable"
            )
        )
        try database.saveServerProfile(&profile)

        let saved = try ExternalStorageProfileSaveWorker.save(
            intent: .init(editingProfile: profile, selectedDirectoryURL: nil, name: "Ignored Edit Name"),
            databaseManager: database,
            runtimeFlags: AppRuntimeFlags()
        )
        XCTAssertEqual(saved.shareName, "external-live-token")
        XCTAssertEqual(saved.externalVolumeParams?.rootBookmarkData, opaqueBookmark)
        XCTAssertEqual(saved.externalVolumeParams?.displayPath, "/Volumes/Unavailable")
        XCTAssertEqual(saved.name, profile.name)
    }

    func testExternalCreateUsesDisplayPathFallbackForUnresolvableExistingBookmark() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        let selected = directory.appendingPathComponent("selected", isDirectory: true)
        let different = directory.appendingPathComponent("different", isDirectory: true)
        try FileManager.default.createDirectory(at: selected, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: different, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        var existing = makeSMBProfile(basePath: "/", credentialRef: "external-ref", thumbnails: false)
        existing.storageType = StorageType.externalVolume.rawValue
        existing.shareName = "external-existing"
        existing.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(
                rootBookmarkData: Data("unresolvable-bookmark".utf8),
                displayPath: selected.path
            )
        )
        try database.saveConnectionProfile(&existing, editingProfileID: nil)

        XCTAssertThrowsError(try ExternalStorageProfileSaveWorker.save(
            intent: .init(editingProfile: nil, selectedDirectoryURL: selected, name: "Duplicate"),
            databaseManager: database,
            runtimeFlags: AppRuntimeFlags()
        ))
        XCTAssertEqual(try database.fetchServerProfiles().count, 1)

        let saved = try ExternalStorageProfileSaveWorker.save(
            intent: .init(editingProfile: nil, selectedDirectoryURL: different, name: "Different"),
            databaseManager: database,
            runtimeFlags: AppRuntimeFlags()
        )
        XCTAssertEqual(saved.externalVolumeParams?.displayPath, different.path)
        XCTAssertEqual(try database.fetchServerProfiles().count, 2)
    }

    func testExternalLocationIdentityIsEphemeralAndNeverPersisted() throws {
        let first = SecurityScopedBookmarkStore.ephemeralLocationIdentities(
            volumeIdentifier: Data([1, 2]),
            fileResourceIdentifier: Data([3, 4]),
            standardizedURL: URL(fileURLWithPath: "/Volumes/Photos")
        )
        let same = SecurityScopedBookmarkStore.ephemeralLocationIdentities(
            volumeIdentifier: Data([1, 2]),
            fileResourceIdentifier: Data([3, 4]),
            standardizedURL: URL(fileURLWithPath: "/Volumes/Root/../Photos")
        )
        let differentFileAtSamePath = SecurityScopedBookmarkStore.ephemeralLocationIdentities(
            volumeIdentifier: Data([1, 2]),
            fileResourceIdentifier: Data([3, 5]),
            standardizedURL: URL(fileURLWithPath: "/Volumes/Photos")
        )
        let differentPath = SecurityScopedBookmarkStore.ephemeralLocationIdentities(
            volumeIdentifier: Data([1, 2]),
            fileResourceIdentifier: Data([3, 4]),
            standardizedURL: URL(fileURLWithPath: "/Volumes/Archive")
        )
        let replacementAtSamePath = SecurityScopedBookmarkStore.ephemeralLocationIdentities(
            volumeIdentifier: Data([9, 9]),
            fileResourceIdentifier: Data([3, 4]),
            standardizedURL: URL(fileURLWithPath: "/Volumes/Photos")
        )
        let partialAtSamePath = SecurityScopedBookmarkStore.ephemeralLocationIdentities(
            volumeIdentifier: Data([1, 2]),
            fileResourceIdentifier: nil,
            standardizedURL: URL(fileURLWithPath: "/Volumes/Photos")
        )
        let fileWithoutVolume = SecurityScopedBookmarkStore.ephemeralLocationIdentities(
            volumeIdentifier: nil,
            fileResourceIdentifier: Data([3, 4]),
            standardizedURL: URL(fileURLWithPath: "/Volumes/Photos")
        )
        let noResourceIdentity = SecurityScopedBookmarkStore.ephemeralLocationIdentities(
            volumeIdentifier: nil,
            fileResourceIdentifier: nil,
            standardizedURL: URL(fileURLWithPath: "/Volumes/Photos")
        )
        let fullUsingPathShapedFileID = SecurityScopedBookmarkStore.ephemeralLocationIdentities(
            volumeIdentifier: Data([1, 2]),
            fileResourceIdentifier: "/Volumes/Photos",
            standardizedURL: URL(fileURLWithPath: "/Volumes/Other")
        )
        XCTAssertEqual(first.fullIdentity, same.fullIdentity)
        XCTAssertEqual(first.volumePathIdentity, same.volumePathIdentity)
        XCTAssertNotEqual(first.fullIdentity, differentFileAtSamePath.fullIdentity)
        XCTAssertEqual(first.volumePathIdentity, differentFileAtSamePath.volumePathIdentity)
        XCTAssertEqual(first.fullIdentity, differentPath.fullIdentity)
        XCTAssertNotEqual(first.volumePathIdentity, differentPath.volumePathIdentity)
        XCTAssertNotEqual(first.fullIdentity, replacementAtSamePath.fullIdentity)
        XCTAssertNotEqual(first.volumePathIdentity, replacementAtSamePath.volumePathIdentity)
        XCTAssertNil(partialAtSamePath.fullIdentity)
        XCTAssertEqual(first.volumePathIdentity, partialAtSamePath.volumePathIdentity)
        XCTAssertNil(fileWithoutVolume.fullIdentity)
        XCTAssertNil(fileWithoutVolume.volumePathIdentity)
        XCTAssertNil(noResourceIdentity.fullIdentity)
        XCTAssertNil(noResourceIdentity.volumePathIdentity)
        XCTAssertNotEqual(first.volumePathIdentity, fullUsingPathShapedFileID.fullIdentity)

        let legacyJSON = try JSONSerialization.data(withJSONObject: [
            "rootBookmarkData": Data([1]).base64EncodedString(),
            "displayPath": "/Volumes/Photos",
            "locationIdentity": "previous-unreleased-value"
        ])
        let legacy = try JSONDecoder().decode(ExternalVolumeConnectionParams.self, from: legacyJSON)
        XCTAssertEqual(legacy.displayPath, "/Volumes/Photos")
        let reencoded = try ServerProfileRecord.encodedConnectionParams(legacy)
        let object = try XCTUnwrap(JSONSerialization.jsonObject(with: reencoded) as? [String: Any])
        XCTAssertNil(object["locationIdentity"])

        var profile = makeSMBProfile(basePath: "/", credentialRef: "external", thumbnails: false)
        profile.storageType = StorageType.externalVolume.rawValue
        profile.connectionParams = reencoded
        XCTAssertNil(profile.duplicateIdentity)
    }

    func testExternalRepickUsesCurrentEphemeralIdentityThenResolvedURLFallback() {
        func location(full: String?, weak: String?, path: String = "/Volumes/Photos") -> ExternalVolumeCurrentLocation {
            ExternalVolumeCurrentLocation(
                fullIdentity: full.map { Data($0.utf8) },
                volumePathIdentity: weak.map { Data($0.utf8) },
                standardizedURL: URL(fileURLWithPath: path)
            )
        }

        let original = ExternalVolumeCurrentLocation(
            fullIdentity: Data("full-a".utf8),
            volumePathIdentity: Data("weak-a".utf8),
            standardizedURL: URL(fileURLWithPath: "/Volumes/Photos")
        )
        let sameStrongDifferentWeak = location(full: "full-a", weak: "weak-b", path: "/Volumes/Renamed")
        let differentStrongSameWeak = location(full: "full-b", weak: "weak-a")
        let partialSameWeak = location(full: nil, weak: "weak-a")
        let anotherPartialSameWeak = location(full: nil, weak: "weak-a")
        let differentWeak = location(full: nil, weak: "weak-b")
        let noIdentityAtSameURL = location(full: nil, weak: nil)
        let noIdentity = location(full: nil, weak: nil, path: "/Volumes/Other")

        XCTAssertTrue(ExternalVolumeLocationPolicy.representsPotentialDuplicate(original, sameStrongDifferentWeak))
        XCTAssertTrue(ExternalVolumeLocationPolicy.canReuseRemoteState(original, sameStrongDifferentWeak))
        XCTAssertTrue(ExternalVolumeLocationPolicy.representsPotentialDuplicate(original, differentStrongSameWeak))
        XCTAssertTrue(ExternalVolumeLocationPolicy.canReuseRemoteState(original, differentStrongSameWeak))
        XCTAssertTrue(ExternalVolumeLocationPolicy.representsPotentialDuplicate(original, partialSameWeak))
        XCTAssertTrue(ExternalVolumeLocationPolicy.canReuseRemoteState(original, partialSameWeak))
        XCTAssertTrue(ExternalVolumeLocationPolicy.representsPotentialDuplicate(partialSameWeak, anotherPartialSameWeak))
        XCTAssertTrue(ExternalVolumeLocationPolicy.canReuseRemoteState(partialSameWeak, anotherPartialSameWeak))
        XCTAssertFalse(ExternalVolumeLocationPolicy.representsPotentialDuplicate(original, differentWeak))
        XCTAssertTrue(ExternalVolumeLocationPolicy.representsPotentialDuplicate(original, noIdentityAtSameURL))
        XCTAssertTrue(ExternalVolumeLocationPolicy.canReuseRemoteState(original, noIdentityAtSameURL))
        XCTAssertFalse(ExternalVolumeLocationPolicy.representsPotentialDuplicate(original, noIdentity))
        XCTAssertTrue(ExternalVolumeLocationPolicy.containsDuplicate(
            candidate: original,
            existingLocations: [differentWeak, differentStrongSameWeak]
        ))
        XCTAssertFalse(ExternalVolumeLocationPolicy.containsDuplicate(
            candidate: original,
            existingLocations: [differentWeak, noIdentity]
        ))
        XCTAssertFalse(ExternalVolumeLocationPolicy.containsDuplicate(
            candidate: noIdentity,
            existingLocations: [original, anotherPartialSameWeak]
        ))
        XCTAssertEqual(ExternalVolumeLocationPolicy.locationToken(
            existingToken: "external-a",
            selectedNewLocation: true,
            existingLocation: sameStrongDifferentWeak,
            candidateLocation: original,
            makeToken: { "external-new" }
        ), "external-a")
        XCTAssertEqual(ExternalVolumeLocationPolicy.locationToken(
            existingToken: "external-a",
            selectedNewLocation: true,
            existingLocation: differentStrongSameWeak,
            candidateLocation: original,
            makeToken: { "external-new" }
        ), "external-a")
        XCTAssertEqual(ExternalVolumeLocationPolicy.locationToken(
            existingToken: "external-a",
            selectedNewLocation: true,
            existingLocation: original,
            candidateLocation: partialSameWeak,
            makeToken: { "external-new" }
        ), "external-a")
        XCTAssertEqual(ExternalVolumeLocationPolicy.locationToken(
            existingToken: "external-a",
            selectedNewLocation: true,
            existingLocation: partialSameWeak,
            candidateLocation: anotherPartialSameWeak,
            makeToken: { "external-new" }
        ), "external-a")
        XCTAssertEqual(ExternalVolumeLocationPolicy.locationToken(
            existingToken: "external-a",
            selectedNewLocation: true,
            existingLocation: original,
            candidateLocation: differentWeak,
            makeToken: { "external-new" }
        ), "external-new")
        XCTAssertEqual(ExternalVolumeLocationPolicy.locationToken(
            existingToken: "external-a",
            selectedNewLocation: true,
            existingLocation: nil,
            candidateLocation: noIdentity,
            makeToken: { "external-new" }
        ), "external-new")
        XCTAssertEqual(ExternalVolumeLocationPolicy.locationToken(
            existingToken: "external-a",
            selectedNewLocation: false,
            existingLocation: nil,
            candidateLocation: noIdentity,
            makeToken: { "external-new" }
        ), "external-a")
    }

    func testExternalBookmarkAndPathRefreshKeepRemoteProfileKeyStable() throws {
        var legacy = makeSMBProfile(basePath: "/", credentialRef: "external-ref", thumbnails: false)
        legacy.id = 7
        legacy.storageType = StorageType.externalVolume.rawValue
        legacy.shareName = "external-location-token"
        legacy.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(rootBookmarkData: Data([1]), displayPath: "/Volumes/Old")
        )
        var refreshed = legacy
        refreshed.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(
                rootBookmarkData: Data([2]),
                displayPath: "/Volumes/Renamed"
            )
        )
        XCTAssertEqual(legacy.remoteDestinationIdentity, refreshed.remoteDestinationIdentity)
        XCTAssertEqual(
            RemoteIndexSyncService.remoteProfileKey(legacy),
            RemoteIndexSyncService.remoteProfileKey(refreshed)
        )
    }

    func testRemoteProfileKeyUsesCanonicalDestinationIdentityAndProfileID() throws {
        var original = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        original.id = 7
        original.host = "SMB://NAS.Local/"
        original.domain = "WORKGROUP"

        var canonicalEdit = original
        canonicalEdit.host = "nas.local"
        canonicalEdit.shareName = "photos"
        canonicalEdit.basePath = "/A/"
        canonicalEdit.domain = "workgroup"
        XCTAssertEqual(
            RemoteIndexSyncService.remoteProfileKey(original),
            RemoteIndexSyncService.remoteProfileKey(canonicalEdit)
        )

        var anotherProfile = canonicalEdit
        anotherProfile.id = 8
        XCTAssertNotEqual(
            RemoteIndexSyncService.remoteProfileKey(original),
            RemoteIndexSyncService.remoteProfileKey(anotherProfile)
        )

        var sftp = original
        sftp.storageType = StorageType.sftp.rawValue
        sftp.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "old")
        )
        var changedHostKey = sftp
        changedHostKey.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "new")
        )
        XCTAssertNotEqual(
            RemoteIndexSyncService.remoteProfileKey(sftp),
            RemoteIndexSyncService.remoteProfileKey(changedHostKey)
        )
    }

    func testInvalidRemoteDestinationIdentityFailsClosed() {
        var first = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        first.id = 7
        first.storageType = StorageType.webdav.rawValue
        first.connectionParams = nil
        var changed = first
        changed.host = "other.local"

        XCTAssertFalse(first.hasSameRemoteDestination(as: changed))
        XCTAssertNotEqual(
            RemoteIndexSyncService.remoteProfileKey(first),
            RemoteIndexSyncService.remoteProfileKey(changed)
        )
    }

    func testSFTPHostKeyCaptureHasHardDeadline() async {
        let start = Date()
        do {
            _ = try await SFTPClient.captureHostKeyFingerprint(host: "127.0.0.1", port: 9, timeout: 0)
            XCTFail("Expected capture deadline")
        } catch {
            XCTAssertLessThan(Date().timeIntervalSince(start), 1)
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
        }
    }

    func testSFTPVerifyRejectsParentPathBeforeConnecting() async throws {
        let config = SFTPClient.Config(
            host: "127.0.0.1",
            port: 9,
            username: "user",
            credential: .password("secret"),
            expectedHostKeyFingerprintSHA256: "host-key"
        )
        let start = Date()
        do {
            try await SFTPClient.verifyBasePathWritable(config: config, basePath: "/home/u/../photos")
            XCTFail("Expected invalid configuration")
        } catch RemoteStorageClientError.invalidConfiguration {
            XCTAssertLessThan(Date().timeIntervalSince(start), 1)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    func testExternalBookmarkRefreshUsesTargetedCompareAndSwap() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        let oldParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(rootBookmarkData: Data([1]), displayPath: "/Volumes/Old")
        )
        let refreshedParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(
                rootBookmarkData: Data([2]),
                displayPath: "/Volumes/New"
            )
        )
        let conflictingParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(rootBookmarkData: Data([3]), displayPath: "/Volumes/Other")
        )
        var profile = makeSMBProfile(basePath: "/", credentialRef: "external-ref", thumbnails: false)
        profile.storageType = StorageType.externalVolume.rawValue
        profile.connectionParams = oldParams
        try database.saveServerProfile(&profile)
        let profileID = try XCTUnwrap(profile.id)
        let profileKeyBeforeRefresh = RemoteIndexSyncService.remoteProfileKey(profile)
        try database.setServerProfileName("Live Name", profileID: profileID)
        try database.setBackgroundBackupEnabled(false, profileID: profileID)
        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastCompletedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastRanAt(Date(), profileID: profileID)

        XCTAssertTrue(try database.refreshExternalVolumeConnectionParams(
            profileID: profileID,
            expectedConnectionParams: oldParams,
            refreshedConnectionParams: refreshedParams
        ))
        let refreshed = try XCTUnwrap(database.fetchServerProfile(id: profileID))
        XCTAssertEqual(refreshed.name, "Live Name")
        XCTAssertFalse(refreshed.backgroundBackupEnabled)
        XCTAssertEqual(refreshed.connectionParams, refreshedParams)
        XCTAssertEqual(RemoteIndexSyncService.remoteProfileKey(refreshed), profileKeyBeforeRefresh)
        XCTAssertEqual(try database.activeServerProfileID(), profileID)
        XCTAssertNotNil(try database.remoteVerifiedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastCompletedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastRanAt(profileID: profileID))
        XCTAssertTrue(database.matchesAcceptedExternalBookmarkRefresh(
            profileID: profileID,
            previousConnectionParams: oldParams,
            currentConnectionParams: refreshedParams
        ))

        XCTAssertFalse(try database.refreshExternalVolumeConnectionParams(
            profileID: profileID,
            expectedConnectionParams: oldParams,
            refreshedConnectionParams: conflictingParams
        ))
        XCTAssertEqual(try database.fetchServerProfile(id: profileID)?.connectionParams, refreshedParams)
    }

    func testDeletingProfileClearsPersistedActiveProfileID() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        var profile = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        try database.saveServerProfile(&profile)
        let profileID = try XCTUnwrap(profile.id)
        try database.setActiveServerProfileID(profileID)

        try database.deleteServerProfile(id: profileID)

        XCTAssertNil(try database.activeServerProfileID())
        XCTAssertTrue(try database.fetchServerProfiles().isEmpty)
    }

    func testConnectionEditPreservesLiveMetadataAndInvalidatesDestinationStateAtomically() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        var original = makeSMBProfile(basePath: "/A", credentialRef: "ref-a", thumbnails: false)
        try database.saveServerProfile(&original)
        let profileID = try XCTUnwrap(original.id)
        let writerID = try XCTUnwrap(original.writerID)

        try database.setServerProfileName("Live Name", profileID: profileID)
        try database.setBackgroundBackupEnabled(false, profileID: profileID)
        try database.setBackgroundBackupMinIntervalMinutes(180, profileID: profileID)
        try database.setBackgroundBackupRequiresWiFi(true, profileID: profileID)
        try database.setGenerateRemoteThumbnails(true, profileID: profileID)
        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastCompletedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastRanAt(Date(), profileID: profileID)

        var edited = original
        edited.basePath = "/B"
        edited.credentialRef = "ref-b"
        try database.saveConnectionProfile(&edited, editingProfileID: profileID)

        let saved = try XCTUnwrap(database.fetchServerProfile(id: profileID))
        XCTAssertEqual(saved.name, "Live Name")
        XCTAssertEqual(saved.basePath, "/B")
        XCTAssertFalse(saved.backgroundBackupEnabled)
        XCTAssertEqual(saved.backgroundBackupMinIntervalMinutes, 180)
        XCTAssertTrue(saved.backgroundBackupRequiresWiFi)
        XCTAssertTrue(saved.generateRemoteThumbnails)
        XCTAssertEqual(saved.writerID, writerID)
        XCTAssertNil(try database.activeServerProfileID())
        XCTAssertNil(try database.remoteVerifiedAt(profileID: profileID))
        XCTAssertNil(try database.backgroundBackupLastCompletedAt(profileID: profileID))
        XCTAssertNil(try database.backgroundBackupLastRanAt(profileID: profileID))
    }

    func testS3EffectiveSigningRegionControlsCacheIdentityAndEditInvalidation() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        var profile = makeSMBProfile(basePath: "/photos", credentialRef: "s3-ref", thumbnails: false)
        profile.storageType = StorageType.s3.rawValue
        profile.host = "objects.example.test"
        profile.port = 443
        profile.shareName = "bucket"
        profile.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "", usePathStyle: true)
        )
        try database.saveConnectionProfile(&profile, editingProfileID: nil)
        let profileID = try XCTUnwrap(profile.id)
        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastCompletedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastRanAt(Date(), profileID: profileID)

        var explicitDefault = profile
        explicitDefault.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "us-east-1", usePathStyle: true)
        )
        XCTAssertTrue(profile.hasSameRemoteDestination(as: explicitDefault))
        XCTAssertEqual(
            RemoteIndexSyncService.remoteProfileKey(profile),
            RemoteIndexSyncService.remoteProfileKey(explicitDefault)
        )
        try database.saveConnectionProfile(&explicitDefault, editingProfileID: profileID)
        XCTAssertEqual(try database.activeServerProfileID(), profileID)
        XCTAssertNotNil(try database.remoteVerifiedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastCompletedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastRanAt(profileID: profileID))

        var changedRegion = explicitDefault
        changedRegion.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "us-west-2", usePathStyle: true)
        )
        XCTAssertFalse(explicitDefault.hasSameRemoteDestination(as: changedRegion))
        XCTAssertNotEqual(
            RemoteIndexSyncService.remoteProfileKey(explicitDefault),
            RemoteIndexSyncService.remoteProfileKey(changedRegion)
        )
        try database.saveConnectionProfile(&changedRegion, editingProfileID: profileID)
        XCTAssertNil(try database.activeServerProfileID())
        XCTAssertNil(try database.remoteVerifiedAt(profileID: profileID))
        XCTAssertNil(try database.backgroundBackupLastCompletedAt(profileID: profileID))
        XCTAssertNil(try database.backgroundBackupLastRanAt(profileID: profileID))
    }

    func testExternalSameLocationRenewalKeepsStateButTrueRepickInvalidatesIt() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        var profile = makeSMBProfile(basePath: "/", credentialRef: "external-ref", thumbnails: false)
        profile.storageType = StorageType.externalVolume.rawValue
        profile.shareName = "external-location-a"
        profile.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(
                rootBookmarkData: Data([1]),
                displayPath: "/Volumes/Old"
            )
        )
        try database.saveConnectionProfile(&profile, editingProfileID: nil)
        let profileID = try XCTUnwrap(profile.id)
        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastCompletedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastRanAt(Date(), profileID: profileID)

        var renewed = profile
        renewed.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(
                rootBookmarkData: Data([2]),
                displayPath: "/Volumes/Renamed"
            )
        )
        try database.saveConnectionProfile(&renewed, editingProfileID: profileID)
        XCTAssertEqual(try database.activeServerProfileID(), profileID)
        XCTAssertNotNil(try database.remoteVerifiedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastCompletedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastRanAt(profileID: profileID))

        var repicked = renewed
        repicked.shareName = "external-location-b"
        repicked.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(
                rootBookmarkData: Data([3]),
                displayPath: "/Volumes/Other"
            )
        )
        try database.saveConnectionProfile(&repicked, editingProfileID: profileID)
        XCTAssertNil(try database.activeServerProfileID())
        XCTAssertNil(try database.remoteVerifiedAt(profileID: profileID))
        XCTAssertNil(try database.backgroundBackupLastCompletedAt(profileID: profileID))
        XCTAssertNil(try database.backgroundBackupLastRanAt(profileID: profileID))
    }

    @MainActor
    func testMaskedCredentialCanBeClearedDirectly() {
        let cell = CredentialTextFieldCell(style: .default, reuseIdentifier: nil)
        cell.configure(
            title: "Password",
            text: "",
            placeholder: "",
            isMasked: true,
            isRevealed: false,
            revealAccessibilityLabel: "Reveal",
            hideAccessibilityLabel: "Hide",
            inputAccessoryView: nil
        )
        var replacement: String?
        cell.onMaskedCredentialEdited = { replacement = $0 }
        let textField = UITextField()
        textField.text = "********"

        let shouldApply = cell.textField(
            textField,
            shouldChangeCharactersIn: NSRange(location: 7, length: 1),
            replacementString: ""
        )

        XCTAssertFalse(shouldApply)
        XCTAssertEqual(replacement, "")
        XCTAssertEqual(textField.text, "")
    }

    func testSettingsFormLayoutPolicyOnlyStacksAccessibilityCategories() {
        XCTAssertFalse(SettingsFormLayoutPolicy.usesVerticalLayout(for: .large))
        XCTAssertFalse(SettingsFormLayoutPolicy.usesVerticalLayout(for: .extraExtraExtraLarge))
        XCTAssertTrue(SettingsFormLayoutPolicy.usesVerticalLayout(for: .accessibilityMedium))
        XCTAssertTrue(SettingsFormLayoutPolicy.usesVerticalLayout(for: .accessibilityExtraExtraExtraLarge))
    }

    @MainActor
    func testSettingsTextFieldFillsAvailableRowWidth() {
        let cell = SettingsTextFieldCell(style: .default, reuseIdentifier: nil)
        cell.frame = CGRect(x: 0, y: 0, width: 320, height: 52)
        cell.configure(title: "Host", text: "NAS", placeholder: "")

        cell.layoutIfNeeded()

        XCTAssertGreaterThan(cell.textField.bounds.width, 180)
    }

    @MainActor
    func testConnectionFailureAlertOffersEditShortcut() {
        let profile = makeSMBProfile(basePath: "/", credentialRef: "ref", thumbnails: false)
        let alert = ConnectionFailureAlertFactory.make(
            profile: profile,
            error: RemoteStorageClientError.unavailable,
            onEdit: {}
        )

        XCTAssertEqual(alert.actions.map(\.title), [
            String(localized: "common.ok"),
            String(localized: "common.edit")
        ])
        XCTAssertEqual(alert.actions.map(\.style), [.cancel, .default])
    }

    private func makeSMBProfile(basePath: String, credentialRef: String, thumbnails: Bool) -> ServerProfileRecord {
        ServerProfileRecord(
            id: nil,
            name: "NAS",
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: "nas.local",
            port: 445,
            shareName: "Photos",
            basePath: basePath,
            username: "alice",
            domain: nil,
            credentialRef: credentialRef,
            backgroundBackupEnabled: true,
            backgroundBackupMinIntervalMinutes: 720,
            backgroundBackupRequiresWiFi: false,
            generateRemoteThumbnails: thumbnails,
            createdAt: Date(timeIntervalSince1970: 1_700_000_000),
            updatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
    }

    private func makeRemoteProfileFixtures() throws -> [(String, ServerProfileRecord)] {
        var smb = makeSMBProfile(basePath: "/Photos", credentialRef: "", thumbnails: false)
        smb.name = "SMB"

        var webDAV = makeSMBProfile(basePath: "/Watermelon", credentialRef: "", thumbnails: false)
        webDAV.name = "WebDAV"
        webDAV.storageType = StorageType.webdav.rawValue
        webDAV.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            WebDAVConnectionParams(scheme: "https")
        )
        webDAV.host = "dav.example.com"
        webDAV.port = 443
        webDAV.shareName = "/mount"

        var s3 = makeSMBProfile(basePath: "/Watermelon", credentialRef: "", thumbnails: false)
        s3.name = "S3"
        s3.storageType = StorageType.s3.rawValue
        s3.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "us-east-1", usePathStyle: true)
        )
        s3.host = "s3.example.com"
        s3.port = 443
        s3.shareName = "photos"
        s3.username = "access-key"

        var sftp = makeSMBProfile(basePath: "/home/alice/photos", credentialRef: "", thumbnails: false)
        sftp.name = "SFTP"
        sftp.storageType = StorageType.sftp.rawValue
        sftp.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "")
        )
        sftp.host = "sftp.example.com"
        sftp.port = 22
        sftp.shareName = ""

        return [
            (StorageType.smb.rawValue, smb),
            (StorageType.webdav.rawValue, webDAV),
            (StorageType.s3.rawValue, s3),
            (StorageType.sftp.rawValue, sftp)
        ]
    }

    private func waitForReachability(
        _ service: ProfileReachabilityService,
        profileID: Int64,
        expected: ProfileReachabilityService.Reachability
    ) async {
        for _ in 0 ..< 1_000 {
            if service.reachability(for: profileID) == expected { return }
            await Task.yield()
        }
        XCTFail("Reachability did not become \(expected)")
    }

    private func verifierTemporaryArtifacts() throws -> Set<String> {
        Set(try FileManager.default.contentsOfDirectory(
            at: FileManager.default.temporaryDirectory,
            includingPropertiesForKeys: nil
        ).map(\.lastPathComponent).filter { $0.hasPrefix(".watermelon-probe-") })
    }

    private func waitForProbeCleanup(
        _ client: InMemoryRemoteStorageClient,
        factory: ProbeCleanupFactoryRecorder? = nil,
        minimumFactoryCount: Int = 0,
        timeout: TimeInterval = 2
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            let entries = try await client.list(path: "/target")
            if entries.isEmpty, (factory?.count ?? 0) >= minimumFactoryCount { return }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        XCTFail("Probe cleanup did not finish")
    }
}

@MainActor
private final class WeakOwnerSFTPConnectionHarness {
    private let onCompletion: () -> Void
    private lazy var runner = ScreenBoundAsyncRunner<String>(
        isScreenActive: { true },
        onStateChanged: {}
    )

    init(onCompletion: @escaping () -> Void) {
        self.onCompletion = onCompletion
    }

    func startFingerprintCapture(capture: @escaping () async -> String) {
        let promptIfNeeded: (String) async -> Bool = { [weak self] _ in
            self != nil
        }
        runner.start(
            operation: {
                let fingerprint = await capture()
                try Task.checkCancellation()
                guard await promptIfNeeded(fingerprint) else { throw CancellationError() }
                return fingerprint
            },
            completion: { [onCompletion] _ in onCompletion() }
        )
    }
}

@MainActor
private final class DismissalSequencingOwner {
    private let onAction: () -> Void
    private var task: Task<Void, Never>?

    init(onAction: @escaping () -> Void) {
        self.onAction = onAction
    }

    deinit {
        task?.cancel()
    }

    func start() {
        task = Task { [weak self] in
            await PresentationDismissalSequencer.performAfterDismissal(
                isPresented: { [weak self] in self != nil },
                action: { [weak self] in self?.onAction() }
            )
        }
    }
}

private final class TestStorageProfileCredentialStore: StorageProfileCredentialStore {
    var values: [String: String]
    var onFirstSave: ((String, String) throws -> Void)?
    private(set) var saveCount = 0

    init(values: [String: String] = [:]) {
        self.values = values
    }

    func save(password: String, account: String) throws {
        values[account] = password
        saveCount += 1
        let callback = onFirstSave
        onFirstSave = nil
        try callback?(password, account)
    }

    func readPassword(account: String) throws -> String {
        guard let value = values[account] else {
            throw KeychainError.unhandled(status: errSecItemNotFound)
        }
        return value
    }

    func delete(account: String) throws {
        values[account] = nil
    }
}

private actor ManualReachabilityProbeHarness {
    private struct Invocation {
        let host: String
        var continuation: CheckedContinuation<ProfileReachabilityService.Reachability, Never>?
    }

    private var invocations: [Invocation] = []
    private var countWaiters: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []

    var invocationCount: Int { invocations.count }

    func probe(_ profile: ServerProfileRecord) async -> ProfileReachabilityService.Reachability {
        await withCheckedContinuation { continuation in
            invocations.append(Invocation(host: profile.host, continuation: continuation))
            let count = invocations.count
            let ready = countWaiters.filter { $0.count <= count }
            countWaiters.removeAll { $0.count <= count }
            ready.forEach { $0.continuation.resume() }
        }
    }

    func waitForInvocationCount(_ count: Int) async {
        if invocations.count >= count { return }
        await withCheckedContinuation { continuation in
            countWaiters.append((count, continuation))
        }
    }

    func completeInvocation(
        at index: Int,
        with result: ProfileReachabilityService.Reachability
    ) {
        guard invocations.indices.contains(index),
              let continuation = invocations[index].continuation else { return }
        invocations[index].continuation = nil
        continuation.resume(returning: result)
    }

    func host(at index: Int) -> String? {
        guard invocations.indices.contains(index) else { return nil }
        return invocations[index].host
    }
}

private final class ReachabilityRefreshSchedulerHarness: @unchecked Sendable {
    private let lock = NSLock()
    private var action: (@Sendable () -> Void)?
    private var immediateCount = 0
    private var periodicCount = 0
    private var cancellations = 0
    private var intervals: [TimeInterval] = []

    var immediateRefreshCount: Int { lock.withLock { immediateCount } }
    var periodicRefreshCount: Int { lock.withLock { periodicCount } }
    var cancellationCount: Int { lock.withLock { cancellations } }
    var scheduledIntervals: [TimeInterval] { lock.withLock { intervals } }

    func schedule(
        interval: TimeInterval,
        action: @escaping @Sendable () -> Void
    ) -> (@Sendable () -> Void) {
        lock.withLock {
            intervals.append(interval)
            self.action = action
        }
        return { [weak self] in
            self?.lock.withLock {
                self?.cancellations += 1
                self?.action = nil
            }
        }
    }

    func recordImmediateRefresh() {
        lock.withLock { immediateCount += 1 }
    }

    func recordPeriodicRefresh() {
        lock.withLock { periodicCount += 1 }
    }

    func fire() {
        let action = lock.withLock { self.action }
        action?()
    }
}

private final class ProbeCleanupFactoryRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private let target: InMemoryRemoteStorageClient
    private var createdCount = 0

    init(target: InMemoryRemoteStorageClient) {
        self.target = target
    }

    var count: Int { lock.withLock { createdCount } }

    func makeClient() -> any RemoteStorageClientProtocol {
        lock.withLock { createdCount += 1 }
        return ForwardingProbeCleanupClient(target: target)
    }
}

private final class NotFoundThenForwardingProbeCleanupFactory: @unchecked Sendable {
    private let lock = NSLock()
    private let target: InMemoryRemoteStorageClient
    private var createdCount = 0

    init(target: InMemoryRemoteStorageClient) {
        self.target = target
    }

    var count: Int { lock.withLock { createdCount } }

    func makeClient() -> any RemoteStorageClientProtocol {
        let count = lock.withLock {
            createdCount += 1
            return createdCount
        }
        if count == 1 {
            return NotFoundProbeCleanupClient()
        }
        return ForwardingProbeCleanupClient(target: target)
    }
}

private actor NotFoundProbeCleanupClient: RemoteStorageClientProtocol {
    func connect() async throws {}
    func disconnect() async {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { nil }
    func list(path: String) async throws -> [RemoteStorageEntry] { [] }
    func metadata(path: String) async throws -> RemoteStorageEntry? { nil }
    func upload(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {}
    func setModificationDate(_ date: Date, forPath path: String) async throws {}
    func download(remotePath: String, localURL: URL) async throws {}
    func exists(path: String) async throws -> Bool { false }
    func delete(path: String) async throws { throw RemoteErrorFixtures.notFound }
    func createDirectory(path: String) async throws {}
    func move(from sourcePath: String, to destinationPath: String) async throws {}
    func copy(from sourcePath: String, to destinationPath: String) async throws {}
}

private actor ForwardingProbeCleanupClient: RemoteStorageClientProtocol {
    let target: InMemoryRemoteStorageClient

    init(target: InMemoryRemoteStorageClient) {
        self.target = target
    }

    func connect() async throws {}
    func disconnect() async {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { nil }
    func list(path: String) async throws -> [RemoteStorageEntry] { [] }
    func metadata(path: String) async throws -> RemoteStorageEntry? { nil }
    func upload(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {}
    func setModificationDate(_ date: Date, forPath path: String) async throws {}
    func download(remotePath: String, localURL: URL) async throws {}
    func exists(path: String) async throws -> Bool { false }
    func delete(path: String) async throws { try await target.delete(path: path) }
    func createDirectory(path: String) async throws {}
    func move(from sourcePath: String, to destinationPath: String) async throws {}
    func copy(from sourcePath: String, to destinationPath: String) async throws {}
}

private actor DeferredCleanupTestGate {
    private var continuation: CheckedContinuation<Void, Never>?
    private var isReleased = false
    private(set) var hookCompleted = false

    func wait() async {
        guard !isReleased else { return }
        await withCheckedContinuation { continuation in
            if isReleased {
                continuation.resume()
            } else {
                self.continuation = continuation
            }
        }
    }

    func release() {
        isReleased = true
        continuation?.resume()
        continuation = nil
    }

    func markHookCompleted() {
        hookCompleted = true
    }
}
