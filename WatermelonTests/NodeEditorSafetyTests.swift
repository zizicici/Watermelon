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
        original.host = "SMB://NAS.Local"
        try database.saveServerProfile(&original)

        var sameDestination = original
        sameDestination.host = "nas.local"
        XCTAssertTrue(original.hasSameRemoteDestination(as: sameDestination))
        XCTAssertEqual(RemoteHostIdentity.canonicalSMB(original.host), "nas.local")
        XCTAssertEqual(original.storageProfile.displaySubtitle, "SMB://nas.local/Photos/A")
        XCTAssertEqual(
            try database.findServerProfile(
                host: "nas.local",
                port: original.port,
                shareName: original.shareName,
                basePath: original.basePath,
                username: original.username,
                domain: original.domain
            )?.id,
            original.id
        )
    }

    func testRemoteStorageWriteVerifierRemovesProbeDirectory() async throws {
        let client = InMemoryRemoteStorageClient()
        try await RemoteStorageWriteVerifier.verify(client: client, basePath: "/target")

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
            try await RemoteStorageWriteVerifier.verify(client: client, basePath: "/target")
            XCTFail("Expected read-back mismatch")
        } catch {
            XCTAssertTrue(error is RemoteStorageClientError)
        }

        let entries = try await client.list(path: "/target")
        let deletedCount = await client.deletedPaths.count
        XCTAssertTrue(entries.isEmpty)
        XCTAssertEqual(deletedCount, 2)
    }

    func testRemoteStorageWriteVerifierRejectsBackendThatOverwritesConditionalCreate() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setIgnoreCreateIfAbsent(true)

        do {
            try await RemoteStorageWriteVerifier.verify(client: client, basePath: "/target", timeout: 5)
            XCTFail("Expected conditional-create verification to fail")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .terminal)
        }

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
            try await RemoteStorageWriteVerifier.verify(client: client, basePath: "/target", timeout: 0.03)
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

    func testRemoteStorageWriteVerifierReclaimsTemporaryFilesWhenOperationNeverReturns() async throws {
        let artifactsBefore = try verifierTemporaryArtifacts()
        let client = InMemoryRemoteStorageClient()
        await client.setOnUpload {
            await withUnsafeContinuation { (_: UnsafeContinuation<Void, Never>) in }
        }

        do {
            try await RemoteStorageWriteVerifier.verify(client: client, basePath: "/target", timeout: 0.03)
            XCTFail("Expected verifier deadline")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
        }

        XCTAssertEqual(try verifierTemporaryArtifacts(), artifactsBefore)
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

    func testExternalRemoteDestinationUsesDisplayPathInsteadOfBookmarkBytes() throws {
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
        XCTAssertFalse(original.hasSameRemoteDestination(as: edited))
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
            ExternalVolumeConnectionParams(rootBookmarkData: Data([2]), displayPath: "/Volumes/New")
        )
        let conflictingParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(rootBookmarkData: Data([3]), displayPath: "/Volumes/Other")
        )
        var profile = makeSMBProfile(basePath: "/", credentialRef: "external-ref", thumbnails: false)
        profile.storageType = StorageType.externalVolume.rawValue
        profile.connectionParams = oldParams
        try database.saveServerProfile(&profile)
        let profileID = try XCTUnwrap(profile.id)
        try database.setServerProfileName("Live Name", profileID: profileID)
        try database.setBackgroundBackupEnabled(false, profileID: profileID)

        XCTAssertTrue(try database.refreshExternalVolumeConnectionParams(
            profileID: profileID,
            expectedConnectionParams: oldParams,
            refreshedConnectionParams: refreshedParams
        ))
        let refreshed = try XCTUnwrap(database.fetchServerProfile(id: profileID))
        XCTAssertEqual(refreshed.name, "Live Name")
        XCTAssertFalse(refreshed.backgroundBackupEnabled)
        XCTAssertEqual(refreshed.connectionParams, refreshedParams)
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

    private func verifierTemporaryArtifacts() throws -> Set<String> {
        Set(try FileManager.default.contentsOfDirectory(
            at: FileManager.default.temporaryDirectory,
            includingPropertiesForKeys: nil
        ).map(\.lastPathComponent).filter { $0.hasPrefix(".watermelon-probe-") })
    }
}
