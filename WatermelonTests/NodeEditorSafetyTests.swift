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

    func testSFTPRemoteDestinationIgnoresCredentialModeButIncludesEndpoint() throws {
        var original = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        original.storageType = StorageType.sftp.rawValue
        original.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "old")
        )

        var edited = original
        edited.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .privateKey, hostKeyFingerprintSHA256: "new")
        )
        XCTAssertTrue(original.hasSameRemoteDestination(as: edited))

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
}
