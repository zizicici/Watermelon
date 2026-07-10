import XCTest
@testable import Watermelon

final class StorageProfileRemoteIdentityTests: XCTestCase {
    private func makeSMBProfile(
        name: String = "server",
        host: String = "host.local",
        shareName: String = "share",
        basePath: String = "/photos",
        username: String = "user",
        defaultResourceStorageCodec: Int = RemoteManifestResource.encryptedStorageCodec
    ) -> ServerProfileRecord {
        var profile = ServerProfileRecord(
            id: 1,
            name: name,
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: host,
            port: 445,
            shareName: shareName,
            basePath: basePath,
            username: username,
            domain: nil,
            credentialRef: "ref",
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date(),
            writerID: "writer"
        )
        profile.defaultResourceStorageCodec = defaultResourceStorageCodec
        return profile
    }

    private func makeSFTPProfile(
        fingerprint: String,
        defaultResourceStorageCodec: Int = RemoteManifestResource.encryptedStorageCodec
    ) throws -> ServerProfileRecord {
        var profile = ServerProfileRecord(
            id: 2,
            name: "sftp",
            storageType: StorageType.sftp.rawValue,
            connectionParams: try ServerProfileRecord.encodedConnectionParams(
                SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: fingerprint)
            ),
            sortOrder: 0,
            host: "host.local",
            port: 22,
            shareName: "",
            basePath: "/photos",
            username: "user",
            domain: nil,
            credentialRef: "ref",
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date(),
            writerID: "writer"
        )
        profile.defaultResourceStorageCodec = defaultResourceStorageCodec
        return profile
    }

    func testRemoteRepositoryIdentityPreservesEncryptedDefaultForPureNameEdit() {
        let original = makeSMBProfile()
        var edited = original
        edited.name = "renamed"

        XCTAssertTrue(original.hasSameRemoteRepositoryIdentity(as: edited))
        XCTAssertFalse(edited.shouldResetDefaultResourceStorageCodec(afterEditingFrom: original))
        XCTAssertEqual(edited.defaultResourceStorageCodec, RemoteManifestResource.encryptedStorageCodec)
    }

    func testRemoteRepositoryIdentityResetsEncryptedDefaultWhenPathChanges() {
        let original = makeSMBProfile()
        var edited = original
        edited.basePath = "/other"

        if edited.shouldResetDefaultResourceStorageCodec(afterEditingFrom: original) {
            edited.defaultResourceStorageCodec = RemoteManifestResource.plaintextStorageCodec
        }

        XCTAssertFalse(original.hasSameRemoteRepositoryIdentity(as: edited))
        XCTAssertEqual(edited.defaultResourceStorageCodec, RemoteManifestResource.plaintextStorageCodec)
    }

    func testSFTPHostKeyFingerprintChangeResetsEncryptedDefault() throws {
        let original = try makeSFTPProfile(fingerprint: "SHA256:first")
        var edited = try makeSFTPProfile(fingerprint: "SHA256:second")

        if edited.shouldResetDefaultResourceStorageCodec(afterEditingFrom: original) {
            edited.defaultResourceStorageCodec = RemoteManifestResource.plaintextStorageCodec
        }

        XCTAssertFalse(original.hasSameRemoteRepositoryIdentity(as: edited))
        XCTAssertEqual(edited.defaultResourceStorageCodec, RemoteManifestResource.plaintextStorageCodec)
    }
}
