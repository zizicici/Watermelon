import XCTest
@testable import Watermelon

final class RepoEncryptionKeyTests: XCTestCase {
    private let repoID = "repo-123"
    private let keyID = "key-abc"
    private let createdAt = "2026-06-08T00:00:00Z"
    private let createdBy = "writer-1b4e28ba"

    private var keyData: Data {
        Data((0..<RepoEncryptionKeyMaterial.byteCount).map { UInt8($0) })
    }

    private var material: RepoEncryptionKeyMaterial {
        get throws {
            try RepoEncryptionKeyMaterial(repoID: repoID, keyID: keyID, keyData: keyData)
        }
    }

    func testRecoveryKeyRoundTripsMaterial() throws {
        let material = try material

        let recoveryKey = RepoEncryptionKeyCodec.recoveryKeyString(for: material)
        let decoded = try RepoEncryptionKeyCodec.decodeRecoveryKey(recoveryKey)

        XCTAssertEqual(decoded, material)
        XCTAssertTrue(recoveryKey.hasPrefix("WMENC1."))
        XCTAssertFalse(recoveryKey.contains("="), "recovery keys use unpadded base64url")
    }

    func testRecoveryKeyDecodeAllowsWhitespace() throws {
        let recoveryKey = RepoEncryptionKeyCodec.recoveryKeyString(for: try material)
        let wrapped = recoveryKey.replacingOccurrences(of: ".", with: ".\n")

        let decoded = try RepoEncryptionKeyCodec.decodeRecoveryKey(wrapped)

        XCTAssertEqual(decoded, try material)
    }

    func testRecoveryKeyRejectsWrongKeyLength() throws {
        let shortKey = [
            "WMENC1",
            Data(repoID.utf8).base64EncodedString(),
            Data(keyID.utf8).base64EncodedString(),
            Data([0x01, 0x02]).base64EncodedString()
        ].joined(separator: ".")

        XCTAssertThrowsError(try RepoEncryptionKeyCodec.decodeRecoveryKey(shortKey)) { error in
            XCTAssertEqual(error as? RepoEncryptionKeyError, .invalidRecoveryKey)
        }
    }

    func testKeyCheckVerifiesManifestActiveKey() throws {
        let keyCheck = try RepoEncryptionKeyCodec.keyCheck(repoID: repoID, keyID: keyID, keyData: keyData)
        let manifest = VersionManifestLite.makeEncryptedManifest(
            createdAt: createdAt,
            createdBy: createdBy,
            repoID: repoID,
            activeKeyID: keyID,
            keyCheck: keyCheck
        )

        let context = try RepoEncryptionContext.verified(manifest: manifest, keyData: keyData)

        XCTAssertEqual(context.repoID, repoID)
        XCTAssertEqual(context.activeKeyID, keyID)
        XCTAssertEqual(context.contentKey, keyData)
    }

    func testKeyCheckRejectsWrongKey() throws {
        let keyCheck = try RepoEncryptionKeyCodec.keyCheck(repoID: repoID, keyID: keyID, keyData: keyData)
        let manifest = VersionManifestLite.makeEncryptedManifest(
            createdAt: createdAt,
            createdBy: createdBy,
            repoID: repoID,
            activeKeyID: keyID,
            keyCheck: keyCheck
        )
        let wrongKey = Data(repeating: 0xff, count: RepoEncryptionKeyMaterial.byteCount)

        XCTAssertThrowsError(try RepoEncryptionContext.verified(manifest: manifest, keyData: wrongKey)) { error in
            XCTAssertEqual(error as? RepoEncryptionKeyError, .keyCheckMismatch)
        }
    }

    func testPlainV2ManifestHasNoEncryptionContext() throws {
        let manifest = VersionManifestLite.makeManifest(createdAt: createdAt, createdBy: createdBy)

        XCTAssertThrowsError(try RepoEncryptionContext.verified(manifest: manifest, keyData: keyData)) { error in
            XCTAssertEqual(error as? RepoEncryptionKeyError, .missingEncryptionConfig)
        }
    }

    func testDeletingOneProfileReferenceKeepsSharedRawKeyUntilLastReference() throws {
        let material = try material
        let firstProfileID: Int64 = 9_000_000_001
        let secondProfileID: Int64 = 9_000_000_002
        let keychain = KeychainService(service: "com.zizicici.watermelon.tests.\(UUID().uuidString)")
        let store = RepoEncryptionKeychainStore(keychain: keychain)

        addTeardownBlock {
            try? keychain.delete(account: RepoEncryptionKeychainStore.account(repoID: material.repoID, keyID: material.keyID))
            try? keychain.delete(account: RepoEncryptionKeychainStore.profileAccount(profileID: firstProfileID))
            try? keychain.delete(account: RepoEncryptionKeychainStore.profileAccount(profileID: secondProfileID))
        }

        try store.save(material)
        try store.saveProfileKeyReference(profileID: firstProfileID, material: material)
        try store.saveProfileKeyReference(profileID: secondProfileID, material: material)

        try store.deleteProfileKey(profileID: firstProfileID)

        XCTAssertEqual(try store.read(repoID: material.repoID, keyID: material.keyID), material)
        assertThrowsKeychainItemNotFound(
            try keychain.readData(account: RepoEncryptionKeychainStore.profileAccount(profileID: firstProfileID))
        )
        XCTAssertNoThrow(
            try keychain.readData(account: RepoEncryptionKeychainStore.profileAccount(profileID: secondProfileID))
        )

        try store.deleteProfileKey(profileID: secondProfileID)

        assertThrowsKeychainItemNotFound(try store.read(repoID: material.repoID, keyID: material.keyID))
        assertThrowsKeychainItemNotFound(
            try keychain.readData(account: RepoEncryptionKeychainStore.profileAccount(profileID: secondProfileID))
        )
    }

    func testDeletingOnlyProfileReferenceKeepsRawKeyEvenWhenLastReference() throws {
        let material = try material
        let profileID: Int64 = 9_000_000_003
        let keychain = KeychainService(service: "com.zizicici.watermelon.tests.\(UUID().uuidString)")
        let store = RepoEncryptionKeychainStore(keychain: keychain)

        addTeardownBlock {
            try? keychain.delete(account: RepoEncryptionKeychainStore.account(repoID: material.repoID, keyID: material.keyID))
            try? keychain.delete(account: RepoEncryptionKeychainStore.profileAccount(profileID: profileID))
        }

        try store.save(material)
        try store.saveProfileKeyReference(profileID: profileID, material: material)

        try store.deleteProfileKeyReference(profileID: profileID)

        XCTAssertEqual(try store.read(repoID: material.repoID, keyID: material.keyID), material)
        assertThrowsKeychainItemNotFound(
            try keychain.readData(account: RepoEncryptionKeychainStore.profileAccount(profileID: profileID))
        )
    }

    func testReadProfileKeyUsesSavedProfileReference() throws {
        let material = try material
        let profileID: Int64 = 9_000_000_004
        let keychain = KeychainService(service: "com.zizicici.watermelon.tests.\(UUID().uuidString)")
        let store = RepoEncryptionKeychainStore(keychain: keychain)

        addTeardownBlock {
            try? keychain.delete(account: RepoEncryptionKeychainStore.account(repoID: material.repoID, keyID: material.keyID))
            try? keychain.delete(account: RepoEncryptionKeychainStore.profileAccount(profileID: profileID))
        }

        try store.save(material)
        try store.saveProfileKeyReference(profileID: profileID, material: material)

        XCTAssertEqual(try store.readProfileKey(profileID: profileID), material)
    }

    private func assertThrowsKeychainItemNotFound<T>(
        _ expression: @autoclosure () throws -> T,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        XCTAssertThrowsError(try expression(), file: file, line: line) { error in
            guard case KeychainError.unhandled(let status) = error else {
                XCTFail("Expected keychain item not found, got \(error)", file: file, line: line)
                return
            }
            XCTAssertEqual(status, errSecItemNotFound, file: file, line: line)
        }
    }
}
