import XCTest
@testable import Watermelon

final class RepoEncryptionSetupServiceTests: XCTestCase {
    private let basePath = "/photos"
    private let createdAt = "2026-07-09T00:00:00Z"
    private let createdBy = "test-device"

    private func makeMaterial(
        repoID: String = "repo-1",
        keyID: String = "key-1",
        byte: UInt8 = 0xAB
    ) throws -> RepoEncryptionKeyMaterial {
        try RepoEncryptionKeyMaterial(
            repoID: repoID,
            keyID: keyID,
            keyData: Data(repeating: byte, count: RepoEncryptionKeyMaterial.byteCount)
        )
    }

    private func service(
        keyStore: MemoryRepoEncryptionKeyStore,
        material: RepoEncryptionKeyMaterial
    ) -> RepoEncryptionSetupService {
        RepoEncryptionSetupService(keyStore: keyStore, makeKeyMaterial: { material })
    }

    func testFreshRepoCommitsEncryptedVersionAndStoresKey() async throws {
        let client = InMemoryRemoteStorageClient()
        let keyStore = MemoryRepoEncryptionKeyStore()
        let material = try makeMaterial()
        let result = try await service(keyStore: keyStore, material: material).enableEncryption(
            client: client,
            basePath: basePath,
            createdAt: createdAt,
            createdBy: createdBy
        )

        XCTAssertEqual(result.action, .createdEncryptedRepo)
        XCTAssertEqual(result.manifest.formatVersion, VersionManifestLite.encryptedFormatVersion)
        XCTAssertEqual(result.manifest.minAppVersion, "1.6.0")
        XCTAssertEqual(result.manifest.repoID, material.repoID)
        XCTAssertEqual(result.context.activeKeyID, material.keyID)
        XCTAssertEqual(try RepoEncryptionKeyCodec.decodeRecoveryKey(result.recoveryKey), material)
        XCTAssertEqual(try keyStore.read(repoID: material.repoID, keyID: material.keyID), material)

        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        let committed = try VersionManifestLite.decode(try XCTUnwrap(versionData))
        XCTAssertEqual(committed.formatVersion, VersionManifestLite.encryptedFormatVersion)
        XCTAssertEqual(committed.encryption?.activeKeyID, material.keyID)
    }

    func testMoveCommitReadbackFaultAfterEncryptedVersionLandedKeepsKeyAndReturnsSuccess() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setOnMove { _, to in
            if to == RepoLayoutLite.versionPath(basePath: self.basePath) {
                await client.enqueueDownloadError(RemoteErrorFixtures.retryable)
            }
        }
        let keyStore = MemoryRepoEncryptionKeyStore()
        let material = try makeMaterial(repoID: "repo-readback", keyID: "key-readback", byte: 0xBC)

        let result = try await service(keyStore: keyStore, material: material).enableEncryption(
            client: client,
            basePath: basePath,
            createdAt: createdAt,
            createdBy: createdBy
        )

        XCTAssertEqual(result.action, .createdEncryptedRepo)
        XCTAssertEqual(result.manifest.repoID, material.repoID)
        XCTAssertEqual(result.context.activeKeyID, material.keyID)
        XCTAssertEqual(try keyStore.read(repoID: material.repoID, keyID: material.keyID), material)

        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        let committed = try VersionManifestLite.decode(try XCTUnwrap(versionData))
        XCTAssertEqual(committed.repoID, material.repoID)
        XCTAssertEqual(committed.encryption?.activeKeyID, material.keyID)
    }

    func testMoveCommitPersistentReadbackFaultKeepsKeyForRetry() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setOnMove { _, to in
            if to == RepoLayoutLite.versionPath(basePath: self.basePath) {
                await client.enqueueDownloadError(RemoteErrorFixtures.retryable)
                await client.enqueueDownloadError(RemoteErrorFixtures.retryable)
            }
        }
        let keyStore = MemoryRepoEncryptionKeyStore()
        let material = try makeMaterial(repoID: "repo-persistent-readback", keyID: "key-persistent-readback", byte: 0xBE)

        do {
            _ = try await service(keyStore: keyStore, material: material).enableEncryption(
                client: client,
                basePath: basePath,
                createdAt: createdAt,
                createdBy: createdBy
            )
            XCTFail("persistent readback fault should surface the commit error")
        } catch {
            XCTAssertEqual((error as NSError).domain, NSURLErrorDomain)
        }

        XCTAssertEqual(try keyStore.read(repoID: material.repoID, keyID: material.keyID), material)
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        let committed = try VersionManifestLite.decode(try XCTUnwrap(versionData))
        XCTAssertEqual(committed.repoID, material.repoID)
        XCTAssertEqual(committed.encryption?.activeKeyID, material.keyID)
    }

    func testDirectPutPostEffectValidEncryptedVersionKeepsKeyAndReturnsSuccess() async throws {
        let client = InMemoryRemoteStorageClient(moveMayNotBeIndependent: true)
        await client.failUploadAfterWrite(
            forPathSuffix: RepoLayoutLite.versionPath(basePath: basePath),
            error: RemoteErrorFixtures.retryable
        )
        let keyStore = MemoryRepoEncryptionKeyStore()
        let material = try makeMaterial(repoID: "repo-direct-put", keyID: "key-direct-put", byte: 0xBD)

        let result = try await service(keyStore: keyStore, material: material).enableEncryption(
            client: client,
            basePath: basePath,
            createdAt: createdAt,
            createdBy: createdBy
        )

        XCTAssertEqual(result.action, .createdEncryptedRepo)
        XCTAssertEqual(result.manifest.repoID, material.repoID)
        XCTAssertEqual(result.context.activeKeyID, material.keyID)
        XCTAssertEqual(try keyStore.read(repoID: material.repoID, keyID: material.keyID), material)

        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        let committed = try VersionManifestLite.decode(try XCTUnwrap(versionData))
        XCTAssertEqual(committed.repoID, material.repoID)
        XCTAssertEqual(committed.encryption?.activeKeyID, material.keyID)
    }

    func testPlainV2RepoUpgradesToEncryptedV3() async throws {
        let client = InMemoryRemoteStorageClient()
        let plain = VersionManifestLite.makeManifest(createdAt: "old", createdBy: "old-device")
        await client.seedFile(
            path: RepoLayoutLite.versionPath(basePath: basePath),
            data: try VersionManifestLite.encode(plain)
        )
        let keyStore = MemoryRepoEncryptionKeyStore()
        let material = try makeMaterial(repoID: "repo-2", keyID: "key-2", byte: 0xCD)

        let result = try await service(keyStore: keyStore, material: material).enableEncryption(
            client: client,
            basePath: basePath,
            createdAt: createdAt,
            createdBy: createdBy
        )

        XCTAssertEqual(result.action, .upgradedPlainRepo)
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        let committed = try VersionManifestLite.decode(try XCTUnwrap(versionData))
        XCTAssertEqual(committed.formatVersion, VersionManifestLite.encryptedFormatVersion)
        XCTAssertEqual(committed.repoID, material.repoID)
    }

    func testExistingEncryptedRepoVerifiesLocalKeyWithoutRecommitting() async throws {
        let client = InMemoryRemoteStorageClient()
        let keyStore = MemoryRepoEncryptionKeyStore()
        let material = try makeMaterial(repoID: "repo-3", keyID: "key-3", byte: 0xEF)
        try keyStore.save(material)
        let manifest = VersionManifestLite.makeEncryptedManifest(
            createdAt: createdAt,
            createdBy: createdBy,
            repoID: material.repoID,
            activeKeyID: material.keyID,
            keyCheck: try RepoEncryptionKeyCodec.keyCheck(repoID: material.repoID, keyID: material.keyID, keyData: material.keyData)
        )
        await client.seedFile(
            path: RepoLayoutLite.versionPath(basePath: basePath),
            data: try VersionManifestLite.encode(manifest)
        )

        let result = try await service(keyStore: keyStore, material: try makeMaterial(byte: 0x11)).enableEncryption(
            client: client,
            basePath: basePath,
            createdAt: createdAt,
            createdBy: createdBy
        )

        XCTAssertEqual(result.action, .verifiedExistingEncryptedRepo)
        XCTAssertEqual(result.keyMaterial, material)
        let uploadedPaths = await client.uploadedPaths
        XCTAssertTrue(uploadedPaths.isEmpty)
    }

    func testLoadExistingContextVerifiesExistingEncryptedRepo() async throws {
        let client = InMemoryRemoteStorageClient()
        let keyStore = MemoryRepoEncryptionKeyStore()
        let material = try makeMaterial(repoID: "repo-load", keyID: "key-load", byte: 0x31)
        try keyStore.save(material)
        let manifest = VersionManifestLite.makeEncryptedManifest(
            createdAt: createdAt,
            createdBy: createdBy,
            repoID: material.repoID,
            activeKeyID: material.keyID,
            keyCheck: try RepoEncryptionKeyCodec.keyCheck(repoID: material.repoID, keyID: material.keyID, keyData: material.keyData)
        )
        await client.seedFile(
            path: RepoLayoutLite.versionPath(basePath: basePath),
            data: try VersionManifestLite.encode(manifest)
        )

        let context = try await service(keyStore: keyStore, material: try makeMaterial(byte: 0x32)).loadExistingContext(
            client: client,
            basePath: basePath
        )

        XCTAssertEqual(context.repoID, material.repoID)
        XCTAssertEqual(context.activeKeyID, material.keyID)
    }

    func testLoadExistingContextDoesNotUpgradePlainV2() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(
            path: RepoLayoutLite.versionPath(basePath: basePath),
            data: try VersionManifestLite.encode(VersionManifestLite.makeManifest(createdAt: "old", createdBy: "old"))
        )

        do {
            _ = try await service(keyStore: MemoryRepoEncryptionKeyStore(), material: try makeMaterial(byte: 0x33)).loadExistingContext(
                client: client,
                basePath: basePath
            )
            XCTFail("ordinary backup prepare must not silently upgrade v2 to v3")
        } catch let error as RepoEncryptionSetupError {
            XCTAssertEqual(error, .missingEncryptedRepo)
        }
    }

    func testImportRecoveryKeyStoresVerifiedKey() async throws {
        let client = InMemoryRemoteStorageClient()
        let keyStore = MemoryRepoEncryptionKeyStore()
        let material = try makeMaterial(repoID: "repo-import", keyID: "key-import", byte: 0x41)
        let manifest = VersionManifestLite.makeEncryptedManifest(
            createdAt: createdAt,
            createdBy: createdBy,
            repoID: material.repoID,
            activeKeyID: material.keyID,
            keyCheck: try RepoEncryptionKeyCodec.keyCheck(repoID: material.repoID, keyID: material.keyID, keyData: material.keyData)
        )
        await client.seedFile(
            path: RepoLayoutLite.versionPath(basePath: basePath),
            data: try VersionManifestLite.encode(manifest)
        )

        let result = try await service(keyStore: keyStore, material: try makeMaterial(byte: 0x42)).importRecoveryKey(
            client: client,
            basePath: basePath,
            recoveryKey: RepoEncryptionKeyCodec.recoveryKeyString(for: material)
        )

        XCTAssertEqual(result.action, .importedRecoveryKey)
        XCTAssertEqual(result.context.repoID, material.repoID)
        XCTAssertEqual(result.context.activeKeyID, material.keyID)
        XCTAssertEqual(try keyStore.read(repoID: material.repoID, keyID: material.keyID), material)
    }

    func testVerifyExistingEncryptedRepoUsesRawKeyWithoutRecoveryKeyImport() async throws {
        let client = InMemoryRemoteStorageClient()
        let keyStore = MemoryRepoEncryptionKeyStore()
        let material = try makeMaterial(repoID: "repo-raw-key-recovery", keyID: "key-raw-key-recovery", byte: 0x45)
        try keyStore.save(material)
        let manifest = VersionManifestLite.makeEncryptedManifest(
            createdAt: createdAt,
            createdBy: createdBy,
            repoID: material.repoID,
            activeKeyID: material.keyID,
            keyCheck: try RepoEncryptionKeyCodec.keyCheck(repoID: material.repoID, keyID: material.keyID, keyData: material.keyData)
        )
        await client.seedFile(
            path: RepoLayoutLite.versionPath(basePath: basePath),
            data: try VersionManifestLite.encode(manifest)
        )

        let result = try await service(
            keyStore: keyStore,
            material: try makeMaterial(byte: 0x46)
        ).verifyExistingEncryptedRepo(client: client, basePath: basePath)

        XCTAssertEqual(result.action, .verifiedExistingEncryptedRepo)
        XCTAssertEqual(result.context.repoID, material.repoID)
        XCTAssertEqual(result.context.activeKeyID, material.keyID)
        XCTAssertEqual(result.keyMaterial, material)
    }

    func testImportRecoveryKeyRejectsWrongRepo() async throws {
        let client = InMemoryRemoteStorageClient()
        let material = try makeMaterial(repoID: "repo-import-a", keyID: "key-import", byte: 0x43)
        let wrong = try makeMaterial(repoID: "repo-import-b", keyID: "key-import", byte: 0x43)
        let manifest = VersionManifestLite.makeEncryptedManifest(
            createdAt: createdAt,
            createdBy: createdBy,
            repoID: material.repoID,
            activeKeyID: material.keyID,
            keyCheck: try RepoEncryptionKeyCodec.keyCheck(repoID: material.repoID, keyID: material.keyID, keyData: material.keyData)
        )
        await client.seedFile(
            path: RepoLayoutLite.versionPath(basePath: basePath),
            data: try VersionManifestLite.encode(manifest)
        )

        do {
            _ = try await service(keyStore: MemoryRepoEncryptionKeyStore(), material: try makeMaterial(byte: 0x44)).importRecoveryKey(
                client: client,
                basePath: basePath,
                recoveryKey: RepoEncryptionKeyCodec.recoveryKeyString(for: wrong)
            )
            XCTFail("wrong repo recovery key must not be stored")
        } catch let error as RepoEncryptionSetupError {
            XCTAssertEqual(error, .keyVerificationFailed)
        }
    }

    func testExistingEncryptedRepoWithoutLocalKeyThrowsMissingKey() async throws {
        let client = InMemoryRemoteStorageClient()
        let material = try makeMaterial(repoID: "repo-4", keyID: "key-4", byte: 0x12)
        let manifest = VersionManifestLite.makeEncryptedManifest(
            createdAt: createdAt,
            createdBy: createdBy,
            repoID: material.repoID,
            activeKeyID: material.keyID,
            keyCheck: try RepoEncryptionKeyCodec.keyCheck(repoID: material.repoID, keyID: material.keyID, keyData: material.keyData)
        )
        await client.seedFile(
            path: RepoLayoutLite.versionPath(basePath: basePath),
            data: try VersionManifestLite.encode(manifest)
        )

        do {
            _ = try await service(keyStore: MemoryRepoEncryptionKeyStore(), material: try makeMaterial(byte: 0x13)).enableEncryption(
                client: client,
                basePath: basePath,
                createdAt: createdAt,
                createdBy: createdBy
            )
            XCTFail("existing encrypted repo must require its active key")
        } catch let error as RepoEncryptionSetupError {
            XCTAssertEqual(error, .missingLocalKey)
        }
    }

    func testExistingEncryptedRepoWithWrongLocalKeyThrowsVerificationFailed() async throws {
        let client = InMemoryRemoteStorageClient()
        let keyStore = MemoryRepoEncryptionKeyStore()
        let correct = try makeMaterial(repoID: "repo-5", keyID: "key-5", byte: 0x21)
        let wrong = try makeMaterial(repoID: "repo-5", keyID: "key-5", byte: 0x22)
        try keyStore.save(wrong)
        let manifest = VersionManifestLite.makeEncryptedManifest(
            createdAt: createdAt,
            createdBy: createdBy,
            repoID: correct.repoID,
            activeKeyID: correct.keyID,
            keyCheck: try RepoEncryptionKeyCodec.keyCheck(repoID: correct.repoID, keyID: correct.keyID, keyData: correct.keyData)
        )
        await client.seedFile(
            path: RepoLayoutLite.versionPath(basePath: basePath),
            data: try VersionManifestLite.encode(manifest)
        )

        do {
            _ = try await service(keyStore: keyStore, material: try makeMaterial(byte: 0x23)).enableEncryption(
                client: client,
                basePath: basePath,
                createdAt: createdAt,
                createdBy: createdBy
            )
            XCTFail("wrong active key bytes must fail verification")
        } catch let error as RepoEncryptionSetupError {
            XCTAssertEqual(error, .keyVerificationFailed)
        }
    }
}

final class MemoryRepoEncryptionKeyStore: RepoEncryptionKeyStore, @unchecked Sendable {
    enum Error: Swift.Error {
        case missing
    }

    private let lock = NSLock()
    private var values: [String: RepoEncryptionKeyMaterial] = [:]

    func save(_ material: RepoEncryptionKeyMaterial) throws {
        lock.withLock {
            values[RepoEncryptionKeychainStore.account(repoID: material.repoID, keyID: material.keyID)] = material
        }
    }

    func read(repoID: String, keyID: String) throws -> RepoEncryptionKeyMaterial {
        try lock.withLock {
            guard let material = values[RepoEncryptionKeychainStore.account(repoID: repoID, keyID: keyID)] else {
                throw Error.missing
            }
            return material
        }
    }

    func delete(repoID: String, keyID: String) throws {
        lock.withLock {
            values[RepoEncryptionKeychainStore.account(repoID: repoID, keyID: keyID)] = nil
        }
    }
}
