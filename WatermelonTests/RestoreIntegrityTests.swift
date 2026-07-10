import XCTest
@testable import Watermelon

// Regression for restore content-integrity: a download that completes but delivers wrong bytes must fail
// verification before import / hash-index write, never be silently accepted as matching the remote.
final class RestoreIntegrityTests: XCTestCase {
    private var tempURL: URL!

    override func setUpWithError() throws {
        tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("restore_integrity_\(UUID().uuidString).bin")
        try Data("watermelon-restore-bytes".utf8).write(to: tempURL)
    }

    override func tearDownWithError() throws {
        if let tempURL { try? FileManager.default.removeItem(at: tempURL) }
    }

    private func instance(resourceHash: Data, fileName: String = "IMG_0001.JPG") -> RemoteAssetResourceInstance {
        RemoteAssetResourceInstance(
            role: 1,
            slot: 0,
            resourceHash: resourceHash,
            fileName: fileName,
            fileSize: 0,
            remoteRelativePath: "2026/06/\(fileName)",
            creationDateMs: nil
        )
    }

    func testMatchingHashPasses() throws {
        let correctHash = try AssetProcessor.contentHash(of: tempURL)
        XCTAssertNoThrow(
            try RestoreService.verifyDownloadedResource(at: tempURL, instance: instance(resourceHash: correctHash))
        )
    }

    func testMismatchedHashThrows() throws {
        let wrongHash = Data(repeating: 0xAB, count: 32)
        XCTAssertThrowsError(
            try RestoreService.verifyDownloadedResource(at: tempURL, instance: instance(resourceHash: wrongHash))
        ) { error in
            guard case RestoreIntegrityError.contentHashMismatch = error else {
                return XCTFail("expected contentHashMismatch, got \(error)")
            }
        }
    }

    // A legacy manifest entry with no recorded hash must not break restore — verification is skipped.
    func testEmptyHashSkipsVerification() throws {
        XCTAssertNoThrow(
            try RestoreService.verifyDownloadedResource(at: tempURL, instance: instance(resourceHash: Data()))
        )
    }

    func testMaterializationCacheKeySeparatesCodecKeyAndRemotePath() {
        let hash = Data(repeating: 0x11, count: RemoteManifestResource.contentHashByteCount)
        let plaintext = instance(resourceHash: hash, fileName: "plain.jpg")
        let encryptedSameHash = RemoteAssetResourceInstance(
            role: ResourceTypeCode.photo,
            slot: 0,
            resourceHash: hash,
            fileName: "cipher.wmenc",
            fileSize: plaintext.fileSize,
            remoteRelativePath: "2026/06/cipher.wmenc",
            creationDateMs: nil,
            storageCodec: RemoteManifestResource.encryptedStorageCodec,
            storedFileSize: 123,
            encryptionKeyID: "key-1"
        )
        let encryptedDifferentPath = RemoteAssetResourceInstance(
            role: ResourceTypeCode.photo,
            slot: 0,
            resourceHash: hash,
            fileName: "cipher.wmenc",
            fileSize: plaintext.fileSize,
            remoteRelativePath: "2026/06/cipher-copy.wmenc",
            creationDateMs: nil,
            storageCodec: RemoteManifestResource.encryptedStorageCodec,
            storedFileSize: 123,
            encryptionKeyID: "key-1"
        )

        XCTAssertNotEqual(
            RestoreService.materializationCacheKey(for: plaintext),
            RestoreService.materializationCacheKey(for: encryptedSameHash)
        )
        XCTAssertNotEqual(
            RestoreService.materializationCacheKey(for: encryptedSameHash),
            RestoreService.materializationCacheKey(for: encryptedDifferentPath)
        )
        XCTAssertNil(RestoreService.materializationCacheKey(for: instance(resourceHash: Data())))
    }

    func testEncryptedResourceMaterializesPlaintextAndOriginalFilename() throws {
        let plaintext = Data("encrypted restore content".utf8)
        try plaintext.write(to: tempURL)
        let hash = try AssetProcessor.contentHash(of: tempURL)
        let key = try RepoEncryptionKeyMaterial(
            repoID: "repo-restore",
            keyID: "key-restore",
            keyData: Data(repeating: 0x33, count: RepoEncryptionKeyMaterial.byteCount)
        )
        let context = RepoEncryptionContext(
            repoID: key.repoID,
            activeKeyID: key.keyID,
            contentKey: key.keyData
        )
        let encryptedURL = temporaryURL("wmenc")
        let decryptedURL = temporaryURL("plain")
        defer {
            try? FileManager.default.removeItem(at: encryptedURL)
            try? FileManager.default.removeItem(at: decryptedURL)
        }

        try FileEncryptionService().encrypt(
            plaintextURL: tempURL,
            encryptedURL: encryptedURL,
            metadata: FileEncryptionMetadata(
                originalFileName: "IMG_SECRET.HEIC",
                resourceType: ResourceTypeCode.photo,
                creationDateMs: 1_800_000_000_000,
                plainSHA256: hash.hexString,
                plainSize: Int64(plaintext.count)
            ),
            keyMaterial: key,
            externalAAD: FileEncryptionService.resourceExternalAAD(contentHash: hash)
        )
        let encryptedSize = try AssetProcessor.localFileSize(of: encryptedURL)
        let encryptedInstance = RemoteAssetResourceInstance(
            role: ResourceTypeCode.photo,
            slot: 0,
            resourceHash: hash,
            fileName: "4b55d935-08c8-41bc-aa62-21445b40d0aa.wmenc",
            fileSize: Int64(plaintext.count),
            remoteRelativePath: "2026/06/4b55d935-08c8-41bc-aa62-21445b40d0aa.wmenc",
            creationDateMs: 1_800_000_000_000,
            storageCodec: RemoteManifestResource.encryptedStorageCodec,
            storedFileSize: encryptedSize,
            encryptionKeyID: key.keyID
        )

        let materialized = try RestoreService.materializeDownloadedResource(
            downloadedURL: encryptedURL,
            decryptedURL: decryptedURL,
            instance: encryptedInstance,
            encryptionContext: context
        )

        XCTAssertEqual(try Data(contentsOf: materialized.fileURL), plaintext)
        XCTAssertEqual(materialized.instance.fileName, "IMG_SECRET.HEIC")
        XCTAssertEqual(materialized.fileURL.pathExtension.lowercased(), "heic")
        XCTAssertNotEqual(materialized.fileURL.pathExtension.lowercased(), RemoteFileNaming.encryptedFileExtension)
        XCTAssertEqual(materialized.temporaryURLs, [materialized.fileURL])
        XCTAssertEqual(materialized.instance.fileSize, Int64(plaintext.count))
        XCTAssertEqual(materialized.instance.remoteRelativePath, encryptedInstance.remoteRelativePath)
    }

    func testEncryptedHashMismatchDiagnosticDoesNotExposeOriginalFilename() throws {
        let plaintext = Data("tampered encrypted restore content".utf8)
        try plaintext.write(to: tempURL)
        let expectedHash = Data(repeating: 0xAB, count: RemoteManifestResource.contentHashByteCount)
        let key = try RepoEncryptionKeyMaterial(
            repoID: "repo-restore",
            keyID: "key-restore",
            keyData: Data(repeating: 0x33, count: RepoEncryptionKeyMaterial.byteCount)
        )
        let context = RepoEncryptionContext(
            repoID: key.repoID,
            activeKeyID: key.keyID,
            contentKey: key.keyData
        )
        let encryptedURL = temporaryURL("wmenc")
        let decryptedURL = temporaryURL("plain")
        defer {
            try? FileManager.default.removeItem(at: encryptedURL)
            try? FileManager.default.removeItem(at: decryptedURL)
        }

        try FileEncryptionService().encrypt(
            plaintextURL: tempURL,
            encryptedURL: encryptedURL,
            metadata: FileEncryptionMetadata(
                originalFileName: "IMG_SECRET.HEIC",
                resourceType: ResourceTypeCode.photo,
                plainSize: Int64(plaintext.count)
            ),
            keyMaterial: key,
            externalAAD: FileEncryptionService.resourceExternalAAD(contentHash: expectedHash)
        )
        let encryptedInstance = RemoteAssetResourceInstance(
            role: ResourceTypeCode.photo,
            slot: 0,
            resourceHash: expectedHash,
            fileName: "4b55d935-08c8-41bc-aa62-21445b40d0aa.wmenc",
            fileSize: Int64(plaintext.count),
            remoteRelativePath: "2026/06/4b55d935-08c8-41bc-aa62-21445b40d0aa.wmenc",
            creationDateMs: nil,
            storageCodec: RemoteManifestResource.encryptedStorageCodec,
            storedFileSize: try AssetProcessor.localFileSize(of: encryptedURL),
            encryptionKeyID: key.keyID
        )

        XCTAssertThrowsError(try RestoreService.materializeDownloadedResource(
            downloadedURL: encryptedURL,
            decryptedURL: decryptedURL,
            instance: encryptedInstance,
            encryptionContext: context
        )) { error in
            guard case RestoreIntegrityError.contentHashMismatch(let fileName, _, _) = error else {
                return XCTFail("expected contentHashMismatch, got \(error)")
            }
            XCTAssertEqual(fileName, encryptedInstance.fileName)
            XCTAssertFalse(error.localizedDescription.contains("IMG_SECRET"))
        }
    }

    func testEncryptedResourceRequiresEncryptionContext() throws {
        let encryptedInstance = RemoteAssetResourceInstance(
            role: ResourceTypeCode.photo,
            slot: 0,
            resourceHash: Data(repeating: 0x11, count: 32),
            fileName: "opaque.wmenc",
            fileSize: 1,
            remoteRelativePath: "2026/06/opaque.wmenc",
            creationDateMs: nil,
            storageCodec: RemoteManifestResource.encryptedStorageCodec,
            storedFileSize: 10,
            encryptionKeyID: "key"
        )
        let decryptedURL = temporaryURL("plain")
        defer { try? FileManager.default.removeItem(at: decryptedURL) }

        XCTAssertThrowsError(try RestoreService.materializeDownloadedResource(
            downloadedURL: tempURL,
            decryptedURL: decryptedURL,
            instance: encryptedInstance,
            encryptionContext: nil
        )) { error in
            XCTAssertEqual(error as? RestoreEncryptionError, .missingEncryptionContext)
        }
    }

    func testEncryptedResourceRequiresValidPlaintextHash() throws {
        let encryptedInstance = RemoteAssetResourceInstance(
            role: ResourceTypeCode.photo,
            slot: 0,
            resourceHash: Data(),
            fileName: "opaque.wmenc",
            fileSize: 1,
            remoteRelativePath: "2026/06/opaque.wmenc",
            creationDateMs: nil,
            storageCodec: RemoteManifestResource.encryptedStorageCodec,
            storedFileSize: 10,
            encryptionKeyID: "key"
        )
        let decryptedURL = temporaryURL("plain")
        defer { try? FileManager.default.removeItem(at: decryptedURL) }

        XCTAssertThrowsError(try RestoreService.materializeDownloadedResource(
            downloadedURL: tempURL,
            decryptedURL: decryptedURL,
            instance: encryptedInstance,
            encryptionContext: nil
        )) { error in
            XCTAssertEqual(error as? RestoreEncryptionError, .invalidEncryptedResourceHash(fileName: "opaque.wmenc"))
        }
    }

    func testEncryptedResourceRequiresManifestKeyIDToMatchActiveKey() throws {
        let context = RepoEncryptionContext(
            repoID: "repo-restore",
            activeKeyID: "active-key",
            contentKey: Data(repeating: 0x33, count: RepoEncryptionKeyMaterial.byteCount)
        )
        let encryptedInstance = RemoteAssetResourceInstance(
            role: ResourceTypeCode.photo,
            slot: 0,
            resourceHash: Data(repeating: 0x11, count: RemoteManifestResource.contentHashByteCount),
            fileName: "opaque.wmenc",
            fileSize: 1,
            remoteRelativePath: "2026/06/opaque.wmenc",
            creationDateMs: nil,
            storageCodec: RemoteManifestResource.encryptedStorageCodec,
            storedFileSize: 10,
            encryptionKeyID: "stale-key"
        )
        let decryptedURL = temporaryURL("plain")
        defer { try? FileManager.default.removeItem(at: decryptedURL) }

        XCTAssertThrowsError(try RestoreService.materializeDownloadedResource(
            downloadedURL: tempURL,
            decryptedURL: decryptedURL,
            instance: encryptedInstance,
            encryptionContext: context
        )) { error in
            XCTAssertEqual(
                error as? RestoreEncryptionError,
                .encryptionKeyMismatch(fileName: "opaque.wmenc", expectedKeyID: "active-key", actualKeyID: "stale-key")
            )
        }
    }

    func testUnsupportedStorageCodecThrows() throws {
        let instance = RemoteAssetResourceInstance(
            role: ResourceTypeCode.photo,
            slot: 0,
            resourceHash: try AssetProcessor.contentHash(of: tempURL),
            fileName: "future.bin",
            fileSize: 1,
            remoteRelativePath: "2026/06/future.bin",
            creationDateMs: nil,
            storageCodec: 99
        )
        let decryptedURL = temporaryURL("plain")
        defer { try? FileManager.default.removeItem(at: decryptedURL) }

        XCTAssertThrowsError(try RestoreService.materializeDownloadedResource(
            downloadedURL: tempURL,
            decryptedURL: decryptedURL,
            instance: instance,
            encryptionContext: nil
        )) { error in
            XCTAssertEqual(error as? RestoreEncryptionError, .unsupportedStorageCodec(fileName: "future.bin", storageCodec: 99))
        }
    }

    private func temporaryURL(_ suffix: String) -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("restore_integrity_\(UUID().uuidString)")
            .appendingPathExtension(suffix)
    }
}
