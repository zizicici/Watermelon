import XCTest
@testable import Watermelon

final class FileEncryptionServiceTests: XCTestCase {
    private let service = FileEncryptionService()
    private let externalAAD = Data("resource:hash-123".utf8)

    private var keyMaterial: RepoEncryptionKeyMaterial {
        get throws {
            try RepoEncryptionKeyMaterial(
                repoID: "repo-123",
                keyID: "key-abc",
                keyData: Data(repeating: 0x11, count: RepoEncryptionKeyMaterial.byteCount)
            )
        }
    }

    func testEncryptDecryptRoundTripsContentAndMetadata() throws {
        let plainURL = tempURL("plain")
        let encryptedURL = tempURL("wmenc")
        let decryptedURL = tempURL("decrypted")
        defer { cleanup([plainURL, encryptedURL, decryptedURL]) }
        let plaintext = Data("hello encrypted watermelon".utf8)
        try plaintext.write(to: plainURL)
        let metadata = FileEncryptionMetadata(
            originalFileName: "IMG_1234.HEIC",
            resourceType: 1,
            creationDateMs: 1_710_000_000_000,
            plainSHA256: "hash",
            plainSize: nil
        )

        try service.encrypt(
            plaintextURL: plainURL,
            encryptedURL: encryptedURL,
            metadata: metadata,
            keyMaterial: try keyMaterial,
            externalAAD: externalAAD
        )
        let decryptedMetadata = try service.decrypt(
            encryptedURL: encryptedURL,
            plaintextURL: decryptedURL,
            keyMaterial: try keyMaterial,
            externalAAD: externalAAD
        )

        XCTAssertEqual(try Data(contentsOf: decryptedURL), plaintext)
        XCTAssertEqual(decryptedMetadata.originalFileName, "IMG_1234.HEIC")
        XCTAssertEqual(decryptedMetadata.resourceType, 1)
        XCTAssertEqual(decryptedMetadata.creationDateMs, 1_710_000_000_000)
        XCTAssertEqual(decryptedMetadata.plainSHA256, "hash")
        XCTAssertEqual(decryptedMetadata.plainSize, Int64(plaintext.count))
        XCTAssertFalse(
            try Data(contentsOf: encryptedURL).range(of: Data("IMG_1234.HEIC".utf8)) != nil,
            "original filename must live only in encrypted metadata"
        )
    }

    func testEncryptDecryptRoundTripsEmptyFile() throws {
        let plainURL = tempURL("empty")
        let encryptedURL = tempURL("wmenc")
        let decryptedURL = tempURL("decrypted")
        defer { cleanup([plainURL, encryptedURL, decryptedURL]) }
        try Data().write(to: plainURL)

        try service.encrypt(
            plaintextURL: plainURL,
            encryptedURL: encryptedURL,
            metadata: FileEncryptionMetadata(originalFileName: "empty.bin"),
            keyMaterial: try keyMaterial,
            externalAAD: externalAAD
        )
        let metadata = try service.decrypt(
            encryptedURL: encryptedURL,
            plaintextURL: decryptedURL,
            keyMaterial: try keyMaterial,
            externalAAD: externalAAD
        )

        XCTAssertEqual(try Data(contentsOf: decryptedURL), Data())
        XCTAssertEqual(metadata.originalFileName, "empty.bin")
        XCTAssertEqual(metadata.plainSize, 0)
    }

    func testDecryptRejectsWrongExternalAAD() throws {
        let plainURL = tempURL("plain")
        let encryptedURL = tempURL("wmenc")
        let decryptedURL = tempURL("decrypted")
        defer { cleanup([plainURL, encryptedURL, decryptedURL]) }
        try Data("content".utf8).write(to: plainURL)
        try service.encrypt(
            plaintextURL: plainURL,
            encryptedURL: encryptedURL,
            metadata: FileEncryptionMetadata(originalFileName: "IMG_1234.HEIC"),
            keyMaterial: try keyMaterial,
            externalAAD: externalAAD
        )

        XCTAssertThrowsError(try service.decrypt(
            encryptedURL: encryptedURL,
            plaintextURL: decryptedURL,
            keyMaterial: try keyMaterial,
            externalAAD: Data("resource:other".utf8)
        )) { error in
            XCTAssertEqual(error as? FileEncryptionError, .authenticationFailed)
        }
    }

    func testDecryptRejectsTamperedCiphertext() throws {
        let plainURL = tempURL("plain")
        let encryptedURL = tempURL("wmenc")
        let decryptedURL = tempURL("decrypted")
        defer { cleanup([plainURL, encryptedURL, decryptedURL]) }
        try Data("content".utf8).write(to: plainURL)
        try service.encrypt(
            plaintextURL: plainURL,
            encryptedURL: encryptedURL,
            metadata: FileEncryptionMetadata(originalFileName: "IMG_1234.HEIC"),
            keyMaterial: try keyMaterial,
            externalAAD: externalAAD
        )
        var encrypted = try Data(contentsOf: encryptedURL)
        encrypted[encrypted.count - 1] ^= 0xff
        try encrypted.write(to: encryptedURL)

        XCTAssertThrowsError(try service.decrypt(
            encryptedURL: encryptedURL,
            plaintextURL: decryptedURL,
            keyMaterial: try keyMaterial,
            externalAAD: externalAAD
        )) { error in
            XCTAssertEqual(error as? FileEncryptionError, .authenticationFailed)
        }
    }

    func testDecryptRejectsTrailingCiphertext() throws {
        let plainURL = tempURL("plain")
        let encryptedURL = tempURL("wmenc")
        let decryptedURL = tempURL("decrypted")
        defer { cleanup([plainURL, encryptedURL, decryptedURL]) }
        try Data("content".utf8).write(to: plainURL)
        try service.encrypt(
            plaintextURL: plainURL,
            encryptedURL: encryptedURL,
            metadata: FileEncryptionMetadata(originalFileName: "IMG_1234.HEIC"),
            keyMaterial: try keyMaterial,
            externalAAD: externalAAD
        )
        let handle = try FileHandle(forWritingTo: encryptedURL)
        try handle.seekToEnd()
        try handle.write(contentsOf: Data([0x00]))
        try handle.close()

        XCTAssertThrowsError(try service.decrypt(
            encryptedURL: encryptedURL,
            plaintextURL: decryptedURL,
            keyMaterial: try keyMaterial,
            externalAAD: externalAAD
        )) { error in
            XCTAssertEqual(error as? FileEncryptionError, .trailingCiphertext)
        }
    }

    func testDecryptRejectsWrongKeyIDBeforeTryingContent() throws {
        let plainURL = tempURL("plain")
        let encryptedURL = tempURL("wmenc")
        let decryptedURL = tempURL("decrypted")
        defer { cleanup([plainURL, encryptedURL, decryptedURL]) }
        try Data("content".utf8).write(to: plainURL)
        try service.encrypt(
            plaintextURL: plainURL,
            encryptedURL: encryptedURL,
            metadata: FileEncryptionMetadata(originalFileName: "IMG_1234.HEIC"),
            keyMaterial: try keyMaterial,
            externalAAD: externalAAD
        )
        let wrongKey = try RepoEncryptionKeyMaterial(
            repoID: "repo-123",
            keyID: "other-key",
            keyData: Data(repeating: 0x11, count: RepoEncryptionKeyMaterial.byteCount)
        )

        XCTAssertThrowsError(try service.decrypt(
            encryptedURL: encryptedURL,
            plaintextURL: decryptedURL,
            keyMaterial: wrongKey,
            externalAAD: externalAAD
        )) { error in
            XCTAssertEqual(error as? FileEncryptionError, .invalidKey)
        }
    }

    private func tempURL(_ suffix: String) -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension(suffix)
    }

    private func cleanup(_ urls: [URL]) {
        for url in urls {
            try? FileManager.default.removeItem(at: url)
        }
    }
}
