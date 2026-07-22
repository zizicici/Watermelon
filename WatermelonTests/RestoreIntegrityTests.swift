import XCTest
@testable import Watermelon

// Regression for restore content-integrity: a download that completes but delivers wrong bytes must fail
// verification before import / hash-index write, never be silently accepted as matching the remote.
final class RestoreIntegrityTests: XCTestCase {
    private var tempURL: URL!
    private var expectedFileSize: Int64!

    override func setUpWithError() throws {
        tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("restore_integrity_\(UUID().uuidString).bin")
        try Data("watermelon-restore-bytes".utf8).write(to: tempURL)
        expectedFileSize = Int64(try XCTUnwrap(
            (FileManager.default.attributesOfItem(atPath: tempURL.path)[.size] as? NSNumber)?.int64Value
        ))
    }

    override func tearDownWithError() throws {
        if let tempURL { try? FileManager.default.removeItem(at: tempURL) }
    }

    private func instance(
        resourceHash: Data,
        fileName: String = "IMG_0001.JPG",
        fileSize: Int64? = nil,
        remoteRelativePath: String? = nil
    ) -> RemoteAssetResourceInstance {
        RemoteAssetResourceInstance(
            role: 1,
            slot: 0,
            resourceHash: resourceHash,
            fileName: fileName,
            fileSize: fileSize ?? expectedFileSize,
            remoteRelativePath: remoteRelativePath ?? "2026/06/\(fileName)",
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

    func testMatchingHashIsAuthoritativeWhenLegacySizeIsWrong() throws {
        let correctHash = try AssetProcessor.contentHash(of: tempURL)
        XCTAssertNoThrow(
            try RestoreService.verifyDownloadedResource(
                at: tempURL,
                instance: instance(resourceHash: correctHash, fileSize: expectedFileSize + 1)
            )
        )
    }

    // A legacy manifest entry with no recorded hash must not break restore — verification is skipped.
    func testEmptyHashSkipsVerification() throws {
        XCTAssertNoThrow(
            try RestoreService.verifyDownloadedResource(at: tempURL, instance: instance(resourceHash: Data()))
        )
    }

    func testMismatchedSizeThrowsBeforeImport() throws {
        XCTAssertThrowsError(
            try RestoreService.verifyDownloadedResource(
                at: tempURL,
                instance: instance(resourceHash: Data(), fileSize: expectedFileSize + 1)
            )
        ) { error in
            guard case RestoreIntegrityError.fileSizeMismatch = error else {
                return XCTFail("expected fileSizeMismatch, got \(error)")
            }
        }
    }

    func testRestoreResourceRejectsPathTraversalAndInvalidManifestShape() {
        XCTAssertFalse(RestoreService.isSafeRestoreResource(instance(
            resourceHash: Data(),
            fileName: "../../outside.jpg",
            remoteRelativePath: "2026/06/../../outside.jpg"
        )))
        XCTAssertFalse(RestoreService.isSafeRestoreResource(instance(
            resourceHash: Data(),
            remoteRelativePath: "2026/13/IMG_0001.JPG"
        )))
        XCTAssertTrue(RestoreService.isSafeRestoreResource(instance(resourceHash: Data())))
    }

    func testRestoreTemporaryURLNeverUsesRemotePathComponents() {
        let url = RestoreService.makeTemporaryRestoreURL(fileName: "../../outside.JPG")
        XCTAssertEqual(
            url.deletingLastPathComponent().standardizedFileURL,
            FileManager.default.temporaryDirectory.standardizedFileURL
        )
        XCTAssertTrue(url.lastPathComponent.hasPrefix("restore_"))
        XCTAssertEqual(url.pathExtension, "JPG")
        XCTAssertFalse(url.lastPathComponent.contains("outside"))
        XCTAssertEqual(RestoreService.safeOriginalFileName("../../outside.JPG"), "outside.JPG")
        XCTAssertEqual(RestoreService.safeOriginalFileName("~photo.jpg"), "~photo.jpg")
    }

}
