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
}
