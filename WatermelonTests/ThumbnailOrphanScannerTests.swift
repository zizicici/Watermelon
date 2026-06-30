import XCTest
@testable import Watermelon

final class ThumbnailOrphanScannerTests: XCTestCase {
    private let basePath = "/repo"

    // A valid 64-hex fingerprint whose first two chars (the shard) are `prefix`.
    private func fingerprint(_ prefix: String) -> String {
        prefix + String(repeating: "0", count: 64 - prefix.count)
    }

    func testFingerprintHexParsing() {
        let valid = fingerprint("ab") // 64-char lowercase hex
        XCTAssertEqual(ThumbnailOrphanScanner.fingerprintHex(fromFileName: valid + ".jpg"), valid)
        XCTAssertNil(ThumbnailOrphanScanner.fingerprintHex(fromFileName: "abcd.jpg"))     // too short
        XCTAssertNil(ThumbnailOrphanScanner.fingerprintHex(fromFileName: valid + ".png")) // wrong extension
        XCTAssertNil(ThumbnailOrphanScanner.fingerprintHex(fromFileName: ".jpg"))         // empty stem
        XCTAssertNil(ThumbnailOrphanScanner.fingerprintHex(fromFileName: "README.jpg"))   // non-hex stem
        XCTAssertNil(ThumbnailOrphanScanner.fingerprintHex(
            fromFileName: String(repeating: "A", count: 64) + ".jpg"
        )) // uppercase rejected (our naming is lowercase)
    }

    func testScanFindsOnlyFingerprintsNotInLiveSet() async throws {
        let client = InMemoryRemoteStorageClient()
        let live = fingerprint("aa")
        let orphanA = fingerprint("bb")
        let orphanB = fingerprint("cc")
        for hex in [live, orphanA, orphanB] {
            await client.seedFile(
                path: RemoteThumbnailPaths.absolutePath(basePath: basePath, fingerprintHex: hex),
                data: Data([1, 2, 3])
            )
        }

        let scanner = ThumbnailOrphanScanner(client: client, basePath: basePath, liveFingerprintHexes: [live])
        let result = try await scanner.scan()

        XCTAssertEqual(Set(result.orphans.map(\.fingerprintHex)), [orphanA, orphanB])
        XCTAssertEqual(result.count, 2)
        XCTAssertEqual(result.totalBytes, 6)
    }

    func testScanIsEmptyWhenThumbnailDirAbsent() async throws {
        let client = InMemoryRemoteStorageClient()
        let scanner = ThumbnailOrphanScanner(client: client, basePath: basePath, liveFingerprintHexes: [])
        let result = try await scanner.scan()
        XCTAssertTrue(result.orphans.isEmpty)
    }

    func testDeleteReVerifiesAndKeepsThumbnailsThatBecameLive() async throws {
        let client = InMemoryRemoteStorageClient()
        let nowLive = fingerprint("dd")
        let stillOrphan = fingerprint("ee")
        let targets = [nowLive, stillOrphan].map { hex in
            ThumbnailOrphan(
                fingerprintHex: hex,
                path: RemoteThumbnailPaths.absolutePath(basePath: basePath, fingerprintHex: hex),
                size: 3
            )
        }
        for target in targets {
            await client.seedFile(path: target.path, data: Data([1, 2, 3]))
        }

        // `nowLive` reappeared in the manifest since the scan → the fresh live set keeps it.
        let scanner = ThumbnailOrphanScanner(client: client, basePath: basePath, liveFingerprintHexes: [nowLive])
        let result = try await scanner.delete(targets, assertOwnership: nil)

        XCTAssertEqual(result.deletedCount, 1)
        XCTAssertEqual(result.failedCount, 1)
        let deleted = await client.deletedPaths
        XCTAssertEqual(
            deleted,
            [RemoteThumbnailPaths.absolutePath(basePath: basePath, fingerprintHex: stillOrphan)]
        )
    }
}
