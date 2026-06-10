import XCTest
@testable import Watermelon

// P09 Track B Phase 7: the shared V1 manifest scanner that cleanup, routing, migration, and remote index
// sync delegate to. Pins deterministic sorted traversal, invalid-directory rejection, strict non-notFound
// fault surfacing, and notFound-as-absence skipping.
final class V1ManifestScannerTests: XCTestCase {
    private let basePath = "/photos"

    private func manifestPath(_ year: Int, _ month: Int) -> String {
        "\(basePath)/\(String(format: "%04d/%02d", year, month))/\(MonthManifestStore.manifestFileName)"
    }

    func testValidMonthsFoundInSortedOrder() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: manifestPath(2024, 5))
        await client.seedFile(path: manifestPath(2023, 12))
        await client.seedFile(path: manifestPath(2024, 1))

        let found = try await V1ManifestScanner(client: client, basePath: basePath).scan()

        XCTAssertEqual(found.map(\.month), [
            LibraryMonthKey(year: 2023, month: 12),
            LibraryMonthKey(year: 2024, month: 1),
            LibraryMonthKey(year: 2024, month: 5)
        ], "manifests are returned in deterministic year-then-month order")
        XCTAssertEqual(
            found.map(\.manifestPath),
            [manifestPath(2023, 12), manifestPath(2024, 1), manifestPath(2024, 5)]
        )
    }

    func testInvalidYearAndMonthDirectoriesIgnored() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: manifestPath(2024, 3))                                            // valid
        await client.seedFile(path: "\(basePath)/abcd/03/\(MonthManifestStore.manifestFileName)")    // non-numeric year
        await client.seedFile(path: "\(basePath)/2024/13/\(MonthManifestStore.manifestFileName)")    // month out of range
        await client.seedFile(path: "\(basePath)/1899/12/\(MonthManifestStore.manifestFileName)")    // year < 1900
        await client.seedFile(path: "\(basePath)/202/03/\(MonthManifestStore.manifestFileName)")     // wrong width

        let found = try await V1ManifestScanner(client: client, basePath: basePath).scan()

        XCTAssertEqual(found.map(\.month), [LibraryMonthKey(year: 2024, month: 3)],
                       "only canonical YYYY/MM directories are scanned")
    }

    func testStrictNonNotFoundFaultSurfaces() async {
        let client = InMemoryRemoteStorageClient()
        let yearEntry = RemoteStorageEntry(
            path: "/photos/2024", name: "2024", isDirectory: true,
            size: 0, creationDate: nil, modificationDate: nil
        )
        await client.enqueueListResult([yearEntry])                   // base list surfaces a year dir
        await client.enqueueListError(RemoteErrorFixtures.retryable)  // year-dir list faults transiently

        do {
            _ = try await V1ManifestScanner(client: client, basePath: basePath).scan()
            XCTFail("a non-notFound fault during the scan must surface")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable,
                           "the transport fault must surface, never read as absence")
        }
    }

    func testNotFoundCandidateManifestIsSkipped() async throws {
        let client = InMemoryRemoteStorageClient()
        // A month directory exists (a sibling photo implies it) but holds no manifest.
        await client.seedFile(path: "\(basePath)/2024/03/IMG_0001.JPG")

        let found = try await V1ManifestScanner(client: client, basePath: basePath).scan()

        XCTAssertTrue(found.isEmpty, "a month dir with no manifest yields nothing — notFound candidate skipped")
    }

    func testAbsentBaseYieldsNoMonths() async throws {
        let client = InMemoryRemoteStorageClient()   // nothing seeded → base list is notFound

        let found = try await V1ManifestScanner(client: client, basePath: basePath).scan()

        XCTAssertTrue(found.isEmpty, "an absent base path is zero months, not a fault")
    }

    func testContainsManifestShortCircuits() async throws {
        let withManifest = InMemoryRemoteStorageClient()
        await withManifest.seedFile(path: manifestPath(2024, 3))
        let present = try await V1ManifestScanner(client: withManifest, basePath: basePath).containsManifest()
        XCTAssertTrue(present)

        let empty = InMemoryRemoteStorageClient()
        await empty.seedDirectory(basePath)
        let absent = try await V1ManifestScanner(client: empty, basePath: basePath).containsManifest()
        XCTAssertFalse(absent, "an empty base has no V1 manifests")
    }

    func testScanCarriesManifestSizeAndModificationDate() async throws {
        let client = InMemoryRemoteStorageClient()
        let modified = Date(timeIntervalSince1970: 1_700_000_000)
        await client.seedFile(path: manifestPath(2024, 3), data: Data([0x01, 0x02, 0x03]), modificationDate: modified)

        let found = try await V1ManifestScanner(client: client, basePath: basePath).scan()

        XCTAssertEqual(found.count, 1)
        XCTAssertEqual(found.first?.size, 3)
        XCTAssertEqual(found.first?.modificationDate, modified)
    }
}
