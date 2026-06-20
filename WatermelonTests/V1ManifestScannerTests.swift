import XCTest
@testable import Watermelon

// P09 Track B Phase 7: the shared V1 manifest scanner that cleanup, routing, migration, and remote index
// sync delegate to. Pins deterministic sorted traversal, invalid-directory rejection, strict fault
// surfacing, and candidate-manifest notFound-as-absence skipping.
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
        await client.seedFile(path: "\(basePath)/202/03/\(MonthManifestStore.manifestFileName)")     // wrong width

        let found = try await V1ManifestScanner(client: client, basePath: basePath).scan()

        XCTAssertEqual(found.map(\.month), [LibraryMonthKey(year: 2024, month: 3)],
                       "only canonical YYYY/MM directories are scanned")
    }

    // LibraryMonthKey.from(date:) has no lower year bound, so a pre-1900 capture date produces a real
    // <1900/MM V1 backup. The scanner must find it (matching RepoLayoutLite.parseMonthKey) so V1→Lite
    // migration / router detection / V1 sync never silently orphan that already-backed-up data.
    func testPre1900YearDirectoriesAreScanned() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: manifestPath(1850, 6))
        await client.seedFile(path: manifestPath(2024, 3))

        let found = try await V1ManifestScanner(client: client, basePath: basePath).scan()

        XCTAssertEqual(found.map(\.month), [
            LibraryMonthKey(year: 1850, month: 6),
            LibraryMonthKey(year: 2024, month: 3)
        ], "pre-1900 V1 months are producible and must be scanned, not dropped")
    }

    func testParseYearAcceptsAnyFourDigitYear() {
        XCTAssertEqual(V1ManifestScanner.parseYear("1850"), 1850)
        XCTAssertEqual(V1ManifestScanner.parseYear("1899"), 1899)
        XCTAssertEqual(V1ManifestScanner.parseYear("2024"), 2024)
        XCTAssertNil(V1ManifestScanner.parseYear("202"), "wrong width is still rejected")
        XCTAssertNil(V1ManifestScanner.parseYear("abcd"), "non-numeric is still rejected")
    }

    // Int() accepts a leading sign ("-001" -> -1, "+123" -> 123, "+1" -> 1) but RepoLayoutLite requires
    // ASCII digits, so a signed dir would migrate into a Lite month the layout cannot round-trip (or
    // normalize a foreign dir onto a real month). The scan boundary must reject signed names.
    func testSignedYearAndMonthNamesRejected() {
        XCTAssertNil(V1ManifestScanner.parseYear("-001"), "Int() would accept -001 (=-1); the Lite layout cannot round-trip it")
        XCTAssertNil(V1ManifestScanner.parseYear("+123"), "a leading plus is not a canonical V1 year directory")
        XCTAssertNil(V1ManifestScanner.parseMonth("+1"), "Int() would accept +1 (=1); reject the signed name")
        XCTAssertNil(V1ManifestScanner.parseMonth("-1"), "negative months are rejected")
        XCTAssertEqual(V1ManifestScanner.parseMonth("01"), 1, "canonical zero-padded months still parse")
        XCTAssertEqual(V1ManifestScanner.parseMonth("12"), 12)
    }

    func testSignedDirectoryNamesIgnoredDuringScan() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: manifestPath(2024, 3))                                                 // valid sibling
        await client.seedFile(path: "\(basePath)/-001/01/\(MonthManifestStore.manifestFileName)")          // signed year
        await client.seedFile(path: "\(basePath)/2024/+1/\(MonthManifestStore.manifestFileName)")           // signed month

        let found = try await V1ManifestScanner(client: client, basePath: basePath).scan()

        XCTAssertEqual(found.map(\.month), [LibraryMonthKey(year: 2024, month: 3)],
                       "only canonical ASCII-digit YYYY/MM directories migrate; signed names Int() would accept are skipped")
    }

    // A directory occupying the candidate V1 manifest slot is damaged/foreign control state. The default
    // scan (router / read-index) skips it and keeps scanning; the migration's strict scan fails closed so the
    // month is never silently dropped before version.json commits.
    func testDirectoryValuedCandidateSkippedByDefaultButFailsClosedForMigration() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: manifestPath(2024, 1))       // a real file manifest sibling
        await client.seedDirectory(manifestPath(2024, 2))        // a directory occupying the manifest slot

        let found = try await V1ManifestScanner(client: client, basePath: basePath).scan()
        XCTAssertEqual(found.map(\.month), [LibraryMonthKey(year: 2024, month: 1)],
                       "the directory-valued candidate is skipped by default so router/read-index keep scanning")

        do {
            _ = try await V1ManifestScanner(client: client, basePath: basePath).scan(failOnDirectoryCandidate: true)
            XCTFail("a directory-valued candidate must fail the migration scan closed")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .v1MonthManifestUnreadable(month: "2024-02"))
        }
    }

    // Router V1 detection: a readable file manifest is decisive (validManifest, even with a directory
    // sibling); a directory-only candidate is damaged control state (directoryCandidateOnly); nothing is none.
    func testV1EvidenceDistinguishesValidDirectoryOnlyAndNone() async throws {
        let withFile = InMemoryRemoteStorageClient()
        await withFile.seedFile(path: manifestPath(2024, 1))
        await withFile.seedDirectory(manifestPath(2024, 2))
        let fileEvidence = try await V1ManifestScanner(client: withFile, basePath: basePath).v1Evidence()
        XCTAssertEqual(fileEvidence, .validManifest, "a readable file manifest is decisive over a directory sibling")

        let dirOnly = InMemoryRemoteStorageClient()
        await dirOnly.seedDirectory(manifestPath(2024, 2))
        let dirEvidence = try await V1ManifestScanner(client: dirOnly, basePath: basePath).v1Evidence()
        XCTAssertEqual(dirEvidence, .directoryCandidateOnly, "a directory-only candidate is damaged V1 control state")

        let empty = InMemoryRemoteStorageClient()
        await empty.seedDirectory(basePath)
        let noneEvidence = try await V1ManifestScanner(client: empty, basePath: basePath).v1Evidence()
        XCTAssertEqual(noneEvidence, .none, "an empty base has no V1 evidence")
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

    func testAbsentBaseSurfacesByDefault() async {
        let client = InMemoryRemoteStorageClient()   // nothing seeded → base list is notFound

        do {
            _ = try await V1ManifestScanner(client: client, basePath: basePath).scan()
            XCTFail("a missing base must surface by default so sync/migration never reads a probe fault as zero months")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .notFound)
        }
    }

    func testAbsentBaseCanBeReadAsEmptyForFreshProbe() async throws {
        let client = InMemoryRemoteStorageClient()

        let found = try await V1ManifestScanner(client: client, basePath: basePath).scan(missingBaseIsEmpty: true)

        XCTAssertTrue(found.isEmpty, "fresh-repo probes may explicitly treat an absent base as zero months")
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
