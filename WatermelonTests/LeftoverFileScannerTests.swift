import XCTest
@testable import Watermelon

final class LeftoverFileScannerTests: XCTestCase {
    private let base = "/base"
    private let month = LibraryMonthKey(year: 2024, month: 3)

    private func monthPath() -> String {
        LeftoverFileScanner.monthDataPath(basePath: base, month: month)
    }

    private func target(_ name: String, size: Int64 = 10) -> LeftoverFile {
        target(month, name, size: size)
    }

    private func target(_ month: LibraryMonthKey, _ name: String, size: Int64 = 10) -> LeftoverFile {
        let path = LeftoverFileScanner.monthDataPath(basePath: base, month: month) + "/" + name
        return LeftoverFile(month: month, fileName: name, path: path, size: size)
    }

    private func seed(_ client: InMemoryRemoteStorageClient, _ month: LibraryMonthKey, _ name: String) async {
        await client.seedFile(path: LeftoverFileScanner.monthDataPath(basePath: base, month: month) + "/" + name, data: Data(count: 10))
    }

    private actor CallCounter {
        private(set) var count = 0
        func bump() { count += 1 }
    }

    private func makeScanner(
        client: InMemoryRemoteStorageClient,
        months: [LibraryMonthKey],
        manifestNames: @escaping LeftoverFileScanner.ManifestNamesProvider
    ) -> LeftoverFileScanner {
        LeftoverFileScanner(client: client, basePath: base, months: months, manifestNames: manifestNames)
    }

    // listing − manifest = leftover
    func testScanReportsFilesAbsentFromManifest() async throws {
        let client = InMemoryRemoteStorageClient()
        for name in ["a.jpg", "b.jpg", "c.jpg"] {
            await client.seedFile(path: monthPath() + "/" + name, data: Data(count: 10))
        }
        let scanner = makeScanner(client: client, months: [month]) { _ in ["a.jpg", "b.jpg"] }

        let result = try await scanner.scan()

        XCTAssertEqual(result.totalCount, 1)
        XCTAssertEqual(result.allFiles.map(\.fileName), ["c.jpg"])
    }

    func testScanReportsNothingWhenManifestCoversListing() async throws {
        let client = InMemoryRemoteStorageClient()
        for name in ["a.jpg", "b.jpg"] {
            await client.seedFile(path: monthPath() + "/" + name, data: Data(count: 10))
        }
        let scanner = makeScanner(client: client, months: [month]) { _ in ["a.jpg", "b.jpg"] }

        let result = try await scanner.scan()

        XCTAssertEqual(result.totalCount, 0)
        XCTAssertTrue(result.groups.isEmpty)
    }

    // A month whose manifest can't be established (nil) is skipped — its data files are never leftover files.
    func testScanSkipsMonthWithoutManifest() async throws {
        let client = InMemoryRemoteStorageClient()
        for name in ["a.jpg", "b.jpg"] {
            await client.seedFile(path: monthPath() + "/" + name, data: Data(count: 10))
        }
        let scanner = makeScanner(client: client, months: [month]) { _ in nil }

        let result = try await scanner.scan()

        XCTAssertEqual(result.totalCount, 0)
    }

    // Directory entries and the manifest sibling are never leftover candidates.
    func testScanExcludesDirectoriesAndManifestFile() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: monthPath() + "/a.jpg", data: Data(count: 10))
        await client.seedFile(path: monthPath() + "/b.jpg", data: Data(count: 10))
        await client.seedFile(path: monthPath() + "/" + MonthManifestStore.manifestFileName, data: Data(count: 5))
        await client.seedDirectory(monthPath() + "/nested")

        let scanner = makeScanner(client: client, months: [month]) { _ in ["a.jpg"] }

        let result = try await scanner.scan()

        XCTAssertEqual(result.allFiles.map(\.fileName), ["b.jpg"])
    }

    func testDeleteRemovesConfirmedLeftover() async throws {
        let client = InMemoryRemoteStorageClient()
        for name in ["a.jpg", "b.jpg", "c.jpg"] {
            await client.seedFile(path: monthPath() + "/" + name, data: Data(count: 10))
        }
        let scanner = makeScanner(client: client, months: [month]) { _ in ["a.jpg", "b.jpg"] }

        let result = try await scanner.delete([target("c.jpg")], assertOwnership: nil)

        XCTAssertEqual(result.deletedCount, 1)
        XCTAssertEqual(result.failedCount, 0)
        let deleted = await client.deletedPaths
        XCTAssertEqual(deleted, [monthPath() + "/c.jpg"])
    }

    // A file recorded by the manifest between scan and delete is no longer a leftover and must be kept.
    func testDeleteSkipsFileNowRecordedByManifest() async throws {
        let client = InMemoryRemoteStorageClient()
        for name in ["a.jpg", "b.jpg", "c.jpg"] {
            await client.seedFile(path: monthPath() + "/" + name, data: Data(count: 10))
        }
        // Manifest now records c.jpg too — it is no longer a leftover.
        let scanner = makeScanner(client: client, months: [month]) { _ in ["a.jpg", "b.jpg", "c.jpg"] }

        let result = try await scanner.delete([target("c.jpg")], assertOwnership: nil)

        XCTAssertEqual(result.deletedCount, 0)
        XCTAssertEqual(result.failedCount, 1)
        let deleted = await client.deletedPaths
        XCTAssertTrue(deleted.isEmpty)
    }

    // A same-named unrecorded file replaced (different size) between scan and confirm must not be deleted —
    // the user reviewed the old bytes.
    func testDeleteSkipsFileReplacedSinceScan() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: monthPath() + "/c.jpg", data: Data(count: 10))
        let scanner = makeScanner(client: client, months: [month]) { _ in [] }
        let reviewed = target("c.jpg", size: 10)
        // The file is swapped for different content (size 999) before the user confirms.
        await client.seedFile(path: monthPath() + "/c.jpg", data: Data(count: 999))

        let result = try await scanner.delete([reviewed], assertOwnership: nil)

        XCTAssertEqual(result.deletedCount, 0)
        XCTAssertEqual(result.failedCount, 1)
        let deleted = await client.deletedPaths
        XCTAssertTrue(deleted.isEmpty, "a file replaced since the scan must not be deleted")
    }

    // A backend that never reports a size lists 0 on both sides; 0 == 0 still deletes (name-only in effect).
    func testDeleteDeletesWhenBothSizesUnknown() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: monthPath() + "/c.jpg", data: Data(count: 0))
        let scanner = makeScanner(client: client, months: [month]) { _ in [] }

        let result = try await scanner.delete([target("c.jpg", size: 0)], assertOwnership: nil)

        XCTAssertEqual(result.deletedCount, 1)
        XCTAssertEqual(result.failedCount, 0)
    }

    // A size that disappears (reviewed 10 → listed 0) or appears (reviewed 0 → listed 999) signals a swapped
    // file and must fail closed, not delete.
    func testDeleteSkipsWhenSizeChangesToOrFromUnknown() async throws {
        let clientA = InMemoryRemoteStorageClient()
        await clientA.seedFile(path: monthPath() + "/c.jpg", data: Data(count: 10))
        let scannerA = makeScanner(client: clientA, months: [month]) { _ in [] }
        await clientA.seedFile(path: monthPath() + "/c.jpg", data: Data(count: 0))
        let resultA = try await scannerA.delete([target("c.jpg", size: 10)], assertOwnership: nil)
        XCTAssertEqual(resultA.deletedCount, 0)
        XCTAssertEqual(resultA.failedCount, 1)
        let deletedA = await clientA.deletedPaths
        XCTAssertTrue(deletedA.isEmpty)

        let clientB = InMemoryRemoteStorageClient()
        await clientB.seedFile(path: monthPath() + "/c.jpg", data: Data(count: 0))
        let scannerB = makeScanner(client: clientB, months: [month]) { _ in [] }
        await clientB.seedFile(path: monthPath() + "/c.jpg", data: Data(count: 999))
        let resultB = try await scannerB.delete([target("c.jpg", size: 0)], assertOwnership: nil)
        XCTAssertEqual(resultB.deletedCount, 0)
        XCTAssertEqual(resultB.failedCount, 1)
        let deletedB = await clientB.deletedPaths
        XCTAssertTrue(deletedB.isEmpty)
    }

    // The catastrophic-deletion guard: a nil manifest at delete time must delete nothing, not everything.
    func testDeleteSkipsEntireMonthWhenManifestUnavailable() async throws {
        let client = InMemoryRemoteStorageClient()
        for name in ["a.jpg", "b.jpg"] {
            await client.seedFile(path: monthPath() + "/" + name, data: Data(count: 10))
        }
        let scanner = makeScanner(client: client, months: [month]) { _ in nil }

        let result = try await scanner.delete([target("a.jpg"), target("b.jpg")], assertOwnership: nil)

        XCTAssertEqual(result.deletedCount, 0)
        XCTAssertEqual(result.failedCount, 2)
        let deleted = await client.deletedPaths
        XCTAssertTrue(deleted.isEmpty)
    }

    func testDeleteStopsWhenOwnershipAssertionFails() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: monthPath() + "/c.jpg", data: Data(count: 10))
        let scanner = makeScanner(client: client, months: [month]) { _ in ["a.jpg"] }

        struct OwnershipLost: Error {}
        do {
            _ = try await scanner.delete([target("c.jpg")]) { throw OwnershipLost() }
            XCTFail("expected ownership assertion to abort the delete")
        } catch is OwnershipLost {
            // expected
        }

        let deleted = await client.deletedPaths
        XCTAssertTrue(deleted.isEmpty, "no file may be deleted once ownership cannot be proven")
    }

    // Ownership is re-proven before EVERY irreversible delete, not once per batch/month.
    func testDeleteAssertsOwnershipBeforeEachFile() async throws {
        let client = InMemoryRemoteStorageClient()
        for name in ["a.jpg", "b.jpg", "c.jpg"] {
            await client.seedFile(path: monthPath() + "/" + name, data: Data(count: 10))
        }
        let scanner = makeScanner(client: client, months: [month]) { _ in [] }
        let counter = CallCounter()

        let result = try await scanner.delete(
            [target("a.jpg"), target("b.jpg"), target("c.jpg")],
            assertOwnership: { await counter.bump() }
        )

        XCTAssertEqual(result.deletedCount, 3)
        let calls = await counter.count
        XCTAssertEqual(calls, 3, "ownership must be proven once per deleted file")
    }

    // A case-/Unicode-variant of a recorded file must never be deleted (case-insensitive backends).
    func testScanFoldsCaseAndDiacriticsAgainstManifest() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: monthPath() + "/photo.jpg", data: Data(count: 10))
        await client.seedFile(path: monthPath() + "/café.jpg", data: Data(count: 10))
        let scanner = makeScanner(client: client, months: [month]) { _ in ["Photo.JPG", "cafe.jpg"] }

        let result = try await scanner.scan()

        XCTAssertEqual(result.totalCount, 0)
    }

    func testScanAndDeleteSpanMultipleMonths() async throws {
        let m1 = LibraryMonthKey(year: 2024, month: 1)
        let m2 = LibraryMonthKey(year: 2024, month: 2)
        let client = InMemoryRemoteStorageClient()
        await seed(client, m1, "x.jpg"); await seed(client, m1, "keep1.jpg")
        await seed(client, m2, "y.jpg"); await seed(client, m2, "keep2.jpg")
        let manifest: [LibraryMonthKey: Set<String>] = [m1: ["keep1.jpg"], m2: ["keep2.jpg"]]
        let scanner = LeftoverFileScanner(client: client, basePath: base, months: [m1, m2]) { manifest[$0] }

        let scan = try await scanner.scan()
        XCTAssertEqual(Set(scan.allFiles.map(\.fileName)), ["x.jpg", "y.jpg"])

        let del = try await scanner.delete(scan.allFiles, assertOwnership: nil)
        XCTAssertEqual(del.deletedCount, 2)
        XCTAssertEqual(del.failedCount, 0)
        let deleted = await client.deletedPaths
        XCTAssertEqual(deleted.count, 2)
    }

    // A month whose manifest faults mid-delete is skipped (its targets fail) but other months proceed.
    func testDeleteContinuesPastAFaultingMonth() async throws {
        let m1 = LibraryMonthKey(year: 2024, month: 1)
        let m2 = LibraryMonthKey(year: 2024, month: 2)
        let client = InMemoryRemoteStorageClient()
        await seed(client, m1, "a.jpg")
        await seed(client, m2, "b.jpg")
        let scanner = LeftoverFileScanner(client: client, basePath: base, months: [m1, m2]) { month in
            if month == m1 { throw RemoteErrorFixtures.retryable }
            return []
        }

        let result = try await scanner.delete([target(m1, "a.jpg"), target(m2, "b.jpg")], assertOwnership: nil)

        XCTAssertEqual(result.deletedCount, 1)
        XCTAssertEqual(result.failedCount, 1)
        let deleted = await client.deletedPaths
        XCTAssertEqual(deleted, [LeftoverFileScanner.monthDataPath(basePath: base, month: m2) + "/b.jpg"])
    }

    // Mixed batch: some deleted, one no-longer-leftover; counts must sum to the target count.
    func testDeleteMixedOutcomeAccounting() async throws {
        let client = InMemoryRemoteStorageClient()
        for name in ["a.jpg", "b.jpg", "c.jpg"] {
            await client.seedFile(path: monthPath() + "/" + name, data: Data(count: 10))
        }
        let scanner = makeScanner(client: client, months: [month]) { _ in ["b.jpg"] }

        let result = try await scanner.delete(
            [target("a.jpg"), target("b.jpg"), target("c.jpg")],
            assertOwnership: nil
        )

        XCTAssertEqual(result.deletedCount, 2)
        XCTAssertEqual(result.failedCount, 1)
        XCTAssertEqual(result.deletedCount + result.failedCount, 3)
    }

    // A non-notFound fault from the manifest provider aborts the whole scan (fail closed).
    func testScanPropagatesManifestFault() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: monthPath() + "/a.jpg", data: Data(count: 10))
        let scanner = makeScanner(client: client, months: [month]) { _ in throw RemoteErrorFixtures.retryable }

        do {
            _ = try await scanner.scan()
            XCTFail("expected scan to fail closed on a transport fault")
        } catch {
            // expected
        }
    }
}
