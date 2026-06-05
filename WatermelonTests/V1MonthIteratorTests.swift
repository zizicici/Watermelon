import XCTest
@testable import Watermelon

/// Differential coverage for the shared V1 `base → year → month` traversal that remote-format detection,
/// migration scan, and the V1 digest scan now share. Pins the `YYYY/MM` domain, ordering, and the two
/// list-failure policies (tolerant detection vs. strict scan) so they can't drift between callers.
final class V1MonthIteratorTests: XCTestCase {
    private let basePath = "/repo"

    private func makeClient() async -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        try? await client.connect()
        return client
    }

    private func collectMonths(
        client: InMemoryRemoteStorageClient,
        options: V1MonthIterator.Options,
        baseEntries: [RemoteStorageEntry]? = nil
    ) async throws -> [(year: Int, month: Int)] {
        var collected: [(year: Int, month: Int)] = []
        try await V1MonthIterator.forEachMonth(
            client: client,
            basePath: basePath,
            options: options,
            baseEntries: baseEntries
        ) { year, month, _ in
            collected.append((year, month))
            return .continue
        }
        return collected
    }

    func testForEachMonth_yieldsValidMonthsAscending() async throws {
        let client = await makeClient()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2024, month: 1)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2024, month: 12)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)

        let months = try await collectMonths(
            client: client,
            options: .init(listFailurePolicy: .propagate, yearOrder: .ascending, monthOrder: .ascending)
        )
        XCTAssertEqual(months.map { "\($0.year)-\($0.month)" }, ["2024-1", "2024-12", "2025-6"])
    }

    func testForEachMonth_descendingYearOrder() async throws {
        let client = await makeClient()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2024, month: 5)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)

        let months = try await collectMonths(
            client: client,
            options: .init(listFailurePolicy: .propagate, yearOrder: .descending, monthOrder: .ascending)
        )
        XCTAssertEqual(months.map { $0.year }, [2025, 2024], "descending year order must visit newest first")
    }

    func testForEachMonth_skipsOutOfRangeAndMalformedDirs() async throws {
        let client = await makeClient()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2023, month: 13)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2023, month: 0)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2024, month: 6)
        // Non-year directories that must be ignored.
        try await client.createDirectory(path: "\(basePath)/abcd")
        try await client.createDirectory(path: "\(basePath)/202")
        try await client.createDirectory(path: "\(basePath)/20255")

        let months = try await collectMonths(
            client: client,
            options: .init(listFailurePolicy: .propagate, yearOrder: .ascending, monthOrder: .ascending)
        )
        XCTAssertEqual(months.map { "\($0.year)-\($0.month)" }, ["2024-6"],
                       "out-of-range months (00/13) and non-4-digit year dirs must be skipped")
    }

    func testForEachMonth_minYearExcludesPre1900() async throws {
        let client = await makeClient()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 1899, month: 6)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 1900, month: 6)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)

        let withMin = try await collectMonths(
            client: client,
            options: .init(listFailurePolicy: .propagate, yearOrder: .ascending, monthOrder: .ascending, minYear: 1900)
        )
        XCTAssertEqual(withMin.map { $0.year }, [1900, 2025], "minYear:1900 must exclude the 1899 dir (digest-scan domain)")

        let withoutMin = try await collectMonths(
            client: client,
            options: .init(listFailurePolicy: .propagate, yearOrder: .ascending, monthOrder: .ascending)
        )
        XCTAssertEqual(withoutMin.map { $0.year }, [1899, 1900, 2025], "default minYear keeps any 4-digit year")
    }

    func testForEachMonth_skipMissingToleratesVanishedYearDir() async throws {
        let client = await makeClient()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)
        // Year dir is listed in the base enumeration but 404s on the follow-up list.
        await client.injectListError(.notFound, for: "\(basePath)/2025")

        let months = try await collectMonths(client: client, options: .init(listFailurePolicy: .skipMissing))
        XCTAssertTrue(months.isEmpty, "skipMissing must treat a vanished year dir as empty, not throw")
    }

    func testForEachMonth_propagateSurfacesYearListError() async throws {
        let client = await makeClient()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)
        await client.injectListError(.transport, for: "\(basePath)/2025")

        do {
            _ = try await collectMonths(client: client, options: .init(listFailurePolicy: .propagate))
            XCTFail("expected transport list error to propagate under .propagate")
        } catch {
            XCTAssertFalse(isStorageNotFoundError(error))
        }
    }

    func testForEachMonth_stopEndsTraversalEarly() async throws {
        let client = await makeClient()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2024, month: 1)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)

        var visited: [(Int, Int)] = []
        try await V1MonthIterator.forEachMonth(
            client: client,
            basePath: basePath,
            options: .init(listFailurePolicy: .propagate, yearOrder: .ascending, monthOrder: .ascending)
        ) { year, month, _ in
            visited.append((year, month))
            return .stop
        }
        XCTAssertEqual(visited.count, 1, "returning .stop must end traversal after the first month (detection's first-hit short-circuit)")
        XCTAssertEqual(visited.first.map { "\($0.0)-\($0.1)" }, "2024-1")
    }

    func testForEachMonth_baseEntriesSeedBypassesBaseList() async throws {
        let client = await makeClient()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)
        let baseEntries = try await client.list(path: basePath)
        // Poison the base list — passing baseEntries means the iterator must not re-list base.
        await client.injectListError(.transport, for: basePath)

        let months = try await collectMonths(
            client: client,
            options: .init(listFailurePolicy: .propagate, yearOrder: .ascending, monthOrder: .ascending),
            baseEntries: baseEntries
        )
        XCTAssertEqual(months.map { "\($0.year)-\($0.month)" }, ["2025-6"],
                       "baseEntries seed must bypass the (poisoned) base list")
    }

    func testMonthContainsManifest_detectsPresenceAndAbsence() async throws {
        let client = await makeClient()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)
        try await client.createDirectory(path: "\(basePath)/2025/07")

        let present = try await V1MonthIterator.monthContainsManifest(
            client: client, monthPath: "\(basePath)/2025/06", listFailurePolicy: .propagate
        )
        XCTAssertTrue(present)

        let absent = try await V1MonthIterator.monthContainsManifest(
            client: client, monthPath: "\(basePath)/2025/07", listFailurePolicy: .propagate
        )
        XCTAssertFalse(absent, "a month dir without the manifest file must report false")
    }

    func testMonthContainsManifest_skipMissingTreatsVanishedDirAsAbsent() async throws {
        let client = await makeClient()
        let result = try await V1MonthIterator.monthContainsManifest(
            client: client, monthPath: "\(basePath)/2025/08", listFailurePolicy: .skipMissing
        )
        XCTAssertFalse(result, "skipMissing must treat a missing month dir as no-manifest, not throw")
    }

    func testMonthContainsManifest_propagateSurfacesListError() async throws {
        let client = await makeClient()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)
        await client.injectListError(.transport, for: "\(basePath)/2025/06")

        do {
            _ = try await V1MonthIterator.monthContainsManifest(
                client: client, monthPath: "\(basePath)/2025/06", listFailurePolicy: .propagate
            )
            XCTFail("expected transport error to propagate under .propagate")
        } catch {
            XCTAssertFalse(isStorageNotFoundError(error))
        }
    }
}
