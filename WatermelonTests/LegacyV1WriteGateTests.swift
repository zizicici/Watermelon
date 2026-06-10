import XCTest
@testable import Watermelon

// P08 Phase 6: the shared compatibility boundary for legacy V1 writers (WatermelonMac migration import).
// Permits a V1 manifest write only against a clearly-V1 or clearly-fresh target; rejects committed Lite
// repos and unsupported/damaged/malformed Lite control trees before any V1 metadata write, and fails
// closed on a probe fault.
final class LegacyV1WriteGateTests: XCTestCase {
    private let basePath = "/photos"

    private func ensureWritable(_ client: InMemoryRemoteStorageClient) async -> Result<Void, Error> {
        do {
            try await LegacyV1WriteGate.ensureWritable(client: client, basePath: basePath)
            return .success(())
        } catch {
            return .failure(error)
        }
    }

    private func assertAllowed(_ client: InMemoryRemoteStorageClient, _ message: String) async {
        if case .failure(let error) = await ensureWritable(client) {
            XCTFail("\(message) — unexpected rejection: \(error)")
        }
    }

    private func assertRejected(
        _ client: InMemoryRemoteStorageClient,
        _ expected: LegacyV1WriteGate.Rejection,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async {
        switch await ensureWritable(client) {
        case .success:
            XCTFail("expected rejection \(expected)", file: file, line: line)
        case .failure(let error):
            XCTAssertEqual(error as? LegacyV1WriteGate.Rejection, expected, file: file, line: line)
        }
    }

    private func versionBytes(formatVersion: Int?, layout: String?) throws -> Data {
        try VersionManifestLite.encode(WatermelonRemoteVersionManifest(
            formatVersion: formatVersion, layout: layout, minAppVersion: "1.5.0",
            createdAt: "2026-01-01T00:00:00Z", createdBy: "seed"
        ))
    }

    // MARK: - Allowed (clearly fresh / clearly V1)

    func testFreshTargetIsAllowed() async {
        let client = InMemoryRemoteStorageClient()   // nothing here → .fresh
        await assertAllowed(client, "a fresh target permits legacy V1 import")
    }

    func testV1TargetIsAllowed() async {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(basePath)/2024/03/\(MonthManifestStore.manifestFileName)", data: Data([0x01]))
        await assertAllowed(client, "a legacy V1 tree permits legacy V1 import")
    }

    // MARK: - Rejected (committed Lite / unsupported / damaged / malformed)

    func testCommittedLiteRepoIsRejected() async throws {
        let client = InMemoryRemoteStorageClient()
        let manifest = VersionManifestLite.makeManifest(createdAt: "t", createdBy: "seed")
        await client.seedFile(path: RepoLayoutLite.versionPath(basePath: basePath), data: try VersionManifestLite.encode(manifest))
        await assertRejected(client, .committedLite)
    }

    func testMalformedVersionIsRejected() async {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: RepoLayoutLite.versionPath(basePath: basePath), data: Data("not json".utf8))
        await assertRejected(client, .damagedControlTree)
    }

    func testDamagedLiteMonthsRejected() async {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(basePath)/.watermelon/months/2024-03.sqlite", data: Data([0x01]))
        await assertRejected(client, .damagedControlTree)
    }

    func testUnsupportedFutureFormatRejected() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: RepoLayoutLite.versionPath(basePath: basePath), data: try versionBytes(formatVersion: 3, layout: "lite-month-sqlite"))
        await assertRejected(client, .unsupportedControlTree)
    }

    func testDevMarkerRejected() async {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/.watermelon/commits")
        await assertRejected(client, .unsupportedControlTree)
    }

    // MARK: - Probe fault fails closed

    func testProbeFaultFailsClosed() async {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueListError(RemoteErrorFixtures.retryable)   // base probe blinks
        await assertRejected(client, .probeFault(.retryable))
    }
}
