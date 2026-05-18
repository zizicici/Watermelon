import XCTest
@testable import Watermelon

final class RemoteIndexV1SyncEngineTests: XCTestCase {
    private let basePath = "/repo"
    private let month = LibraryMonthKey(year: 2025, month: 1)

    func testInitialSyncStagesFoundManifestMonth() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await seedManifest(client: client, month: month, marker: 0x11)

        let result = try await RemoteIndexV1SyncEngine().sync(
            client: client,
            basePath: basePath,
            previousDigests: [:],
            onSyncProgress: nil
        )

        XCTAssertEqual(Set(result.changedMonths.keys), [month])
        XCTAssertTrue(result.removedMonths.isEmpty)
        XCTAssertEqual(result.remoteMonthCount, 1)
    }

    func testNilManifestModifiedAtForcesChanged() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await seedManifest(client: client, month: month, marker: 0x33, setModifiedAt: false)

        let first = try await RemoteIndexV1SyncEngine().sync(
            client: client,
            basePath: basePath,
            previousDigests: [:],
            onSyncProgress: nil
        )
        let second = try await RemoteIndexV1SyncEngine().sync(
            client: client,
            basePath: basePath,
            previousDigests: first.effectiveRemoteDigests,
            onSyncProgress: nil
        )

        XCTAssertEqual(Set(second.changedMonths.keys), [month])
    }

    func testRemovedMonthIsReportedButNotEmittedAsEngineProgress() async throws {
        let previous = [
            month: RemoteMonthManifestDigest(month: month, manifestSize: 12, manifestModifiedAtMs: 34)
        ]
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: basePath)
        let progress = ProgressRecorder()

        let result = try await RemoteIndexV1SyncEngine().sync(
            client: client,
            basePath: basePath,
            previousDigests: previous,
            onSyncProgress: { value in
                progress.append(value)
            }
        )

        XCTAssertEqual(result.removedMonths, [month])
        let captured = progress.values()
        XCTAssertEqual(captured.map(\.current), [0])
        XCTAssertEqual(captured.map(\.total), [1])
    }

    func testNonNotFoundManifestMetadataErrorPropagates() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let manifestPath = try await seedManifest(client: client, month: month, marker: 0x44)
        await client.injectMetadataError(.transport, for: manifestPath)

        do {
            _ = try await RemoteIndexV1SyncEngine().sync(
                client: client,
                basePath: basePath,
                previousDigests: [:],
                onSyncProgress: nil
            )
            XCTFail("expected transport error to propagate")
        } catch {
            XCTAssertFalse(isStorageNotFoundError(error))
        }
    }

    func testNonNotFoundYearListErrorPropagates() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        _ = try await seedManifest(client: client, month: month, marker: 0x55)
        await client.injectListError(.transport, for: "\(basePath)/\(month.year)")

        do {
            _ = try await RemoteIndexV1SyncEngine().sync(
                client: client,
                basePath: basePath,
                previousDigests: [:],
                onSyncProgress: nil
            )
            XCTFail("expected transport error to propagate")
        } catch {
            XCTAssertFalse(isStorageNotFoundError(error))
        }
    }

    private func seedManifest(
        client: InMemoryRemoteStorageClient,
        month: LibraryMonthKey,
        marker: UInt8,
        setModifiedAt: Bool = true
    ) async throws -> String {
        let bytes = Data(repeating: marker, count: 8)
        let hash = TestFixtures.fingerprint(marker)
        let fingerprint = TestFixtures.computedFingerprint(for: [
            (role: ResourceTypeCode.photo, slot: 0, contentHash: hash)
        ])
        let resource = TestFixtures.remoteResource(
            year: month.year,
            month: month.month,
            contentHash: hash,
            fileSize: Int64(bytes.count),
            fileName: "asset-\(marker).jpg"
        )
        let asset = TestFixtures.remoteAsset(
            year: month.year,
            month: month.month,
            fingerprint: fingerprint,
            totalFileSizeBytes: Int64(bytes.count)
        )
        let link = TestFixtures.remoteLink(
            year: month.year,
            month: month.month,
            assetFingerprint: fingerprint,
            resourceHash: hash,
            logicalName: "asset-\(marker).jpg"
        )
        await client.injectFile(path: "\(basePath)/\(resource.physicalRemotePath)", data: bytes)
        let store = try await MonthManifestStore.loadSeeded(
            client: client,
            basePath: basePath,
            year: month.year,
            month: month.month,
            seed: MonthManifestStore.Seed(resources: [resource], assets: [asset], assetResourceLinks: [link])
        )
        let manifestPath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: String(format: "%04d/%02d/%@", month.year, month.month, MonthManifestStore.manifestFileName)
        )
        try await client.upload(
            localURL: store.localManifestURL,
            remotePath: manifestPath,
            respectTaskCancellation: true,
            onProgress: nil
        )
        if setModifiedAt {
            await client.setModificationDateForTest(Date(timeIntervalSince1970: TimeInterval(marker)), path: manifestPath)
        }
        return manifestPath
    }
}

private final class ProgressRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [RemoteSyncProgress] = []

    func append(_ value: RemoteSyncProgress) {
        lock.withLock {
            storage.append(value)
        }
    }

    func values() -> [RemoteSyncProgress] {
        lock.withLock { storage }
    }
}
