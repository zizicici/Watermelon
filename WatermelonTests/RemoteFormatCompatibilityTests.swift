import XCTest
@testable import Watermelon

// P08 Phase 6: future flag-off compatibility. `verify` rejects only a parseable committed
// `.watermelon/version.json` (a real Lite/foreign format commit) and tolerates a half-created marker so a
// future flag-off build can still operate the V1 tree beneath. Transport/download faults surface — they
// are never read as safe V1. NOTE: already-released clients reject ANY `.watermelon`; this relaxation only
// protects future flag-off builds and cannot change a binary already in the field.
final class RemoteFormatCompatibilityTests: XCTestCase {
    private let basePath = "/photos"
    private let service = RemoteFormatCompatibilityService()

    private func makeProfile() -> ServerProfileRecord {
        ServerProfileRecord(
            id: 1, name: "server", storageType: StorageType.smb.rawValue, connectionParams: nil,
            sortOrder: 0, host: "host.local", port: 445, shareName: "share", basePath: basePath,
            username: "user", domain: nil, credentialRef: "ref", backgroundBackupEnabled: false,
            createdAt: Date(), updatedAt: Date(), writerID: nil
        )
    }

    private func verify(_ client: InMemoryRemoteStorageClient) async -> Result<Void, Error> {
        do {
            try await service.verify(client: client, profile: makeProfile())
            return .success(())
        } catch {
            return .failure(error)
        }
    }

    private func assertTolerated(_ client: InMemoryRemoteStorageClient, _ message: String) async {
        if case .failure(let error) = await verify(client) {
            XCTFail("\(message) — unexpected throw: \(error)")
        }
    }

    // MARK: - No marker

    func testNoMarkerIsTolerated() async {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(basePath)   // base exists (prepare creates it), no `.watermelon`
        await assertTolerated(client, "a base path without `.watermelon` is V1")
    }

    // MARK: - Committed version → reject + surface min app version

    func testCommittedLiteVersionIsRejectedAndSurfacesMinAppVersion() async throws {
        let client = InMemoryRemoteStorageClient()
        let manifest = VersionManifestLite.makeManifest(createdAt: "2026-01-01T00:00:00Z", createdBy: "seed")
        await client.seedFile(path: RepoLayoutLite.versionPath(basePath: basePath), data: try VersionManifestLite.encode(manifest))

        guard case .failure(let error) = await verify(client) else {
            return XCTFail("a committed Lite version.json must be rejected")
        }
        guard case BackupCompatibilityError.remoteFormatUnsupported(let minVersion) = error else {
            return XCTFail("unexpected error: \(error)")
        }
        XCTAssertEqual(minVersion, VersionManifestLite.minAppVersion)
    }

    func testCommittedForeignFutureVersionIsRejected() async throws {
        let client = InMemoryRemoteStorageClient()
        let future = WatermelonRemoteVersionManifest(
            formatVersion: 3, layout: "crdt-commit-log", minAppVersion: "9.9.9", createdAt: "x", createdBy: "y"
        )
        await client.seedFile(path: RepoLayoutLite.versionPath(basePath: basePath), data: try VersionManifestLite.encode(future))

        guard case .failure(let error) = await verify(client) else {
            return XCTFail("a committed foreign version.json must be rejected")
        }
        guard case BackupCompatibilityError.remoteFormatUnsupported(let minVersion) = error else {
            return XCTFail("unexpected error: \(error)")
        }
        XCTAssertEqual(minVersion, "9.9.9")
    }

    // MARK: - Half-created marker → tolerated

    func testBareMarkerIsTolerated() async {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/.watermelon")   // bare marker, no version.json
        await assertTolerated(client, "a bare `.watermelon` with no committed version is tolerated")
    }

    func testEmptyLocksDirWithoutVersionIsTolerated() async {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(RepoLayoutLite.locksDirectoryPath(basePath: basePath))   // `.watermelon/locks`, no version
        await assertTolerated(client, "an empty `.watermelon/locks` with no committed version is tolerated")
    }

    func testMalformedVersionIsTolerated() async {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: RepoLayoutLite.versionPath(basePath: basePath), data: Data("not json".utf8))
        await assertTolerated(client, "an unparsable version.json must not permanently block future flag-off V1")
    }

    func testVersionWithoutFormatVersionIsTolerated() async throws {
        let client = InMemoryRemoteStorageClient()
        let incomplete = WatermelonRemoteVersionManifest(
            formatVersion: nil, layout: "lite-month-sqlite", minAppVersion: "1.5.0", createdAt: "x", createdBy: "y"
        )
        await client.seedFile(path: RepoLayoutLite.versionPath(basePath: basePath), data: try VersionManifestLite.encode(incomplete))
        await assertTolerated(client, "a version.json missing its format version is not a committed marker")
    }

    // MARK: - Probe faults surface (never read as safe V1)

    func testRetryableVersionDownloadFaultSurfaces() async {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/.watermelon")
        await client.enqueueDownloadError(RemoteErrorFixtures.retryable)   // version.json probe blinks

        guard case .failure(let error) = await verify(client) else {
            return XCTFail("a retryable version.json probe fault must surface, not be tolerated as safe V1")
        }
        XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
    }

    func testTerminalVersionDownloadFaultSurfaces() async {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/.watermelon")
        await client.enqueueDownloadError(RemoteErrorFixtures.terminal)

        guard case .failure(let error) = await verify(client) else {
            return XCTFail("a terminal version.json probe fault must surface")
        }
        XCTAssertNotEqual(RemoteFaultLite.classify(error), .notFound)
    }

    func testBaseListFaultSurfaces() async {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueListError(RemoteErrorFixtures.retryable)   // base probe blinks

        guard case .failure = await verify(client) else {
            return XCTFail("a base-path LIST fault must surface, not be read as no marker")
        }
    }
}
