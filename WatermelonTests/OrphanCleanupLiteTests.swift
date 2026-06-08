import XCTest
@testable import Watermelon

// P08 (P08-MaintenanceCleanup): scoped Lite metadata cleanup. Confirms only the whitelist is deleted —
// month-manifest .tmp/.bak scratch, old V1 manifests, and (foreground only) expired locks — while photo
// data, Lite month manifests, non-whitelisted files, and directories are never touched.
final class OrphanCleanupLiteTests: XCTestCase {
    private let basePath = "/photos"
    private let base = Date(timeIntervalSince1970: 1_700_000_000)

    private func newWriterID() -> String { UUID().uuidString.lowercased() }

    private var monthsDir: String { RepoLayoutLite.monthsDirectoryPath(basePath: basePath) }

    private func cleanup(_ client: InMemoryRemoteStorageClient, lockExpiry: TimeInterval = WriteLockService.expiry) -> OrphanCleanupLite {
        OrphanCleanupLite(client: client, basePath: basePath, lockExpiry: lockExpiry)
    }

    // MARK: - Whitelist deletes; everything else survives

    func testForegroundDeletesWhitelistedAndPreservesEverythingElse() async throws {
        let client = InMemoryRemoteStorageClient()
        let tmpPath = monthsDir + "/manifest_aaa.tmp"
        let bakPath = monthsDir + "/manifest_bbb.bak"
        let litePath = monthsDir + "/2024-03.sqlite"
        let nonWhitelistedPath = monthsDir + "/notes.json"
        let v1ManifestPath = "\(basePath)/2024/03/\(MonthManifestStore.manifestFileName)"
        let photoPath = "\(basePath)/2024/03/IMG_0001.JPG"
        let versionPath = RepoLayoutLite.versionPath(basePath: basePath)

        await client.seedFile(path: tmpPath, data: Data([0x01]))
        await client.seedFile(path: bakPath, data: Data([0x02]))
        await client.seedFile(path: litePath, data: Data([0x03]))
        await client.seedFile(path: nonWhitelistedPath, data: Data([0x04]))
        await client.seedFile(path: v1ManifestPath, data: Data([0x05]))
        await client.seedFile(path: photoPath, data: Data([0x06]))
        await client.seedFile(path: versionPath, data: Data([0x07]))
        await client.seedDirectory(monthsDir + "/subdir")

        let staleWriter = newWriterID()
        await client.seedLock(basePath: basePath, writerID: staleWriter, modificationDate: base.addingTimeInterval(-(WriteLockService.expiry + 60)))

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        XCTAssertTrue(deleted.contains(tmpPath))
        XCTAssertTrue(deleted.contains(bakPath))
        XCTAssertTrue(deleted.contains(v1ManifestPath))
        XCTAssertTrue(deleted.contains(RepoLayoutLite.lockPath(basePath: basePath, writerID: staleWriter)!))

        // Survivors.
        let liteSurvives = await client.fileData(path: litePath)
        let nonWhitelistedSurvives = await client.fileData(path: nonWhitelistedPath)
        let photoSurvives = await client.fileData(path: photoPath)
        let versionSurvives = await client.fileData(path: versionPath)
        let subdirSurvives = try await client.exists(path: monthsDir + "/subdir")
        XCTAssertNotNil(liteSurvives, "Lite .sqlite month manifest must survive")
        XCTAssertNotNil(nonWhitelistedSurvives, "non-whitelisted file must survive")
        XCTAssertNotNil(photoSurvives, "photo/resource bytes must never be touched")
        XCTAssertNotNil(versionSurvives, "version.json must survive")
        XCTAssertTrue(subdirSurvives, "directories must never be deleted")
    }

    func testGoneScratchAndManifestActuallyRemoved() async throws {
        let client = InMemoryRemoteStorageClient()
        let tmpPath = monthsDir + "/manifest_aaa.tmp"
        let v1ManifestPath = "\(basePath)/2024/03/\(MonthManifestStore.manifestFileName)"
        await client.seedFile(path: tmpPath, data: Data([0x01]))
        await client.seedFile(path: v1ManifestPath, data: Data([0x02]))

        _ = await cleanup(client).run(mode: .foreground, now: base)

        let tmpGone = await client.fileData(path: tmpPath)
        let v1Gone = await client.fileData(path: v1ManifestPath)
        XCTAssertNil(tmpGone)
        XCTAssertNil(v1Gone)
    }

    // MARK: - Background never touches locks

    func testBackgroundNeverDeletesLocks() async throws {
        let client = InMemoryRemoteStorageClient()
        let fresh = newWriterID()
        let stale = newWriterID()
        let unknown = newWriterID()
        await client.seedLock(basePath: basePath, writerID: fresh, modificationDate: base.addingTimeInterval(-60))
        await client.seedLock(basePath: basePath, writerID: stale, modificationDate: base.addingTimeInterval(-(WriteLockService.expiry + 60)))
        await client.seedLock(basePath: basePath, writerID: unknown, modificationDate: nil)
        let tmpPath = monthsDir + "/manifest_aaa.tmp"
        await client.seedFile(path: tmpPath, data: Data([0x01]))

        let deleted = await cleanup(client).run(mode: .background, now: base)

        let freshExists = await client.lockExists(basePath: basePath, writerID: fresh)
        let staleExists = await client.lockExists(basePath: basePath, writerID: stale)
        let unknownExists = await client.lockExists(basePath: basePath, writerID: unknown)
        XCTAssertTrue(freshExists, "background must not delete a fresh lock")
        XCTAssertTrue(staleExists, "background must not delete a stale lock")
        XCTAssertTrue(unknownExists, "background must not delete an unknown-mtime lock")
        XCTAssertFalse(deleted.contains(where: { $0.hasSuffix(".\(RepoLayoutLite.lockFileExtension)") }), "background must delete no lock")
        XCTAssertTrue(deleted.contains(tmpPath), "background still cleans non-lock scratch")
    }

    // MARK: - Foreground stale-lock expiry boundary + nil mtime

    func testForegroundLockExpiryBoundaryAndNilMtime() async throws {
        let client = InMemoryRemoteStorageClient()
        let atBoundary = newWriterID()      // age == expiry → fresh, not deleted
        let pastBoundary = newWriterID()    // age == expiry + 1 → stale, deleted
        let unknown = newWriterID()         // nil mtime → not stale, not deleted
        await client.seedLock(basePath: basePath, writerID: atBoundary, modificationDate: base.addingTimeInterval(-WriteLockService.expiry))
        await client.seedLock(basePath: basePath, writerID: pastBoundary, modificationDate: base.addingTimeInterval(-(WriteLockService.expiry + 1)))
        await client.seedLock(basePath: basePath, writerID: unknown, modificationDate: nil)

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        XCTAssertFalse(deleted.contains(RepoLayoutLite.lockPath(basePath: basePath, writerID: atBoundary)!), "a lock exactly at expiry is not yet stale")
        XCTAssertTrue(deleted.contains(RepoLayoutLite.lockPath(basePath: basePath, writerID: pastBoundary)!), "a lock past expiry is stale")
        XCTAssertFalse(deleted.contains(RepoLayoutLite.lockPath(basePath: basePath, writerID: unknown)!), "an unknown-mtime lock is never stale")

        let atBoundaryExists = await client.lockExists(basePath: basePath, writerID: atBoundary)
        let unknownExists = await client.lockExists(basePath: basePath, writerID: unknown)
        XCTAssertTrue(atBoundaryExists)
        XCTAssertTrue(unknownExists)
    }

    func testInjectedLockExpiryIsHonored() async throws {
        let client = InMemoryRemoteStorageClient()
        let writer = newWriterID()
        await client.seedLock(basePath: basePath, writerID: writer, modificationDate: base.addingTimeInterval(-20))

        let deleted = await cleanup(client, lockExpiry: 10).run(mode: .foreground, now: base)

        XCTAssertTrue(deleted.contains(RepoLayoutLite.lockPath(basePath: basePath, writerID: writer)!), "injected 10s expiry makes a 20s-old lock stale")
    }

    // MARK: - No-op safety

    func testMissingDirectoriesAreNoOp() async {
        let client = InMemoryRemoteStorageClient()
        let deleted = await cleanup(client).run(mode: .foreground, now: base)
        let deletedPaths = await client.deletedPaths
        XCTAssertTrue(deleted.isEmpty)
        XCTAssertTrue(deletedPaths.isEmpty)
    }

    func testDeleteFaultIsSuccessEquivalent() async throws {
        let client = InMemoryRemoteStorageClient()
        let tmpPath = monthsDir + "/manifest_aaa.tmp"
        await client.seedFile(path: tmpPath, data: Data([0x01]))
        await client.enqueueDeleteError(RemoteErrorFixtures.notFound)   // delete-time notFound

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        XCTAssertFalse(deleted.contains(tmpPath), "a swallowed delete is not reported as deleted")
        // Cleanup completed without throwing — that is the success-equivalence guarantee.
    }

    // MARK: - V1 scope is exact

    func testOnlyExactV1ManifestNameIsDeleted() async throws {
        let client = InMemoryRemoteStorageClient()
        let exactManifest = "\(basePath)/2024/03/\(MonthManifestStore.manifestFileName)"
        let siblingTmp = "\(basePath)/2024/03/manifest_x.tmp"   // not in the V1 whitelist
        let nestedPhoto = "\(basePath)/2024/03/IMG_0002.JPG"
        await client.seedFile(path: exactManifest, data: Data([0x01]))
        await client.seedFile(path: siblingTmp, data: Data([0x02]))
        await client.seedFile(path: nestedPhoto, data: Data([0x03]))

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        XCTAssertEqual(deleted, [exactManifest], "only the exact YYYY/MM/.watermelon_manifest.sqlite is whitelisted")
        let siblingSurvives = await client.fileData(path: siblingTmp)
        let photoSurvives = await client.fileData(path: nestedPhoto)
        XCTAssertNotNil(siblingSurvives, "a .tmp inside a V1 month dir is out of P08 scope")
        XCTAssertNotNil(photoSurvives)
    }
}
