import XCTest
import GRDB
@testable import Watermelon

// P08 (P08-MaintenanceCleanup): scoped Lite metadata cleanup. Confirms only whitelisted metadata is deleted
// while photo data, V1 manifests, Lite month manifests, non-whitelisted files, and directories are never touched.
final class OrphanCleanupLiteTests: XCTestCase {
    private let basePath = "/photos"
    private let base = Date(timeIntervalSince1970: 1_700_000_000)

    private func newWriterID() -> String { UUID().uuidString.lowercased() }

    private var monthsDir: String { RepoLayoutLite.monthsDirectoryPath(basePath: basePath) }

    private func cleanup(
        _ client: InMemoryRemoteStorageClient,
        currentWriterID: String? = nil,
        lockExpiry: TimeInterval = WriteLockService.expiry,
        assertOwnership: MonthManifestOwnershipAssertion? = nil
    ) -> OrphanCleanupLite {
        OrphanCleanupLite(
            client: client,
            basePath: basePath,
            currentWriterID: currentWriterID,
            lockExpiry: lockExpiry,
            assertOwnership: assertOwnership
        )
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
        await client.seedFile(path: litePath, data: try makeMonthSqliteData())
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
        XCTAssertFalse(deleted.contains(v1ManifestPath))
        XCTAssertTrue(deleted.contains(RepoLayoutLite.lockPath(basePath: basePath, writerID: staleWriter)!))

        // Survivors.
        let liteSurvives = await client.fileData(path: litePath)
        let nonWhitelistedSurvives = await client.fileData(path: nonWhitelistedPath)
        let v1Survives = await client.fileData(path: v1ManifestPath)
        let photoSurvives = await client.fileData(path: photoPath)
        let versionSurvives = await client.fileData(path: versionPath)
        let subdirSurvives = try await client.exists(path: monthsDir + "/subdir")
        XCTAssertNotNil(liteSurvives, "Lite .sqlite month manifest must survive")
        XCTAssertNotNil(nonWhitelistedSurvives, "non-whitelisted file must survive")
        XCTAssertNotNil(v1Survives, "old V1 manifests are migration evidence and must survive cleanup")
        XCTAssertNotNil(photoSurvives, "photo/resource bytes must never be touched")
        XCTAssertNotNil(versionSurvives, "version.json must survive")
        XCTAssertTrue(subdirSurvives, "directories must never be deleted")
    }

    func testScratchRemovedAndV1ManifestPreserved() async throws {
        let client = InMemoryRemoteStorageClient()
        let tmpPath = monthsDir + "/manifest_aaa.tmp"
        let v1ManifestPath = "\(basePath)/2024/03/\(MonthManifestStore.manifestFileName)"
        await client.seedFile(path: tmpPath, data: Data([0x01]))
        await client.seedFile(path: v1ManifestPath, data: Data([0x02]))

        _ = await cleanup(client).run(mode: .foreground, now: base)

        let tmpGone = await client.fileData(path: tmpPath)
        let v1Survives = await client.fileData(path: v1ManifestPath)
        XCTAssertNil(tmpGone)
        XCTAssertNotNil(v1Survives)
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

    // MARK: - V1 tree is outside cleanup scope

    func testV1MonthTreeIsPreserved() async throws {
        let client = InMemoryRemoteStorageClient()
        let exactManifest = "\(basePath)/2024/03/\(MonthManifestStore.manifestFileName)"
        let siblingTmp = "\(basePath)/2024/03/manifest_x.tmp"
        let nestedPhoto = "\(basePath)/2024/03/IMG_0002.JPG"
        await client.seedFile(path: exactManifest, data: Data([0x01]))
        await client.seedFile(path: siblingTmp, data: Data([0x02]))
        await client.seedFile(path: nestedPhoto, data: Data([0x03]))

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        XCTAssertTrue(deleted.isEmpty)
        let exactSurvives = await client.fileData(path: exactManifest)
        let siblingSurvives = await client.fileData(path: siblingTmp)
        let photoSurvives = await client.fileData(path: nestedPhoto)
        XCTAssertNotNil(exactSurvives)
        XCTAssertNotNil(siblingSurvives)
        XCTAssertNotNil(photoSurvives)
    }

    // MARK: - Writer context + second confirmation (Phase 2)

    func testForegroundDoesNotDeleteOwnActiveLockEvenIfMtimeExpired() async {
        let client = InMemoryRemoteStorageClient()
        let me = newWriterID()
        let other = newWriterID()
        let expiredDate = base.addingTimeInterval(-(WriteLockService.expiry + 60))
        // The current writer's own lock can look expired by mtime (clock skew) but must be kept.
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: expiredDate)
        // A genuinely stale foreign lock must still be cleaned.
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: expiredDate)

        let deleted = await cleanup(client, currentWriterID: me).run(mode: .foreground, now: base)

        let ownExists = await client.lockExists(basePath: basePath, writerID: me)
        let otherExists = await client.lockExists(basePath: basePath, writerID: other)
        XCTAssertTrue(ownExists, "the current writer's own active lock must never be deleted")
        XCTAssertFalse(deleted.contains(RepoLayoutLite.lockPath(basePath: basePath, writerID: me)!))
        XCTAssertFalse(otherExists, "a genuinely stale foreign lock is still cleaned")
        XCTAssertTrue(deleted.contains(RepoLayoutLite.lockPath(basePath: basePath, writerID: other)!))
    }

    func testForegroundDoesNotDeleteForeignLockFreshenedOnSecondConfirmation() async {
        let client = InMemoryRemoteStorageClient()
        let other = newWriterID()
        await client.seedLock(
            basePath: basePath, writerID: other,
            modificationDate: base.addingTimeInterval(-(WriteLockService.expiry + 60))
        )

        // The foreign lock is refreshed (mtime → fresh) between the two confirmation reads.
        let basePath = self.basePath
        let freshDate = base.addingTimeInterval(-30)
        await client.setOnDownload { path in
            if path == RepoLayoutLite.lockPath(basePath: basePath, writerID: other) {
                await client.setLockModificationDate(basePath: basePath, writerID: other, to: freshDate)
            }
        }

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        let exists = await client.lockExists(basePath: basePath, writerID: other)
        XCTAssertTrue(exists, "a foreign lock freshened during the second confirmation must not be deleted")
        XCTAssertFalse(deleted.contains(RepoLayoutLite.lockPath(basePath: basePath, writerID: other)!))
    }

    func testForegroundDeletesForeignLockWhenSameTokenStillStale() async {
        let client = InMemoryRemoteStorageClient()
        let other = newWriterID()
        let body = LockFileBody(writerID: other, sessionToken: "s", lockToken: "t", generation: 2)
        await client.seedLock(
            basePath: basePath, writerID: other,
            modificationDate: base.addingTimeInterval(-(WriteLockService.expiry + 60)), body: body
        )

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        let exists = await client.lockExists(basePath: basePath, writerID: other)
        XCTAssertFalse(exists, "an unchanged stale foreign lock is deleted after second confirmation")
        XCTAssertTrue(deleted.contains(RepoLayoutLite.lockPath(basePath: basePath, writerID: other)!))
    }

    func testForegroundDoesNotDeleteExpiredUndecodableForeignLock() async {
        let client = InMemoryRemoteStorageClient()
        let other = newWriterID()
        // Expired by mtime but the body does not decode to a LockFileBody — no token proof.
        await client.seedUndecodableLock(
            basePath: basePath, writerID: other,
            modificationDate: base.addingTimeInterval(-(WriteLockService.expiry + 60))
        )

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        let exists = await client.lockExists(basePath: basePath, writerID: other)
        XCTAssertTrue(exists, "an expired but undecodable foreign lock has no token proof and must not be deleted")
        XCTAssertFalse(deleted.contains(RepoLayoutLite.lockPath(basePath: basePath, writerID: other)!))
    }

    // MARK: - Repair-first month scratch cleanup (P06 Phase 4)

    private func makeMonthSqliteData(marker: Int = 0) throws -> Data {
        let tmpDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("WT-orphan-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tmpDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tmpDir) }
        let dbURL = tmpDir.appendingPathComponent("month.sqlite")
        let queue = try DatabaseQueue(path: dbURL.path)
        try MonthManifestStore.migrate(queue)
        if marker != 0 {
            try queue.write { db in
                try db.execute(sql: "PRAGMA user_version = \(marker)")
            }
        }
        try queue.close()
        return try Data(contentsOf: dbURL)
    }

    // A pre-migration shipped manifest: resources/assets carried creationDateNs/backedUpAtNs. The load
    // path migrates these to *Ms in place; this fixture builds the un-migrated bytes a migrated canonical
    // (or a flush's .bak of one) would carry.
    private func makeLegacyNsMonthSqliteData() throws -> Data {
        let tmpDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("WT-orphan-legacy-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tmpDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tmpDir) }
        let dbURL = tmpDir.appendingPathComponent("month.sqlite")
        let queue = try DatabaseQueue(path: dbURL.path)
        try queue.write { db in
            try db.execute(sql: """
            CREATE TABLE resources (
              fileName TEXT PRIMARY KEY NOT NULL,
              contentHash BLOB NOT NULL,
              fileSize INTEGER NOT NULL,
              resourceType INTEGER NOT NULL,
              creationDateNs INTEGER,
              backedUpAtNs INTEGER NOT NULL
            )
            """)
            try db.execute(sql: """
            CREATE TABLE assets (
              assetFingerprint BLOB PRIMARY KEY NOT NULL,
              creationDateNs INTEGER,
              backedUpAtNs INTEGER NOT NULL,
              resourceCount INTEGER NOT NULL,
              totalFileSizeBytes INTEGER NOT NULL
            )
            """)
            try db.execute(sql: """
            CREATE TABLE asset_resources (
              assetFingerprint BLOB NOT NULL,
              resourceHash BLOB NOT NULL,
              role INTEGER NOT NULL,
              slot INTEGER NOT NULL,
              PRIMARY KEY(assetFingerprint, role, slot)
            )
            """)
            let ns: Int64 = 1_700_000_000_000_000_000
            try db.execute(
                sql: "INSERT INTO resources (fileName, contentHash, fileSize, resourceType, creationDateNs, backedUpAtNs) VALUES (?, ?, ?, ?, ?, ?)",
                arguments: ["IMG_0001.HEIC", Data([0xAA]), Int64(100), Int64(0), ns, ns]
            )
            try db.execute(
                sql: "INSERT INTO assets (assetFingerprint, creationDateNs, backedUpAtNs, resourceCount, totalFileSizeBytes) VALUES (?, ?, ?, ?, ?)",
                arguments: [Data([0xBB]), ns, ns, Int64(1), Int64(100)]
            )
            try db.execute(
                sql: "INSERT INTO asset_resources (assetFingerprint, resourceHash, role, slot) VALUES (?, ?, ?, ?)",
                arguments: [Data([0xBB]), Data([0xAA]), Int64(0), Int64(0)]
            )
        }
        try queue.close()
        return try Data(contentsOf: dbURL)
    }

    private func makeVersionData() throws -> Data {
        try VersionManifestLite.encode(
            VersionManifestLite.makeManifest(
                createdAt: "2026-06-08T00:00:00Z",
                createdBy: "test-writer"
            )
        )
    }

    private func scratchPath(month: LibraryMonthKey, suffix: String) -> String {
        monthsDir + "/\(RepoLayoutLite.monthFilename(month: month)).\(UUID().uuidString).\(suffix)"
    }

    private func versionScratchPath(suffix: String) -> String {
        RepoLayoutLite.repoDirectoryPath(basePath: basePath) + "/version_\(UUID().uuidString).json.\(suffix)"
    }

    func testFinalMissingValidBakRestoresCanonical() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let bakPath = scratchPath(month: month, suffix: "bak")
        let valid = try makeMonthSqliteData()
        await client.seedFile(path: bakPath, data: valid)

        _ = await cleanup(client).run(mode: .foreground, now: base)

        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        let restored = await client.fileData(path: canonicalPath)
        XCTAssertEqual(restored, valid, "a sound .bak must be restored to the canonical month path")
        let bakGone = await client.fileData(path: bakPath)
        XCTAssertNil(bakGone, "the .bak is consumed by the restore move")
    }

    func testFinalMissingValidTmpRestoresCanonicalWhenNoBak() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let tmpPath = scratchPath(month: month, suffix: "tmp")
        let valid = try makeMonthSqliteData()
        await client.seedFile(path: tmpPath, data: valid)

        _ = await cleanup(client).run(mode: .foreground, now: base)

        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        let restored = await client.fileData(path: canonicalPath)
        XCTAssertEqual(restored, valid, "a sole sound .tmp must be restored when there is no valid .bak")
    }

    func testFinalMissingInvalidTmpIsDeletedAlongsideValidTmpRestore() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let validTmp = scratchPath(month: month, suffix: "tmp")
        let junkBak = scratchPath(month: month, suffix: "bak")
        let valid = try makeMonthSqliteData()
        await client.seedFile(path: validTmp, data: valid)
        await client.seedFile(path: junkBak, data: Data([0x01]))   // not a sound manifest

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        let restored = await client.fileData(path: canonicalPath)
        XCTAssertEqual(restored, valid, "the one sound candidate is restored")
        let junkGone = await client.fileData(path: junkBak)
        XCTAssertNil(junkGone, "unsound leftover scratch is removed once the month is restored")
        XCTAssertTrue(deleted.contains(junkBak))
    }

    func testRestoreReadbackFailureBlocksSiblingScratchDeletion() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let validBak = scratchPath(month: month, suffix: "bak")
        let junkTmp = scratchPath(month: month, suffix: "tmp")
        let valid = try makeMonthSqliteData()
        let junk = Data([0x01])
        await client.seedFile(path: validBak, data: valid)
        await client.seedFile(path: junkTmp, data: junk)

        func entry(_ path: String, data: Data) -> RemoteStorageEntry {
            RemoteStorageEntry(
                path: path,
                name: path.split(separator: "/").last.map(String.init) ?? path,
                isDirectory: false,
                size: Int64(data.count),
                creationDate: nil,
                modificationDate: nil
            )
        }
        await client.enqueueListResult([
            entry(validBak, data: valid),
            entry(junkTmp, data: junk)
        ])
        await client.enqueueDownloadData(valid)
        await client.enqueueDownloadData(junk)
        await client.enqueueDownloadData(junk)

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        let junkSurvives = await client.fileData(path: junkTmp)
        XCTAssertEqual(junkSurvives, junk, "unverified restore must not authorize deleting sibling scratch")
        XCTAssertFalse(deleted.contains(junkTmp))
    }

    func testFinalMissingPrefersUniqueValidTmpOverBak() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let tmpPath = scratchPath(month: month, suffix: "tmp")
        let bakPath = scratchPath(month: month, suffix: "bak")
        let tmpData = try makeMonthSqliteData(marker: 1)
        let bakData = try makeMonthSqliteData(marker: 2)
        await client.seedFile(path: tmpPath, data: tmpData)
        await client.seedFile(path: bakPath, data: bakData)

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        let canonical = await client.fileData(path: canonicalPath)
        XCTAssertEqual(canonical, tmpData, "the newer temp candidate is preferred over its rename backup")
        let tmpGone = await client.fileData(path: tmpPath)
        let bakSurvives = await client.fileData(path: bakPath)
        XCTAssertNil(tmpGone, "the selected temp scratch is consumed by restore")
        XCTAssertNotNil(bakSurvives, "valid but unselected recovery material stays in place")
        XCTAssertFalse(deleted.contains(bakPath))
    }

    func testAmbiguousValidScratchWithoutUniqueTmpIsLeftInPlace() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let tmpA = scratchPath(month: month, suffix: "tmp")
        let tmpB = scratchPath(month: month, suffix: "tmp")
        let valid = try makeMonthSqliteData()
        await client.seedFile(path: tmpA, data: valid)
        await client.seedFile(path: tmpB, data: valid)

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        let canonical = await client.fileData(path: RepoLayoutLite.monthPath(basePath: basePath, month: month))
        XCTAssertNil(canonical, "ambiguous temp candidates must not be auto-restored")
        let tmpASurvives = await client.fileData(path: tmpA)
        let tmpBSurvives = await client.fileData(path: tmpB)
        XCTAssertNotNil(tmpASurvives)
        XCTAssertNotNil(tmpBSurvives)
        XCTAssertFalse(deleted.contains(tmpA))
        XCTAssertFalse(deleted.contains(tmpB))
    }

    func testValidScratchWithUnparseableTargetIsLeftInPlace() async throws {
        let client = InMemoryRemoteStorageClient()
        // A legacy/opaque scratch name we cannot map to a month, but whose bytes are a sound manifest:
        // unplaceable and possibly the only recoverable copy → leave it untouched.
        let opaquePath = monthsDir + "/manifest_\(UUID().uuidString).tmp"
        let valid = try makeMonthSqliteData()
        await client.seedFile(path: opaquePath, data: valid)

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        let survives = await client.fileData(path: opaquePath)
        XCTAssertNotNil(survives, "a sound but unplaceable scratch must not be destroyed")
        XCTAssertFalse(deleted.contains(opaquePath))
    }

    // MARK: - Stranded V1→Lite migration publish temp (P03 R01 F1)

    private func migrationTempPath() -> String {
        monthsDir + "/\(RepoLayoutLite.migrationPublishTempPrefix)\(UUID().uuidString).tmp"
    }

    // The proven residue path (F1): a hard crash mid-publish strands a sound "migrate_<uuid>.tmp". Resume
    // re-uploads a fresh temp and moves it to the canonical, so the old temp is never enumerated by the
    // migration and must be reclaimed by cleanup beside the now-valid canonical month.
    func testStrandedMigrationPublishTempReclaimedBesideCanonical() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        let valid = try makeMonthSqliteData()
        await client.seedFile(path: canonicalPath, data: valid)
        let stranded = migrationTempPath()
        await client.seedFile(path: stranded, data: valid)

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        XCTAssertTrue(deleted.contains(stranded), "a stranded migration publish temp is reclaimable residue")
        let strandedGone = await client.fileData(path: stranded)
        let canonicalSurvives = await client.fileData(path: canonicalPath)
        XCTAssertNil(strandedGone)
        XCTAssertEqual(canonicalSurvives, valid, "the canonical month is untouched")
    }

    // A migration temp is transient, never a recovery copy: it must be reclaimed, not restored to a
    // canonical month. Genuine opaque recovery scratch stays preserved, proving the bucket split.
    func testMigrationPublishTempIsTransientNotRestoredWhileOpaqueScratchSurvives() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let valid = try makeMonthSqliteData()
        let stranded = migrationTempPath()
        await client.seedFile(path: stranded, data: valid)
        let opaquePath = monthsDir + "/manifest_\(UUID().uuidString).tmp"
        await client.seedFile(path: opaquePath, data: valid)

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        XCTAssertTrue(deleted.contains(stranded))
        let strandedGone = await client.fileData(path: stranded)
        let canonical = await client.fileData(path: RepoLayoutLite.monthPath(basePath: basePath, month: month))
        let opaqueSurvives = await client.fileData(path: opaquePath)
        XCTAssertNil(strandedGone)
        XCTAssertNil(canonical, "a migration temp must never be restored as a canonical month")
        XCTAssertNotNil(opaqueSurvives, "genuine opaque recovery scratch is still preserved")
        XCTAssertFalse(deleted.contains(opaquePath))
    }

    // Background cleanup runs month-scratch reclaim too, so a stranded migration temp is reclaimed even
    // when a later write is a background pass.
    func testBackgroundReclaimsStrandedMigrationPublishTemp() async throws {
        let client = InMemoryRemoteStorageClient()
        let stranded = migrationTempPath()
        let valid = try makeMonthSqliteData()
        await client.seedFile(path: stranded, data: valid)

        let deleted = await cleanup(client).run(mode: .background, now: base)

        XCTAssertTrue(deleted.contains(stranded))
        let strandedGone = await client.fileData(path: stranded)
        XCTAssertNil(strandedGone)
    }

    // Ownership lost mid-cleanup must not delete the migration temp — same destructive gate as all scratch.
    func testOwnershipLossBlocksMigrationPublishTempReclaim() async throws {
        let client = InMemoryRemoteStorageClient()
        let stranded = migrationTempPath()
        await client.seedFile(path: stranded, data: try makeMonthSqliteData())

        let deleted = await cleanup(client, assertOwnership: { throw LiteRepoError.ownershipLost })
            .run(mode: .foreground, now: base)

        XCTAssertFalse(deleted.contains(stranded))
        let survives = await client.fileData(path: stranded)
        XCTAssertNotNil(survives, "lost ownership must leave the temp in place")
    }

    func testExistingCanonicalDeletesOnlyInvalidScratch() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        let valid = try makeMonthSqliteData()
        await client.seedFile(path: canonicalPath, data: valid)
        let validBak = scratchPath(month: month, suffix: "bak")
        let invalidTmp = scratchPath(month: month, suffix: "tmp")
        await client.seedFile(path: validBak, data: valid)
        await client.seedFile(path: invalidTmp, data: Data([0x01]))

        // Survivors that must never be touched by repair-first cleanup.
        let photoPath = "\(basePath)/2024/03/IMG_0001.JPG"
        let versionPath = RepoLayoutLite.versionPath(basePath: basePath)
        await client.seedFile(path: photoPath, data: Data([0x06]))
        await client.seedFile(path: versionPath, data: Data([0x07]))
        await client.seedDirectory(monthsDir + "/subdir")

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        XCTAssertFalse(deleted.contains(validBak), "valid scratch stays as recovery material even when canonical is valid")
        XCTAssertTrue(deleted.contains(invalidTmp), "proven junk scratch is still cleaned")
        let validBakSurvives = await client.fileData(path: validBak)
        let invalidTmpGone = await client.fileData(path: invalidTmp)
        XCTAssertNotNil(validBakSurvives)
        XCTAssertNil(invalidTmpGone)
        let canonicalSurvives = await client.fileData(path: canonicalPath)
        XCTAssertEqual(canonicalSurvives, valid, "the canonical month sqlite must never be touched")
        let photoSurvives = await client.fileData(path: photoPath)
        let versionSurvives = await client.fileData(path: versionPath)
        let subdirSurvives = try await client.exists(path: monthsDir + "/subdir")
        XCTAssertNotNil(photoSurvives, "photo bytes are never touched")
        XCTAssertNotNil(versionSurvives, "version.json is never touched")
        XCTAssertTrue(subdirSurvives, "directories are never deleted")
    }

    func testPartialListingCannotRestoreScratchOverExistingCanonical() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        let canonical = try makeMonthSqliteData(marker: 1)
        let oldScratch = try makeMonthSqliteData(marker: 2)
        let leftoverBak = scratchPath(month: month, suffix: "bak")
        await client.seedFile(path: canonicalPath, data: canonical)
        await client.seedFile(path: leftoverBak, data: oldScratch)
        await client.enqueueListResult([
            RemoteStorageEntry(
                path: leftoverBak,
                name: (leftoverBak as NSString).lastPathComponent,
                isDirectory: false,
                size: Int64(oldScratch.count),
                creationDate: nil,
                modificationDate: nil
            )
        ])

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        let canonicalSurvives = await client.fileData(path: canonicalPath)
        XCTAssertEqual(canonicalSurvives, canonical, "a partial LIST must not let scratch overwrite canonical")
        XCTAssertFalse(deleted.contains(leftoverBak), "scratch stays for a later non-ambiguous cleanup pass")
    }

    func testInvalidCanonicalIsRepairedFromValidScratch() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        let valid = try makeMonthSqliteData()
        let leftoverBak = scratchPath(month: month, suffix: "bak")
        await client.seedFile(path: canonicalPath, data: Data([0x01]))
        await client.seedFile(path: leftoverBak, data: valid)

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        let canonical = await client.fileData(path: canonicalPath)
        XCTAssertEqual(canonical, valid, "invalid canonical should be repaired from the validated scratch")
        let leftoverBakGone = await client.fileData(path: leftoverBak)
        XCTAssertNil(leftoverBakGone, "selected scratch is consumed after repair")
        XCTAssertFalse(deleted.contains(leftoverBak), "the selected recovery scratch is consumed, not counted as junk deletion")
    }

    // MARK: - Legacy-ns schema manifests are sound (R01 F1 regression)

    func testLegacyNsSchemaManifestIsSoundNotJunk() throws {
        // The cleanup oracle must agree with the load path: a creationDateNs/backedUpAtNs manifest is
        // loadable after migration, so it must not be classified as invalid/junk.
        let legacy = try makeLegacyNsMonthSqliteData()
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("WT-oracle-\(UUID().uuidString).sqlite")
        defer { try? FileManager.default.removeItem(at: url) }
        try legacy.write(to: url)
        XCTAssertTrue(MonthManifestStore.isValidMonthManifestFile(at: url),
                      "a legacy-ns schema manifest is sound and must validate like the load path")
    }

    func testFinalMissingSoleLegacyNsBakIsRestoredNotDeleted() async throws {
        // Canonical absent, only recovery material is a legacy-ns .bak: it must be restored, not destroyed.
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let bakPath = scratchPath(month: month, suffix: "bak")
        let legacy = try makeLegacyNsMonthSqliteData()
        await client.seedFile(path: bakPath, data: legacy)

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        let restored = await client.fileData(path: canonicalPath)
        XCTAssertEqual(restored, legacy, "the sole legacy-ns .bak is recoverable and must be restored to canonical")
        let bakGone = await client.fileData(path: bakPath)
        XCTAssertNil(bakGone, "the .bak is consumed by the restore move")
        XCTAssertFalse(deleted.contains(bakPath), "restoration consumes the scratch; it is not a junk deletion")
    }

    func testFinalMissingLegacyNsBakSurvivesAlongsideJunkTmp() async throws {
        // The proven loss path (F1): canonical absent + legacy-ns .bak (loadable) + corrupt .tmp. Before
        // the fix both validated .invalid and the legacy .bak — the only sound copy — was deleted. After
        // the fix the .bak is recognized as valid recovery material and restored; only the junk .tmp drops.
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let bakPath = scratchPath(month: month, suffix: "bak")
        let tmpPath = scratchPath(month: month, suffix: "tmp")
        let legacy = try makeLegacyNsMonthSqliteData()
        await client.seedFile(path: bakPath, data: legacy)
        await client.seedFile(path: tmpPath, data: Data([0x01]))   // proven junk

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        let restored = await client.fileData(path: canonicalPath)
        XCTAssertEqual(restored, legacy, "the legacy-ns .bak is the sound recovery copy and must be restored")
        let bakConsumed = await client.fileData(path: bakPath)
        XCTAssertNil(bakConsumed, "the .bak is consumed by the restore move, not destroyed as junk")
        let tmpGone = await client.fileData(path: tmpPath)
        XCTAssertNil(tmpGone, "proven junk .tmp is still cleaned")
        XCTAssertTrue(deleted.contains(tmpPath))
        XCTAssertFalse(deleted.contains(bakPath))
    }

    func testCommittedVersionAllowsVersionScratchCleanup() async throws {
        let client = InMemoryRemoteStorageClient()
        let versionPath = RepoLayoutLite.versionPath(basePath: basePath)
        let tmpPath = versionScratchPath(suffix: "tmp")
        let bakPath = versionScratchPath(suffix: "bak")
        await client.seedFile(path: versionPath, data: try makeVersionData())
        await client.seedFile(path: tmpPath, data: try makeVersionData())
        await client.seedFile(path: bakPath, data: Data("not json".utf8))

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        XCTAssertTrue(deleted.contains(tmpPath))
        XCTAssertTrue(deleted.contains(bakPath))
        let tmpGone = await client.fileData(path: tmpPath)
        let bakGone = await client.fileData(path: bakPath)
        let versionSurvives = await client.fileData(path: versionPath)
        XCTAssertNil(tmpGone)
        XCTAssertNil(bakGone)
        XCTAssertNotNil(versionSurvives)
    }

    func testMissingCommittedVersionLeavesVersionScratchForRouterRepair() async throws {
        let client = InMemoryRemoteStorageClient()
        let bakPath = versionScratchPath(suffix: "bak")
        await client.seedFile(path: bakPath, data: try makeVersionData())

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        XCTAssertFalse(deleted.contains(bakPath))
        let bakSurvives = await client.fileData(path: bakPath)
        XCTAssertNotNil(bakSurvives)
    }

    func testOwnershipLossBlocksScratchRestoreAndDeletes() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let validBak = scratchPath(month: month, suffix: "bak")
        let invalidTmp = scratchPath(month: month, suffix: "tmp")
        let valid = try makeMonthSqliteData()
        await client.seedFile(path: validBak, data: valid)
        await client.seedFile(path: invalidTmp, data: Data([0x01]))

        let deleted = await cleanup(client, assertOwnership: { throw LiteRepoError.ownershipLost }).run(mode: .foreground, now: base)

        let canonical = await client.fileData(path: RepoLayoutLite.monthPath(basePath: basePath, month: month))
        let bakSurvives = await client.fileData(path: validBak)
        let tmpSurvives = await client.fileData(path: invalidTmp)
        XCTAssertNil(canonical, "lost ownership must block scratch restore")
        XCTAssertEqual(bakSurvives, valid, "recoverable scratch stays in place")
        XCTAssertNotNil(tmpSurvives, "proven junk is not deleted after ownership loss")
        XCTAssertTrue(deleted.isEmpty)
        let movedPaths = await client.movedPaths
        let deletedPaths = await client.deletedPaths
        XCTAssertTrue(movedPaths.isEmpty)
        XCTAssertTrue(deletedPaths.isEmpty)
    }

    // MARK: - Validation-fault safety (R02): inconclusive download/read fault is never destructive

    func testFinalMissingValidationDownloadFaultLeavesParseableCandidateInPlace() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let bakPath = scratchPath(month: month, suffix: "bak")
        let valid = try makeMonthSqliteData()
        await client.seedFile(path: bakPath, data: valid)
        // The validation download blinks (transient/permission/backend fault) — inconclusive, not invalid.
        await client.enqueueDownloadError(RemoteErrorFixtures.retryable)

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        let bakSurvives = await client.fileData(path: bakPath)
        XCTAssertEqual(bakSurvives, valid, "an inconclusive validation fault must leave the only candidate in place")
        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        let canonical = await client.fileData(path: canonicalPath)
        XCTAssertNil(canonical, "no canonical may be fabricated from an unvalidated candidate")
        XCTAssertFalse(deleted.contains(bakPath))
    }

    func testValidationDownloadFaultLeavesUnparseableScratchInPlace() async throws {
        let client = InMemoryRemoteStorageClient()
        // Bytes that WOULD be proven unsound if readable — proving that an unread (faulting) candidate is
        // still left in place because its recoverability is unknown.
        let opaquePath = monthsDir + "/manifest_\(UUID().uuidString).tmp"
        await client.seedFile(path: opaquePath, data: Data([0x01]))
        await client.enqueueDownloadError(RemoteErrorFixtures.retryable)

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        let survives = await client.fileData(path: opaquePath)
        XCTAssertNotNil(survives, "an inconclusive validation fault must leave unparseable scratch in place")
        XCTAssertFalse(deleted.contains(opaquePath))
    }

    func testFinalMissingFaultingCandidateBlocksRestoreAndLeavesAllInPlace() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let bakPath = scratchPath(month: month, suffix: "bak")
        let tmpPath = scratchPath(month: month, suffix: "tmp")
        let valid = try makeMonthSqliteData()
        await client.seedFile(path: bakPath, data: valid)
        await client.seedFile(path: tmpPath, data: valid)
        // Exactly one of the two validation downloads blinks (FIFO): whichever is read first is
        // inconclusive, the other validates. A faulting candidate must block a confident restore.
        await client.enqueueDownloadError(RemoteErrorFixtures.retryable)

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        let bakSurvives = await client.fileData(path: bakPath)
        let tmpSurvives = await client.fileData(path: tmpPath)
        XCTAssertNotNil(bakSurvives, "a faulting sibling must block restore; candidates stay in place")
        XCTAssertNotNil(tmpSurvives)
        let canonical = await client.fileData(path: RepoLayoutLite.monthPath(basePath: basePath, month: month))
        XCTAssertNil(canonical, "no restore while a candidate's validation is inconclusive")
        XCTAssertTrue(deleted.isEmpty, "nothing is deleted when the month's recovery picture is uncertain")
    }

    func testOwnershipLossBlocksExpiredLockDelete() async throws {
        let client = InMemoryRemoteStorageClient()
        let writer = newWriterID()
        await client.seedLock(
            basePath: basePath,
            writerID: writer,
            modificationDate: base.addingTimeInterval(-(WriteLockService.expiry + 60))
        )

        let deleted = await cleanup(client, assertOwnership: { throw LiteRepoError.ownershipLost }).run(mode: .foreground, now: base)

        let exists = await client.lockExists(basePath: basePath, writerID: writer)
        XCTAssertTrue(exists, "lost ownership must leave foreign locks untouched")
        XCTAssertTrue(deleted.isEmpty)
        let deletedPaths = await client.deletedPaths
        XCTAssertTrue(deletedPaths.isEmpty)
    }
}
