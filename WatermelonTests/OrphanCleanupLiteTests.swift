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
        assertOwnership: (@Sendable () async -> Bool)? = nil
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

    private func makeMonthSqliteData() throws -> Data {
        let tmpDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("WT-orphan-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tmpDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tmpDir) }
        let dbURL = tmpDir.appendingPathComponent("month.sqlite")
        let queue = try DatabaseQueue(path: dbURL.path)
        try MonthManifestStore.migrate(queue)
        try queue.close()
        return try Data(contentsOf: dbURL)
    }

    private func scratchPath(month: LibraryMonthKey, suffix: String) -> String {
        monthsDir + "/\(RepoLayoutLite.monthFilename(month: month)).\(UUID().uuidString).\(suffix)"
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

    func testAmbiguousValidScratchIsNotDeletedWhenSoleRecoveryMaterial() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let tmpPath = scratchPath(month: month, suffix: "tmp")
        let bakPath = scratchPath(month: month, suffix: "bak")
        let valid = try makeMonthSqliteData()
        await client.seedFile(path: tmpPath, data: valid)
        await client.seedFile(path: bakPath, data: valid)

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        // Two sound candidates, no canonical: the choice is unsafe → restore nothing, delete nothing.
        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        let canonical = await client.fileData(path: canonicalPath)
        XCTAssertNil(canonical, "ambiguous scratch must not be auto-restored")
        let tmpSurvives = await client.fileData(path: tmpPath)
        let bakSurvives = await client.fileData(path: bakPath)
        XCTAssertNotNil(tmpSurvives, "ambiguous valid scratch must not be deleted")
        XCTAssertNotNil(bakSurvives)
        XCTAssertFalse(deleted.contains(tmpPath))
        XCTAssertFalse(deleted.contains(bakPath))
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

    func testExistingCanonicalAllowsCleanupOfLeftoverScratch() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        let valid = try makeMonthSqliteData()
        await client.seedFile(path: canonicalPath, data: valid)
        let leftoverBak = scratchPath(month: month, suffix: "bak")
        await client.seedFile(path: leftoverBak, data: valid)

        // Survivors that must never be touched by repair-first cleanup.
        let photoPath = "\(basePath)/2024/03/IMG_0001.JPG"
        let versionPath = RepoLayoutLite.versionPath(basePath: basePath)
        await client.seedFile(path: photoPath, data: Data([0x06]))
        await client.seedFile(path: versionPath, data: Data([0x07]))
        await client.seedDirectory(monthsDir + "/subdir")

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        XCTAssertTrue(deleted.contains(leftoverBak), "leftover scratch is cleaned when canonical is present")
        let canonicalSurvives = await client.fileData(path: canonicalPath)
        XCTAssertEqual(canonicalSurvives, valid, "the canonical month sqlite must never be touched")
        let photoSurvives = await client.fileData(path: photoPath)
        let versionSurvives = await client.fileData(path: versionPath)
        let subdirSurvives = try await client.exists(path: monthsDir + "/subdir")
        XCTAssertNotNil(photoSurvives, "photo bytes are never touched")
        XCTAssertNotNil(versionSurvives, "version.json is never touched")
        XCTAssertTrue(subdirSurvives, "directories are never deleted")
    }

    func testInvalidCanonicalDoesNotAuthorizeScratchDeletion() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        let valid = try makeMonthSqliteData()
        let leftoverBak = scratchPath(month: month, suffix: "bak")
        await client.seedFile(path: canonicalPath, data: Data([0x01]))
        await client.seedFile(path: leftoverBak, data: valid)

        let deleted = await cleanup(client).run(mode: .foreground, now: base)

        let scratchSurvives = await client.fileData(path: leftoverBak)
        XCTAssertEqual(scratchSurvives, valid, "scratch may be the only recoverable copy when canonical is invalid")
        XCTAssertFalse(deleted.contains(leftoverBak))
    }

    func testOwnershipLossBlocksScratchRestoreAndDeletes() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let validBak = scratchPath(month: month, suffix: "bak")
        let invalidTmp = scratchPath(month: month, suffix: "tmp")
        let valid = try makeMonthSqliteData()
        await client.seedFile(path: validBak, data: valid)
        await client.seedFile(path: invalidTmp, data: Data([0x01]))

        let deleted = await cleanup(client, assertOwnership: { false }).run(mode: .foreground, now: base)

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

        let deleted = await cleanup(client, assertOwnership: { false }).run(mode: .foreground, now: base)

        let exists = await client.lockExists(basePath: basePath, writerID: writer)
        XCTAssertTrue(exists, "lost ownership must leave foreign locks untouched")
        XCTAssertTrue(deleted.isEmpty)
        let deletedPaths = await client.deletedPaths
        XCTAssertTrue(deletedPaths.isEmpty)
    }
}
