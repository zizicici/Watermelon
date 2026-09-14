import XCTest
@testable import Watermelon

final class AppCacheManagerTests: XCTestCase {
    private var root: URL!
    private var locations: AppCacheManager.Locations!
    private let fm = FileManager.default

    private var remoteSnapshotDirectory: URL {
        locations.dataRoots[0].appendingPathComponent("Library/Application Support/RemoteManifestSnapshotCache")
    }

    override func setUpWithError() throws {
        root = fm.temporaryDirectory.appendingPathComponent("AppCacheManagerTests-\(UUID().uuidString)")
        let data = root.appendingPathComponent("Data")
        locations = AppCacheManager.Locations(
            thumbnails: [data.appendingPathComponent("Library/Caches/Thumbnails")],
            originals: data.appendingPathComponent("Library/Caches/Originals"),
            temporary: data.appendingPathComponent("tmp"),
            executionLogs: data.appendingPathComponent("Library/Caches/ExecutionLogs"),
            bundle: root.appendingPathComponent("App.app"),
            dataRoots: [data],
            localData: [
                data.appendingPathComponent("Library/Application Support/Watermelon"),
                data.appendingPathComponent("Library/Preferences"),
            ],
            documents: data.appendingPathComponent("Documents"),
            library: data.appendingPathComponent("Library"),
            database: data.appendingPathComponent("Library/Application Support/Watermelon/database.sqlite")
        )
    }

    override func tearDownWithError() throws {
        try? fm.removeItem(at: root)
    }

    func testOnlyUnchangedPreviousLaunchTemporaryFilesCanBeCleared() throws {
        let old = try write(locations.temporary, "restore_\(UUID().uuidString).mov", bytes: 10)
        let changed = try write(locations.temporary, "orig_\(UUID().uuidString).jpg", bytes: 20)
        let replaced = try write(locations.temporary, "imp_\(UUID().uuidString).jpg", bytes: 30)
        let manager = AppCacheManager(locations: locations)
        let current = try write(locations.temporary, "\(UUID().uuidString).mov", bytes: 40)
        try fm.setAttributes([.modificationDate: Date(timeIntervalSince1970: 1)], ofItemAtPath: current.path)
        try Data(repeating: 1, count: 21).write(to: changed)
        try Data(repeating: 1, count: 30).write(to: replaced, options: .atomic)

        let before = try XCTUnwrap(manager.usage()[.temporaryFiles])
        XCTAssertEqual(before.bytes, 101)
        XCTAssertEqual(before.clearableBytes, 10)
        XCTAssertTrue(manager.clearFiles(.temporaryFiles))
        XCTAssertFalse(fm.fileExists(atPath: old.path))
        for url in [changed, replaced, current] { XCTAssertTrue(fm.fileExists(atPath: url.path)) }
        XCTAssertEqual(manager.usage()[.temporaryFiles]?.clearableBytes, 0)
    }

    func testPreviousImportHardLinkRemainsClearableAfterOriginalCacheAccess() throws {
        let cache = OriginalPhotoCache(root: locations.originals)
        let source = try write(locations.temporary, "source", bytes: 100)
        let cached = try XCTUnwrap(cache.store(movingFrom: source, forKey: "aabb")).url
        let old = locations.temporary.appendingPathComponent("imp_\(UUID().uuidString).mov")
        try fm.linkItem(at: cached, to: old)
        let ordinary = try write(locations.temporary, "imp_\(UUID().uuidString).mov", bytes: 20)
        for url in [cached, ordinary] {
            try fm.setAttributes([.modificationDate: Date(timeIntervalSince1970: 1)], ofItemAtPath: url.path)
        }
        let manager = AppCacheManager(locations: locations)
        let current = locations.temporary.appendingPathComponent("imp_\(UUID().uuidString).mov")
        try fm.linkItem(at: cached, to: current)
        XCTAssertEqual(cache.url(forKey: "aabb"), cached)
        try Data(repeating: 2, count: 20).write(to: ordinary)
        try fm.setAttributes([.modificationDate: Date(timeIntervalSince1970: 2)], ofItemAtPath: ordinary.path)

        var lease: LocalCacheFileAccess.Lease? = LocalCacheFileAccess.shared.protect(old)
        withExtendedLifetime(lease) {
            XCTAssertEqual(manager.usage(for: .temporaryFiles).clearableBytes, 0)
            XCTAssertTrue(manager.clearFiles(.temporaryFiles))
            XCTAssertTrue(fm.fileExists(atPath: old.path))
        }
        lease = nil
        XCTAssertEqual(manager.usage(for: .temporaryFiles).clearableBytes, 100)
        XCTAssertTrue(manager.clearFiles(.originals))
        XCTAssertTrue(manager.clearFiles(.temporaryFiles))
        XCTAssertFalse(fm.fileExists(atPath: cached.path))
        XCTAssertFalse(fm.fileExists(atPath: old.path))
        XCTAssertEqual(try Data(contentsOf: current).count, 100)
        XCTAssertEqual(try Data(contentsOf: ordinary), Data(repeating: 2, count: 20))
        XCTAssertEqual(manager.usage(for: .temporaryFiles).clearableBytes, 0)
    }

    func testPreviousImportHardLinksStillRequireOriginalIdentityAndSize() throws {
        let first = try write(locations.originals, "aa/first", bytes: 30)
        let second = try write(locations.originals, "bb/second", bytes: 40)
        let resized = locations.temporary.appendingPathComponent("imp_\(UUID().uuidString).mov")
        let replaced = locations.temporary.appendingPathComponent("imp_\(UUID().uuidString).mov")
        let lateLink = try write(locations.temporary, "imp_\(UUID().uuidString).mov", bytes: 50)
        try fm.linkItem(at: first, to: resized)
        try fm.linkItem(at: second, to: replaced)
        let manager = AppCacheManager(locations: locations)
        try Data(repeating: 2, count: 31).write(to: first)
        try Data(repeating: 2, count: 40).write(to: replaced, options: .atomic)
        try fm.linkItem(at: lateLink, to: locations.originals.appendingPathComponent("late-link"))
        try fm.setAttributes([.modificationDate: Date(timeIntervalSince1970: 1)], ofItemAtPath: lateLink.path)

        XCTAssertEqual(manager.usage(for: .temporaryFiles).clearableBytes, 0)
        XCTAssertTrue(manager.clearFiles(.temporaryFiles))
        for url in [resized, replaced, lateLink] { XCTAssertTrue(fm.fileExists(atPath: url.path)) }
    }

    func testStagedSelectionsFromCurrentSessionArePreserved() throws {
        let oldSession = locations.stagedFiles.appendingPathComponent(UUID().uuidString)
        let old = try write(oldSession, "old.mov", bytes: 30)
        let manager = AppCacheManager(locations: locations)
        let newSession = locations.stagedFiles.appendingPathComponent(UUID().uuidString)
        let current = try write(newSession, "current.mov", bytes: 70)

        XCTAssertEqual(manager.usage()[.stagedFiles]?.bytes, 100)
        XCTAssertEqual(manager.usage()[.stagedFiles]?.clearableBytes, 30)
        XCTAssertTrue(manager.clearFiles(.stagedFiles))
        XCTAssertFalse(fm.fileExists(atPath: old.path))
        XCTAssertTrue(fm.fileExists(atPath: current.path))
    }

    func testUnknownFilesAndSymlinkTargetsAreNeverCleared() throws {
        let unknown = try write(locations.temporary, "user-document.mov", bytes: 10)
        let outside = try write(root, "outside.mov", bytes: 20)
        let link = locations.temporary.appendingPathComponent("orig_\(UUID().uuidString).mov")
        try fm.createSymbolicLink(at: link, withDestinationURL: outside)
        try fm.createDirectory(at: locations.originals, withIntermediateDirectories: true)
        try fm.createSymbolicLink(at: locations.originals.appendingPathComponent("linked-directory"), withDestinationURL: root)
        let manager = AppCacheManager(locations: locations)

        XCTAssertTrue(manager.clearFiles(.temporaryFiles))
        XCTAssertTrue(manager.clearFiles(.originals))
        XCTAssertEqual(manager.usage()[.temporaryFiles]?.bytes, 0)
        XCTAssertTrue(fm.fileExists(atPath: unknown.path))
        XCTAssertEqual(try Data(contentsOf: outside).count, 20)
        XCTAssertTrue(fm.fileExists(atPath: link.path))
    }

    func testOverviewCountsHardLinksOnceAndKeepsAppSeparateFromData() throws {
        let original = try write(locations.originals, "ab/abcdef", bytes: 100)
        try fm.createDirectory(at: locations.temporary, withIntermediateDirectories: true)
        try fm.linkItem(at: original, to: locations.temporary.appendingPathComponent("imp_\(UUID().uuidString).mov"))
        _ = try write(locations.thumbnails[0], "thumbnail", bytes: 10)
        _ = try write(locations.localData[0], "database.sqlite", bytes: 20)
        _ = try write(locations.temporary, "framework-file", bytes: 30)
        _ = try write(remoteSnapshotDirectory, "snapshot.json", bytes: 40)
        _ = try write(locations.executionLogs, "manual/session.log", bytes: 50)
        _ = try write(locations.bundle!, "binary", bytes: 200)
        let snapshot = AppCacheManager(locations: locations).snapshot()

        XCTAssertEqual(snapshot.app.bytes, 200)
        XCTAssertEqual(snapshot.data.bytes, 250)
        XCTAssertEqual(snapshot.categories[.originals]?.bytes, 100)
        XCTAssertEqual(snapshot.categories[.temporaryFiles]?.bytes, 100)
        XCTAssertEqual(snapshot.categories[.localData]?.bytes, 20)
        XCTAssertEqual(snapshot.categories[.otherData]?.bytes, 70)
        XCTAssertFalse(snapshot.app.hasErrors)
        XCTAssertFalse(snapshot.data.hasErrors)
    }

    func testActiveLogsAreKeptUntilWriterFinalizes() async throws {
        let old = try write(locations.executionLogs, "manual/old.log", bytes: 10)
        let active = locations.executionLogs.appendingPathComponent("manual/active.log")
        let writer = ExecutionLogSessionWriter(fileURL: active, kind: .manual, startedAt: Date())
        await writer.appendLog("In progress", level: .info)
        let manager = AppCacheManager(locations: locations)

        XCTAssertEqual(manager.usage()[.executionLogs]?.clearableBytes, 10)
        XCTAssertTrue(manager.clearFiles(.executionLogs))
        XCTAssertFalse(fm.fileExists(atPath: old.path))
        XCTAssertTrue(fm.fileExists(atPath: active.path))
        await writer.finalize()
        XCTAssertTrue(manager.clearFiles(.executionLogs))
        XCTAssertFalse(fm.fileExists(atPath: active.path))
    }

    func testProtectedOriginalCacheIsCountedButNotCleared() throws {
        let original = try write(locations.originals, "ab/key", bytes: 100)
        let manager = AppCacheManager(locations: locations)
        var lease: LocalCacheFileAccess.Lease? = LocalCacheFileAccess.shared.protect(locations.originals)
        withExtendedLifetime(lease) {
            XCTAssertEqual(manager.usage()[.originals]?.bytes, 100)
            XCTAssertEqual(manager.usage()[.originals]?.clearableBytes, 0)
            XCTAssertTrue(manager.clearFiles(.originals))
            XCTAssertTrue(fm.fileExists(atPath: original.path))
        }
        lease = nil
        XCTAssertTrue(manager.clearFiles(.originals))
        XCTAssertFalse(fm.fileExists(atPath: original.path))
    }

    func testRemoteSnapshotsAreCountedAsReadOnlyOtherData() throws {
        let snapshot = try write(remoteSnapshotDirectory, "profile.json", bytes: 10)
        let unknown = try write(remoteSnapshotDirectory, "unknown.sqlite", bytes: 20)
        let database = try write(locations.localData[0], "database.sqlite", bytes: 30)
        let manager = AppCacheManager(locations: locations)

        XCTAssertEqual(manager.usage()[.otherData]?.bytes, 30)
        XCTAssertEqual(manager.usage()[.otherData]?.clearableBytes, 0)
        XCTAssertEqual(manager.snapshot().data.bytes, 60)
        for category in AppCacheManager.Category.allCases where category.allowsClearing {
            XCTAssertTrue(manager.clearFiles(category))
        }
        XCTAssertFalse(manager.clearFiles(.localData))
        XCTAssertFalse(manager.clearFiles(.otherData))
        XCTAssertTrue(fm.fileExists(atPath: snapshot.path))
        XCTAssertTrue(fm.fileExists(atPath: unknown.path))
        XCTAssertTrue(fm.fileExists(atPath: database.path))
    }

    func testOtherDataDetailsIdentifyDirectoriesWithoutDoubleCountingKnownCategories() throws {
        _ = try write(remoteSnapshotDirectory, "profile.json", bytes: 10)
        _ = try write(locations.library!, "Caches/Network/response", bytes: 20)
        _ = try write(locations.library!, "Application Support/Service/state", bytes: 30)
        _ = try write(locations.library!, "SplashBoard/snapshot", bytes: 40)
        _ = try write(locations.documents!, "file", bytes: 50)
        _ = try write(locations.temporary, "framework/nested/file", bytes: 60)
        _ = try write(locations.temporary, "unknown", bytes: 70)
        _ = try write(locations.originals, "cached", bytes: 100)
        _ = try write(locations.localData[0], "database.sqlite", bytes: 100)
        _ = try write(locations.temporary, "orig_\(UUID().uuidString).jpg", bytes: 100)
        _ = try write(locations.stagedFiles, "\(UUID().uuidString)/selected", bytes: 100)
        let manager = AppCacheManager(locations: locations)
        let details = manager.otherDataDetails()
        let entries = Dictionary(uniqueKeysWithValues: details.entries.map { ($0.path, $0) })

        XCTAssertEqual(details.total.bytes, 280)
        XCTAssertEqual(details.total.bytes, manager.usage(for: .otherData).bytes)
        XCTAssertEqual(entries.count, 7)
        XCTAssertEqual(entries["Library/Application Support/RemoteManifestSnapshotCache"]?.usage.bytes, 10)
        XCTAssertEqual(entries["Library/Application Support/RemoteManifestSnapshotCache"]?.kind, .remoteIndex)
        XCTAssertEqual(entries["Library/Caches/Network"]?.usage.bytes, 20)
        XCTAssertEqual(entries["Library/Application Support/Service"]?.usage.bytes, 30)
        XCTAssertEqual(entries["Library/SplashBoard"]?.usage.bytes, 40)
        XCTAssertEqual(entries["Documents"]?.usage.bytes, 50)
        XCTAssertEqual(entries["tmp/framework"]?.usage.bytes, 60)
        XCTAssertEqual(entries["tmp"]?.usage.bytes, 70)
        XCTAssertTrue(details.entries.allSatisfy { !$0.usage.hasErrors && $0.usage.clearableBytes == 0 })
    }

    func testOtherDataDetailsDeduplicateHardLinksAndSurfaceIncompleteScans() throws {
        let source = try write(locations.library!, "Caches/Network/file", bytes: 40)
        try fm.linkItem(at: source, to: source.deletingLastPathComponent().appendingPathComponent("copy"))
        try fm.createDirectory(at: locations.documents!, withIntermediateDirectories: true)
        try fm.linkItem(at: source, to: locations.documents!.appendingPathComponent("shared"))
        let details = AppCacheManager(locations: locations).otherDataDetails()
        XCTAssertEqual(details.total.bytes, 40)
        XCTAssertEqual(details.entries.count, 2)
        XCTAssertTrue(details.entries.allSatisfy { $0.usage.bytes == 40 })

        let invalidRoot = try write(root, "not-a-directory", bytes: 10)
        locations.dataRoots.append(invalidRoot)
        let incomplete = AppCacheManager(locations: locations).otherDataDetails()
        XCTAssertTrue(incomplete.total.hasErrors)
        XCTAssertEqual(incomplete.entries.filter { $0.usage.hasErrors }.map(\.path), ["not-a-directory"])
        XCTAssertEqual(incomplete.total.bytes, 40)
    }

    func testLocalDataDetailsSeparateDatabaseFilesAndPreferencesWithoutReadingContents() throws {
        _ = try write(locations.localData[0], "database.sqlite", bytes: 100)
        _ = try write(locations.localData[0], "database.sqlite-wal", bytes: 200)
        _ = try write(locations.localData[0], "database.sqlite-shm", bytes: 30)
        _ = try write(locations.localData[0], "database.sqlite-journal", bytes: 40)
        _ = try write(locations.localData[1], "app.plist", bytes: 5)
        _ = try write(locations.localData[1], "sdk.plist", bytes: 6)
        _ = try write(locations.localData[0], "archive/database.sqlite", bytes: 7)
        let outside = try write(root, "outside", bytes: 999)
        try fm.createSymbolicLink(at: locations.localData[0].appendingPathComponent("link"), withDestinationURL: outside)
        _ = try write(remoteSnapshotDirectory, "remote.json", bytes: 10)
        let manager = AppCacheManager(locations: locations)
        let details = manager.localDataDetails()
        let entries = Dictionary(uniqueKeysWithValues: details.entries.map { ($0.path, $0) })
        let prefix = "Library/Application Support/Watermelon/"

        XCTAssertEqual(details.total.bytes, 388)
        XCTAssertEqual(details.total.bytes, manager.usage(for: .localData).bytes)
        XCTAssertEqual(entries.count, 7)
        XCTAssertEqual(entries[prefix + "database.sqlite"]?.kind, .database)
        XCTAssertEqual(entries[prefix + "database.sqlite"]?.usage.bytes, 100)
        XCTAssertEqual(entries[prefix + "database.sqlite-wal"]?.kind, .databaseLog)
        XCTAssertEqual(entries[prefix + "database.sqlite-wal"]?.usage.bytes, 200)
        XCTAssertEqual(entries[prefix + "database.sqlite-shm"]?.kind, .databaseMemory)
        XCTAssertEqual(entries[prefix + "database.sqlite-shm"]?.usage.bytes, 30)
        XCTAssertEqual(entries[prefix + "database.sqlite-journal"]?.kind, .databaseJournal)
        XCTAssertEqual(entries[prefix + "archive/database.sqlite"]?.kind, .localFiles)
        XCTAssertEqual(entries["Library/Preferences/app.plist"]?.kind, .preferences)
        XCTAssertEqual(entries["Library/Preferences/sdk.plist"]?.usage.bytes, 6)
        XCTAssertTrue(details.entries.allSatisfy { !$0.usage.hasErrors && $0.usage.clearableBytes == 0 })
        XCTAssertEqual(manager.usage(for: .otherData).bytes, 10)
        XCTAssertEqual(manager.snapshot().data.bytes, 398)
        XCTAssertFalse(manager.clearFiles(.localData))
    }

    func testLocalDataDetailsKeepSharedFileTotalsAndIdentifyFailedDirectories() throws {
        let source = try write(locations.localData[0], "database.sqlite", bytes: 40)
        try fm.linkItem(at: source, to: locations.localData[0].appendingPathComponent("copy"))
        let failed = try write(locations.library!, "InvalidData", bytes: 10)
        locations.localData.append(failed)
        let details = AppCacheManager(locations: locations).localDataDetails()
        XCTAssertEqual(details.total.bytes, 40)
        XCTAssertTrue(details.total.hasErrors)
        XCTAssertEqual(details.entries.count, 3)
        XCTAssertEqual(details.entries.filter { $0.usage.hasErrors }.map(\.path), ["Library/InvalidData"])
        XCTAssertEqual(details.entries.filter { !$0.usage.hasErrors }.map { $0.usage.bytes }, [40, 40])
    }

    func testScanFailureIsNotReportedAsAnEmptyCache() throws {
        _ = try write(locations.originals.deletingLastPathComponent(), locations.originals.lastPathComponent, bytes: 10)
        let manager = AppCacheManager(locations: locations)
        XCTAssertTrue(try XCTUnwrap(manager.usage()[.originals]).hasErrors)
        XCTAssertTrue(manager.snapshot().data.hasErrors)
        XCTAssertFalse(manager.clearFiles(.originals))
    }

    func testOwnedTemporaryNamesCoverMediaAndMetadataButExcludeUnrelatedFiles() {
        let id = UUID().uuidString
        for name in [id, "\(id).mov", "restore_\(id).mov", "restore_import_\(id).jpg",
                     "imp_\(id).mov", "live_local_\(id).mov", "Watermelon-Transfer-\(id)",
                     ".sftp-download-\(id).tmp", "thumb_up_abcd_\(id).jpg",
                     "month_manifest_2026_9_\(id).sqlite-wal", "remote_compare_\(id)_file.mov",
                     "v1lite_\(id).sqlite-wal", "legacy-v1-prune-\(id).json",
                     "movecheck-\(id)", "movecheck-\(id)-verify", "s3-probe-\(id)-a",
                     "s3-probe-\(id)-download", ".watermelon-probe-\(id)-upload-b"] {
            XCTAssertTrue(AppCacheManager.isOwnedTemporaryFile(name), name)
        }
        for name in ["user.mov", "database.sqlite", "orig_user.mov", "\(id)-unrelated", ".DS_Store",
                     "v1lite_user.sqlite", "movecheck-\(id)-user", "s3-probe-\(id)-user"] {
            XCTAssertFalse(AppCacheManager.isOwnedTemporaryFile(name), name)
        }
    }

    func testMigrationAndProbeLeftoversAreClearableWithoutDeletingCurrentFiles() throws {
        let old = try write(locations.temporary, "v1lite_\(UUID().uuidString).sqlite", bytes: 100)
        let probe = try write(locations.temporary, ".watermelon-probe-\(UUID().uuidString)-download", bytes: 10)
        let manager = AppCacheManager(locations: locations)
        let current = try write(locations.temporary, "v1lite_\(UUID().uuidString).sqlite", bytes: 50)
        XCTAssertEqual(manager.usage()[.temporaryFiles]?.clearableBytes, 110)
        XCTAssertTrue(manager.clearFiles(.temporaryFiles))
        XCTAssertFalse(fm.fileExists(atPath: old.path))
        XCTAssertFalse(fm.fileExists(atPath: probe.path))
        XCTAssertTrue(fm.fileExists(atPath: current.path))
    }

    func testThumbnailClearFailureIsReportedWhenOldFilesRemain() async throws {
        let old = try write(locations.thumbnails[0], "old", bytes: 10)
        let manager = AppCacheManager(locations: locations, thumbnailClearer: {})
        let success = await manager.clear(.thumbnails)
        XCTAssertFalse(success)
        XCTAssertTrue(fm.fileExists(atPath: old.path))
    }

    func testManualOriginalLimitChangePreservesActiveFilesAndTrimsAfterRelease() throws {
        let cache = OriginalPhotoCache(root: locations.originals)
        let first = try write(locations.temporary, "source", bytes: 100)
        let second = try write(locations.temporary, "another-source", bytes: 50)
        let protected = try XCTUnwrap(cache.store(movingFrom: first, forKey: "aabb")).url
        let unused = try XCTUnwrap(cache.store(movingFrom: second, forKey: "ccdd")).url
        var lease: LocalCacheFileAccess.Lease? = LocalCacheFileAccess.shared.protect(protected)
        withExtendedLifetime(lease) {
            cache.enforceCap(maxBytes: 0, preservingActiveFiles: true)
            XCTAssertTrue(fm.fileExists(atPath: protected.path))
            XCTAssertFalse(fm.fileExists(atPath: unused.path))
        }
        lease = nil
        cache.enforceCap(maxBytes: 0, preservingActiveFiles: true)
        XCTAssertEqual(cache.diskSizeBytes(), 0)
    }

    @discardableResult
    private func write(_ directory: URL, _ name: String, bytes: Int) throws -> URL {
        let url = directory.appendingPathComponent(name)
        try fm.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        try Data(repeating: 1, count: bytes).write(to: url)
        return url
    }
}
