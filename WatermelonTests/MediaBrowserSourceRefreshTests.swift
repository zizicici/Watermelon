import Foundation
import SQLite3
import XCTest
@testable import Watermelon

@MainActor
final class MediaBrowserSourceRefreshTests: XCTestCase {
    func testRemoteProjectionRendersCapturedCommitsWithoutMakingStalePresenceAuthoritative() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        for _ in 0..<4 {
            fixture.seedChanges.publish()
            let revision = fixture.cache.currentRevision()
            let capturedSize = fixture.input.fileSize
            guard case .loaded(let content) = await fixture.remote.load() else {
                return XCTFail("Ordinary commits must not starve a captured remote projection")
            }
            let item = try XCTUnwrap(content.sections.flatMap(\.items).first)
            XCTAssertEqual(content.sections.flatMap(\.items).count, 1)
            XCTAssertEqual(item.fileSize, capturedSize)
            XCTAssertEqual(item.localIdentifier, "local")
            XCTAssertGreaterThan(fixture.cache.currentRevision(), revision)
            XCTAssertFalse(fixture.presence.isRemotePresenceAuthoritative)
            XCTAssertEqual(fixture.presence.backupPresenceVerdict(fixture.input.fingerprint), .unknown)
        }
        fixture.input.setReadMutation(.none)
        fixture.seedChanges.publish()
        guard case .loaded(let current) = await fixture.remote.load() else {
            return XCTFail("Expected the trailing refresh to become current")
        }
        XCTAssertEqual(current.sections.flatMap(\.items).first?.fileSize, fixture.input.fileSize)
        XCTAssertTrue(fixture.presence.isRemotePresenceAuthoritative)
        XCTAssertEqual(fixture.presence.backupPresenceVerdict(fixture.input.fingerprint), .complete)
    }

    func testLocalSourcePreservesCapturedPresenceAndFiltersWhileRemoteRevisionAdvances() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        for _ in 0..<4 {
            fixture.seedChanges.publish()
            let revision = fixture.cache.currentRevision()
            guard case .loaded(let content) = await fixture.local.load() else {
                return XCTFail("Local enumeration must not depend on committing remote authority")
            }
            let items = content.sections.flatMap(\.items)
            XCTAssertEqual(items.map(\.localIdentifier), ["local"])
            XCTAssertEqual(items.first?.presence, .both)
            let backedUp = MediaBrowserSnapshot(sections: content.sections, filter: .backedUp)
            let localOnly = MediaBrowserSnapshot(sections: content.sections, filter: .localOnly)
            XCTAssertEqual(backedUp.orderedItemIDs, items.map(\.id))
            XCTAssertTrue(localOnly.isEmpty)
            XCTAssertGreaterThan(fixture.cache.currentRevision(), revision)
            XCTAssertFalse(fixture.presence.isRemotePresenceAuthoritative)
            XCTAssertEqual(fixture.presence.backupPresenceVerdict(fixture.input.fingerprint), .unknown)
        }
        fixture.input.setReadMutation(.none)
        fixture.seedChanges.publish()
        guard case .loaded(let current) = await fixture.local.load() else {
            return XCTFail("Expected current local presence")
        }
        XCTAssertEqual(current.sections.flatMap(\.items).first?.presence, .both)
        XCTAssertTrue(fixture.presence.isRemotePresenceAuthoritative)
    }

    func testMergedSourceDeduplicatesUsingItsCapturedRemoteProjectionDuringCommits() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let merged = MergedMediaSource(localSource: fixture.local, remoteSource: fixture.remote)
        for _ in 0..<4 {
            fixture.seedChanges.publish()
            let capturedSize = fixture.input.fileSize
            guard case .loaded(let content) = await merged.load() else {
                return XCTFail("A same-scope captured projection must remain renderable")
            }
            let items = content.sections.flatMap(\.items)
            XCTAssertEqual(items.count, 1)
            XCTAssertEqual(items.first?.presence, .both)
            XCTAssertEqual(items.first?.fileSize, capturedSize)
            XCTAssertEqual(content.localFingerprintAliases, ["local": fixture.input.fingerprint])
            XCTAssertFalse(fixture.presence.isRemotePresenceAuthoritative)
        }
    }

    func testLocalSourcePreservesCapturedPresenceDuringLocalInvalidations() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        fixture.input.setReadMutation(.local)
        for _ in 0..<4 {
            guard case .loaded(let content) = await fixture.local.load() else {
                return XCTFail("Local invalidations must not starve a captured projection")
            }
            XCTAssertEqual(content.sections.flatMap(\.items).first?.presence, .both)
            XCTAssertEqual(fixture.presence.backupPresenceVerdict(fixture.input.fingerprint), .unknown)
        }
        fixture.input.setReadMutation(.none)
        guard case .loaded = await fixture.local.load() else { return XCTFail("Expected recovery") }
        XCTAssertEqual(fixture.presence.backupPresenceVerdict(fixture.input.fingerprint), .complete)
    }

    func testProfileSwitchDuringInputLoadingStillRejectsEverySource() async throws {
        for mode in [MediaBrowserMode.local, .remote, .merged] {
            let fixture = try Fixture()
            defer { fixture.close() }
            fixture.input.setReadMutation(.profile)
            let source: MediaBrowserSource
            switch mode {
            case .local: source = fixture.local
            case .remote: source = fixture.remote
            case .merged: source = MergedMediaSource(localSource: fixture.local, remoteSource: fixture.remote)
            }
            guard case .stale = await source.load() else {
                return XCTFail("A profile switch must reject the previous scope in \(mode)")
            }
            XCTAssertFalse(fixture.presence.isRemotePresenceAuthoritative)
        }
    }

    func testTimeZoneSwitchDuringProjectionStillRejectsCapturedContent() async throws {
        let previous = MonthGroupingTimeZonePreference.current
        defer { MonthGroupingTimeZonePreference.setCurrent(previous) }
        for mode in [MediaBrowserMode.local, .remote, .merged] {
            MonthGroupingTimeZonePreference.setCurrent(.init(mode: .fixedOffset, offsetSeconds: 0))
            let fixture = try Fixture()
            defer { fixture.close() }
            fixture.input.setReadMutation(.timeZone)
            let source: MediaBrowserSource
            switch mode {
            case .local: source = fixture.local
            case .remote: source = fixture.remote
            case .merged: source = MergedMediaSource(localSource: fixture.local, remoteSource: fixture.remote)
            }
            guard case .stale = await source.load() else {
                return XCTFail("A changed grouping time zone is a semantic fence in \(mode)")
            }
        }
    }

    func testFailedLocalPresenceReadIsUnavailableAndRecoversWithoutACommit() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let presence = LibraryPresenceIndex(hashIndexRepository: fixture.dependencies.hashIndexRepository,
            coordinator: fixture.dependencies.backupCoordinator, profileKey: { nil })
        fixture.interruptDatabaseReads(true)
        defer { fixture.interruptDatabaseReads(false) }
        let failed = await presence.prepareLocalBrowserProjection()
        XCTAssertNil(failed)
        XCTAssertFalse(presence.isRemotePresenceAuthoritative)

        fixture.interruptDatabaseReads(false)
        let recovered = await presence.prepareLocalBrowserProjection()
        XCTAssertNotNil(recovered)
        XCTAssertTrue(presence.isRemotePresenceAuthoritative)
    }

    func testLocalEnumerationReadFailureDoesNotPublishUnfingerprintedContent() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        fixture.input.setReadMutation(.none)
        let source = LocalMediaSource(photoLibraryService: fixture.dependencies.photoLibraryService,
            hashIndexRepository: fixture.dependencies.hashIndexRepository, presenceIndex: fixture.presence,
            query: .library(.photos), fileSizeLoader: { _ in [:] })
        fixture.interruptDatabaseReads(true)
        defer { fixture.interruptDatabaseReads(false) }
        guard case .stale = await source.load() else {
            return XCTFail("An unreadable index must not look like a successfully loaded empty index")
        }
        fixture.interruptDatabaseReads(false)
        guard case .loaded = await source.load() else {
            return XCTFail("A later read must recover without a new commit")
        }
    }

    private final class Fixture {
        let directory: URL
        let dependencies: DependencyContainer
        let cache: RemoteLibrarySnapshotCache
        let input: ChangingInput
        let seedChanges = ChangeSignal<Void>()
        let presence: LibraryPresenceIndex
        let local: LocalMediaSource
        let remote: RemoteMediaSource

        init() throws {
            directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            dependencies = DependencyContainer(
                databaseManager: try DatabaseManager(databaseURL: directory.appendingPathComponent("index.sqlite")),
                reconcileOneDriveAccounts: false
            )
            let profile = ServerProfileRecord(id: nil, name: "test", storageType: StorageType.webdav.rawValue,
                connectionParams: nil, sortOrder: 0, host: "fixture.local", port: 0, shareName: "test", basePath: "/p",
                username: "test", domain: nil, credentialRef: "test", backgroundBackupEnabled: false,
                createdAt: Date(), updatedAt: Date(), writerID: nil)
            cache = RemoteLibrarySnapshotCache()
            input = ChangingInput(cache: cache, profileKey: RemoteIndexSyncService.remoteProfileKey(profile),
                seedChanges: seedChanges)
            let coordinator = BackupCoordinator(photoLibraryService: dependencies.photoLibraryService,
                storageClientFactory: dependencies.storageClientFactory, hashIndexRepository: dependencies.hashIndexRepository,
                databaseManager: dependencies.databaseManager, remoteIndexService: RemoteIndexSyncService(snapshotCache: cache))
            let input = input
            presence = LibraryPresenceIndex(hashIndexRepository: dependencies.hashIndexRepository,
                coordinator: coordinator, profileKey: { input.profileKey },
                homeLocalSeedProvider: { input.loadSeed() }, homeLocalSeedChanges: seedChanges)
            local = LocalMediaSource(photoLibraryService: dependencies.photoLibraryService,
                hashIndexRepository: dependencies.hashIndexRepository, presenceIndex: presence, fileSizeLoader: { _ in [:] })
            remote = RemoteMediaSource(service: RemoteThumbnailService(storageClientFactory: dependencies.storageClientFactory,
                presenceIndex: presence, profile: profile, password: ""))
        }

        func close() {
            try? dependencies.databaseManager.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }

        func interruptDatabaseReads(_ interrupt: Bool) {
            dependencies.databaseManager.dbQueue.writeWithoutTransaction { db in
                if interrupt {
                    sqlite3_progress_handler(db.sqliteConnection, 1, { _ in 1 }, nil)
                } else {
                    sqlite3_progress_handler(db.sqliteConnection, 0, nil, nil)
                }
            }
        }
    }

    private final class ChangingInput: @unchecked Sendable {
        enum Mutation { case commit, local, profile, timeZone, none }

        let fingerprint: Data
        private let cache: RemoteLibrarySnapshotCache
        private let seedChanges: ChangeSignal<Void>
        private let hash = Data(repeating: 7, count: 32)
        private let month = LibraryMonthKey(year: 2026, month: 1)
        private let seed: HomeBrowserLocalSeed
        private let lock = NSLock()
        private var key: String
        private var size: Int64 = 100
        private var mutation = Mutation.commit

        var profileKey: String { lock.withLock { key } }
        var fileSize: Int64 { lock.withLock { size } }

        init(cache: RemoteLibrarySnapshotCache, profileKey: String, seedChanges: ChangeSignal<Void>) {
            self.cache = cache
            self.seedChanges = seedChanges
            key = profileKey
            fingerprint = BackupAssetResourcePlanner.assetFingerprint(
                resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)])
            seed = HomeBrowserLocalSeed(localIDByFingerprint: [fingerprint: "local"], assets: [
                .init(localIdentifier: "local", month: month, kind: .photo, creationDateMs: 1_000, fingerprint: fingerprint)
            ], monthGroupingTimeZone: .frozenCurrent())
            cache.setProfileKey(profileKey)
            commit(fileSize: size)
        }

        func setReadMutation(_ mutation: Mutation) { lock.withLock { self.mutation = mutation } }

        func loadSeed() -> HomeBrowserLocalSeed {
            let mutation = lock.withLock { mutation }
            switch mutation {
            case .commit:
                let size = lock.withLock { size += 1; return size }
                commit(fileSize: size)
            case .profile:
                let key = lock.withLock { key += "-other"; return key }
                cache.setProfileKey(key)
            case .local:
                seedChanges.publish()
            case .timeZone:
                MonthGroupingTimeZonePreference.setCurrent(.init(mode: .fixedOffset, offsetSeconds: 3_600))
            case .none: break
            }
            return seed
        }

        private func commit(fileSize: Int64) {
            cache.replaceMonth(month,
                resources: [TestFixtures.remoteResource(year: month.year, month: month.month, contentHash: hash, fileSize: fileSize)],
                assets: [TestFixtures.remoteAsset(year: month.year, month: month.month, fingerprint: fingerprint,
                    creationDateMs: 1_000, totalFileSizeBytes: fileSize)],
                assetResourceLinks: [TestFixtures.remoteLink(year: month.year, month: month.month,
                    assetFingerprint: fingerprint, resourceHash: hash)])
        }
    }
}
