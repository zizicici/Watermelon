import XCTest
@testable import Watermelon

final class RestoreOriginTests: XCTestCase {
    private var directory: URL!
    private var database: DatabaseManager!
    private var repository: ContentHashIndexRepository!
    private let importedAt = Date(timeIntervalSince1970: 2_000)

    override func setUpWithError() throws {
        directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        database = try DatabaseManager(databaseURL: directory.appendingPathComponent("index.sqlite"))
        repository = ContentHashIndexRepository(databaseManager: database)
    }

    override func tearDownWithError() throws {
        repository = nil
        database = nil
        try FileManager.default.removeItem(at: directory)
    }

    private func instances(metadata: UInt8 = 7, video: UInt8 = 2) -> [RemoteAssetResourceInstance] {
        [2, 5, 6, 7].map { role in
            RemoteAssetResourceInstance(role: role, slot: 0,
                resourceHash: Data(repeating: role == 7 ? metadata : role == 2 ? video : UInt8(role), count: 32),
                fileName: "resource-\(role)", fileSize: 100, remoteRelativePath: "2026/01/resource-\(role)", creationDateMs: 1_000)
        }
    }

    private func fingerprint(_ resources: [RemoteAssetResourceInstance]) -> Data {
        BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: resources.map {
            (role: $0.role, slot: $0.slot, contentHash: $0.resourceHash)
        })
    }

    private func start(_ resources: [RemoteAssetResourceInstance]? = nil, incomplete: Bool = false) throws {
        try repository.recordRestoreImport(profileKey: "A", remoteFingerprint: fingerprint(instances()),
            assetLocalIdentifier: "restored", instances: resources ?? instances(), isIncomplete: incomplete, importedAt: importedAt)
    }

    private func index(_ resources: [RemoteAssetResourceInstance], modificationDateMs: Int64 = 1_999_000) throws {
        try repository.writeHashIndex(assetLocalIdentifier: "restored", remoteAssetFingerprint: fingerprint(instances()),
            instances: resources, modificationDateMs: modificationDateMs)
    }

    func testVerifiedOriginSurvivesRestartAndIndexRebuildWithoutReplacingActualFingerprint() throws {
        try start()
        XCTAssertTrue(try repository.fetchRestoreOrigins(profileKey: "A").isEmpty)
        let actual = instances(metadata: 8)
        try index(actual)
        let local = try XCTUnwrap(repository.fetchAssetHashCaches(assetIDs: ["restored"])["restored"])
        XCTAssertEqual(local.assetFingerprint, fingerprint(actual))
        XCTAssertNotEqual(local.assetFingerprint, fingerprint(instances()))
        repository = ContentHashIndexRepository(databaseManager: try DatabaseManager(databaseURL: directory.appendingPathComponent("index.sqlite")))
        XCTAssertEqual(try repository.fetchRestoreOrigins(profileKey: "A").count, 1)
        try repository.clearLocalHashIndex()
        XCTAssertTrue(try repository.fetchRestoreOrigins(profileKey: "A").isEmpty)
        try index(actual)
        XCTAssertEqual(try repository.fetchRestoreOrigins(profileKey: "A").count, 1)
        XCTAssertTrue(try repository.fetchRestoreOrigins(profileKey: "B").isEmpty)
        XCTAssertTrue(try repository.fetchRestoreOrigins(profileKey: nil).isEmpty)
        XCTAssertTrue(try repository.fetchRestoreOrigins(profileKey: "A", assetIDs: ["another"]).isEmpty)
    }

    func testEditOrDeletionInvalidatesOrigin() throws {
        try start()
        try index(instances(metadata: 8))
        try index(instances(metadata: 9, video: 10))
        XCTAssertTrue(try repository.fetchRestoreOrigins(profileKey: "A").isEmpty)
        try repository.deleteIndexEntries(assetIDs: ["restored"])
        XCTAssertTrue(try repository.fetchRestoreOrigins(profileKey: "A").isEmpty)
    }

    func testFingerprintLookupKeepsCurrentTwinsAndExcludesOtherSourcesAndProfiles() throws {
        let source = instances(), actual = instances(metadata: 8)
        let remote = fingerprint(source)
        for (id, profile, resources) in [("restored", "A", source), ("twin", "A", source),
                                          ("other-source", "A", instances(video: 9)), ("other-profile", "B", source)] {
            try repository.recordRestoreImport(profileKey: profile, remoteFingerprint: fingerprint(resources),
                assetLocalIdentifier: id, instances: resources, isIncomplete: false, importedAt: importedAt)
            try repository.writeHashIndex(assetLocalIdentifier: id, remoteAssetFingerprint: fingerprint(resources),
                instances: id == "other-source" ? resources : actual, modificationDateMs: 1_999_000)
        }
        XCTAssertEqual(Set(try repository.fetchRestoreOrigins(profileKey: "A", remoteFingerprints: [remote])
            .map(\.assetLocalIdentifier)), ["restored", "twin"])
        XCTAssertEqual(try repository.fetchRestoreOrigins(profileKey: "B", remoteFingerprints: [remote])
            .map(\.assetLocalIdentifier), ["other-profile"])
        XCTAssertTrue(try repository.fetchRestoreOrigins(profileKey: "A", remoteFingerprints: [fingerprint(actual)]).isEmpty)
        XCTAssertTrue(try repository.fetchRestoreOrigins(profileKey: "A", remoteFingerprints: []).isEmpty)
        XCTAssertTrue(try repository.fetchRestoreOrigins(profileKey: nil, remoteFingerprints: [remote]).isEmpty)
        try index(instances(metadata: 9, video: 10))
        XCTAssertEqual(try repository.fetchRestoreOrigins(profileKey: "A", remoteFingerprints: [remote])
            .map(\.assetLocalIdentifier), ["twin"])
        try repository.deleteIndexEntries(assetIDs: ["twin"])
        XCTAssertTrue(try repository.fetchRestoreOrigins(profileKey: "A", remoteFingerprints: [remote]).isEmpty)
    }

    func testFingerprintLookupChunksLargeRequestsWithoutLoadingUnrequestedOrigins() throws {
        let fingerprints = (0..<1_005).map { value in
            Data([UInt8(value >> 8), UInt8(value & 255)] + Array(repeating: UInt8(0), count: 30))
        }
        try database.write { db in
            for (index, fingerprint) in fingerprints.enumerated() {
                let id = "restored-\(index)"
                try db.execute(sql: "INSERT INTO local_assets (assetLocalIdentifier, assetFingerprint, updatedAt) VALUES (?, ?, ?)",
                    arguments: [id, fingerprint, importedAt])
                try db.execute(sql: """
                    INSERT INTO restore_origins
                    (profileKey, remoteFingerprint, assetLocalIdentifier, localFingerprint, sourceResources, completeCandidate, isEquivalent, importedAtMs)
                    VALUES ('A', ?, ?, ?, ?, 1, 1, ?)
                    """, arguments: [fingerprint, id, fingerprint, Data("[]".utf8), importedAt.millisecondsSinceEpoch])
            }
        }
        let requested = Set(fingerprints.prefix(905))
        let origins = try repository.fetchRestoreOrigins(profileKey: "A", remoteFingerprints: requested)
        XCTAssertEqual(origins.count, requested.count)
        XCTAssertEqual(Set(origins.map(\.remoteFingerprint)), requested)
        XCTAssertEqual(try repository.fetchRestoreOrigins(profileKey: "A", remoteFingerprints: [fingerprints[904]])
            .map(\.assetLocalIdentifier), ["restored-904"])
    }

    func testPartialImportDoesNotClaimCompleteRemoteAsset() throws {
        try start(incomplete: true)
        try index(instances(metadata: 8))
        XCTAssertTrue(try repository.fetchRestoreOrigins(profileKey: "A").isEmpty)
    }

    func testDroppedResourcesDoNotClaimCompleteRemoteAsset() throws {
        let subset = Array(instances().prefix(1))
        try start(subset)
        try index(subset)
        XCTAssertTrue(try repository.fetchRestoreOrigins(profileKey: "A").isEmpty)
    }

    func testChangedMediaDuringImportDoesNotEstablishEquivalence() throws {
        try start()
        try index(instances(metadata: 8, video: 9))
        XCTAssertTrue(try repository.fetchRestoreOrigins(profileKey: "A").isEmpty)
    }

    func testPendingImportEditedBeforeRecoveryDoesNotEstablishEquivalence() throws {
        try start()
        try index(instances(metadata: 8), modificationDateMs: 2_001_000)
        XCTAssertTrue(try repository.fetchRestoreOrigins(profileKey: "A").isEmpty)
    }

    func testPendingImportCanBeRecoveredByRegularHashIndexWriter() throws {
        try start()
        let actual = instances(metadata: 8)
        try repository.upsertAssetHashSnapshot(assetLocalIdentifier: "restored", assetFingerprint: fingerprint(actual),
            resources: actual.map { LocalAssetResourceHashRecord(role: $0.role, slot: $0.slot, contentHash: $0.resourceHash, fileSize: $0.fileSize) },
            totalFileSizeBytes: 400, modificationDateMs: 1_999_000)
        XCTAssertEqual(try repository.fetchRestoreOrigins(profileKey: "A").count, 1)
    }

    func testHomeCountsOriginalAndRestoredCopyAgainstOneRemoteIdentity() {
        let remote = fingerprint(instances()), local = fingerprint(instances(metadata: 8))
        let engine = HomeLocalIndexEngine()
        engine.restoreOrigins = RestoreOriginIndex([RestoreOrigin(assetLocalIdentifier: "restored", localFingerprint: local, remoteFingerprint: remote)])
        let now = Date()
        _ = engine.reload(payload: TestFixtures.initialPayload([[
            TestFixtures.snapshot(id: "original", year: 2026, month: 1, kind: .video),
            TestFixtures.snapshot(id: "restored", year: 2026, month: 1, kind: .video)
        ]]), fingerprintByAsset: ["original": .init(fingerprint: remote, updatedAt: now),
                                 "restored": .init(fingerprint: local, updatedAt: now)], remoteFingerprintsForMonth: { _ in [remote] })
        XCTAssertEqual(engine.localMonthSummary(for: .init(year: 2026, month: 1))?.backedUpCount, 1)
        XCTAssertEqual(engine.localMonthSummary(for: .init(year: 2026, month: 1))?.videoCount, 2)
        XCTAssertEqual(engine.currentBrowserLocalSeed()?.localIDByFingerprint[local], "restored")
    }

    func testBrowserBindsOnlyCurrentVerifiedVariantAndProjectsRemoteIdentity() throws {
        let remote = fingerprint(instances()), local = fingerprint(instances(metadata: 8))
        let origins = RestoreOriginIndex([RestoreOrigin(assetLocalIdentifier: "restored", localFingerprint: local, remoteFingerprint: remote)])
        XCTAssertEqual(LibraryPresenceIndex.selectCurrentHandles(mapHits: [remote: "restored"], alternativesByFingerprint: [:],
            currentFingerprintsByAssetID: ["restored": local], restoreOrigins: origins), [remote: "restored"])
        XCTAssertTrue(LibraryPresenceIndex.selectCurrentHandles(mapHits: [remote: "restored"], alternativesByFingerprint: [:],
            currentFingerprintsByAssetID: ["restored": Data([9])], restoreOrigins: origins).isEmpty)
        XCTAssertTrue(LibraryPresenceIndex.selectCurrentHandles(mapHits: [remote: "restored"], alternativesByFingerprint: [:],
            currentFingerprintsByAssetID: [:], restoreOrigins: origins).isEmpty)
        let seed = HomeBrowserLocalSeed(localIDByFingerprint: [local: "restored"], assets: [
            HomeBrowserLocalAsset(localIdentifier: "restored", month: .init(year: 2026, month: 1), kind: .video,
                creationDateMs: 1_000, fingerprint: local)
        ], monthGroupingTimeZone: .frozenCurrent())
        let sections = try XCTUnwrap(LocalMediaSource.sections(from: seed, backedUpFingerprints: [remote], restoreOrigins: origins))
        XCTAssertEqual(sections.first?.items.first?.fingerprint, remote)
        XCTAssertEqual(sections.first?.items.first?.presence, .both)
        XCTAssertTrue(try XCTUnwrap(LocalMediaSource.sections(from: seed, backedUpFingerprints: [remote],
            excludingBackedUpFingerprints: [remote], restoreOrigins: origins)).isEmpty)
    }
}
