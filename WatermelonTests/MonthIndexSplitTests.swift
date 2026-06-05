import XCTest
@testable import Watermelon

/// Unit coverage for the P10-W2 split of `V2MonthIndexes` into focused authorities:
/// `PendingCommitBuffer`, `RepoMonthCommittedState`, `MonthPresenceProjection`, `SnapshotProjection`,
/// and the thin coordinator facade. Pure-unit: no DB or V2 runtime services required.
final class MonthIndexSplitTests: XCTestCase {
    private let year = 2026
    private let month = 1
    private let writerID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

    // MARK: - PendingCommitBuffer

    func testPendingCommitBuffer_snapshotPending_deterministicOrderAndLimitDrainsAssetsFirst() {
        let buffer = PendingCommitBuffer()
        let a1 = TestFixtures.assetFingerprint(0x01)
        let a2 = TestFixtures.assetFingerprint(0x02)
        let t1 = TestFixtures.assetFingerprint(0x03)
        buffer.insertAssetAdd(a2)
        buffer.insertAssetAdd(a1)
        buffer.insertTombstone(t1)

        XCTAssertTrue(buffer.hasUncommittedOps)
        XCTAssertEqual(buffer.pendingOpsCount, 3)

        let all = buffer.snapshotPending()
        XCTAssertEqual(all.assets, [a1, a2], "assets must drain in lexicographic rawValue order")
        XCTAssertEqual(all.tombstones, [t1])

        // limit caps total ops, assets first then tombstones.
        XCTAssertEqual(buffer.snapshotPending(limit: 1).assets, [a1])
        XCTAssertEqual(buffer.snapshotPending(limit: 1).tombstones, [])
        XCTAssertEqual(buffer.snapshotPending(limit: 2).assets, [a1, a2])
        XCTAssertEqual(buffer.snapshotPending(limit: 2).tombstones, [])
        XCTAssertEqual(buffer.snapshotPending(limit: 3).tombstones, [t1])
    }

    func testPendingCommitBuffer_removeCommitted_removesOnlyStampedFingerprints() {
        let buffer = PendingCommitBuffer()
        let a1 = TestFixtures.assetFingerprint(0x01)
        let a2 = TestFixtures.assetFingerprint(0x02)
        let t1 = TestFixtures.assetFingerprint(0x03)
        buffer.insertAssetAdd(a1)
        buffer.insertAssetAdd(a2)
        buffer.insertTombstone(t1)

        // Mirror a chunked flush that only stamped a1.
        buffer.removeCommitted(assets: [a1], tombstones: [])

        XCTAssertFalse(buffer.containsAssetAdd(a1), "stamped fingerprint must be cleared")
        XCTAssertTrue(buffer.containsAssetAdd(a2), "unstamped remainder must survive for the next chunk")
        XCTAssertTrue(buffer.hasUncommittedOps)
        XCTAssertEqual(buffer.pendingOpsCount, 2)
    }

    // MARK: - RepoMonthCommittedState

    func testRepoMonthCommittedState_recordCommitStampsRowsAndConvertsToRepoMonthState() {
        let committed = RepoMonthCommittedState(year: year, month: month, materializedState: .empty)
        let fp = TestFixtures.assetFingerprint(0xC1)
        let hash = TestFixtures.fingerprint(0xD1)
        let path = "2026/01/p.jpg"
        let asset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: fp,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 100
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: fp, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "p.jpg"
        )
        committed.putAsset(asset, links: [link])

        let resource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: path,
            contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let key = RemotePhysicalPathKey(path)
        committed.recordCommit(
            assetClocks: [fp: 5],
            tombstoneClocks: [:],
            committedResources: [key: resource],
            committedResourceClocks: [key: 5],
            writerID: writerID, seq: 7
        )

        let state = committed.currentMaterializedState()
        XCTAssertEqual(state.assets[fp]?.stamp, OpStamp(writerID: writerID, seq: 7, clock: 5),
                       "asset row must carry the commit stamp after recordCommit")
        XCTAssertEqual(state.resources[key]?.contentHash, hash,
                       "committed resource baseline must round-trip into the snapshot state")
        XCTAssertEqual(
            state.assetResources[AssetResourceKey(assetFingerprint: fp, role: ResourceTypeCode.photo, slot: 0)]?.resourceHash,
            hash
        )
    }

    // MARK: - MonthPresenceProjection (byte-exact NFC/NFD)

    func testMonthPresenceProjection_exactMatchBackend_nfdListingDoesNotMarkNfcResourcePresent() {
        let hash = TestFixtures.fingerprint(0x5A)
        let nfcPath = "2026/01/caf\u{00E9}.jpg"
        let nfdLeaf = "cafe\u{0301}.jpg"
        var materialized = RepoMonthState.empty
        materialized.resources[RemotePhysicalPathKey(nfcPath)] = SnapshotResourceRow(
            physicalRemotePath: nfcPath, contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0, crypto: nil
        )

        let presence = MonthPresenceProjection(
            year: year, month: month,
            materializedState: materialized,
            remoteFilesByName: [nfdLeaf: MonthManifestStore.RemoteFileMetadata(size: 100)],
            verifiedMissingHashes: nil,
            nameCase: .caseSensitive
        )

        XCTAssertNil(presence.findResourceByHash(hash),
                     "exact-match backend must not treat a same-size NFD object as the committed NFC key")
        XCTAssertEqual(presence.physicallyMissingHashesSnapshot(), [hash],
                       "the unlisted NFC resource must project physically missing")
    }

    func testMonthPresenceProjection_sameHashNfcAndNfdTwins_oneListed_findableAndNotMissing() {
        let nfcLeaf = "caf\u{00E9}.jpg"
        let nfdLeaf = "cafe\u{0301}.jpg"
        let nfcPath = "2026/01/\(nfcLeaf)"
        let nfdPath = "2026/01/\(nfdLeaf)"
        let hash = TestFixtures.fingerprint(0x7C)
        var materialized = RepoMonthState.empty
        materialized.resources[RemotePhysicalPathKey(nfcPath)] = SnapshotResourceRow(
            physicalRemotePath: nfcPath, contentHash: hash, fileSize: 321,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0, crypto: nil
        )
        materialized.resources[RemotePhysicalPathKey(nfdPath)] = SnapshotResourceRow(
            physicalRemotePath: nfdPath, contentHash: hash, fileSize: 321,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0, crypto: nil
        )
        // Only the NFD twin is physically listed.
        let entries = [
            RemoteStorageEntry(path: "/repo/\(nfdPath)", name: nfdLeaf, isDirectory: false, size: 321, creationDate: nil, modificationDate: nil)
        ]
        let byteExact = MonthManifestStore.listedSizesByPresenceKey(entries: entries, nameCase: .caseSensitive)

        let presence = MonthPresenceProjection(
            year: year, month: month,
            materializedState: materialized,
            remoteFilesByName: MonthManifestStore.dedupedRemoteFilesByName(entries: entries, year: year, month: month),
            listedSizesByPresenceKey: byteExact,
            verifiedMissingHashes: nil,
            nameCase: .caseSensitive
        )

        XCTAssertTrue(presence.physicallyMissingHashesSnapshot().isEmpty,
                      "shared hash has a present NFD twin — must not be published physically missing")
        XCTAssertEqual(presence.findResourceByHash(hash)?.physicalRemotePath, nfdPath,
                       "findResourceByHash must resolve to the present twin, not nil")
    }

    // MARK: - SnapshotProjection differential normalization

    func testSnapshotProjection_normalize_isPathAgnosticForEquivalentContent() {
        let hash = TestFixtures.fingerprint(0x10)
        let fp = TestFixtures.assetFingerprint(0x20)
        func tuple(path: String) -> (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink]) {
            (
                [RemoteManifestResource(
                    year: year, month: month, physicalRemotePath: path,
                    contentHash: hash, fileSize: 100,
                    resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
                )],
                [RemoteManifestAsset(
                    year: year, month: month, assetFingerprint: fp,
                    creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 100
                )],
                [RemoteAssetResourceLink(
                    year: year, month: month, assetFingerprint: fp, resourceHash: hash,
                    role: ResourceTypeCode.photo, slot: 0, logicalName: (path as NSString).lastPathComponent
                )]
            )
        }
        // V1 keys resources by logical name, V2 by physical path; the differential projection collapses
        // both to content-hash facts so equivalent content compares equal regardless of path spelling.
        XCTAssertEqual(
            SnapshotProjection.normalize(tuple(path: "2026/01/photo.jpg")),
            SnapshotProjection.normalize(tuple(path: "2026/01/photo_2.jpg"))
        )
    }

    func testSnapshotProjection_normalize_distinguishesDifferentContent() {
        let hash = TestFixtures.fingerprint(0x10)
        let fp = TestFixtures.assetFingerprint(0x20)
        func tuple(size: Int64) -> (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink]) {
            (
                [RemoteManifestResource(
                    year: year, month: month, physicalRemotePath: "2026/01/photo.jpg",
                    contentHash: hash, fileSize: size,
                    resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
                )],
                [RemoteManifestAsset(
                    year: year, month: month, assetFingerprint: fp,
                    creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: size
                )],
                []
            )
        }
        XCTAssertNotEqual(
            SnapshotProjection.normalize(tuple(size: 100)),
            SnapshotProjection.normalize(tuple(size: 200))
        )
    }

    // MARK: - Facade coordination

    func testFacade_subsetReplacement_tombstonesPartialAndKeepsPendingDurableBoundary() throws {
        let indexes = V2MonthIndexes(
            year: year, month: month,
            materializedState: .empty,
            remoteFilesByName: [:],
            verifiedMissingHashes: nil,
            nameCase: .caseSensitive
        )
        let photoHash = TestFixtures.fingerprint(0xA1)
        let videoHash = TestFixtures.fingerprint(0xA2)
        let partialFP = TestFixtures.assetFingerprint(0xB1)
        let fullFP = TestFixtures.assetFingerprint(0xB2)

        _ = try indexes.upsertResource(RemoteManifestResource(
            year: year, month: month, physicalRemotePath: "2026/01/p.jpg",
            contentHash: photoHash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        ))
        _ = try indexes.upsertResource(RemoteManifestResource(
            year: year, month: month, physicalRemotePath: "2026/01/p.mov",
            contentHash: videoHash, fileSize: 200,
            resourceType: ResourceTypeCode.pairedVideo, creationDateMs: nil, backedUpAtMs: 0
        ))

        // Partial A (photo only), then mark it committed.
        try indexes.upsertAsset(
            RemoteManifestAsset(year: year, month: month, assetFingerprint: partialFP,
                                creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 100),
            links: [RemoteAssetResourceLink(year: year, month: month, assetFingerprint: partialFP, resourceHash: photoHash,
                                            role: ResourceTypeCode.photo, slot: 0, logicalName: "p.jpg")],
            replacingSubsetFingerprints: []
        )
        XCTAssertTrue(indexes.containsPendingAssetFingerprint(partialFP), "pending add is not yet durable")
        indexes.recordCommit(assetClocks: [partialFP: 1], tombstoneClocks: [:],
                             committedResources: [:], committedResourceClocks: [:], writerID: writerID, seq: 1)
        XCTAssertTrue(indexes.containsAssetFingerprint(partialFP))
        XCTAssertFalse(indexes.containsPendingAssetFingerprint(partialFP), "commit clears the pending add")

        // Full B (photo + paired video) supersedes A.
        try indexes.upsertAsset(
            RemoteManifestAsset(year: year, month: month, assetFingerprint: fullFP,
                                creationDateMs: nil, backedUpAtMs: 2, resourceCount: 2, totalFileSizeBytes: 300),
            links: [
                RemoteAssetResourceLink(year: year, month: month, assetFingerprint: fullFP, resourceHash: photoHash,
                                        role: ResourceTypeCode.photo, slot: 0, logicalName: "p.jpg"),
                RemoteAssetResourceLink(year: year, month: month, assetFingerprint: fullFP, resourceHash: videoHash,
                                        role: ResourceTypeCode.pairedVideo, slot: 0, logicalName: "p.mov")
            ],
            replacingSubsetFingerprints: [partialFP]
        )

        XCTAssertFalse(indexes.containsAssetFingerprint(partialFP), "subset-replaced partial is dropped from committed rows")
        XCTAssertTrue(indexes.containsAssetFingerprint(fullFP))
        XCTAssertTrue(indexes.containsPendingAssetFingerprint(fullFP))
        XCTAssertEqual(Set(indexes.unsortedSnapshot().assets.map(\.assetFingerprint)), [fullFP],
                       "published asset set must drop the replaced partial")

        // Commit B + tombstone A.
        indexes.recordCommit(assetClocks: [fullFP: 3], tombstoneClocks: [partialFP: 4],
                             committedResources: [:], committedResourceClocks: [:], writerID: writerID, seq: 2)
        let state = indexes.currentMaterializedState()
        XCTAssertNotNil(state.assets[fullFP], "superseding asset must be materialized")
        XCTAssertNil(state.assets[partialFP], "replaced partial must not be in materialized assets")
        XCTAssertNotNil(state.deletedAssetStamps[partialFP], "replaced partial must carry a tombstone stamp")
    }
}
