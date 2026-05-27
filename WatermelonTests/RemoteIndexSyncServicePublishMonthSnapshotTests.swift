import XCTest
@testable import Watermelon

final class RemoteIndexSyncServicePublishMonthSnapshotTests: XCTestCase {
    private let monthKey = LibraryMonthKey(year: 2026, month: 1)

    func testPublish_authoritativeStore_emptySnapshot_seedsFreshnessFlagWithEmptySet() {
        let ris = RemoteIndexSyncService()
        let store = FakeMonthStore(year: 2026, month: 1, authoritative: true)

        ris.publishMonthSnapshot(of: store, for: monthKey)

        // Authoritative + empty snapshot ⇒ helper passes physicallyMissingHashes: Set<Data>()
        // ⇒ replaceCachedMonth inserts into physicalPresenceOverlayFreshMonths
        // ⇒ verifiedPhysicallyMissingHashes returns the empty set (flag set, intersection empty).
        let fresh = ris.verifiedPhysicallyMissingHashes(for: monthKey)
        XCTAssertEqual(fresh, Set<Data>(),
                       "authoritative empty store must seed freshness flag with empty set")
    }

    func testPublish_nonAuthoritativeStore_emptySnapshot_leavesFreshnessFlagUnset() {
        let ris = RemoteIndexSyncService()
        let store = FakeMonthStore(year: 2026, month: 1, authoritative: false)

        ris.publishMonthSnapshot(of: store, for: monthKey)

        // Non-authoritative ⇒ helper passes physicallyMissingHashes: nil
        // ⇒ replaceCachedMonth removes from physicalPresenceOverlayFreshMonths
        // ⇒ verifiedPhysicallyMissingHashes returns nil.
        let fresh = ris.verifiedPhysicallyMissingHashes(for: monthKey)
        XCTAssertNil(fresh,
                     "non-authoritative store must not seed freshness flag")
    }

    func testPublish_authoritativeStore_nonEmptyMissingSet_propagatesAuthoritativeMissingHashes() {
        let ris = RemoteIndexSyncService()
        let missingHash = TestFixtures.fingerprint(0xAA)
        // Fixture must include a resource row whose contentHash matches missingHash —
        // RepoCommittedView.replaceMonth intersects physicallyMissingHashes with still-present
        // resource hashes, so empty-resource fixtures would drop the missing set entirely.
        let resource = RemoteManifestResource(
            year: 2026, month: 1,
            physicalRemotePath: "2026/01/photo.jpg",
            contentHash: missingHash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 0
        )
        let store = FakeMonthStore(
            year: 2026, month: 1,
            authoritative: true,
            resources: [resource],
            missingHashes: [missingHash]
        )

        ris.publishMonthSnapshot(of: store, for: monthKey)

        // The helper passes the authoritative set into replaceCachedMonth; the stored set
        // is the still-present intersection. Here the seeded resource's contentHash matches
        // missingHash, so the intersection equals the input.
        let fresh = ris.verifiedPhysicallyMissingHashes(for: monthKey)
        XCTAssertEqual(fresh, [missingHash],
                       "authoritative store with matching resource must pass missing-set through; intersection preserves it")
    }

    func testPublish_nonAuthoritativeStore_neverPropagatesMissingHashes() {
        let ris = RemoteIndexSyncService()
        let missingHash = TestFixtures.fingerprint(0xBB)
        // Construct a store whose snapshot() reports a non-empty missing set BUT
        // whose authoritative flag is false. The helper must NOT propagate the missing set
        // (the rule is: missing-hashes only when the store flags them authoritative).
        // Seeding a matching resource here would only matter if the helper were buggy and
        // propagated the missing-set anyway; including it keeps the test honest in that case.
        let resource = RemoteManifestResource(
            year: 2026, month: 1,
            physicalRemotePath: "2026/01/photo.jpg",
            contentHash: missingHash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 0
        )
        let store = FakeMonthStore(
            year: 2026, month: 1,
            authoritative: false,
            resources: [resource],
            missingHashes: [missingHash]
        )

        ris.publishMonthSnapshot(of: store, for: monthKey)

        let fresh = ris.verifiedPhysicallyMissingHashes(for: monthKey)
        XCTAssertNil(fresh,
                     "non-authoritative missing set must not seed freshness flag")
    }
}

// MARK: - Minimal BackupMonthStore conformance for helper-level testing

private final class FakeMonthStore: BackupMonthStore, @unchecked Sendable {
    let year: Int
    let month: Int
    private let isAuthoritative: Bool
    private let resources: [RemoteManifestResource]
    private let assets: [RemoteManifestAsset]
    private let links: [RemoteAssetResourceLink]
    private let missingHashes: Set<Data>

    init(
        year: Int,
        month: Int,
        authoritative: Bool,
        resources: [RemoteManifestResource] = [],
        assets: [RemoteManifestAsset] = [],
        links: [RemoteAssetResourceLink] = [],
        missingHashes: Set<Data> = []
    ) {
        self.year = year
        self.month = month
        self.isAuthoritative = authoritative
        self.resources = resources
        self.assets = assets
        self.links = links
        self.missingHashes = missingHashes
    }

    var monthRelativePath: String { "\(year)/\(String(format: "%02d", month))" }
    var monthAbsolutePath: String { "/repo/\(monthRelativePath)" }
    var v2Services: BackupV2RuntimeServices? { nil }
    var dirty: Bool { false }
    var hasAnyAsset: Bool { !assets.isEmpty }

    func containsAssetFingerprint(_ fingerprint: AssetFingerprint) -> Bool { false }
    func containsDurableAssetFingerprint(_ fingerprint: AssetFingerprint) -> Bool { false }
    var hasUncommittedV2Ops: Bool { false }
    func isAssetIncomplete(_ fingerprint: AssetFingerprint) -> Bool { false }
    func findResourceByHash(_ contentHash: Data) -> RemoteManifestResource? { nil }
    func findByFileName(_ logicalName: String) -> RemoteManifestResource? { nil }
    func existingFileNames() -> Set<String> { [] }
    func existingCollisionKeys() -> Set<String> { [] }
    func remoteFileSize(named logicalName: String) -> Int64? { nil }
    func upsertAsset(
        _ asset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink],
        replacingSubsetFingerprints: Set<AssetFingerprint>
    ) throws { }
    func upsertResource(_ resource: RemoteManifestResource) throws -> RemoteManifestResource { resource }
    func markRemoteFile(name: String, size: Int64) { }

    func unsortedSnapshot() -> (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink]) {
        (resources, assets, links)
    }

    var presence: RemotePresenceSnapshot.Month {
        RemotePresenceSnapshot.Month(missingHashes: missingHashes, isAuthoritative: isAuthoritative)
    }

    func flushToRemote(ignoreCancellation: Bool) async throws -> BackupMonthFlushDelta { .none }
}
