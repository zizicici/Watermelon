import Foundation

struct RepoMonthState: Equatable, Sendable {
    var assets: [Data: SnapshotAssetRow]
    var resources: [String: SnapshotResourceRow]
    var assetResources: [AssetResourceKey: SnapshotAssetResourceRow]
    var deletedAssetFingerprints: Set<Data>
    /// Materializer's addAsset apply checks this to block a stale add from
    /// resurrecting a fp that was tombstoned at a higher clock.
    var deletedAssetStamps: [Data: OpStamp]

    init(
        assets: [Data: SnapshotAssetRow],
        resources: [String: SnapshotResourceRow],
        assetResources: [AssetResourceKey: SnapshotAssetResourceRow],
        deletedAssetFingerprints: Set<Data>,
        deletedAssetStamps: [Data: OpStamp] = [:]
    ) {
        self.assets = assets
        self.resources = resources
        self.assetResources = assetResources
        self.deletedAssetFingerprints = deletedAssetFingerprints
        self.deletedAssetStamps = deletedAssetStamps
    }

    static var empty: RepoMonthState {
        RepoMonthState(
            assets: [:],
            resources: [:],
            assetResources: [:],
            deletedAssetFingerprints: []
        )
    }
}

struct AssetResourceKey: Hashable, Sendable {
    let assetFingerprint: Data
    let role: Int
    let slot: Int
}

struct RepoSnapshotState: Equatable, Sendable {
    var months: [LibraryMonthKey: RepoMonthState]
    var observedClock: UInt64

    static var empty: RepoSnapshotState {
        RepoSnapshotState(months: [:], observedClock: 0)
    }
}
