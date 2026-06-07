import Foundation

struct RepoMonthState: Equatable, Sendable {
    var assets: [AssetFingerprint: SnapshotAssetRow]
    /// Byte-exact keyed so NFC/NFD-distinct physical paths on exact-name backends
    /// survive as two rows instead of collapsing to one Swift-String key.
    var resources: [RemotePhysicalPathKey: SnapshotResourceRow]
    var assetResources: [AssetResourceKey: SnapshotAssetResourceRow]
    var deletedAssetStamps: [AssetFingerprint: OpStamp]
    /// Parallel to `deletedAssetStamps`: the observation basis of each tombstone, carried so a checkpoint
    /// baseline preserves the heal across the snapshot boundary. Absent for legacy/basis-free tombstones.
    var deletedAssetBasis: [AssetFingerprint: TombstoneObservationBasis]

    init(
        assets: [AssetFingerprint: SnapshotAssetRow],
        resources: [RemotePhysicalPathKey: SnapshotResourceRow],
        assetResources: [AssetResourceKey: SnapshotAssetResourceRow],
        deletedAssetStamps: [AssetFingerprint: OpStamp],
        deletedAssetBasis: [AssetFingerprint: TombstoneObservationBasis] = [:]
    ) {
        self.assets = assets
        self.resources = resources
        self.assetResources = assetResources
        self.deletedAssetStamps = deletedAssetStamps
        self.deletedAssetBasis = deletedAssetBasis
    }

    static var empty: RepoMonthState {
        RepoMonthState(
            assets: [:],
            resources: [:],
            assetResources: [:],
            deletedAssetStamps: [:],
            deletedAssetBasis: [:]
        )
    }
}

struct AssetResourceKey: Hashable, Sendable {
    let assetFingerprint: AssetFingerprint
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
