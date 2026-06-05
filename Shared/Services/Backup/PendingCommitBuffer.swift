import Foundation

/// In-session pending V2 ops not yet covered by a durable commit-log file: asset adds and
/// tombstones. Provides deterministic, optionally-limited snapshots for chunked flushes and removes
/// only the fingerprints a commit actually stamped — `removeAll()` would drop a later chunk's
/// remainder before its own commit file lands.
final class PendingCommitBuffer {
    private(set) var pendingV2AssetFingerprints: Set<AssetFingerprint> = []
    private(set) var pendingV2TombstoneFingerprints: Set<AssetFingerprint> = []

    var hasUncommittedOps: Bool {
        !pendingV2AssetFingerprints.isEmpty || !pendingV2TombstoneFingerprints.isEmpty
    }

    var pendingOpsCount: Int {
        pendingV2AssetFingerprints.count + pendingV2TombstoneFingerprints.count
    }

    func containsAssetAdd(_ fingerprint: AssetFingerprint) -> Bool {
        pendingV2AssetFingerprints.contains(fingerprint)
    }

    /// Pop pending fingerprints in deterministic order; caller stamps committed rows after the
    /// commit log write succeeds. `limit` caps the total ops (assets + tombstones) returned,
    /// draining assets first then tombstones. A non-nil `limit` enables the U01 hard-cap
    /// chunked-flush contract.
    func snapshotPending(limit: Int? = nil) -> (assets: [AssetFingerprint], tombstones: [AssetFingerprint]) {
        let assets = pendingV2AssetFingerprints.sorted(by: { $0.rawValue.lexicographicallyPrecedes($1.rawValue) })
        let tombstones = pendingV2TombstoneFingerprints.sorted(by: { $0.rawValue.lexicographicallyPrecedes($1.rawValue) })
        guard let limit, limit >= 0 else {
            return (assets, tombstones)
        }
        if assets.count >= limit {
            return (Array(assets.prefix(limit)), [])
        }
        let remaining = limit - assets.count
        return (assets, Array(tombstones.prefix(remaining)))
    }

    func insertAssetAdd(_ fingerprint: AssetFingerprint) {
        pendingV2AssetFingerprints.insert(fingerprint)
    }

    func removeAssetAdd(_ fingerprint: AssetFingerprint) {
        pendingV2AssetFingerprints.remove(fingerprint)
    }

    func insertTombstone(_ fingerprint: AssetFingerprint) {
        pendingV2TombstoneFingerprints.insert(fingerprint)
    }

    func removeTombstone(_ fingerprint: AssetFingerprint) {
        pendingV2TombstoneFingerprints.remove(fingerprint)
    }

    /// Remove only the fingerprints this commit actually stamped — chunked flushes (U01 hard cap)
    /// write the remainder in subsequent commit files; clearing everything here would silently drop
    /// them from the in-memory pending set.
    func removeCommitted(assets: Set<AssetFingerprint>, tombstones: Set<AssetFingerprint>) {
        for fp in assets {
            pendingV2AssetFingerprints.remove(fp)
        }
        for fp in tombstones {
            pendingV2TombstoneFingerprints.remove(fp)
        }
    }
}
