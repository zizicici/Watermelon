import Foundation

/// Shared snapshot-trust primitives, carved out of the materializer, checkpoint lightweight acceptance,
/// snapshot-delete scanner, and post-delete authority checker so the four consumers enforce one set of
/// resource-path / row-stamp rules instead of parallel copies. Body-state structural validation (links,
/// zero-link assets, out-of-month replay ops) stays in `RepoMonthStateValidator`; this holds only the
/// header-adjacent trust checks the trust pipelines own.
enum SnapshotTrustPolicy {
    /// A resource's physical path must be `<YYYY>/<MM>/<leaf>` for the owning month, leaf non-empty.
    static func resourcePathBelongsToMonth(_ path: String, month: LibraryMonthKey) -> Bool {
        let components = RemotePathBuilder.normalizeRelativePath(path)
            .split(separator: "/", omittingEmptySubsequences: false)
        guard components.count == 3, !components[2].isEmpty else { return false }
        let expectedYear = String(format: "%04d", month.year)
        let expectedMonth = String(format: "%02d", month.month)
        return String(components[0]) == expectedYear && String(components[1]) == expectedMonth
    }

    /// A row stamp is workable when its clock is below the adoption ceiling, not above the snapshot's own
    /// filename lamport, and its (writer, seq) is covered by the snapshot header.
    static func rowStampIsWorkable(
        _ stamp: OpStamp,
        covered: CoveredRanges,
        filenameLamport: UInt64
    ) -> Bool {
        if stamp.clock >= LamportClock.maxAdoptableValue { return false }
        if stamp.clock > filenameLamport { return false }
        return covered.contains(writerID: stamp.writerID, seq: stamp.seq)
    }

    /// The strict body-trust check shared by the snapshot-delete scanner and the post-delete authority
    /// checker: in-month unique resource paths, workable row stamps, asset-typed validated deleted keys,
    /// every link backed by an asset row plus a resource hash, no zero-link asset row, and no deleted key
    /// colliding with a live asset row.
    static func snapshotBodyIsMaterializerTrusted(
        _ file: SnapshotFile,
        month: LibraryMonthKey,
        filenameLamport: UInt64
    ) -> Bool {
        guard filenameLamport < LamportClock.maxAdoptableValue else { return false }
        var assets: Set<AssetFingerprint> = []
        for asset in file.assets {
            assets.insert(asset.assetFingerprint)
        }
        var resourcePaths: Set<RemotePhysicalPathKey> = []
        var resourceHashes: Set<Data> = []
        for resource in file.resources {
            if !resourcePathBelongsToMonth(resource.physicalRemotePath, month: month) {
                return false
            }
            let key = RemotePhysicalPathKey(resource.physicalRemotePath)
            if resourcePaths.contains(key) { return false }
            resourcePaths.insert(key)
            resourceHashes.insert(resource.contentHash)
        }
        let covered = file.header.covered
        for asset in file.assets {
            if !rowStampIsWorkable(asset.stamp, covered: covered, filenameLamport: filenameLamport) {
                return false
            }
        }
        for resource in file.resources {
            if !rowStampIsWorkable(resource.stamp, covered: covered, filenameLamport: filenameLamport) {
                return false
            }
        }
        for deletedKey in file.deletedKeys {
            if !rowStampIsWorkable(deletedKey.stamp, covered: covered, filenameLamport: filenameLamport) {
                return false
            }
            guard deletedKey.keyType == .asset else { return false }
            do {
                _ = try RepoWireValidator.validateHash(deletedKey.keyValue, field: "keyValue")
            } catch {
                return false
            }
        }
        var linkedAssets: Set<AssetFingerprint> = []
        for link in file.assetResources {
            guard assets.contains(link.assetFingerprint) else { return false }
            guard resourceHashes.contains(link.resourceHash) else { return false }
            linkedAssets.insert(link.assetFingerprint)
        }
        for asset in file.assets where !linkedAssets.contains(asset.assetFingerprint) {
            return false
        }
        for deletedKey in file.deletedKeys where deletedKey.keyType == .asset {
            guard let fp = try? RepoWireValidator.validateAssetFingerprint(deletedKey.keyValue, field: "keyValue") else {
                return false
            }
            if assets.contains(fp) { return false }
        }
        return true
    }
}

/// Covered-max baseline selection shared by the materializer's snapshot accept and the checkpoint's
/// lightweight acceptance. The winner's `covered` must be a superset of every other candidate's; multiple
/// covered-max candidates resolve by highest lamport, then highest writerID, then highest runIDPrefix (the
/// `>=` favors the later index on an exact triple tie). Iterates in the supplied order, so callers must
/// pass candidates in their existing order to preserve tie resolution.
enum SnapshotCoveredMaxSelector {
    struct Candidate {
        let covered: CoveredRanges
        let lamport: UInt64
        let writerID: String
        let runIDPrefix: String
    }

    /// Index of the covered-max candidate, or nil when no single candidate's coverage dominates all others.
    static func selectIndex(_ candidates: [Candidate]) -> Int? {
        var coveredMaxIdx: Int?
        for (i, candidate) in candidates.enumerated() {
            let isSuperset = candidates.enumerated().allSatisfy { (j, other) in
                i == j || candidate.covered.superset(of: other.covered)
            }
            guard isSuperset else { continue }
            guard let prev = coveredMaxIdx else {
                coveredMaxIdx = i
                continue
            }
            let prevCandidate = candidates[prev]
            if candidate.lamport != prevCandidate.lamport {
                coveredMaxIdx = candidate.lamport > prevCandidate.lamport ? i : prev
            } else if candidate.writerID != prevCandidate.writerID {
                coveredMaxIdx = candidate.writerID > prevCandidate.writerID ? i : prev
            } else {
                coveredMaxIdx = candidate.runIDPrefix >= prevCandidate.runIDPrefix ? i : prev
            }
        }
        return coveredMaxIdx
    }
}
