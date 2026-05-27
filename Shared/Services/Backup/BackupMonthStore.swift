import Foundation

protocol BackupMonthStore: AnyObject {
    var year: Int { get }
    var month: Int { get }
    var monthRelativePath: String { get }
    var monthAbsolutePath: String { get }
    var v2Services: BackupV2RuntimeServices? { get }
    var dirty: Bool { get }

    var hasAnyAsset: Bool { get }

    func containsAssetFingerprint(_ fingerprint: AssetFingerprint) -> Bool
    /// Same as `containsAssetFingerprint` but rejects in-session pending V2 rows that have not yet
    /// been covered by a committed commit-log file. Cache-reuse short-circuits that write the local
    /// hash-index immediately must use this predicate so the "hash-index row ⇒ durable remote
    /// commit" invariant holds under batch commits.
    func containsDurableAssetFingerprint(_ fingerprint: AssetFingerprint) -> Bool
    /// True iff there are V2 row-writes (asset adds or tombstones) that have not yet landed on
    /// remote. V1 always returns false (V1 commits eagerly inside `upsertAsset`). Callers use
    /// this to distinguish "all chunks committed, only snapshot failed" (false) from "earlier
    /// chunks committed, a later chunk failed" (true) — the partial-multi-chunk case requires
    /// different downstream handling (no publish, rollback chunk-N+1 remainder).
    var hasUncommittedV2Ops: Bool { get }
    func isAssetIncomplete(_ fingerprint: AssetFingerprint) -> Bool

    /// Multi-path hashes resolve lex-min for deterministic legacy callers.
    func findResourceByHash(_ contentHash: Data) -> RemoteManifestResource?
    func findByFileName(_ logicalName: String) -> RemoteManifestResource?

    func existingFileNames() -> Set<String>
    /// Cached folded-key set avoids N-squared upload preparation per month.
    func existingCollisionKeys() -> Set<String>
    func remoteFileSize(named logicalName: String) -> Int64?

    func upsertAsset(
        _ asset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink],
        replacingSubsetFingerprints: Set<AssetFingerprint>
    ) throws

    /// Older partial assets whose links are a strict subset of `keys`.
    /// The incoming bundle supersedes them and the caller should tombstone via
    /// `upsertAsset(..., replacingSubsetFingerprints:)`. Default returns []; only stores that
    /// track per-fingerprint links provide a real implementation.
    func findStrictSubsetAssetFingerprints(
        forResourceKeys keys: Set<AssetResourceLinkKey>
    ) -> [AssetFingerprint]

    func hasStrictSubsetAssetFingerprint(
        forResourceKeys keys: Set<AssetResourceLinkKey>
    ) -> Bool

    @discardableResult
    func upsertResource(_ resource: RemoteManifestResource) throws -> RemoteManifestResource

    func markRemoteFile(name: String, size: Int64)

    func unsortedSnapshot() -> (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink])

    /// V2-only physical-presence overlay; V1 reports `.absent` semantics via an authoritative empty Month.
    var presence: RemotePresenceSnapshot.Month { get }

    @discardableResult
    func commitPendingAssetToRemote(ignoreCancellation: Bool) async throws -> BackupMonthFlushDelta

    @discardableResult
    func flushToRemote(ignoreCancellation: Bool) async throws -> BackupMonthFlushDelta
}

extension MonthManifestStore: BackupMonthStore {
    var hasAnyAsset: Bool { !assetsByFingerprint.isEmpty }
    var presence: RemotePresenceSnapshot.Month {
        RemotePresenceSnapshot.Month(missingHashes: [], isAuthoritative: true)
    }
    /// V1 commits eagerly inside `upsertAsset`, so any present fingerprint is durable.
    func containsDurableAssetFingerprint(_ fingerprint: AssetFingerprint) -> Bool {
        containsAssetFingerprint(fingerprint)
    }
    /// V1 has no batch lifecycle — every `upsertAsset` is durable on return.
    var hasUncommittedV2Ops: Bool { false }
}

extension BackupMonthStore {
    func upsertAsset(_ asset: RemoteManifestAsset, links: [RemoteAssetResourceLink]) throws {
        try upsertAsset(asset, links: links, replacingSubsetFingerprints: [])
    }

    func findStrictSubsetAssetFingerprints(
        forResourceKeys keys: Set<AssetResourceLinkKey>
    ) -> [AssetFingerprint] { [] }

    func hasStrictSubsetAssetFingerprint(
        forResourceKeys keys: Set<AssetResourceLinkKey>
    ) -> Bool {
        !findStrictSubsetAssetFingerprints(forResourceKeys: keys).isEmpty
    }

    @discardableResult
    func commitPendingAssetToRemote(ignoreCancellation: Bool) async throws -> BackupMonthFlushDelta {
        .none
    }
}
