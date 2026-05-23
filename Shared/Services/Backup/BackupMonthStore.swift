import Foundation

protocol BackupMonthStore: AnyObject {
    var year: Int { get }
    var month: Int { get }
    var monthRelativePath: String { get }
    var monthAbsolutePath: String { get }
    var v2Services: BackupV2RuntimeServices? { get }
    var dirty: Bool { get }

    var hasAnyAsset: Bool { get }

    func containsAssetFingerprint(_ fingerprint: Data) -> Bool
    func isAssetIncomplete(_ fingerprint: Data) -> Bool

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
        replacingSubsetFingerprints: Set<Data>
    ) throws

    /// Older partial assets whose links are a strict subset of `keys`.
    /// The incoming bundle supersedes them and the caller should tombstone via
    /// `upsertAsset(..., replacingSubsetFingerprints:)`. Default returns []; only stores that
    /// track per-fingerprint links provide a real implementation.
    func findStrictSubsetAssetFingerprints(
        forResourceKeys keys: Set<AssetResourceLinkKey>
    ) -> [Data]

    func hasStrictSubsetAssetFingerprint(
        forResourceKeys keys: Set<AssetResourceLinkKey>
    ) -> Bool

    @discardableResult
    func upsertResource(_ resource: RemoteManifestResource) throws -> RemoteManifestResource

    func markRemoteFile(name: String, size: Int64)

    func unsortedSnapshot() -> (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink])

    /// V2-only physical-presence overlay; V1 returns empty.
    func physicallyMissingHashesSnapshot() -> Set<Data>
    var physicallyMissingHashesAreAuthoritative: Bool { get }

    @discardableResult
    func commitPendingAssetToRemote(ignoreCancellation: Bool) async throws -> MonthManifestStore.FlushDelta

    @discardableResult
    func flushToRemote(ignoreCancellation: Bool) async throws -> MonthManifestStore.FlushDelta
}

extension MonthManifestStore: BackupMonthStore {
    var hasAnyAsset: Bool { !assetsByFingerprint.isEmpty }
    var physicallyMissingHashesAreAuthoritative: Bool { true }
    func physicallyMissingHashesSnapshot() -> Set<Data> { [] }
}

extension BackupMonthStore {
    func upsertAsset(_ asset: RemoteManifestAsset, links: [RemoteAssetResourceLink]) throws {
        try upsertAsset(asset, links: links, replacingSubsetFingerprints: [])
    }

    func findStrictSubsetAssetFingerprints(
        forResourceKeys keys: Set<AssetResourceLinkKey>
    ) -> [Data] { [] }

    func hasStrictSubsetAssetFingerprint(
        forResourceKeys keys: Set<AssetResourceLinkKey>
    ) -> Bool {
        !findStrictSubsetAssetFingerprints(forResourceKeys: keys).isEmpty
    }

    @discardableResult
    func commitPendingAssetToRemote(ignoreCancellation: Bool) async throws -> MonthManifestStore.FlushDelta {
        .none
    }
}
