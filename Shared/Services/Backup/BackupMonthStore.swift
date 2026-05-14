import Foundation

/// Shared interface for V1 (`MonthManifestStore`) and V2 (`V2MonthSession`) so the
/// parallel executors don't branch on `v2Services != nil` per call site.
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

    /// V2 multi-writer can publish the same hash under multiple paths; this returns
    /// the lex-min one for determinism. Callers needing every path use the cache /
    /// snapshot APIs directly.
    func findResourceByHash(_ contentHash: Data) -> RemoteManifestResource?
    func findByFileName(_ logicalName: String) -> RemoteManifestResource?

    func existingFileNames() -> Set<String>
    /// Cached folded-key set; called per upload, recomputing each time would be N² per month.
    func existingCollisionKeys() -> Set<String>
    func remoteFileSize(named logicalName: String) -> Int64?

    func upsertAsset(
        _ asset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink],
        replacingSubsetFingerprints: Set<Data>
    ) throws

    @discardableResult
    func upsertResource(_ resource: RemoteManifestResource) throws -> RemoteManifestResource

    func markRemoteFile(name: String, size: Int64)

    func unsortedSnapshot() -> (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink])

    /// V2-only physical-presence overlay; V1 returns empty.
    func physicallyMissingHashesSnapshot() -> Set<Data>
    var physicallyMissingHashesAreAuthoritative: Bool { get }

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
}
