import Foundation
import Photos

protocol ContentHashIndexRepositoryProtocol: Sendable {
    func upsertAssetResource(
        assetLocalIdentifier: String,
        role: Int,
        slot: Int,
        contentHash: Data,
        fileSize: Int64
    ) throws

    func upsertAssetFingerprint(
        assetLocalIdentifier: String,
        assetFingerprint: Data,
        resourceCount: Int,
        totalFileSizeBytes: Int64
    ) throws

    func fetchHashMapByAsset() throws -> [String: [Data]]
    func fetchHashMapByAsset(assetIDs: Set<String>) throws -> [String: [Data]]
    func fetchAssetFingerprintsByAsset() throws -> [String: Data]
    func fetchAssetFingerprintsByAsset(assetIDs: Set<String>) throws -> [String: Data]
    func fetchAssetHashCaches() throws -> [String: LocalAssetHashCache]
}

protocol RemoteManifestIndexScannerProtocol: Sendable {
    func scanManifestDigests(
        client: RemoteStorageClientProtocol,
        basePath: String,
        cancellationController: BackupCancellationController?
    ) async throws -> [LibraryMonthKey: RemoteMonthManifestDigest]
}

protocol PhotoLibraryServiceProtocol: Sendable {
    func authorizationStatus() -> PHAuthorizationStatus
    func requestAuthorization() async -> PHAuthorizationStatus
    func fetchAssetsResult(ascendingByCreationDate: Bool) -> PHFetchResult<PHAsset>
    func exportResourceToTempFile(
        _ resource: PHAssetResource,
        cancellationController: BackupCancellationController?
    ) async throws -> URL
}

protocol RemoteLibrarySnapshotCacheProtocol: AnyObject, Sendable {
    func current() -> RemoteLibrarySnapshot
    func state(since revision: UInt64?) -> RemoteLibrarySnapshotState
    func upsertResource(_ item: RemoteManifestResource)
    func upsertAsset(_ asset: RemoteManifestAsset, links: [RemoteAssetResourceLink]?)
    func replaceMonth(
        _ month: LibraryMonthKey,
        resources: [RemoteManifestResource],
        assets: [RemoteManifestAsset],
        assetResourceLinks: [RemoteAssetResourceLink]
    ) -> Bool
    func removeMonth(_ month: LibraryMonthKey) -> Bool
    func reset()
}
