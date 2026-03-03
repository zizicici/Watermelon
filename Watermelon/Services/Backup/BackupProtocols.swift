import Foundation
import Photos

protocol ContentHashIndexRepositoryProtocol: Sendable {
    func upsertAssetHashSnapshot(
        assetLocalIdentifier: String,
        assetFingerprint: Data,
        resources: [LocalAssetResourceHashRecord],
        totalFileSizeBytes: Int64
    ) throws

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
    func fetchAssetHashCaches(assetIDs: Set<String>) throws -> [String: LocalAssetHashCache]
    func fetchTotalFileSizeBytes(assetIDs: Set<String>) throws -> Int64
    func fetchLocalHashIndexStats() throws -> LocalHashIndexStats
    func clearLocalHashIndex() throws
    func fetchIndexedAssetIDs() throws -> [String]
    func deleteIndexEntries(assetIDs: [String]) throws
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
    func exportResourceToTempFileAndDigest(
        _ resource: PHAssetResource,
        cancellationController: BackupCancellationController?
    ) async throws -> ExportedResourceFile
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

extension RemoteManifestIndexScannerProtocol {
    func scanManifestDigests(
        client: RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> [LibraryMonthKey: RemoteMonthManifestDigest] {
        try await scanManifestDigests(
            client: client,
            basePath: basePath,
            cancellationController: nil
        )
    }
}
