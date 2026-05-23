import Foundation
import Photos

enum LocalHashIndexTrust {
    struct CacheFields: Equatable, Sendable {
        let updatedAt: Date
        let selectionVersion: Int
        let resourceSignature: Data?
    }

    struct AssetShape: Equatable, Sendable {
        let modificationDate: Date?
        let currentResourceSignature: Data
    }

    static func canTrust(_ cache: CacheFields, for shape: AssetShape) -> Bool {
        if let mtime = shape.modificationDate, mtime > cache.updatedAt { return false }
        return signatureMatches(cache, currentSignature: shape.currentResourceSignature)
    }

    static func signatureMatches(_ cache: CacheFields, currentSignature: Data) -> Bool {
        guard cache.selectionVersion >= BackupAssetResourcePlanner.currentSelectionVersion else { return false }
        guard let cachedSignature = cache.resourceSignature else { return false }
        return cachedSignature == currentSignature
    }

    static func cacheFieldsPassCheapChecks(_ cache: CacheFields, modificationDate: Date?) -> Bool {
        if let mtime = modificationDate, mtime > cache.updatedAt { return false }
        guard cache.selectionVersion >= BackupAssetResourcePlanner.currentSelectionVersion else { return false }
        return cache.resourceSignature != nil
    }

    static func canTrust(_ cache: CacheFields, for asset: PHAsset) -> Bool {
        guard cacheFieldsPassCheapChecks(cache, modificationDate: asset.modificationDate) else { return false }
        let ordered = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(
            from: PHAssetResource.assetResources(for: asset)
        )
        return cache.resourceSignature == BackupAssetResourcePlanner.resourceSignature(orderedResources: ordered)
    }

    static func signatureMatches(_ cache: CacheFields, currentSignatureForAsset asset: PHAsset) -> Bool {
        guard cache.selectionVersion >= BackupAssetResourcePlanner.currentSelectionVersion else { return false }
        guard let cachedSignature = cache.resourceSignature else { return false }
        let ordered = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(
            from: PHAssetResource.assetResources(for: asset)
        )
        return cachedSignature == BackupAssetResourcePlanner.resourceSignature(orderedResources: ordered)
    }
}

extension LocalHashIndexTrust.AssetShape {
    init(asset: PHAsset) {
        let ordered = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(
            from: PHAssetResource.assetResources(for: asset)
        )
        self.init(
            modificationDate: asset.modificationDate,
            currentResourceSignature: BackupAssetResourcePlanner.resourceSignature(orderedResources: ordered)
        )
    }
}

extension LocalAssetFingerprintRecord {
    var trustFields: LocalHashIndexTrust.CacheFields {
        LocalHashIndexTrust.CacheFields(
            updatedAt: updatedAt,
            selectionVersion: selectionVersion,
            resourceSignature: resourceSignature
        )
    }
}

extension LocalAssetHashCache {
    var trustFields: LocalHashIndexTrust.CacheFields {
        LocalHashIndexTrust.CacheFields(
            updatedAt: updatedAt,
            selectionVersion: selectionVersion,
            resourceSignature: resourceSignature
        )
    }
}

extension IndexedAssetRow {
    var trustFields: LocalHashIndexTrust.CacheFields {
        LocalHashIndexTrust.CacheFields(
            updatedAt: updatedAt,
            selectionVersion: selectionVersion,
            resourceSignature: resourceSignature
        )
    }
}

extension DuplicateIndexedAssetRow {
    var trustFields: LocalHashIndexTrust.CacheFields {
        LocalHashIndexTrust.CacheFields(
            updatedAt: updatedAt,
            selectionVersion: selectionVersion,
            resourceSignature: resourceSignature
        )
    }
}
