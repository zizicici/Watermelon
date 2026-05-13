import Foundation
import Photos

struct BackupResumePlan {
    let resumedExecutionMode: BackupRunMode?
}

/// V2 must not subtract `completedAssetIDs` directly: a pre-flush pause leaves reducer-acked items not yet durable, so resume reads committed-fingerprint state instead.
enum BackupResumeDedupMode: Sendable {
    case v1CompletedIDs
    case v2(RemoteViewHandle)
}

final class BackupResumePlanner {
    private let photoLibraryService: PhotoLibraryService
    private let hashIndexRepository: ContentHashIndexRepository?

    init(photoLibraryService: PhotoLibraryService, hashIndexRepository: ContentHashIndexRepository? = nil) {
        self.photoLibraryService = photoLibraryService
        self.hashIndexRepository = hashIndexRepository
    }

    func makePlan(
        pausedMode: BackupRunMode,
        completedAssetIDs: Set<String>,
        dedupMode: BackupResumeDedupMode
    ) async throws -> BackupResumePlan {
        switch pausedMode {
        case .retry(let assetIDs):
            let pending = try await filterPending(
                assetIDs: assetIDs,
                completedAssetIDs: completedAssetIDs,
                dedupMode: dedupMode
            )
            return BackupResumePlan(
                resumedExecutionMode: pending.isEmpty ? nil : .retry(assetIDs: pending)
            )

        case .scoped(let assetIDs):
            let pending = try await filterPending(
                assetIDs: assetIDs,
                completedAssetIDs: completedAssetIDs,
                dedupMode: dedupMode
            )
            return BackupResumePlan(
                resumedExecutionMode: pending.isEmpty ? nil : .scoped(assetIDs: pending)
            )

        case .full:
            let pendingAssetIDs = try await computePendingAssetIDsForFullRun(
                excluding: completedAssetIDs,
                dedupMode: dedupMode
            )
            return BackupResumePlan(
                resumedExecutionMode: pendingAssetIDs.isEmpty ? nil : .scoped(assetIDs: pendingAssetIDs)
            )
        }
    }

    private func filterPending(
        assetIDs: Set<String>,
        completedAssetIDs: Set<String>,
        dedupMode: BackupResumeDedupMode
    ) async throws -> Set<String> {
        switch dedupMode {
        case .v1CompletedIDs:
            return assetIDs.subtracting(completedAssetIDs)
        case .v2(let handle):
            if handle.overlayFreshness == .stale {
                return assetIDs
            }
            guard let hashIndexRepository else {
                return assetIDs
            }
            let committedView = handle.committedAssetFingerprintsByMonth
            var pending = assetIDs
            let records = try hashIndexRepository.fetchAssetFingerprintRecords(assetIDs: pending)
            let covered = await Self.assetIDsCoveredByRemote(records: records, committedView: committedView)
            pending.subtract(covered)
            return pending
        }
    }

    /// Pre-v4 cache rows default to selectionVersion=0 and have no resourceSignature; trusting them would skip uploads under stale selection rules.
    private static func cacheRowIsTrustworthy(_ record: LocalAssetFingerprintRecord) -> Bool {
        guard record.selectionVersion >= BackupAssetResourcePlanner.currentSelectionVersion else { return false }
        return record.resourceSignature != nil
    }

    /// Mirrors AssetProcessor.processWithLocalCache: a same-asset-id resource-shape change leaves the cached fingerprint stale, so the planner must refuse to trust it.
    @MainActor
    private static func cachedSignatureMatchesCurrent(record: LocalAssetFingerprintRecord, phAsset: PHAsset) -> Bool {
        guard let cachedSignature = record.resourceSignature else { return false }
        let currentResources = PHAssetResource.assetResources(for: phAsset)
        let ordered = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(from: currentResources)
        let currentSignature = BackupAssetResourcePlanner.resourceSignature(orderedResources: ordered)
        return cachedSignature == currentSignature
    }

    private static func libraryMonth(from date: Date?) -> LibraryMonthKey {
        LibraryMonthKey.from(date: date)
    }

    @MainActor
    private static func phAssets(forAssetIDs ids: [String]) -> [String: PHAsset] {
        guard !ids.isEmpty else { return [:] }
        let fetched = PHAsset.fetchAssets(withLocalIdentifiers: ids, options: nil)
        var result: [String: PHAsset] = [:]
        result.reserveCapacity(fetched.count)
        for index in 0 ..< fetched.count {
            let asset = fetched.object(at: index)
            result[asset.localIdentifier] = asset
        }
        return result
    }

    @MainActor
    private static func assetIDsForFullRun(photoLibraryService: PhotoLibraryService) -> [String] {
        let assets = photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true)
        var ids: [String] = []
        ids.reserveCapacity(assets.count)
        for index in 0 ..< assets.count {
            ids.append(assets.object(at: index).localIdentifier)
        }
        return ids
    }

    @MainActor
    private static func assetIDsCoveredByRemote(
        records: [String: LocalAssetFingerprintRecord],
        committedView: PerMonth<Set<Data>>
    ) -> Set<String> {
        let phAssets = phAssets(forAssetIDs: Array(records.keys))
        var covered: Set<String> = []
        covered.reserveCapacity(records.count)
        for (id, record) in records {
            guard cacheRowIsTrustworthy(record) else { continue }
            guard let phAsset = phAssets[id] else { continue }
            guard cachedSignatureMatchesCurrent(record: record, phAsset: phAsset) else { continue }
            let month = libraryMonth(from: phAsset.creationDate)
            guard committedView.contains(record.fingerprint, in: month) else { continue }
            if let modDate = phAsset.modificationDate, modDate > record.updatedAt { continue }
            covered.insert(id)
        }
        return covered
    }

    private func computePendingAssetIDsForFullRun(
        excluding completedAssetIDs: Set<String>,
        dedupMode: BackupResumeDedupMode
    ) async throws -> Set<String> {
        let status = photoLibraryService.authorizationStatus()
        let authorized: Bool
        if status == .authorized || status == .limited {
            authorized = true
        } else {
            let requested = await photoLibraryService.requestAuthorization()
            authorized = (requested == .authorized || requested == .limited)
        }
        guard authorized else {
            throw BackupError.photoPermissionDenied
        }

        let allAssetIDs = await Self.assetIDsForFullRun(photoLibraryService: photoLibraryService)
        let hashIndexRepository = self.hashIndexRepository
        let planningTask = Task.detached(priority: .userInitiated) {
            let useHashDedup: Bool
            switch dedupMode {
            case .v1CompletedIDs: useHashDedup = false
            case .v2: useHashDedup = true
            }
            var pendingIDs: [String] = []
            pendingIDs.reserveCapacity(max(allAssetIDs.count - (useHashDedup ? 0 : completedAssetIDs.count), 0))
            for assetID in allAssetIDs {
                try Task.checkCancellation()
                if useHashDedup || !completedAssetIDs.contains(assetID) {
                    pendingIDs.append(assetID)
                }
            }
            var pending = Set(pendingIDs)
            if case .v2(let handle) = dedupMode, handle.overlayFreshness == .fresh {
                guard let repository = hashIndexRepository else {
                    return pending
                }
                let committedView = handle.committedAssetFingerprintsByMonth
                try Task.checkCancellation()
                let records = try repository.fetchAssetFingerprintRecords(assetIDs: pending)
                try Task.checkCancellation()
                let covered = await Self.assetIDsCoveredByRemote(records: records, committedView: committedView)
                try Task.checkCancellation()
                pending.subtract(covered)
            }
            return pending
        }
        return try await withTaskCancellationHandler {
            try await planningTask.value
        } onCancel: {
            planningTask.cancel()
        }
    }
}
