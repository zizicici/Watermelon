import Foundation
import Photos

struct BackupResumePlan {
    let resumedExecutionMode: BackupRunMode?
}

/// V2 `completedAssetIDs` can outrun commit-log truth: a pre-flush pause leaves reducer-acked items not yet durable. Make the dedup contract explicit so V2 paths can't accidentally fall back to V1 subtraction.
enum BackupResumeDedupMode: Sendable {
    case v1CompletedIDs
    case v2FreshCommittedView(PerMonth<Set<Data>>)
    case v2StaleOverlay
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
            let pending = try filterPending(
                assetIDs: assetIDs,
                completedAssetIDs: completedAssetIDs,
                dedupMode: dedupMode
            )
            return BackupResumePlan(
                resumedExecutionMode: pending.isEmpty ? nil : .retry(assetIDs: pending)
            )

        case .scoped(let assetIDs):
            let pending = try filterPending(
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
    ) throws -> Set<String> {
        switch dedupMode {
        case .v1CompletedIDs:
            return assetIDs.subtracting(completedAssetIDs)
        case .v2StaleOverlay:
            // Reprocess everything; executor's monthStore.containsAssetFingerprint dedups against durable state.
            return assetIDs
        case .v2FreshCommittedView(let committedView):
            guard let hashIndexRepository else {
                assertionFailure("BackupResumePlanner: V2 dedup requires hashIndexRepository")
                return assetIDs.subtracting(completedAssetIDs)
            }
            var pending = assetIDs
            let records = try hashIndexRepository.fetchAssetFingerprintRecords(assetIDs: pending)
            let assetDates = Self.assetDates(forAssetIDs: Array(records.keys))
            for (id, record) in records {
                guard let dates = assetDates[id] else { continue }
                let month = Self.libraryMonth(from: dates.creation)
                guard committedView.contains(record.fingerprint, in: month) else { continue }
                if let modDate = dates.modification, modDate > record.updatedAt { continue }
                pending.remove(id)
            }
            return pending
        }
    }

    private static func libraryMonth(from date: Date?) -> LibraryMonthKey {
        LibraryMonthKey.from(date: date)
    }

    private struct AssetDates {
        let creation: Date?
        let modification: Date?
    }

    private static func assetDates(forAssetIDs ids: [String]) -> [String: AssetDates] {
        guard !ids.isEmpty else { return [:] }
        let fetched = PHAsset.fetchAssets(withLocalIdentifiers: ids, options: nil)
        var result: [String: AssetDates] = [:]
        result.reserveCapacity(fetched.count)
        for index in 0 ..< fetched.count {
            let asset = fetched.object(at: index)
            result[asset.localIdentifier] = AssetDates(
                creation: asset.creationDate,
                modification: asset.modificationDate
            )
        }
        return result
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

        let photoLibraryService = self.photoLibraryService
        let hashIndexRepository = self.hashIndexRepository
        return try await Task.detached(priority: .userInitiated) {
            let assets = photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true)
            let useHashDedup: Bool
            switch dedupMode {
            case .v1CompletedIDs: useHashDedup = false
            case .v2FreshCommittedView, .v2StaleOverlay: useHashDedup = true
            }
            var pendingIDs: [String] = []
            pendingIDs.reserveCapacity(max(assets.count - (useHashDedup ? 0 : completedAssetIDs.count), 0))
            for index in 0 ..< assets.count {
                try Task.checkCancellation()
                let assetID = assets.object(at: index).localIdentifier
                if useHashDedup || !completedAssetIDs.contains(assetID) {
                    pendingIDs.append(assetID)
                }
            }
            var pending = Set(pendingIDs)
            if case .v2FreshCommittedView(let committedView) = dedupMode {
                guard let repository = hashIndexRepository else {
                    assertionFailure("BackupResumePlanner: V2 dedup requires hashIndexRepository")
                    return pending
                }
                let records = try repository.fetchAssetFingerprintRecords(assetIDs: pending)
                let dates = Self.assetDates(forAssetIDs: Array(records.keys))
                for (id, record) in records {
                    guard let assetDates = dates[id] else { continue }
                    let month = Self.libraryMonth(from: assetDates.creation)
                    guard committedView.contains(record.fingerprint, in: month) else { continue }
                    if let modDate = assetDates.modification, modDate > record.updatedAt { continue }
                    pending.remove(id)
                }
            }
            return pending
        }.value
    }
}
