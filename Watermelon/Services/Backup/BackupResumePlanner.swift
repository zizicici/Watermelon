import Foundation
import Photos

struct BackupResumePlan {
    let resumedExecutionMode: BackupRunMode?
}

final class BackupResumePlanner {
    private let photoLibraryService: PhotoLibraryService
    private let hashIndexRepository: ContentHashIndexRepository?

    init(photoLibraryService: PhotoLibraryService, hashIndexRepository: ContentHashIndexRepository? = nil) {
        self.photoLibraryService = photoLibraryService
        self.hashIndexRepository = hashIndexRepository
    }

    /// Nil = V1 path (use `completedAssetIDs`). Non-nil = V2: defer to committed-fp presence since a pre-flush pause can leave reducer-completed but uncommitted assets.
    func makePlan(
        pausedMode: BackupRunMode,
        completedAssetIDs: Set<String>,
        committedAssetFingerprintsByMonth: PerMonth<Set<Data>>? = nil
    ) async throws -> BackupResumePlan {
        switch pausedMode {
        case .retry(let assetIDs):
            let pending = try filterPending(
                assetIDs: assetIDs,
                completedAssetIDs: completedAssetIDs,
                committedView: committedAssetFingerprintsByMonth
            )
            return BackupResumePlan(
                resumedExecutionMode: pending.isEmpty ? nil : .retry(assetIDs: pending)
            )

        case .scoped(let assetIDs):
            let pending = try filterPending(
                assetIDs: assetIDs,
                completedAssetIDs: completedAssetIDs,
                committedView: committedAssetFingerprintsByMonth
            )
            return BackupResumePlan(
                resumedExecutionMode: pending.isEmpty ? nil : .scoped(assetIDs: pending)
            )

        case .full:
            let pendingAssetIDs = try await computePendingAssetIDsForFullRun(
                excluding: completedAssetIDs,
                committedView: committedAssetFingerprintsByMonth
            )
            return BackupResumePlan(
                resumedExecutionMode: pendingAssetIDs.isEmpty ? nil : .scoped(assetIDs: pendingAssetIDs)
            )
        }
    }

    private func filterPending(
        assetIDs: Set<String>,
        completedAssetIDs: Set<String>,
        committedView: PerMonth<Set<Data>>?
    ) throws -> Set<String> {
        guard let committedView else {
            return assetIDs.subtracting(completedAssetIDs)
        }
        guard let hashIndexRepository else {
            // Miswired V2 fallback keeps executor-acked assets from being reprocessed.
            assertionFailure("BackupResumePlanner: committedView present but hashIndexRepository is nil; V2 dedup would no-op")
            return assetIDs.subtracting(completedAssetIDs)
        }
        var pending = assetIDs
        let records = try hashIndexRepository.fetchAssetFingerprintRecords(assetIDs: pending)
        let assetDates = Self.assetDates(forAssetIDs: Array(records.keys))
        for (id, record) in records {
            // Assets with no creationDate land in 1970-01 (writer side: LibraryMonthKey.from(date:)).
            guard let dates = assetDates[id] else { continue }
            let month = Self.libraryMonth(from: dates.creation)
            // Per-month dedup: each month owns its physical files even when content is identical.
            guard committedView.contains(record.fingerprint, in: month) else { continue }
            if let modDate = dates.modification, modDate > record.updatedAt { continue }
            pending.remove(id)
        }
        return pending
    }

    private static func libraryMonth(from date: Date?) -> LibraryMonthKey {
        // Must match writer side; `Calendar.current` yields locale-specific eras.
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
        committedView: PerMonth<Set<Data>>?
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
            let v2Mode = committedView != nil
            let canDedupByHash = v2Mode && hashIndexRepository != nil
            var pendingIDs: [String] = []
            pendingIDs.reserveCapacity(max(assets.count - (canDedupByHash ? 0 : completedAssetIDs.count), 0))
            for index in 0 ..< assets.count {
                try Task.checkCancellation()
                let assetID = assets.object(at: index).localIdentifier
                if canDedupByHash || !completedAssetIDs.contains(assetID) {
                    pendingIDs.append(assetID)
                }
            }
            var pending = Set(pendingIDs)
            if v2Mode && hashIndexRepository == nil {
                assertionFailure("BackupResumePlanner: committedView present but hashIndexRepository is nil; V2 dedup would no-op")
            }
            if let committedView, let repository = hashIndexRepository {
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
