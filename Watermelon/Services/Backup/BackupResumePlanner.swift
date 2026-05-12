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

    func makePlan(
        pausedMode: BackupRunMode,
        completedAssetIDs: Set<String>,
        alreadyBackedUpFingerprintsByMonth: PerMonth<Set<Data>> = PerMonth<Set<Data>>()
    ) async throws -> BackupResumePlan {
        switch pausedMode {
        case .retry(let assetIDs):
            let pending = try filterPending(
                assetIDs: assetIDs,
                completedAssetIDs: completedAssetIDs,
                backedUpFingerprintsByMonth: alreadyBackedUpFingerprintsByMonth
            )
            return BackupResumePlan(
                resumedExecutionMode: pending.isEmpty ? nil : .retry(assetIDs: pending)
            )

        case .scoped(let assetIDs):
            let pending = try filterPending(
                assetIDs: assetIDs,
                completedAssetIDs: completedAssetIDs,
                backedUpFingerprintsByMonth: alreadyBackedUpFingerprintsByMonth
            )
            return BackupResumePlan(
                resumedExecutionMode: pending.isEmpty ? nil : .scoped(assetIDs: pending)
            )

        case .full:
            let pendingAssetIDs = try await computePendingAssetIDsForFullRun(
                excluding: completedAssetIDs,
                backedUpFingerprintsByMonth: alreadyBackedUpFingerprintsByMonth
            )
            return BackupResumePlan(
                resumedExecutionMode: pendingAssetIDs.isEmpty ? nil : .scoped(assetIDs: pendingAssetIDs)
            )
        }
    }

    private func filterPending(
        assetIDs: Set<String>,
        completedAssetIDs: Set<String>,
        backedUpFingerprintsByMonth: PerMonth<Set<Data>>
    ) throws -> Set<String> {
        var pending = assetIDs.subtracting(completedAssetIDs)
        if backedUpFingerprintsByMonth.isEmpty { return pending }
        guard let hashIndexRepository else {
            assertionFailure("BackupResumePlanner: hashIndexRepository is nil but backedUpFingerprintsByMonth is non-empty; resume will re-enqueue everything.")
            return pending
        }
        let records = try hashIndexRepository.fetchAssetFingerprintRecords(assetIDs: pending)
        let assetDates = Self.assetDates(forAssetIDs: Array(records.keys))
        for (id, record) in records {
            // Match writer-side: assets with no creationDate land in 1970-01 (see
            // `LibraryMonthKey.from(date:)`). Falling through here would skip
            // dedup and resume those assets unconditionally each time.
            guard let dates = assetDates[id] else { continue }
            let month = Self.libraryMonth(from: dates.creation)
            // Per-month dedup: skip ONLY if the asset's own month already has this fingerprint
            // committed. Cross-month fingerprint matches don't justify skipping — each month
            // owns its physical files even when the content is identical.
            guard backedUpFingerprintsByMonth.contains(record.fingerprint, in: month) else { continue }
            // A user edit since the cache → fingerprint is stale; fall through to preflight.
            if let modDate = dates.modification, modDate > record.updatedAt { continue }
            pending.remove(id)
        }
        return pending
    }

    private static func libraryMonth(from date: Date?) -> LibraryMonthKey {
        // Must match the writer side (BackupMonthScheduler / cache) which uses
        // `LibraryMonthKey.from(date:)` (Gregorian). Calendar.current would yield
        //令和8 (Japanese imperial) or 2569 (Buddhist) for 2026, breaking the
        // per-month dedup lookup entirely on those locales.
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
        backedUpFingerprintsByMonth: PerMonth<Set<Data>>
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
            var pendingIDs: [String] = []
            pendingIDs.reserveCapacity(max(assets.count - completedAssetIDs.count, 0))
            for index in 0 ..< assets.count {
                try Task.checkCancellation()
                let assetID = assets.object(at: index).localIdentifier
                if !completedAssetIDs.contains(assetID) {
                    pendingIDs.append(assetID)
                }
            }
            var pending = Set(pendingIDs)
            if !backedUpFingerprintsByMonth.isEmpty && hashIndexRepository == nil {
                assertionFailure("BackupResumePlanner: hashIndexRepository is nil but backedUpFingerprintsByMonth is non-empty; full-run will re-enqueue everything.")
            }
            if !backedUpFingerprintsByMonth.isEmpty, let repository = hashIndexRepository {
                let records = try repository.fetchAssetFingerprintRecords(assetIDs: pending)
                let dates = Self.assetDates(forAssetIDs: Array(records.keys))
                for (id, record) in records {
                    guard let assetDates = dates[id] else { continue }
                    let month = Self.libraryMonth(from: assetDates.creation)
                    guard backedUpFingerprintsByMonth.contains(record.fingerprint, in: month) else { continue }
                    if let modDate = assetDates.modification, modDate > record.updatedAt { continue }
                    pending.remove(id)
                }
            }
            return pending
        }.value
    }
}
