import Foundation
import Photos

struct BackupResumePlan {
    let resumedExecutionMode: BackupRunMode?
    /// Non-clean months whose known assets were routed out of `resumedExecutionMode`. Non-empty means
    /// blocked work exists, so the controller must not report resume complete-as-done.
    let repairRequiredMonths: Set<LibraryMonthKey>

    init(resumedExecutionMode: BackupRunMode?, repairRequiredMonths: Set<LibraryMonthKey> = []) {
        self.resumedExecutionMode = resumedExecutionMode
        self.repairRequiredMonths = repairRequiredMonths
    }

    var hasRepairRequiredWork: Bool { !repairRequiredMonths.isEmpty }
}

enum BackupResumeDedupMode: Sendable {
    case v1CompletedIDs
    case v2(RemoteViewHandle)
}

/// Pure partition of resolved asset months against the committed view's non-clean set. Kept separate
/// from safe-to-skip coverage so non-clean-month assets are surfaced as repair-required, never absorbed
/// as covered. Months are derived from `PHAsset.creationDate` by the caller; assets absent from
/// `monthsByAssetID` (no resolvable PHAsset) stay conservative and are never routed.
enum BackupResumeNonCleanRouter {
    static func route(
        monthsByAssetID: [PhotoKitLocalIdentifier: LibraryMonthKey],
        nonCleanMonths: Set<LibraryMonthKey>
    ) -> BackupResumeNonCleanRouting {
        guard !nonCleanMonths.isEmpty else { return BackupResumeNonCleanRouting() }
        var routedAssetIDs: Set<PhotoKitLocalIdentifier> = []
        var blockedMonths: Set<LibraryMonthKey> = []
        for (assetID, month) in monthsByAssetID where nonCleanMonths.contains(month) {
            routedAssetIDs.insert(assetID)
            blockedMonths.insert(month)
        }
        return BackupResumeNonCleanRouting(routedAssetIDs: routedAssetIDs, blockedMonths: blockedMonths)
    }
}

struct BackupResumeNonCleanRouting: Equatable, Sendable {
    var routedAssetIDs: Set<PhotoKitLocalIdentifier>
    var blockedMonths: Set<LibraryMonthKey>

    init(
        routedAssetIDs: Set<PhotoKitLocalIdentifier> = [],
        blockedMonths: Set<LibraryMonthKey> = []
    ) {
        self.routedAssetIDs = routedAssetIDs
        self.blockedMonths = blockedMonths
    }
}

final class BackupResumePlanner {
    private let photoLibraryService: PhotoLibraryService
    private let hashIndexRepository: ContentHashIndexRepository?
    private let coverageWorker: BackupResumeCoverageWorker

    init(photoLibraryService: PhotoLibraryService, hashIndexRepository: ContentHashIndexRepository? = nil) {
        self.photoLibraryService = photoLibraryService
        self.hashIndexRepository = hashIndexRepository
        self.coverageWorker = BackupResumeCoverageWorker(photoLibraryService: photoLibraryService)
    }

    func makePlan(
        pausedMode: BackupRunMode,
        completedAssetIDs: Set<PhotoKitLocalIdentifier>,
        dedupMode: BackupResumeDedupMode
    ) async throws -> BackupResumePlan {
        switch pausedMode {
        case .retry(let assetIDs):
            let resolution = try await filterPending(
                assetIDs: assetIDs,
                completedAssetIDs: completedAssetIDs,
                dedupMode: dedupMode
            )
            return BackupResumePlan(
                resumedExecutionMode: resolution.pendingAssetIDs.isEmpty ? nil : .retry(assetIDs: resolution.pendingAssetIDs),
                repairRequiredMonths: resolution.repairRequiredMonths
            )

        case .scoped(let assetIDs):
            let resolution = try await filterPending(
                assetIDs: assetIDs,
                completedAssetIDs: completedAssetIDs,
                dedupMode: dedupMode
            )
            return BackupResumePlan(
                resumedExecutionMode: resolution.pendingAssetIDs.isEmpty ? nil : .scoped(assetIDs: resolution.pendingAssetIDs),
                repairRequiredMonths: resolution.repairRequiredMonths
            )

        case .full:
            let resolution = try await computePendingAssetIDsForFullRun(
                excluding: completedAssetIDs,
                dedupMode: dedupMode
            )
            return BackupResumePlan(
                resumedExecutionMode: resolution.pendingAssetIDs.isEmpty ? nil : .scoped(assetIDs: resolution.pendingAssetIDs),
                repairRequiredMonths: resolution.repairRequiredMonths
            )
        }
    }

    /// Pending assets to execute plus the non-clean months whose known assets were routed out.
    private struct PendingResolution: Sendable {
        var pendingAssetIDs: Set<PhotoKitLocalIdentifier>
        var repairRequiredMonths: Set<LibraryMonthKey>
    }

    private func filterPending(
        assetIDs: Set<PhotoKitLocalIdentifier>,
        completedAssetIDs: Set<PhotoKitLocalIdentifier>,
        dedupMode: BackupResumeDedupMode
    ) async throws -> PendingResolution {
        switch dedupMode {
        case .v1CompletedIDs:
            return PendingResolution(pendingAssetIDs: assetIDs.subtracting(completedAssetIDs), repairRequiredMonths: [])
        case .v2(let handle):
            guard let hashIndexRepository else {
                return PendingResolution(pendingAssetIDs: assetIDs.subtracting(completedAssetIDs), repairRequiredMonths: [])
            }
            let safeToSkip = handle.safeToSkipAssetFingerprintsByMonth
            let nonCleanMonths = handle.nonCleanMonths
            let coverageWorker = self.coverageWorker
            return try await Self.runDetached {
                var pending = assetIDs
                // Route non-clean-month assets out before safe-to-skip so they surface as repair-required
                // rather than being absorbed as covered.
                var repairRequiredMonths: Set<LibraryMonthKey> = []
                if !nonCleanMonths.isEmpty {
                    let routing = try await coverageWorker.assetIDsInNonCleanMonths(assetIDs: pending, nonCleanMonths: nonCleanMonths)
                    pending.subtract(routing.routedAssetIDs)
                    repairRequiredMonths = routing.blockedMonths
                }
                let records = try hashIndexRepository.fetchAssetFingerprintRecords(assetIDs: pending)
                let covered = try await coverageWorker.assetIDsCoveredByRemote(records: records, safeToSkip: safeToSkip)
                pending.subtract(covered)
                return PendingResolution(pendingAssetIDs: pending, repairRequiredMonths: repairRequiredMonths)
            }
        }
    }

    private func computePendingAssetIDsForFullRun(
        excluding completedAssetIDs: Set<PhotoKitLocalIdentifier>,
        dedupMode: BackupResumeDedupMode
    ) async throws -> PendingResolution {
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

        let hashIndexRepository = self.hashIndexRepository
        let coverageWorker = self.coverageWorker
        return try await Self.runDetached {
            let allAssetIDs = await coverageWorker.assetIDsForFullRun()
            try Task.checkCancellation()
            let useHashDedup: Bool
            switch dedupMode {
            case .v1CompletedIDs: useHashDedup = false
            case .v2: useHashDedup = true
            }
            var pendingIDs: [PhotoKitLocalIdentifier] = []
            pendingIDs.reserveCapacity(max(allAssetIDs.count - (useHashDedup ? 0 : completedAssetIDs.count), 0))
            for assetID in allAssetIDs {
                try Task.checkCancellation()
                if useHashDedup || !completedAssetIDs.contains(assetID) {
                    pendingIDs.append(assetID)
                }
            }
            var pending = Set(pendingIDs)
            if case .v2(let handle) = dedupMode {
                guard let repository = hashIndexRepository else {
                    return PendingResolution(pendingAssetIDs: pending.subtracting(completedAssetIDs), repairRequiredMonths: [])
                }
                let safeToSkip = handle.safeToSkipAssetFingerprintsByMonth
                let nonCleanMonths = handle.nonCleanMonths
                var repairRequiredMonths: Set<LibraryMonthKey> = []
                if !nonCleanMonths.isEmpty {
                    try Task.checkCancellation()
                    let routing = try await coverageWorker.assetIDsInNonCleanMonths(assetIDs: pending, nonCleanMonths: nonCleanMonths)
                    pending.subtract(routing.routedAssetIDs)
                    repairRequiredMonths = routing.blockedMonths
                }
                try Task.checkCancellation()
                let records = try repository.fetchAssetFingerprintRecords(assetIDs: pending)
                try Task.checkCancellation()
                let covered = try await coverageWorker.assetIDsCoveredByRemote(records: records, safeToSkip: safeToSkip)
                try Task.checkCancellation()
                pending.subtract(covered)
                return PendingResolution(pendingAssetIDs: pending, repairRequiredMonths: repairRequiredMonths)
            }
            return PendingResolution(pendingAssetIDs: pending, repairRequiredMonths: [])
        }
    }

    private static func runDetached<T: Sendable>(
        _ operation: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        let task = Task.detached(priority: .userInitiated) {
            try await operation()
        }
        return try await withTaskCancellationHandler {
            try await task.value
        } onCancel: {
            task.cancel()
        }
    }
}

private final class BackupResumeCoverageWorker: @unchecked Sendable {
    private static let coverageChunkSize = 256

    private let photoLibraryService: PhotoLibraryService
    private let queue = DispatchQueue(
        label: "com.zizicici.watermelon.backupResume.coverage",
        qos: .userInitiated
    )

    init(photoLibraryService: PhotoLibraryService) {
        self.photoLibraryService = photoLibraryService
    }

    func assetIDsForFullRun() async -> [PhotoKitLocalIdentifier] {
        await withCheckedContinuation { continuation in
            queue.async { [photoLibraryService] in
                let assets = photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true)
                var ids: [PhotoKitLocalIdentifier] = []
                ids.reserveCapacity(assets.count)
                for index in 0 ..< assets.count {
                    ids.append(PhotoKitLocalIdentifier(assets.object(at: index)))
                }
                continuation.resume(returning: ids)
            }
        }
    }

    func assetIDsCoveredByRemote(
        records: [PhotoKitLocalIdentifier: LocalAssetFingerprintRecord],
        safeToSkip: PerMonth<Set<AssetFingerprint>>
    ) async throws -> Set<PhotoKitLocalIdentifier> {
        let entries = Array(records)
        var covered: Set<PhotoKitLocalIdentifier> = []
        covered.reserveCapacity(records.count)
        var offset = 0
        while offset < entries.count {
            try Task.checkCancellation()
            let end = min(offset + Self.coverageChunkSize, entries.count)
            let chunk = Array(entries[offset ..< end])
            let chunkCovered = await assetIDsCoveredByRemoteChunk(records: chunk, safeToSkip: safeToSkip)
            covered.formUnion(chunkCovered)
            offset = end
            await Task.yield()
        }
        return covered
    }

    private func assetIDsCoveredByRemoteChunk(
        records: [(PhotoKitLocalIdentifier, LocalAssetFingerprintRecord)],
        safeToSkip: PerMonth<Set<AssetFingerprint>>
    ) async -> Set<PhotoKitLocalIdentifier> {
        await withCheckedContinuation { continuation in
            queue.async {
                let phAssets = Self.phAssets(forAssetIDs: records.map { $0.0 })
                var covered: Set<PhotoKitLocalIdentifier> = []
                covered.reserveCapacity(records.count)
                for (id, record) in records {
                    guard let phAsset = phAssets[id] else { continue }
                    // mtime is checked at the end of this loop.
                    guard LocalHashIndexTrust.signatureMatches(record.trustFields, currentSignatureForAsset: phAsset) else { continue }
                    let month = LibraryMonthKey.from(date: phAsset.creationDate)
                    guard safeToSkip.contains(record.fingerprint, in: month) else { continue }
                    if let modDate = phAsset.modificationDate, modDate > record.updatedAt { continue }
                    covered.insert(id)
                }
                continuation.resume(returning: covered)
            }
        }
    }

    /// Classify pending assets whose `PHAsset.creationDate` month is non-clean. Assets with no
    /// resolvable PHAsset (unknown month) are left out — conservatively kept pending by the caller.
    func assetIDsInNonCleanMonths(
        assetIDs: Set<PhotoKitLocalIdentifier>,
        nonCleanMonths: Set<LibraryMonthKey>
    ) async throws -> BackupResumeNonCleanRouting {
        guard !nonCleanMonths.isEmpty else { return BackupResumeNonCleanRouting() }
        let entries = Array(assetIDs)
        var routedAssetIDs: Set<PhotoKitLocalIdentifier> = []
        var blockedMonths: Set<LibraryMonthKey> = []
        var offset = 0
        while offset < entries.count {
            try Task.checkCancellation()
            let end = min(offset + Self.coverageChunkSize, entries.count)
            let chunk = Array(entries[offset ..< end])
            let chunkResult = await assetIDsInNonCleanMonthsChunk(assetIDs: chunk, nonCleanMonths: nonCleanMonths)
            routedAssetIDs.formUnion(chunkResult.routedAssetIDs)
            blockedMonths.formUnion(chunkResult.blockedMonths)
            offset = end
            await Task.yield()
        }
        return BackupResumeNonCleanRouting(routedAssetIDs: routedAssetIDs, blockedMonths: blockedMonths)
    }

    private func assetIDsInNonCleanMonthsChunk(
        assetIDs: [PhotoKitLocalIdentifier],
        nonCleanMonths: Set<LibraryMonthKey>
    ) async -> BackupResumeNonCleanRouting {
        await withCheckedContinuation { continuation in
            queue.async {
                let phAssets = Self.phAssets(forAssetIDs: assetIDs)
                var monthsByAssetID: [PhotoKitLocalIdentifier: LibraryMonthKey] = [:]
                monthsByAssetID.reserveCapacity(assetIDs.count)
                for id in assetIDs {
                    guard let phAsset = phAssets[id] else { continue }
                    monthsByAssetID[id] = LibraryMonthKey.from(date: phAsset.creationDate)
                }
                continuation.resume(
                    returning: BackupResumeNonCleanRouter.route(monthsByAssetID: monthsByAssetID, nonCleanMonths: nonCleanMonths)
                )
            }
        }
    }

    private static func phAssets(forAssetIDs ids: [PhotoKitLocalIdentifier]) -> [PhotoKitLocalIdentifier: PHAsset] {
        guard !ids.isEmpty else { return [:] }
        let fetched = PHAsset.fetchAssets(withLocalIdentifiers: ids.rawValues, options: nil)
        var result: [PhotoKitLocalIdentifier: PHAsset] = [:]
        result.reserveCapacity(fetched.count)
        for index in 0 ..< fetched.count {
            let asset = fetched.object(at: index)
            result[PhotoKitLocalIdentifier(asset)] = asset
        }
        return result
    }
}
