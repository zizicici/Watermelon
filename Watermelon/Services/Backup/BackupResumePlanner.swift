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
    private let coverageWorker: BackupResumeCoverageWorker

    init(photoLibraryService: PhotoLibraryService, hashIndexRepository: ContentHashIndexRepository? = nil) {
        self.photoLibraryService = photoLibraryService
        self.hashIndexRepository = hashIndexRepository
        self.coverageWorker = BackupResumeCoverageWorker(photoLibraryService: photoLibraryService)
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
            guard let hashIndexRepository else {
                return assetIDs.subtracting(completedAssetIDs)
            }
            let committedView = handle.committedAssetFingerprintsByMonth
            let coverageWorker = self.coverageWorker
            return try await Self.runDetached {
                var pending = assetIDs
                let records = try hashIndexRepository.fetchAssetFingerprintRecords(assetIDs: pending)
                let covered = try await coverageWorker.assetIDsCoveredByRemote(records: records, committedView: committedView)
                pending.subtract(covered)
                return pending
            }
        }
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
            var pendingIDs: [String] = []
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
                    return pending.subtracting(completedAssetIDs)
                }
                let committedView = handle.committedAssetFingerprintsByMonth
                try Task.checkCancellation()
                let records = try repository.fetchAssetFingerprintRecords(assetIDs: pending)
                try Task.checkCancellation()
                let covered = try await coverageWorker.assetIDsCoveredByRemote(records: records, committedView: committedView)
                try Task.checkCancellation()
                pending.subtract(covered)
            }
            return pending
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

    func assetIDsForFullRun() async -> [String] {
        await withCheckedContinuation { continuation in
            queue.async { [photoLibraryService] in
                let assets = photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true)
                var ids: [String] = []
                ids.reserveCapacity(assets.count)
                for index in 0 ..< assets.count {
                    ids.append(assets.object(at: index).localIdentifier)
                }
                continuation.resume(returning: ids)
            }
        }
    }

    func assetIDsCoveredByRemote(
        records: [String: LocalAssetFingerprintRecord],
        committedView: PerMonth<Set<Data>>
    ) async throws -> Set<String> {
        let entries = Array(records)
        var covered: Set<String> = []
        covered.reserveCapacity(records.count)
        var offset = 0
        while offset < entries.count {
            try Task.checkCancellation()
            let end = min(offset + Self.coverageChunkSize, entries.count)
            let chunk = Array(entries[offset ..< end])
            let chunkCovered = await assetIDsCoveredByRemoteChunk(records: chunk, committedView: committedView)
            covered.formUnion(chunkCovered)
            offset = end
            await Task.yield()
        }
        return covered
    }

    private func assetIDsCoveredByRemoteChunk(
        records: [(String, LocalAssetFingerprintRecord)],
        committedView: PerMonth<Set<Data>>
    ) async -> Set<String> {
        await withCheckedContinuation { continuation in
            queue.async {
                let phAssets = Self.phAssets(forAssetIDs: records.map { $0.0 })
                var covered: Set<String> = []
                covered.reserveCapacity(records.count)
                for (id, record) in records {
                    guard Self.cacheRowIsTrustworthy(record) else { continue }
                    guard let phAsset = phAssets[id] else { continue }
                    guard Self.cachedSignatureMatchesCurrent(record: record, phAsset: phAsset) else { continue }
                    let month = LibraryMonthKey.from(date: phAsset.creationDate)
                    guard committedView.contains(record.fingerprint, in: month) else { continue }
                    if let modDate = phAsset.modificationDate, modDate > record.updatedAt { continue }
                    covered.insert(id)
                }
                continuation.resume(returning: covered)
            }
        }
    }

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

    private static func cacheRowIsTrustworthy(_ record: LocalAssetFingerprintRecord) -> Bool {
        guard record.selectionVersion >= BackupAssetResourcePlanner.currentSelectionVersion else { return false }
        return record.resourceSignature != nil
    }

    private static func cachedSignatureMatchesCurrent(record: LocalAssetFingerprintRecord, phAsset: PHAsset) -> Bool {
        guard let cachedSignature = record.resourceSignature else { return false }
        let currentResources = PHAssetResource.assetResources(for: phAsset)
        let ordered = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(from: currentResources)
        let currentSignature = BackupAssetResourcePlanner.resourceSignature(orderedResources: ordered)
        return cachedSignature == currentSignature
    }
}
