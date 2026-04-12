import Foundation
import Photos

struct BackupResumePlan {
    let pendingAssetIDs: Set<String>

    var resumedExecutionMode: BackupRunMode? {
        guard !pendingAssetIDs.isEmpty else { return nil }
        return executionModeFactory(pendingAssetIDs)
    }

    private let executionModeFactory: (Set<String>) -> BackupRunMode

    init(
        pendingAssetIDs: Set<String>,
        executionModeFactory: @escaping (Set<String>) -> BackupRunMode
    ) {
        self.pendingAssetIDs = pendingAssetIDs
        self.executionModeFactory = executionModeFactory
    }
}

final class BackupResumePlanner {
    private let photoLibraryService: PhotoLibraryService

    init(photoLibraryService: PhotoLibraryService) {
        self.photoLibraryService = photoLibraryService
    }

    func makePlan(
        pausedMode: BackupRunMode,
        completedAssetIDs: Set<String>
    ) async throws -> BackupResumePlan {
        switch pausedMode {
        case .retry(let assetIDs):
            let pendingAssetIDs = assetIDs.subtracting(completedAssetIDs)
            return BackupResumePlan(pendingAssetIDs: pendingAssetIDs) { .retry(assetIDs: $0) }

        case .scoped(let assetIDs):
            let pendingAssetIDs = assetIDs.subtracting(completedAssetIDs)
            return BackupResumePlan(pendingAssetIDs: pendingAssetIDs) { .scoped(assetIDs: $0) }

        case .full:
            let pendingAssetIDs = try await computePendingAssetIDsForFullRun(excluding: completedAssetIDs)
            return BackupResumePlan(pendingAssetIDs: pendingAssetIDs) { .scoped(assetIDs: $0) }
        }
    }

    private func computePendingAssetIDsForFullRun(
        excluding completedAssetIDs: Set<String>
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
        return try await Task.detached(priority: .userInitiated) {
            let assets = photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true)
            var pending = Set<String>()
            pending.reserveCapacity(max(assets.count - completedAssetIDs.count, 0))

            for index in 0 ..< assets.count {
                try Task.checkCancellation()
                let assetID = assets.object(at: index).localIdentifier
                if !completedAssetIDs.contains(assetID) {
                    pending.insert(assetID)
                }
            }

            return pending
        }.value
    }
}
