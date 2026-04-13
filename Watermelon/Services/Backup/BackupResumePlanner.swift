import Foundation
import Photos

struct BackupResumePlan {
    let resumedExecutionMode: BackupRunMode?
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
            return BackupResumePlan(
                resumedExecutionMode: pendingAssetIDs.isEmpty ? nil : .retry(assetIDs: pendingAssetIDs)
            )

        case .scoped(let assetIDs):
            let pendingAssetIDs = assetIDs.subtracting(completedAssetIDs)
            return BackupResumePlan(
                resumedExecutionMode: pendingAssetIDs.isEmpty ? nil : .scoped(assetIDs: pendingAssetIDs)
            )

        case .full:
            let pendingAssetIDs = try await computePendingAssetIDsForFullRun(excluding: completedAssetIDs)
            return BackupResumePlan(
                resumedExecutionMode: pendingAssetIDs.isEmpty ? nil : .scoped(assetIDs: pendingAssetIDs)
            )
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
