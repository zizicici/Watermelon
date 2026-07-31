import Foundation

enum LocalHashIndexPreflightPolicy {
    static func requiresSingleUploadWorker(
        initialResult: LocalHashIndexBuildResult,
        uploadAssetIDs: Set<String>,
        iCloudMode: ICloudPhotoBackupMode
    ) -> Bool {
        guard iCloudMode == .enable else { return false }
        return !initialResult.unavailableAssetIDs
            .union(initialResult.networkPendingAssetIDs)
            .intersection(uploadAssetIDs)
            .isEmpty
    }

    static func merging(
        _ initial: LocalHashIndexBuildResult,
        recovery: LocalHashIndexBuildResult
    ) -> LocalHashIndexBuildResult {
        LocalHashIndexBuildResult(
            requestedAssetIDs: initial.requestedAssetIDs,
            readyAssetIDs: initial.readyAssetIDs.union(
                recovery.readyAssetIDs
            ),
            unavailableAssetIDs: recovery.unavailableAssetIDs,
            failedAssetIDs: initial.failedAssetIDs.union(
                recovery.failedAssetIDs
            ),
            missingAssetIDs: initial.missingAssetIDs.union(
                recovery.missingAssetIDs
            ),
            networkPendingAssetIDs:
                initial.networkPendingAssetIDs.union(
                    recovery.networkPendingAssetIDs
                )
        )
    }
}
