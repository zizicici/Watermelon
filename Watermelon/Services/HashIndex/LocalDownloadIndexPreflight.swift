import Foundation

struct LocalIndexIncompleteError: LocalizedError {
    let result: LocalHashIndexBuildResult
    let iCloudPhotoBackupMode: ICloudPhotoBackupMode

    var errorDescription: String? {
        var parts: [String] = []
        if !result.unavailableAssetIDs.isEmpty {
            parts.append(String.localizedStringWithFormat(String(localized: "home.execution.log.unavailableItems"), result.unavailableAssetIDs.count))
        }
        if !result.failedAssetIDs.isEmpty {
            parts.append(String.localizedStringWithFormat(String(localized: "home.execution.log.failedItems"), result.failedAssetIDs.count))
        }
        let detail = parts.joined(separator: ", ")
        if !result.unavailableAssetIDs.isEmpty, iCloudPhotoBackupMode == .disable {
            return String(format: String(localized: "home.execution.log.indexIncompleteICloud"), detail)
        }
        return String(format: String(localized: "home.execution.log.indexIncomplete"), detail)
    }
}

enum LocalDownloadIndexPreflight {
    @MainActor
    static func run(
        assetIDs: Set<String>,
        buildService: any LocalHashIndexBuilding,
        iCloudPhotoBackupMode: ICloudPhotoBackupMode,
        onReady: (Set<String>) -> Void
    ) async throws {
        try Task.checkCancellation()
        guard !assetIDs.isEmpty else { return }
        let initial = try await buildService.buildIndex(
            for: assetIDs, workerCount: 2, allowNetworkAccess: false,
            progressHandler: nil, tickHandler: nil
        )
        onReady(initial.readyAssetIDs)
        try Task.checkCancellation()
        var result = initial
        if !initial.unavailableAssetIDs.isEmpty, iCloudPhotoBackupMode == .enable {
            let recovery = try await buildService.buildIndex(
                for: initial.unavailableAssetIDs, workerCount: 1, allowNetworkAccess: true,
                progressHandler: nil, tickHandler: nil
            )
            onReady(recovery.readyAssetIDs)
            try Task.checkCancellation()
            result = LocalHashIndexBuildResult(
                requestedAssetIDs: initial.requestedAssetIDs,
                readyAssetIDs: initial.readyAssetIDs.union(recovery.readyAssetIDs),
                unavailableAssetIDs: recovery.unavailableAssetIDs,
                failedAssetIDs: initial.failedAssetIDs.union(recovery.failedAssetIDs),
                missingAssetIDs: initial.missingAssetIDs.union(recovery.missingAssetIDs),
                networkPendingAssetIDs: initial.networkPendingAssetIDs.union(recovery.networkPendingAssetIDs)
            )
        }
        guard result.incompleteAssetIDs.isEmpty else {
            throw LocalIndexIncompleteError(result: result, iCloudPhotoBackupMode: iCloudPhotoBackupMode)
        }
    }
}
