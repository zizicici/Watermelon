import Foundation
import Photos

final class BackupPlanner {
    func planIncrementalBackup(
        localResources: [LocalPhotoResource],
        existingResources: [BackupResourceRecord]
    ) -> [PlannedBackupItem] {
        let existingKeySet = Set(existingResources.map { "\($0.assetLocalIdentifier)|\($0.fingerprint)" })
        var occupiedRemotePaths = Set(existingResources.map(\.remoteRelativePath))
        var plannedItems: [PlannedBackupItem] = []
        plannedItems.reserveCapacity(localResources.count)

        for resource in localResources {
            let fingerprint = FingerprintBuilder.makeFingerprint(
                assetLocalIdentifier: resource.assetLocalIdentifier,
                resourceType: resource.resourceType,
                originalFilename: resource.originalFilename,
                fileSize: resource.fileSize,
                uti: resource.uti,
                resourceModificationDate: resource.resourceModificationDate
            )

            let key = "\(resource.assetLocalIdentifier)|\(fingerprint)"
            if existingKeySet.contains(key) {
                continue
            }

            var duplicateIndex = 0
            var remotePath = RemotePathBuilder.buildRelativePath(
                originalFilename: resource.originalFilename,
                creationDate: resource.asset.creationDate,
                duplicateIndex: duplicateIndex
            )

            while occupiedRemotePaths.contains(remotePath) {
                duplicateIndex += 1
                remotePath = RemotePathBuilder.buildRelativePath(
                    originalFilename: resource.originalFilename,
                    creationDate: resource.asset.creationDate,
                    duplicateIndex: duplicateIndex
                )
            }

            occupiedRemotePaths.insert(remotePath)
            plannedItems.append(PlannedBackupItem(localResource: resource, fingerprint: fingerprint, remoteRelativePath: remotePath))
        }

        return plannedItems
    }
}
