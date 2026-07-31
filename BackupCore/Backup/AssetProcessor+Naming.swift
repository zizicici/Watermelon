import Foundation
import Photos

extension AssetProcessor {
    static func preferredAssetNameStem(
        asset: PHAsset,
        selectedResources: [BackupSelectedResource]
    ) -> String {
        RemoteFileNaming.preferredAssetNameStem(
            orderedResources: selectedResources.map { selected in
                RemoteFileNaming.ResourceIdentity(
                    role: selected.role,
                    slot: selected.slot,
                    originalFilename: PhotoLibraryService.safeOriginalFilename(for: selected.resource)
                )
            },
            fallbackTimestampMs: LibraryCreationDate.normalized(asset.creationDate).milliseconds
        )
    }

    static func preferredRemoteFileName(
        preferredAssetNameStem: String,
        selected: BackupSelectedResource
    ) -> String {
        RemoteFileNaming.preferredRemoteFileName(
            preferredAssetNameStem: preferredAssetNameStem,
            resource: RemoteFileNaming.ResourceIdentity(
                role: selected.role,
                slot: selected.slot,
                originalFilename: PhotoLibraryService.safeOriginalFilename(for: selected.resource)
            )
        )
    }

    func makeLocalResource(
        asset: PHAsset,
        selected: BackupSelectedResource,
        preferredAssetNameStem: String
    ) -> LocalPhotoResource {
        LocalPhotoResource(
            asset: asset,
            resource: selected.resource,
            assetLocalIdentifier: asset.localIdentifier,
            resourceLocalIdentifier: "\(asset.localIdentifier)::\(selected.role)::\(selected.slot)",
            preferredRemoteFileName: Self.preferredRemoteFileName(
                preferredAssetNameStem: preferredAssetNameStem,
                selected: selected
            ),
            resourceRole: selected.role,
            resourceSlot: selected.slot,
            resourceType: PhotoLibraryService.resourceTypeName(selected.resource.type),
            resourceTypeCode: selected.role,
            uti: PhotoLibraryService.safeUniformTypeIdentifier(for: selected.resource),
            originalFilename: PhotoLibraryService.safeOriginalFilename(for: selected.resource),
            fileSize: PhotoLibraryService.resourceFileSize(selected.resource),
            resourceModificationDate: asset.modificationDate
        )
    }

    static func contentHash(
        of fileURL: URL,
        cancellationController: BackupCancellationController? = nil
    ) throws -> Data {
        try FileDigestService.sha256(
            of: fileURL,
            cancellationController: cancellationController
        )
    }

    static func contentHashAndSize(
        of fileURL: URL,
        cancellationController: BackupCancellationController? = nil
    ) throws -> (hash: Data, size: Int64) {
        try FileDigestService.sha256AndSize(
            of: fileURL,
            cancellationController: cancellationController
        )
    }

    static func elapsedSeconds(since start: CFAbsoluteTime) -> TimeInterval {
        max(CFAbsoluteTimeGetCurrent() - start, 0)
    }

    static func totalSizeBytes(of selectedResources: [BackupSelectedResource]) -> Int64 {
        selectedResources.reduce(Int64(0)) { partial, selected in
            partial + max(PhotoLibraryService.resourceFileSize(selected.resource), 0)
        }
    }
}
