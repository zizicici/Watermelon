import CryptoKit
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
                    originalFilename: selected.resource.originalFilename
                )
            },
            fallbackTimestampMs: asset.creationDate?.millisecondsSinceEpoch
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
                originalFilename: selected.resource.originalFilename
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
            uti: selected.resource.uniformTypeIdentifier,
            originalFilename: selected.resource.originalFilename,
            fileSize: PhotoLibraryService.resourceFileSize(selected.resource),
            resourceModificationDate: asset.modificationDate
        )
    }

    static func contentHash(
        of fileURL: URL,
        cancellationController: BackupCancellationController? = nil
    ) throws -> Data {
        try contentHashAndSize(of: fileURL, cancellationController: cancellationController).hash
    }

    static func contentHashAndSize(
        of fileURL: URL,
        cancellationController: BackupCancellationController? = nil
    ) throws -> (hash: Data, size: Int64) {
        let fileHandle = try FileHandle(forReadingFrom: fileURL)
        defer {
            try? fileHandle.close()
        }

        var hasher = SHA256()
        var totalBytes: Int64 = 0
        while true {
            try cancellationController?.throwIfCancelled()
            try Task.checkCancellation()
            let shouldContinue: Bool = try autoreleasepool {
                let chunk = try fileHandle.read(upToCount: hashBufferSize) ?? Data()
                guard !chunk.isEmpty else { return false }
                hasher.update(data: chunk)
                totalBytes += Int64(chunk.count)
                return true
            }
            if !shouldContinue { break }
        }

        return (Data(hasher.finalize()), totalBytes)
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
