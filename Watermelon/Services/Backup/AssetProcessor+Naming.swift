import CryptoKit
import Foundation
import Photos

extension AssetProcessor {
    static func preferredAssetNameStem(
        asset: PHAsset,
        selectedResources: [BackupSelectedResource]
    ) -> String {
        let rolePriority = [
            ResourceTypeCode.photo,
            ResourceTypeCode.video,
            ResourceTypeCode.fullSizePhoto,
            ResourceTypeCode.fullSizeVideo,
            ResourceTypeCode.alternatePhoto,
            ResourceTypeCode.pairedVideo
        ]

        for role in rolePriority {
            if let preferred = selectedResources.first(where: { $0.role == role && $0.slot == 0 }) {
                let stem = sanitizedFileStem(from: preferred.resource.originalFilename)
                if !stem.isEmpty {
                    return stem
                }
            }
        }

        if let first = selectedResources.first {
            let stem = sanitizedFileStem(from: first.resource.originalFilename)
            if !stem.isEmpty {
                return stem
            }
        }

        return "asset_\(asset.creationDate?.millisecondsSinceEpoch ?? 0)"
    }

    static func preferredRemoteFileName(
        preferredAssetNameStem: String,
        selected: BackupSelectedResource
    ) -> String {
        let sanitizedOriginalName = RemotePathBuilder.sanitizeFilename(selected.resource.originalFilename)
        let originalExt = (sanitizedOriginalName as NSString).pathExtension
        let originalStem = sanitizedFileStem(from: selected.resource.originalFilename)

        let baseStem: String = {
            if !preferredAssetNameStem.isEmpty {
                return preferredAssetNameStem
            }
            let fallback = sanitizedFileStem(from: selected.resource.originalFilename)
            return fallback.isEmpty ? "resource" : fallback
        }()

        let isPrimary = selected.slot == 0 &&
            (selected.role == ResourceTypeCode.photo ||
                selected.role == ResourceTypeCode.video ||
                selected.role == ResourceTypeCode.pairedVideo)
        let stem: String
        if isPrimary {
            stem = baseStem
        } else {
            var detailStem = originalStem
            if detailStem.isEmpty {
                detailStem = fallbackResourceLabel(forRole: selected.role)
            }

            let baseLower = baseStem.lowercased()
            let detailLower = detailStem.lowercased()

            if detailLower == baseLower {
                detailStem = fallbackResourceLabel(forRole: selected.role)
            }

            let updatedDetailLower = detailStem.lowercased()
            if updatedDetailLower.hasPrefix(baseLower + "_") ||
                updatedDetailLower.hasPrefix(baseLower + "-") ||
                updatedDetailLower == baseLower {
                stem = detailStem
            } else {
                stem = "\(baseStem)_\(detailStem)"
            }
        }

        if originalExt.isEmpty {
            return stem
        }
        return "\(stem).\(originalExt)"
    }

    static func fallbackResourceLabel(forRole role: Int) -> String {
        let raw = PhotoLibraryService.resourceTypeName(from: role)
        let separatedCamel = raw.replacingOccurrences(
            of: "([a-z0-9])([A-Z])",
            with: "$1 $2",
            options: .regularExpression
        )
        let normalized = separatedCamel.replacingOccurrences(
            of: "[^A-Za-z0-9]+",
            with: " ",
            options: .regularExpression
        )
        let words = normalized
            .split(separator: " ")
            .map { token in
                token.prefix(1).uppercased() + token.dropFirst()
            }
        if words.isEmpty {
            return "Resource\(max(role, 0))"
        }
        return words.joined()
    }

    static func sanitizedFileStem(from originalFilename: String) -> String {
        let sanitized = RemotePathBuilder.sanitizeFilename(originalFilename)
        return (sanitized as NSString).deletingPathExtension
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
