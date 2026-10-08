import Foundation
import Photos

enum RestoreImportPlan {
    static func normalize(_ instances: [RemoteAssetResourceInstance]) -> [RemoteAssetResourceInstance] {
        let hasPrimaryPhoto = instances.contains { $0.role == ResourceTypeCode.photo }
        let hasStandaloneVideo = instances.contains {
            [ResourceTypeCode.video, ResourceTypeCode.fullSizeVideo, ResourceTypeCode.adjustmentBaseVideo].contains($0.role)
        }
        let prefersVideo = !hasPrimaryPhoto && hasStandaloneVideo
        let hasPhoto = !prefersVideo && instances.contains { ResourceRole.isPhotoSide($0.role) }
        let clip = instances.first { $0.role == ResourceTypeCode.pairedVideo }
        let primaryRole = hasPhoto ? ResourceTypeCode.photo : ResourceTypeCode.video
        let priority = hasPhoto ? ResourceRole.photoSidePriority : ResourceRole.videoSidePriority
        guard let primary = firstByPriority(instances, priority: priority) else { return instances }

        if primary.role == primaryRole {
            let candidates = instances.filter {
                if prefersVideo { return !ResourceRole.isPairedVideoSide($0.role) }
                if hasPhoto && clip == nil { return !ResourceRole.isVideoSide($0.role) }
                return true
            }
            if supports(candidates) { return candidates }
        }
        let promoted = primary.promoted(toRole: primaryRole)
        if hasPhoto, let clip, supports([promoted, clip]) {
            return [promoted, clip]
        }
        return [promoted]
    }

    private static func supports(_ instances: [RemoteAssetResourceInstance]) -> Bool {
        PHAssetCreationRequest.supportsAssetResourceTypes(instances.map { NSNumber(value: $0.role) })
    }

    private static func firstByPriority(_ instances: [RemoteAssetResourceInstance], priority: [Int]) -> RemoteAssetResourceInstance? {
        for role in priority {
            if let match = instances.first(where: { $0.role == role }) { return match }
        }
        return nil
    }
}

private extension RemoteAssetResourceInstance {
    func promoted(toRole newRole: Int) -> RemoteAssetResourceInstance {
        RemoteAssetResourceInstance(
            role: newRole,
            slot: 0,
            resourceHash: resourceHash,
            fileName: fileName,
            fileSize: fileSize,
            remoteRelativePath: remoteRelativePath,
            creationDateMs: creationDateMs
        )
    }
}
