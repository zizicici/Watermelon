import Foundation

// Turns the resolvable resource subset of one remote asset into a set PHAssetCreationRequest will accept.
//
// PhotoKit needs exactly one PRIMARY resource of the asset's kind (`.photo` for a photo/Live Photo, `.video` for
// a video) and a request that isn't self-contradictory. So we pick the kind from what actually resolved, then:
//   · a COMPLETE record passes through with FULL FIDELITY — we DENY only the roles PhotoKit would reject for that
//     kind (a Live-clip / paired-video-side role on a non-Live import, or a cross-kind role), and keep everything
//     else, INCLUDING roles this planner doesn't model (audio, a future PHAssetResourceType). An allowlist would
//     silently drop an unmodeled role and restore a complete asset to a different fingerprint (a spurious new asset).
//   · an INCOMPLETE subset that lost its primary is rebuilt into the minimal valid asset by promoting the best
//     surviving same-side resource (a paired-clip-only record → a standalone `.video`; a photo missing `.photo`
//     but with `.fullSizePhoto` → the full-size becomes the primary; a Live missing its still → promote + keep clip).
// A promoted record is a NEW, differently-fingerprinted asset — the caller already took the user's informed consent.
// Promoted primaries are emitted at slot 0 so the on-device fingerprint matches a plain re-backup of the same file.
enum RestoreImportPlan {
    static func normalize(_ instances: [RemoteAssetResourceInstance]) -> [RemoteAssetResourceInstance] {
        let hasPhotoSide = instances.contains { ResourceRole.isPhotoSide($0.role) }
        // A Live Photo needs the canonical `.pairedVideo`; a derived-only paired resource (full-size /
        // adjustment-base) can't stand in as the clip, so it does NOT make the asset Live.
        let hasClip = instances.contains { $0.role == ResourceTypeCode.pairedVideo }
        let hasVideoSide = instances.contains { ResourceRole.isVideoSide($0.role) }

        if hasPhotoSide && hasClip {
            // Live Photo: keep everything (photo side + clip + adjuncts + audio + any unmodeled role).
            return buildKind(instances, primaryRole: ResourceTypeCode.photo, primaryCandidates: ResourceRole.photoSidePriority, live: true, rejects: { _ in false })
        } else if hasPhotoSide {
            // Standalone photo: a still can't carry a video / Live-clip resource → drop any video-side role.
            return buildKind(instances, primaryRole: ResourceTypeCode.photo, primaryCandidates: ResourceRole.photoSidePriority, live: false, rejects: { ResourceRole.isVideoSide($0) })
        } else if hasVideoSide {
            // Standalone video: a video can't carry a still or a Live clip → drop photo-side and paired-video-side roles.
            return buildKind(instances, primaryRole: ResourceTypeCode.video, primaryCandidates: ResourceRole.videoSidePriority, live: false, rejects: { ResourceRole.isPhotoSide($0) || ResourceRole.isPairedVideoSide($0) })
        }
        return instances   // no real media (config-only / phantom is dropped upstream); leave as-is
    }

    // Primary present → keep the whole record minus the roles `rejects` marks invalid for this kind (full fidelity
    // for a complete record). Primary missing → rebuild the minimal valid asset from the best surviving resource(s).
    private static func buildKind(
        _ instances: [RemoteAssetResourceInstance],
        primaryRole: Int,
        primaryCandidates: [Int],
        live: Bool,
        rejects: (Int) -> Bool
    ) -> [RemoteAssetResourceInstance] {
        if instances.contains(where: { $0.role == primaryRole }) {
            return instances.filter { !rejects($0.role) }
        }
        guard let primary = firstByPriority(instances, priority: primaryCandidates) else {
            return instances.filter { !rejects($0.role) }
        }
        if live, let clip = instances.first(where: { $0.role == ResourceTypeCode.pairedVideo }) {
            return [primary.promoted(toRole: primaryRole), clip]
        }
        return [primary.promoted(toRole: primaryRole)]
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
            creationDateMs: creationDateMs,
            storageCodec: storageCodec,
            storedFileSize: storedFileSize,
            encryptionKeyID: encryptionKeyID
        )
    }
}
