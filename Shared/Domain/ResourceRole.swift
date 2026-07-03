import Foundation

// Canonical model for a manifest resource's role code. ALL role classification (photo/video/live-side) and
// photo-side/video-side selection derive from here, so a classifier and a picker can't drift apart (the
// adjustmentBasePhoto bug was exactly a picker that omitted a role its classifier accepted).
//
// A role code IS the PHAssetResourceType rawValue (see PhotoLibraryService.resourceTypeCode) — no transform.
// Deliberately does NOT own the fingerprint role ordering (BackupAssetResourcePlanner) or remote file naming
// (RemoteFileNaming): those are the dedup key and must stay byte-stable.
enum ResourceRole {
    // Photo-side roles in display/import preference order. Same set as the legacy `isPhotoLike` predicate;
    // `adjustmentBasePhoto` is included here so the picker never returns nil for an asset the classifier
    // counted as having a photo side.
    static let photoSidePriority: [Int] = [
        ResourceTypeCode.photo,
        ResourceTypeCode.fullSizePhoto,
        ResourceTypeCode.alternatePhoto,
        ResourceTypeCode.adjustmentBasePhoto,
        ResourceTypeCode.photoProxy,
    ]

    // Video-side roles in preference order (same set as the legacy `isVideoLike` predicate).
    static let videoSidePriority: [Int] = [
        ResourceTypeCode.video,
        ResourceTypeCode.fullSizeVideo,
        ResourceTypeCode.pairedVideo,
        ResourceTypeCode.fullSizePairedVideo,
        ResourceTypeCode.adjustmentBasePairedVideo,
        ResourceTypeCode.adjustmentBaseVideo,
    ]

    // Paired-video (Live clip) roles (same set as the legacy `isPairedVideo` predicate).
    static let pairedVideoRoles: [Int] = [
        ResourceTypeCode.pairedVideo,
        ResourceTypeCode.fullSizePairedVideo,
        ResourceTypeCode.adjustmentBasePairedVideo,
    ]

    // Config/metadata roles that carry no restorable media on their own (e.g. the adjustment sidecar). A
    // manifest record whose only resolvable resources are these is not a meaningful backup — restoring it
    // yields no photo/video. Single definition shared by isAssetIncomplete / cleanupMissingResources /
    // hasBackedUpMedia so the "is this a real backup" test can't drift between the backup and browser sides.
    static let metadataOnlyRoles: Set<Int> = [ResourceTypeCode.adjustmentData]

    static func isPhotoSide(_ code: Int) -> Bool { photoSidePriority.contains(code) }
    static func isVideoSide(_ code: Int) -> Bool { videoSidePriority.contains(code) }
    static func isPairedVideoSide(_ code: Int) -> Bool { pairedVideoRoles.contains(code) }
    static func isMetadataOnly(_ code: Int) -> Bool { metadataOnlyRoles.contains(code) }

    // A role that carries actual photo/video content — restorable as a real asset AND displayable in the browser.
    // The adjustment sidecar, audio, and any role this app doesn't model are NOT displayable media on their own,
    // so a record whose only resolvable resources are those isn't a real backup and isn't shown. (Audio-only
    // assets don't occur in a normal Photos library; this just keeps "backed up" ⟺ "displayable" coherent.)
    static func isDisplayableMedia(_ code: Int) -> Bool { isPhotoSide(code) || isVideoSide(code) }

    // "Is this a real backup" — the one rule every consumer (Home resolver, browser builder, presence index)
    // must apply to the resolvable roles of a manifest record. Named so a new consumer can't silently diverge
    // (a missing inline copy of this test was a real bug). Callers filter to resolvable resources first.
    static func containsRealMedia(_ roles: [Int]) -> Bool { roles.contains { isDisplayableMedia($0) } }

    // Two-side taxonomy shared by every media-kind classifier: paired-video + photo → Live; else video if any
    // video-side role; else photo. Returns raw booleans so callers over different kind enums (AlbumMediaKind,
    // photo/video buckets, LegacyBundleKind) all agree.
    static func classify(roles: [Int]) -> (isLivePhoto: Bool, isVideo: Bool) {
        let hasPaired = roles.contains { isPairedVideoSide($0) }
        let hasPhoto = roles.contains { isPhotoSide($0) }
        let isLive = hasPaired && hasPhoto
        let isVideo = !isLive && roles.contains { isVideoSide($0) }
        return (isLive, isVideo)
    }
}
