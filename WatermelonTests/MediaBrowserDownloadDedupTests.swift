import XCTest
@testable import Watermelon

// Batch download collapses same-fingerprint remote twins (a grouping-TZ re-upload spans two months) to one
// restore. The selection arrives in Set order, so the dedup must deterministically prefer a COMPLETE twin
// over an incomplete one — otherwise a damaged month can win and the batch imports a partial subset (or skips)
// despite a restorable copy being selected.
final class MediaBrowserDownloadDedupTests: XCTestCase {
    private let monthA = LibraryMonthKey(year: 2024, month: 1)
    private let monthB = LibraryMonthKey(year: 2024, month: 2)

    private func remote(id: String, fp: Data, month: LibraryMonthKey, incomplete: Bool) -> MediaBrowserItem {
        MediaBrowserItem(
            id: id,
            kind: .photo,
            creationDateMs: 0,
            presence: .remoteOnly,
            localIdentifier: nil,
            fingerprint: fp,
            photoRemoteRelativePath: "\(month.year)-\(month.month)/\(id).jpg",
            videoRemoteRelativePath: nil,
            remoteMonth: month,
            isIncomplete: incomplete
        )
    }

    func testPrefersCompleteTwinRegardlessOfOrder() {
        let fp = Data([1, 2, 3])
        let complete = remote(id: "c", fp: fp, month: monthA, incomplete: false)
        let incomplete = remote(id: "i", fp: fp, month: monthB, incomplete: true)

        // Incomplete first in the (Set-ordered) selection…
        let a = MediaBrowserActionRunner.dedupedForDownload([incomplete, complete])
        XCTAssertEqual(a.count, 1)
        XCTAssertEqual(a.first?.id, "c", "the complete twin must win even when the incomplete one is first")
        XCTAssertEqual(a.first?.isIncomplete, false)

        // …and complete first: same result.
        let b = MediaBrowserActionRunner.dedupedForDownload([complete, incomplete])
        XCTAssertEqual(b.count, 1)
        XCTAssertEqual(b.first?.id, "c")
    }

    func testAllIncompleteTwinsCollapseToOne() {
        let fp = Data([9])
        let items = [
            remote(id: "i1", fp: fp, month: monthA, incomplete: true),
            remote(id: "i2", fp: fp, month: monthB, incomplete: true),
        ]
        let out = MediaBrowserActionRunner.dedupedForDownload(items)
        XCTAssertEqual(out.count, 1, "no complete twin exists → keep exactly one incomplete representative")
        XCTAssertEqual(out.first?.isIncomplete, true)
    }

    func testDistinctFingerprintsAllKept() {
        let out = MediaBrowserActionRunner.dedupedForDownload([
            remote(id: "a", fp: Data([1]), month: monthA, incomplete: false),
            remote(id: "b", fp: Data([2]), month: monthA, incomplete: true),
            remote(id: "c", fp: Data([3]), month: monthB, incomplete: false),
        ])
        XCTAssertEqual(Set(out.map(\.id)), ["a", "b", "c"])
    }

    func testNonRemoteOnlyAndFingerprintlessDropped() {
        let both = MediaBrowserItem(
            id: "both", kind: .photo, creationDateMs: 0, presence: .both, localIdentifier: "L",
            fingerprint: Data([5]), photoRemoteRelativePath: nil, videoRemoteRelativePath: nil, remoteMonth: monthA
        )
        let noFingerprint = MediaBrowserItem(
            id: "nofp", kind: .photo, creationDateMs: 0, presence: .remoteOnly, localIdentifier: nil,
            fingerprint: nil, photoRemoteRelativePath: nil, videoRemoteRelativePath: nil, remoteMonth: monthA
        )
        let keep = remote(id: "keep", fp: Data([6]), month: monthA, incomplete: false)
        let out = MediaBrowserActionRunner.dedupedForDownload([both, noFingerprint, keep])
        XCTAssertEqual(out.map(\.id), ["keep"])
    }

    // MARK: - All-incomplete twins: keep the richest recoverable copy, deterministically

    private func incompleteTwin(id: String, fp: Data, month: LibraryMonthKey, kind: AlbumMediaKind, photo: Bool, video: Bool) -> MediaBrowserItem {
        MediaBrowserItem(
            id: id,
            kind: kind,
            creationDateMs: 0,
            presence: .remoteOnly,
            localIdentifier: nil,
            fingerprint: fp,
            photoRemoteRelativePath: photo ? "\(month.year)-\(month.month)/\(id).jpg" : nil,
            videoRemoteRelativePath: video ? "\(month.year)-\(month.month)/\(id).mov" : nil,
            remoteMonth: month,
            isIncomplete: true
        )
    }

    func testPrefersRicherIncompleteTwinRegardlessOfOrder() {
        let fp = Data([7, 7])
        // Same damaged asset in two months: month A still recovers both sides (Live), month B only the still.
        let live = incompleteTwin(id: "live", fp: fp, month: monthA, kind: .livePhoto, photo: true, video: true)
        let photoOnly = incompleteTwin(id: "photo", fp: fp, month: monthB, kind: .photo, photo: true, video: false)

        for order in [[live, photoOnly], [photoOnly, live]] {
            let out = MediaBrowserActionRunner.dedupedForDownload(order)
            XCTAssertEqual(out.count, 1)
            XCTAssertEqual(out.first?.id, "live", "the twin recovering more media sides must win over a single-side twin")
        }
    }

    func testComplementaryIncompleteTwinsBothKeptRegardlessOfOrder() {
        let fp = Data([4, 2])
        // The same damaged asset split across two months: month A recovers only the still, month B only the paired
        // video. They are complementary, not interchangeable — collapsing them would drop a selected recoverable side.
        let photoSide = incompleteTwin(id: "p", fp: fp, month: monthA, kind: .photo, photo: true, video: false)
        let videoSide = incompleteTwin(id: "v", fp: fp, month: monthB, kind: .video, photo: false, video: true)
        for order in [[photoSide, videoSide], [videoSide, photoSide]] {
            let kept = Set(MediaBrowserActionRunner.dedupedForDownload(order).map(\.id))
            XCTAssertEqual(kept, ["p", "v"], "both complementary sides must survive; neither selected side may be dropped")
        }
    }

    func testBothSidesTwinSubsumesRedundantSingleSideTwins() {
        let fp = Data([1, 1, 1])
        let live = incompleteTwin(id: "live", fp: fp, month: monthA, kind: .livePhoto, photo: true, video: true)
        let photoOnly = incompleteTwin(id: "photo", fp: fp, month: monthB, kind: .photo, photo: true, video: false)
        let videoOnly = incompleteTwin(id: "video", fp: fp, month: monthB, kind: .video, photo: false, video: true)
        // A both-sides twin already covers photo AND video → single-side twins add nothing (no double import).
        for order in [[live, photoOnly, videoOnly], [videoOnly, photoOnly, live], [photoOnly, live, videoOnly]] {
            let kept = MediaBrowserActionRunner.dedupedForDownload(order)
            XCTAssertEqual(kept.map(\.id), ["live"], "a both-sides twin subsumes complementary single-side twins")
        }
    }

    func testComplementaryAndRedundantIncompleteTwinsResolveDeterministically() {
        let fp = Data([8, 8])
        // x,z recover only the photo side; y recovers only the video side. x and y are complementary (both survive);
        // z is redundant with x. The kept SET must be identical regardless of the Set-ordered input.
        let x = incompleteTwin(id: "x", fp: fp, month: monthA, kind: .photo, photo: true, video: false)
        let y = incompleteTwin(id: "y", fp: fp, month: monthB, kind: .video, photo: false, video: true)
        let z = incompleteTwin(id: "z", fp: fp, month: monthA, kind: .photo, photo: true, video: false)

        let permutations = [[x, y, z], [z, y, x], [y, x, z], [y, z, x], [x, z, y], [z, x, y]]
        for perm in permutations {
            let kept = Set(MediaBrowserActionRunner.dedupedForDownload(perm).map(\.id))
            XCTAssertEqual(kept, ["x", "y"], "keep photo (via lowest-id x) + video (y); drop redundant photo z")
        }
    }

    // MARK: - Resolved-descriptor dedup (stale-snapshot divergence between selection and resolution)

    private func instance(hash: UInt8) -> RemoteAssetResourceInstance {
        RemoteAssetResourceInstance(role: 1, slot: 0, resourceHash: Data([hash]), fileName: "r\(hash)",
                                    fileSize: 1, remoteRelativePath: "m/r\(hash)", creationDateMs: nil)
    }

    private func descriptor(identity: UInt8, hashes: [UInt8]) -> RestoreService.RestoreItemDescriptor {
        RestoreService.RestoreItemDescriptor(instances: hashes.map { instance(hash: $0) }, identity: Data([identity]))
    }

    private func resourceHashes(_ d: RestoreService.RestoreItemDescriptor) -> Set<Data> {
        Set(d.instances.map(\.resourceHash))
    }

    func testStaleTwinsResolvingToIdenticalSideCollapse() {
        // A vanished preferred month fell back by fingerprint → two same-fingerprint descriptors resolve the same
        // video side. Importing both would duplicate the asset.
        let out = MediaBrowserActionRunner.dedupeResolvedDescriptors([
            descriptor(identity: 1, hashes: [9]),
            descriptor(identity: 1, hashes: [9]),
        ])
        XCTAssertEqual(out.count, 1, "identical same-identity resolutions must not import the asset twice")
    }

    func testComplementaryResolvedDescriptorsBothKept() {
        // Disjoint sides (photo hash 1 vs video hash 2) of the same fingerprint → both must restore.
        let out = MediaBrowserActionRunner.dedupeResolvedDescriptors([
            descriptor(identity: 1, hashes: [1]),
            descriptor(identity: 1, hashes: [2]),
        ])
        XCTAssertEqual(out.count, 2)
        XCTAssertEqual(Set(out.flatMap { $0.instances.map(\.resourceHash) }), [Data([1]), Data([2])])
    }

    func testSupersetDescriptorSubsumesSubset() {
        // A repaired month resolves both sides (Live: 1+2); a stale twin resolves only the video (2). The standalone
        // video is already contained in the Live import → drop it, keep the superset — regardless of input order.
        for order in [[descriptor(identity: 1, hashes: [2]), descriptor(identity: 1, hashes: [1, 2])],
                      [descriptor(identity: 1, hashes: [1, 2]), descriptor(identity: 1, hashes: [2])]] {
            let out = MediaBrowserActionRunner.dedupeResolvedDescriptors(order)
            XCTAssertEqual(out.count, 1)
            XCTAssertEqual(out.first.map(resourceHashes), [Data([1]), Data([2])], "keep the superset (Live)")
        }
    }

    func testDifferentFingerprintsWithSharedHashNotCollapsed() {
        // Same resource hash under two DIFFERENT asset identities is not a duplicate asset → keep both.
        let out = MediaBrowserActionRunner.dedupeResolvedDescriptors([
            descriptor(identity: 1, hashes: [5]),
            descriptor(identity: 2, hashes: [5]),
        ])
        XCTAssertEqual(out.count, 2, "distinct fingerprints are never collapsed")
    }

    func testPartialOverlapSameIdentityDescriptorsMerge() {
        // Stale resolution: one twin resolves {photo 1, pairedVideo 2}, the other {pairedVideo 2, fullSizePaired 3}.
        // They share resource 2 but neither is a subset → keeping both would import the shared clip twice. Merge.
        for order in [[descriptor(identity: 1, hashes: [1, 2]), descriptor(identity: 1, hashes: [2, 3])],
                      [descriptor(identity: 1, hashes: [2, 3]), descriptor(identity: 1, hashes: [1, 2])]] {
            let out = MediaBrowserActionRunner.dedupeResolvedDescriptors(order)
            XCTAssertEqual(out.count, 1, "overlapping same-identity descriptors merge (shared resource imported once)")
            XCTAssertEqual(out.first.map(resourceHashes), [Data([1]), Data([2]), Data([3])])
        }
    }

    private func instance(role: Int, hash: Data) -> RemoteAssetResourceInstance {
        RemoteAssetResourceInstance(role: role, slot: 0, resourceHash: hash, fileName: "f\(role)",
                                    fileSize: 1, remoteRelativePath: "m/f\(role)", creationDateMs: nil)
    }

    func testLegacyEmptyHashComplementarySidesBothKept() {
        // Legacy no-hash manifest: complementary photo-side (role 1) and video-side (role 3) resolve with EMPTY
        // resourceHash. Identity is role|slot|hash, so they must NOT collapse into one {emptyHash} set and drop a side.
        let photo = RestoreService.RestoreItemDescriptor(instances: [instance(role: 1, hash: Data())], identity: Data([7]))
        let video = RestoreService.RestoreItemDescriptor(instances: [instance(role: 3, hash: Data())], identity: Data([7]))
        let out = MediaBrowserActionRunner.dedupeResolvedDescriptors([photo, video])
        XCTAssertEqual(out.count, 2, "legacy empty-hash complementary roles must both survive")
    }
}
