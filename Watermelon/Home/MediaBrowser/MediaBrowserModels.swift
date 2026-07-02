import Foundation

// Whether an item exists only on device, only on the remote backup, or on both. Computed by
// cross-referencing the remote snapshot's fingerprints with the local hash index.
enum MediaPresence: Hashable, Sendable {
    case localOnly
    case remoteOnly
    case both
}

enum MediaBrowserMode: Hashable, Sendable {
    case local
    case remote
    case merged
}

// One browsable item, source-agnostic. `id` is a per-item unique identity + cell reuse token (local
// identifier locally; fingerprint hex + remote path remotely, since one fingerprint may span two remote
// months). Dedup across sources keys on `fingerprint`, not `id`. Handles carry whatever the materializer
// needs per source.
struct MediaBrowserItem: Hashable, Sendable {
    let id: String
    let kind: AlbumMediaKind
    let creationDateMs: Int64
    var presence: MediaPresence
    var localIdentifier: String?   // merged mode may graft a local handle onto a remote twin (→ .both)
    let fingerprint: Data?
    let photoRemoteRelativePath: String?
    let videoRemoteRelativePath: String?

    var isVideo: Bool { kind == .video }
    var isLivePhoto: Bool { kind == .livePhoto }
    var fingerprintHex: String? { fingerprint?.hexString }
}

struct MediaBrowserSection: Hashable, Sendable {
    let month: LibraryMonthKey
    var items: [MediaBrowserItem]
}

struct MaterializedVideo: Sendable {
    let url: URL
    let isTemporary: Bool
}

enum MediaBrowserActionKind: Hashable, Sendable {
    case share
    case download
    case upload
    case deleteLocal
    case deleteRemote
}
