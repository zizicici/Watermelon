import Foundation
import Photos

// Whether an item exists only on device, only on the remote backup, or on both. Computed by
// cross-referencing the remote snapshot's fingerprints with the local hash index.
enum MediaPresence: Hashable, Sendable {
    case localOnly
    case remoteOnly
    case both

    // The single derivation from the two facts LibraryPresenceIndex owns.
    static func of(onDevice: Bool, onRemote: Bool) -> MediaPresence {
        if onDevice && onRemote { return .both }
        return onDevice ? .localOnly : .remoteOnly
    }
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
    let remoteMonth: LibraryMonthKey?   // remote manifest month (needed to delete from the backup)
    // The remote manifest record is incomplete (missing resource / fingerprint divergence / metadata-only), so
    // downloading it can only import the resolvable subset — a NEW asset with a different fingerprint. Surfaced
    // with an incomplete badge, and Download asks for confirmation. Always false for on-device items.
    var isIncomplete: Bool = false

    var isVideo: Bool { kind == .video }
    var isLivePhoto: Bool { kind == .livePhoto }
    var fingerprintHex: String? { fingerprint?.hexString }

    // Deletable from the backup: on the remote AND carries the fingerprint+month the manifest delete needs.
    var isRemoteDeletable: Bool { presence != .localOnly && fingerprint != nil && remoteMonth != nil }
    // Deletable from the device: has a live PHAsset handle.
    var isDeviceDeletable: Bool { localIdentifier != nil }
}

// Which batch actions a grid multi-selection offers, decided purely from the selected items. Kept out of the
// view controller so the rules are unit-testable. Rules (user-specified):
//  · Upload only when EVERY item is local-only; Download only when EVERY item is remote-only — a mixed
//    selection offers neither (no download-then-upload "complement").
//  · Delete is always available (one button); its confirmation states the from-backup / from-device breakdown,
//    and it removes each item from every place it lives (a "both" item is counted on both sides).
// The three multi-select batch operations (a synthetic "delete" that maps onto per-item deleteLocal/deleteRemote).
enum BatchAction: Hashable {
    case upload
    case download
    case delete
}

// `remoteCount` counts only items that can actually be deleted from the backup (fingerprint + remote month). The
// Local tab's source carries no remote month, so a backed-up on-device item there is device-only by design — a
// delete in the on-device view must not silently remove the cloud backup.
enum BatchActionResolver {
    struct Result: Equatable {
        let showsUpload: Bool
        let showsDownload: Bool
        let deviceCount: Int   // items that will be deleted from the device
        let remoteCount: Int   // items that will be removed from the backup
        var showsDelete: Bool { deviceCount > 0 || remoteCount > 0 }
    }

    static func resolve(_ items: [MediaBrowserItem]) -> Result {
        guard !items.isEmpty else {
            return Result(showsUpload: false, showsDownload: false, deviceCount: 0, remoteCount: 0)
        }
        // Upload/Download availability mirrors the executors' own guards (a local-only item is uploadable; a
        // remote-only item with a fingerprint is downloadable) so the button can't show for a no-op batch.
        return Result(
            showsUpload: items.allSatisfy { $0.presence == .localOnly && $0.localIdentifier != nil },
            showsDownload: items.allSatisfy { $0.presence == .remoteOnly && $0.fingerprint != nil },
            deviceCount: items.filter(\.isDeviceDeletable).count,
            remoteCount: items.filter(\.isRemoteDeletable).count
        )
    }
}

// A file guaranteed BY CONSTRUCTION to carry a valid extension — safe to hand to PHAssetCreationRequest,
// PHLivePhoto.request, or UIActivityViewController (all key off the extension). `make` is the ONLY
// constructor: an extensionless URL (a content-addressed cache original) is materialized into a
// correctly-named temp (hard link — no data copy; the cache inode is untouched — copy fallback across
// volumes) so no consumer can pass a bare cache URL. Temporary files must be deleted once consumed.
struct ImportReadyFile: Sendable {
    let url: URL
    let type: PHAssetResourceType
    let isTemporary: Bool

    private init(url: URL, type: PHAssetResourceType, isTemporary: Bool) {
        self.url = url
        self.type = type
        self.isTemporary = isTemporary
    }

    static func make(url: URL, type: PHAssetResourceType, isTemporary: Bool, extensionFrom remotePath: String?) -> ImportReadyFile {
        guard url.pathExtension.isEmpty,
              let ext = remotePath.map({ ($0 as NSString).pathExtension }), !ext.isEmpty else {
            return ImportReadyFile(url: url, type: type, isTemporary: isTemporary)
        }
        let fm = FileManager.default
        let dest = fm.temporaryDirectory.appendingPathComponent("imp_\(UUID().uuidString).\(ext)")
        do {
            try fm.linkItem(at: url, to: dest)
        } catch {
            guard (try? fm.copyItem(at: url, to: dest)) != nil else {
                return ImportReadyFile(url: url, type: type, isTemporary: isTemporary)
            }
        }
        if isTemporary { try? fm.removeItem(at: url) }   // the link/copy is what we hand over now
        return ImportReadyFile(url: dest, type: type, isTemporary: true)
    }
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
    case download      // save a remote-only item into the Photos library
    case upload        // back up an on-device-only item to the connected remote
    case deleteLocal   // remove from the Photos library
    case deleteRemote  // remove from the backup (irreversible)

    var symbolName: String {
        switch self {
        case .share: return "square.and.arrow.up"
        case .download: return "arrow.down.circle"
        case .upload: return "arrow.up.circle"
        case .deleteLocal: return "trash"
        case .deleteRemote: return "trash.slash"
        }
    }

    var title: String {
        switch self {
        case .share: return String(localized: "mediaBrowser.action.share")
        case .download: return String(localized: "mediaBrowser.action.download")
        case .upload: return String(localized: "mediaBrowser.action.upload")
        case .deleteLocal: return String(localized: "mediaBrowser.action.deleteLocal")
        case .deleteRemote: return String(localized: "mediaBrowser.action.deleteRemote")
        }
    }

    var isDestructive: Bool { self == .deleteLocal || self == .deleteRemote }
}
