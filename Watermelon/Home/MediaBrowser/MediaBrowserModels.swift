import Foundation
import Photos

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

enum MediaBrowserItemID: Hashable, Sendable, Comparable {
    case local(String)
    case remote(fingerprint: Data, storageMonth: LibraryMonthKey)

    static func < (lhs: MediaBrowserItemID, rhs: MediaBrowserItemID) -> Bool {
        switch (lhs, rhs) {
        case (.local(let lhs), .local(let rhs)):
            return lhs < rhs
        case (.local, .remote):
            return true
        case (.remote, .local):
            return false
        case let (.remote(lhsFingerprint, lhsMonth), .remote(rhsFingerprint, rhsMonth)):
            if lhsFingerprint != rhsFingerprint {
                return lhsFingerprint.lexicographicallyPrecedes(rhsFingerprint)
            }
            return lhsMonth < rhsMonth
        }
    }
}

struct LocalMediaReference: Hashable, Sendable {
    let identifier: String
    let fingerprint: Data?
}

struct RemoteMediaReference: Hashable, Sendable {
    let fingerprint: Data
    let photoRelativePath: String?
    let videoRelativePath: String?
    let photoContentHash: Data?
    let videoContentHash: Data?
    let storageMonth: LibraryMonthKey
    let isIncomplete: Bool
}

enum MediaBrowserBacking: Hashable, Sendable {
    case local(LocalMediaReference, isBackedUp: Bool)
    case remote(RemoteMediaReference, local: LocalMediaReference?)
}

struct MediaBrowserItem: Hashable, Sendable {
    let id: MediaBrowserItemID
    let kind: AlbumMediaKind
    let creationDateMs: Int64
    private(set) var backing: MediaBrowserBacking

    var presence: MediaPresence {
        switch backing {
        case .local(_, let isBackedUp):
            return isBackedUp ? .both : .localOnly
        case .remote(_, let local):
            return local == nil ? .remoteOnly : .both
        }
    }

    var localIdentifier: String? {
        switch backing {
        case .local(let local, _): return local.identifier
        case .remote(_, let local): return local?.identifier
        }
    }

    var fingerprint: Data? {
        switch backing {
        case .local(let local, _): return local.fingerprint
        case .remote(let remote, _): return remote.fingerprint
        }
    }

    var photoRemoteRelativePath: String? {
        guard case .remote(let remote, _) = backing else { return nil }
        return remote.photoRelativePath
    }

    var videoRemoteRelativePath: String? {
        guard case .remote(let remote, _) = backing else { return nil }
        return remote.videoRelativePath
    }

    var photoContentHash: Data? {
        guard case .remote(let remote, _) = backing else { return nil }
        return remote.photoContentHash
    }

    var videoContentHash: Data? {
        guard case .remote(let remote, _) = backing else { return nil }
        return remote.videoContentHash
    }

    var remoteMonth: LibraryMonthKey? {
        guard case .remote(let remote, _) = backing else { return nil }
        return remote.storageMonth
    }

    var isIncomplete: Bool {
        guard case .remote(let remote, _) = backing else { return false }
        return remote.isIncomplete
    }

    var isVideo: Bool { kind == .video }
    var isLivePhoto: Bool { kind == .livePhoto }
    var fingerprintHex: String? { fingerprint?.hexString }

    // Deletable from the backup: on the remote AND carries the fingerprint+month the manifest delete needs.
    var isRemoteDeletable: Bool {
        if case .remote = backing { return true }
        return false
    }
    // Deletable from the device: has a live PHAsset handle.
    var isDeviceDeletable: Bool { localIdentifier != nil }

    init(
        kind: AlbumMediaKind,
        creationDateMs: Int64,
        localIdentifier: String,
        fingerprint: Data?,
        isBackedUp: Bool
    ) {
        id = .local(localIdentifier)
        self.kind = kind
        self.creationDateMs = creationDateMs
        backing = .local(
            LocalMediaReference(identifier: localIdentifier, fingerprint: fingerprint),
            isBackedUp: isBackedUp
        )
    }

    init(
        kind: AlbumMediaKind,
        creationDateMs: Int64,
        localIdentifier: String?,
        remote: RemoteMediaReference
    ) {
        id = .remote(
            fingerprint: remote.fingerprint,
            storageMonth: remote.storageMonth
        )
        self.kind = kind
        self.creationDateMs = creationDateMs
        backing = .remote(
            remote,
            local: localIdentifier.map { LocalMediaReference(identifier: $0, fingerprint: remote.fingerprint) }
        )
    }

    mutating func attachLocalIdentifier(_ identifier: String) {
        switch backing {
        case .local:
            return
        case .remote(let remote, _):
            backing = .remote(
                remote,
                local: LocalMediaReference(identifier: identifier, fingerprint: remote.fingerprint)
            )
        }
    }

    mutating func removeLocalIdentifier() {
        switch backing {
        case .remote(let remote, _):
            backing = .remote(remote, local: nil)
        case .local:
            return
        }
    }
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
        let summary = MediaLibraryActionPolicy.batchSummary(
            for: items.map {
                MediaLibraryBatchItem(
                    presence: $0.presence.libraryActionPresence,
                    canDeleteLocal: $0.isDeviceDeletable,
                    canDeleteRemote: $0.isRemoteDeletable
                )
            }
        )
        return Result(
            showsUpload: summary.showsUpload,
            showsDownload: summary.showsDownload,
            deviceCount: summary.localDeleteCount,
            remoteCount: summary.remoteDeleteCount
        )
    }
}

private extension MediaPresence {
    var libraryActionPresence: MediaLibraryPresence {
        switch self {
        case .localOnly: return .localOnly
        case .remoteOnly: return .remoteOnly
        case .both: return .both
        }
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
    let items: [MediaBrowserItem]
}

struct MediaBrowserSnapshot: Sendable {
    static let empty = MediaBrowserSnapshot(sections: [])

    let months: [LibraryMonthKey]
    private let sections: [MediaBrowserSection]
    private let sectionOffsets: [Int]
    private let itemIndexByID: [MediaBrowserItemID: Int]

    init(sections: [MediaBrowserSection]) {
        self.sections = sections
        months = sections.map(\.month)
        let itemCount = sections.reduce(0) { $0 + $1.items.count }
        var offsets: [Int] = []
        offsets.reserveCapacity(sections.count + 1)
        offsets.append(0)
        var runningCount = 0
        for section in sections {
            runningCount += section.items.count
            offsets.append(runningCount)
        }
        sectionOffsets = offsets

        var indexesByID: [MediaBrowserItemID: Int] = [:]
        indexesByID.reserveCapacity(itemCount)
        var flatIndex = 0
        for section in sections {
            for item in section.items {
                indexesByID[item.id] = flatIndex
                flatIndex += 1
            }
        }
        itemIndexByID = indexesByID
    }

    var itemCount: Int { sectionOffsets.last ?? 0 }

    var isEmpty: Bool {
        itemCount == 0
    }

    var itemIDs: some Collection<MediaBrowserItemID> {
        itemIndexByID.keys
    }

    func itemIDs(inSection sectionIndex: Int) -> [MediaBrowserItemID] {
        guard sections.indices.contains(sectionIndex) else { return [] }
        return sections[sectionIndex].items.map(\.id)
    }

    func item(section sectionIndex: Int, item itemIndex: Int) -> MediaBrowserItem? {
        guard sections.indices.contains(sectionIndex) else { return nil }
        let items = sections[sectionIndex].items
        guard items.indices.contains(itemIndex) else { return nil }
        return items[itemIndex]
    }

    func item(at index: Int) -> MediaBrowserItem? {
        guard index >= 0, index < itemCount else { return nil }
        var lowerBound = 0
        var upperBound = sections.count
        while lowerBound < upperBound {
            let middle = (lowerBound + upperBound) / 2
            if sectionOffsets[middle + 1] <= index {
                lowerBound = middle + 1
            } else {
                upperBound = middle
            }
        }
        let sectionIndex = lowerBound
        return sections[sectionIndex].items[index - sectionOffsets[sectionIndex]]
    }

    func item(id: MediaBrowserItemID) -> MediaBrowserItem? {
        guard let index = itemIndexByID[id] else { return nil }
        return item(at: index)
    }

    func index(of id: MediaBrowserItemID) -> Int? {
        itemIndexByID[id]
    }
}

@MainActor
final class MediaBrowserSession {
    private(set) var revision: UInt64 = 0
    private(set) var snapshot = MediaBrowserSnapshot.empty

    func replace(with snapshot: MediaBrowserSnapshot) {
        revision &+= 1
        self.snapshot = snapshot
    }

    func reset() {
        replace(with: .empty)
    }
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
