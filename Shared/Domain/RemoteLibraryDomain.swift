import Foundation
#if os(iOS)
import Photos
#endif

struct RemoteManifestAsset: Hashable, Identifiable {
    let year: Int
    let month: Int
    let assetFingerprint: Data
    let creationDateMs: Int64?
    let backedUpAtMs: Int64
    let resourceCount: Int
    let totalFileSizeBytes: Int64

    var id: String {
        monthKey + "/" + assetFingerprintHex
    }

    var monthKey: String {
        String(format: "%04d-%02d", year, month)
    }

    var creationDate: Date {
        if let creationDateMs {
            return Date(millisecondsSinceEpoch: creationDateMs)
        }
        return Date(millisecondsSinceEpoch: backedUpAtMs)
    }

    var assetFingerprintHex: String {
        assetFingerprint.hexString
    }
}

struct RemoteAssetResourceLink: Hashable {
    let year: Int
    let month: Int
    let assetFingerprint: Data
    let resourceHash: Data
    let role: Int
    let slot: Int

    var monthKey: String {
        String(format: "%04d-%02d", year, month)
    }

    var assetID: String {
        monthKey + "/" + assetFingerprint.hexString
    }
}

struct RemoteManifestResource: Hashable, Identifiable {
    let year: Int
    let month: Int
    let fileName: String
    let contentHash: Data
    let fileSize: Int64
    let resourceType: Int
    let creationDateMs: Int64?
    let backedUpAtMs: Int64

    var id: String {
        monthKey + "/" + fileName
    }

    var monthKey: String {
        String(format: "%04d-%02d", year, month)
    }

    var remoteRelativePath: String {
        String(format: "%04d/%02d/%@", year, month, fileName)
    }

    var contentHashHex: String {
        contentHash.hexString
    }
}

struct RemoteAssetResourceInstance: Hashable, Identifiable, Sendable {
    let role: Int
    let slot: Int
    let resourceHash: Data
    let fileName: String
    let fileSize: Int64
    let remoteRelativePath: String
    let creationDateMs: Int64?

    var id: String {
        "\(role)|\(slot)|\(resourceHash.hexString)"
    }

    #if os(iOS)
    var resourceType: PHAssetResourceType? {
        guard role > 0 else { return nil }
        return PHAssetResourceType(rawValue: role)
    }
    #endif

    var contentHashHex: String {
        resourceHash.hexString
    }
}

/// Cheap summary of a remote manifest sync. Does not materialize the per-asset
/// resource/link arrays, so it's safe to hand to callers that only need totals (log lines,
/// gating decisions). When a caller actually needs the flat arrays, ask the service for a
/// full `RemoteLibrarySnapshot` explicitly.
struct RemoteIndexSyncDigest: Sendable {
    let resourceCount: Int
    let assetCount: Int
    let linkCount: Int

    var totalEntryCount: Int { resourceCount + assetCount + linkCount }
}

struct RemoteLibrarySnapshot {
    let resources: [RemoteManifestResource]
    let assets: [RemoteManifestAsset]
    let assetResourceLinks: [RemoteAssetResourceLink]

    init(
        resources: [RemoteManifestResource],
        assets: [RemoteManifestAsset],
        assetResourceLinks: [RemoteAssetResourceLink] = []
    ) {
        self.resources = resources
        self.assets = assets
        self.assetResourceLinks = assetResourceLinks
    }

    var totalCount: Int {
        assets.count
    }

    var totalResourceCount: Int {
        resources.count
    }
}

struct LibraryMonthKey: Hashable, Comparable, Sendable {
    let year: Int
    let month: Int

    var text: String {
        String(format: "%04d-%02d", year, month)
    }

    var displayText: String {
        let components = DateComponents(year: year, month: month)
        guard let date = Calendar.current.date(from: components) else {
            return text
        }
        return Self.displayTextFormatter.string(from: date)
    }

    private static let displayTextFormatter: DateFormatter = {
        let f = DateFormatter()
        f.setLocalizedDateFormatFromTemplate("yyyyMMM")
        return f
    }()

    static func < (lhs: LibraryMonthKey, rhs: LibraryMonthKey) -> Bool {
        if lhs.year == rhs.year {
            return lhs.month < rhs.month
        }
        return lhs.year < rhs.year
    }

    private static let monthCalendar = Calendar(identifier: .gregorian)

    static func from(date: Date?) -> LibraryMonthKey {
        let date = date ?? Date(timeIntervalSince1970: 0)
        let comps = monthCalendar.dateComponents([.year, .month], from: date)
        return LibraryMonthKey(year: comps.year ?? 1970, month: comps.month ?? 1)
    }
}

struct RemoteLibraryMonthDelta {
    let month: LibraryMonthKey
    let resources: [RemoteManifestResource]
    let assets: [RemoteManifestAsset]
    let assetResourceLinks: [RemoteAssetResourceLink]
}

struct RemoteMonthManifestDigest: Hashable {
    let month: LibraryMonthKey
    let manifestSize: Int64
    let manifestModifiedAtMs: Int64?
}

struct RemoteLibrarySnapshotState {
    let revision: UInt64
    let isFullSnapshot: Bool
    let monthDeltas: [RemoteLibraryMonthDelta]
}

struct RemoteSyncProgress: Hashable, Sendable {
    let current: Int
    let total: Int
}

enum ResourceTypeCode {
    static let photo = 1              // PHAssetResourceType.photo
    static let video = 2              // .video
    static let audio = 3              // .audio
    static let alternatePhoto = 4     // .alternatePhoto
    static let fullSizePhoto = 5      // .fullSizePhoto
    static let fullSizeVideo = 6      // .fullSizeVideo
    static let adjustmentData = 7     // .adjustmentData
    static let adjustmentBasePhoto = 8 // .adjustmentBasePhoto
    static let pairedVideo = 9        // .pairedVideo
    static let fullSizePairedVideo = 10 // .fullSizePairedVideo
    static let adjustmentBasePairedVideo = 11 // .adjustmentBasePairedVideo
    static let adjustmentBaseVideo = 12 // .adjustmentBaseVideo
    static let photoProxy = 19        // .photoProxy

    static func isPhotoLike(_ code: Int) -> Bool {
        code == photo || code == alternatePhoto || code == fullSizePhoto || code == adjustmentBasePhoto || code == photoProxy
    }

    static func isPairedVideo(_ code: Int) -> Bool {
        code == pairedVideo || code == fullSizePairedVideo || code == adjustmentBasePairedVideo
    }

    static func isVideoLike(_ code: Int) -> Bool {
        code == video || code == fullSizeVideo || code == pairedVideo || code == fullSizePairedVideo || code == adjustmentBasePairedVideo || code == adjustmentBaseVideo
    }
}
