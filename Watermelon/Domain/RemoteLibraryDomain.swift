import Foundation

struct RemoteManifestAsset: Hashable, Identifiable {
    let year: Int
    let month: Int
    let assetFingerprint: Data
    let creationDateNs: Int64?
    let backedUpAtNs: Int64
    let resourceCount: Int

    var id: String {
        monthKey + "/" + assetFingerprintHex
    }

    var monthKey: String {
        String(format: "%04d-%02d", year, month)
    }

    var creationDate: Date {
        if let creationDateNs {
            return Date(timeIntervalSince1970: Double(creationDateNs) / 1_000_000_000)
        }
        return Date(timeIntervalSince1970: Double(backedUpAtNs) / 1_000_000_000)
    }

    var assetFingerprintHex: String {
        assetFingerprint.map { String(format: "%02x", $0) }.joined()
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
        monthKey + "/" + assetFingerprint.map { String(format: "%02x", $0) }.joined()
    }
}

struct RemoteManifestResource: Hashable, Identifiable {
    let year: Int
    let month: Int
    let fileName: String
    let contentHash: Data
    let fileSize: Int64
    let resourceType: Int
    let creationDateNs: Int64?
    let backedUpAtNs: Int64

    var id: String {
        monthKey + "/" + fileName
    }

    var monthKey: String {
        String(format: "%04d-%02d", year, month)
    }

    var remoteRelativePath: String {
        String(format: "%04d/%02d/%@", year, month, fileName)
    }

    var creationDate: Date {
        if let creationDateNs {
            return Date(timeIntervalSince1970: Double(creationDateNs) / 1_000_000_000)
        }
        return Date(timeIntervalSince1970: Double(backedUpAtNs) / 1_000_000_000)
    }

    var contentHashHex: String {
        contentHash.map { String(format: "%02x", $0) }.joined()
    }
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

    var assetFingerprintSet: Set<Data> {
        Set(assets.map(\.assetFingerprint))
    }
}

struct LibraryMonthKey: Hashable, Comparable {
    let year: Int
    let month: Int

    var text: String {
        String(format: "%04d-%02d", year, month)
    }

    static func < (lhs: LibraryMonthKey, rhs: LibraryMonthKey) -> Bool {
        if lhs.year == rhs.year {
            return lhs.month < rhs.month
        }
        return lhs.year < rhs.year
    }
}

struct RemoteLibraryMonthDelta {
    let month: LibraryMonthKey
    let resources: [RemoteManifestResource]
    let assets: [RemoteManifestAsset]
    let assetResourceLinks: [RemoteAssetResourceLink]
}

struct RemoteLibrarySnapshotState {
    let revision: UInt64
    let isFullSnapshot: Bool
    let monthDeltas: [RemoteLibraryMonthDelta]
}

enum ResourceTypeCode {
    static let unknown = 0
    static let photo = 1
    static let video = 2
    static let audio = 3
    static let alternatePhoto = 4
    static let fullSizePhoto = 5
    static let fullSizeVideo = 6
    static let pairedVideo = 7
    static let adjustmentData = 8
    static let adjustmentBasePhoto = 9
    static let photoProxy = 10

    static func isPhotoLike(_ code: Int) -> Bool {
        code == photo || code == alternatePhoto || code == fullSizePhoto || code == adjustmentBasePhoto || code == photoProxy
    }

    static func isVideoLike(_ code: Int) -> Bool {
        code == video || code == fullSizeVideo || code == pairedVideo
    }
}
