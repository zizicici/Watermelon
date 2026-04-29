import Foundation

enum LegacyMediaKind {
    case image
    case video
}

enum LegacyTimestampSource: String {
    case exif
    case quickTime
    case mtime
    case unknown
}

struct LegacyFileCandidate: Identifiable, Hashable {
    let id = UUID()
    let url: URL
    let sanitizedStem: String          // file stem after RemotePathBuilder.sanitizeFilename + extension stripped
    let originalFilename: String       // sanitized filename WITH extension
    let lowercasedExtension: String    // e.g. "heic", "mov"
    let kind: LegacyMediaKind
    let fileSize: Int64
    let timestamp: Date?
    let timestampSource: LegacyTimestampSource

    static func == (lhs: LegacyFileCandidate, rhs: LegacyFileCandidate) -> Bool {
        lhs.id == rhs.id
    }
    func hash(into hasher: inout Hasher) {
        hasher.combine(id)
    }
}

enum LegacyBundleKind: Hashable {
    case photo
    case video
    case livePhoto
}

struct LegacyResourceComponent: Hashable {
    let role: Int
    let slot: Int
    let url: URL
    let originalFilename: String
    let fileSize: Int64
    let contentHash: Data
}

struct LegacyAssetBundle: Identifiable, Hashable {
    let id = UUID()
    let kind: LegacyBundleKind
    let creationDate: Date?
    let timestampSource: LegacyTimestampSource
    let resources: [LegacyResourceComponent]
    let assetFingerprint: Data

    var totalFileSize: Int64 {
        resources.reduce(0) { $0 + $1.fileSize }
    }

    static func == (lhs: LegacyAssetBundle, rhs: LegacyAssetBundle) -> Bool {
        lhs.id == rhs.id
    }
    func hash(into hasher: inout Hasher) {
        hasher.combine(id)
    }
}

struct LegacyMonthPlan: Identifiable {
    let id: LibraryMonthKey
    let month: LibraryMonthKey
    let bundles: [LegacyAssetBundle]

    var totalAssetCount: Int { bundles.count }
    var totalResourceCount: Int { bundles.reduce(0) { $0 + $1.resources.count } }
    var totalFileSize: Int64 { bundles.reduce(0) { $0 + $1.totalFileSize } }
}

struct LegacyScanReport {
    let plans: [LegacyMonthPlan]
    let unscheduledCandidates: [LegacyFileCandidate]   // skipped because timestamp couldn't be determined
    let warnings: [String]
}
