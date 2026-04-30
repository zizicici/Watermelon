import Foundation

enum LegacyMediaKind {
    case image
    case video
}

enum LegacyMediaExtensions {
    static let imageExtensions: Set<String> = [
        "heic", "heif", "jpg", "jpeg", "png", "gif",
        "tiff", "tif", "webp", "dng", "raw",
        "cr2", "cr3", "nef", "arw", "rw2", "orf", "raf", "srw"
    ]
    static let videoExtensions: Set<String> = [
        "mp4", "mov", "m4v", "hevc"
    ]
    /// Extensions eligible for perceptual-hash dedup. Other formats (RAW, TIFF, PNG, …) are
    /// skipped — same-shot duplicates across those formats are rare in practice.
    static let perceptualHashExtensions: Set<String> = [
        "heic", "heif", "jpg", "jpeg"
    ]

    static func kind(forExtension lowercasedExt: String) -> LegacyMediaKind? {
        if imageExtensions.contains(lowercasedExt) { return .image }
        if videoExtensions.contains(lowercasedExt) { return .video }
        return nil
    }
}

enum LegacyTimestampSource: String {
    case exif
    case quickTime
    case mtime
    case unknown
}

struct LegacyFileCandidate: Identifiable, Hashable {
    let id = UUID()
    let remotePath: String                // path on the connected storage client
    let parentDirectory: String           // remotePath's parent, normalized; used as a grouping key
    let sanitizedStem: String
    let originalFilename: String
    let lowercasedExtension: String
    let kind: LegacyMediaKind
    let fileSize: Int64
    let timestamp: Date?
    let timestampSource: LegacyTimestampSource
    let contentHash: Data?                // nil when scheduling will skip this file (missing timestamp etc)
    let dhash: Data?                      // 8-byte perceptual hash; nil for non-image kinds or compute failures

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

enum LegacyBundleSource: String {
    case scanner
    case manifest
}

/// Pre-computed at scan time so the result UI can show what will happen on commit.
/// Executor still runs its own checks at commit time and is authoritative.
enum LegacyBundleAction: Equatable, Hashable {
    case insertNew
    case skipExactMatch
    case skipEnclosed
    case skipPerceptualDuplicate
    case replacesSubsets(count: Int)
}

struct LegacyResourceComponent: Hashable {
    let role: Int
    let slot: Int
    let remotePath: String
    let originalFilename: String
    let fileSize: Int64
    let contentHash: Data
    let dhash: Data?
}

struct LegacyAssetBundle: Identifiable, Hashable {
    let id = UUID()
    let kind: LegacyBundleKind
    let source: LegacyBundleSource
    let creationDate: Date?
    let timestampSource: LegacyTimestampSource
    let resources: [LegacyResourceComponent]
    let assetFingerprint: Data
    /// Authoritative month bucket. When nil, the bundle is bucketed by creationDate.
    /// Manifest-driven bundles set this to the source manifest's (year, month) so cross-month
    /// creationDates don't get rerouted on import.
    let preferredMonth: LibraryMonthKey?
    var action: LegacyBundleAction = .insertNew

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
    let unscheduledCandidates: [LegacyFileCandidate]
    let warnings: [String]
}
