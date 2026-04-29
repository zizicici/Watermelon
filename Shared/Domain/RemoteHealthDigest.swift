import Foundation

struct IncompleteAssetEntry: Sendable, Identifiable {
    enum Reason: Sendable {
        case manifestOrphan
    }

    let id: Data
    let month: LibraryMonthKey
    let creationDate: Date?
    let representativeFileName: String?
    let missingResourceCount: Int
    let totalResourceCount: Int
    let missingResourceHashes: [Data]
    let reason: Reason
}

struct RemoteHealthDigest: Sendable {
    let totalAssets: Int
    let totalResources: Int
    let totalSizeBytes: Int64
    let incompleteAssets: [IncompleteAssetEntry]
    let lastIndexSyncedAt: Date?
}
