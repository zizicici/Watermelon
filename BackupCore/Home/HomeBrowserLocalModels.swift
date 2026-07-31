import Foundation

struct HomeBrowserLocalSeed: Sendable {
    let localIDByFingerprint: [Data: String]
    let assets: [HomeBrowserLocalAsset]
    let monthGroupingTimeZone: MonthGroupingTimeZonePreference
}

struct HomeBrowserLocalAsset: Sendable {
    let localIdentifier: String
    let month: LibraryMonthKey
    let kind: AlbumMediaKind
    let creationDateMs: Int64
    let fingerprint: Data?
}
