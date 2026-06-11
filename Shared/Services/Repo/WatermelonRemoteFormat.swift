import Foundation

nonisolated enum WatermelonRemoteFormat {
    static let markerDirectoryName = ".watermelon"
    static let versionFileName = "version.json"
}

nonisolated struct WatermelonRemoteVersionManifest: Codable, Equatable {
    let formatVersion: Int?
    let layout: String?
    let minAppVersion: String?
    let createdAt: String?
    let createdBy: String?

    enum CodingKeys: String, CodingKey {
        case formatVersion = "format_version"
        case layout
        case minAppVersion = "min_app_version"
        case createdAt = "created_at"
        case createdBy = "created_by"
    }
}
