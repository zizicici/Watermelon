import Foundation

nonisolated enum WatermelonRemoteFormat {
    static let markerDirectoryName = ".watermelon"
    static let versionFileName = "version.json"
}

nonisolated struct WatermelonRemoteVersionManifest: Codable, Equatable {
    let formatVersion: Int?
    let minAppVersion: String?
    let createdAt: String?
    let createdBy: String?
    let repoID: String?
    let encryption: Encryption?

    init(
        formatVersion: Int?,
        minAppVersion: String?,
        createdAt: String?,
        createdBy: String?,
        repoID: String? = nil,
        encryption: Encryption? = nil
    ) {
        self.formatVersion = formatVersion
        self.minAppVersion = minAppVersion
        self.createdAt = createdAt
        self.createdBy = createdBy
        self.repoID = repoID
        self.encryption = encryption
    }

    struct Encryption: Codable, Equatable, Sendable {
        let mode: String?
        let contentCodec: String?
        let activeKeyID: String?
        let keys: [Key]?
        let manifestEncrypted: Bool?
        let resourceMetadataEncrypted: Bool?

        init(
            mode: String?,
            contentCodec: String?,
            activeKeyID: String?,
            keys: [Key]?,
            manifestEncrypted: Bool?,
            resourceMetadataEncrypted: Bool?
        ) {
            self.mode = mode
            self.contentCodec = contentCodec
            self.activeKeyID = activeKeyID
            self.keys = keys
            self.manifestEncrypted = manifestEncrypted
            self.resourceMetadataEncrypted = resourceMetadataEncrypted
        }

        enum CodingKeys: String, CodingKey {
            case mode
            case contentCodec = "content_codec"
            case activeKeyID = "active_key_id"
            case keys
            case manifestEncrypted = "manifest_encrypted"
            case resourceMetadataEncrypted = "resource_metadata_encrypted"
        }
    }

    struct Key: Codable, Equatable, Sendable {
        let kid: String?
        let alg: String?
        let status: String?
        let createdAt: String?
        let keyCheck: String?

        init(
            kid: String?,
            alg: String?,
            status: String?,
            createdAt: String?,
            keyCheck: String?
        ) {
            self.kid = kid
            self.alg = alg
            self.status = status
            self.createdAt = createdAt
            self.keyCheck = keyCheck
        }

        enum CodingKeys: String, CodingKey {
            case kid
            case alg
            case status
            case createdAt = "created_at"
            case keyCheck = "key_check"
        }
    }

    enum CodingKeys: String, CodingKey {
        case formatVersion = "format_version"
        case minAppVersion = "min_app_version"
        case createdAt = "created_at"
        case createdBy = "created_by"
        case repoID = "repo_id"
        case encryption
    }
}
