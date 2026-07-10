import Foundation
import GRDB

enum StorageType: String, Codable {
    case smb
    case webdav
    case externalVolume
    case s3
    case sftp
}

enum RemoteHostIdentity {
    static func canonical(_ host: String) -> String {
        host.lowercased()
    }

    static func canonicalSMB(_ host: String) -> String {
        let canonicalHost = canonical(host)
        guard canonicalHost.hasPrefix("smb://") else { return canonicalHost }
        return String(canonicalHost.dropFirst("smb://".count))
    }
}

struct ServerProfileRecord: Codable, FetchableRecord, MutablePersistableRecord, Identifiable, Sendable {
    static let databaseTableName = "server_profiles"

    var id: Int64?
    var name: String
    var storageType: String
    var connectionParams: Data?
    var sortOrder: Int
    var host: String
    var port: Int
    var shareName: String
    var basePath: String
    var username: String
    var domain: String?
    var credentialRef: String
    var backgroundBackupEnabled: Bool = false
    var backgroundBackupMinIntervalMinutes: Int = 1440
    var backgroundBackupRequiresWiFi: Bool = true
    var generateRemoteThumbnails: Bool = false
    var createdAt: Date
    var updatedAt: Date
    var writerID: String? = nil

    var resolvedStorageType: StorageType {
        StorageType(rawValue: storageType) ?? .smb
    }

    mutating func didInsert(_ inserted: InsertionSuccess) {
        id = inserted.rowID
    }

    func hasSameRemoteDestination(as other: ServerProfileRecord) -> Bool {
        guard resolvedStorageType == other.resolvedStorageType else { return false }
        switch resolvedStorageType {
        case .smb:
            return RemoteHostIdentity.canonicalSMB(host) == RemoteHostIdentity.canonicalSMB(other.host) &&
                port == other.port &&
                shareName == other.shareName &&
                RemotePathBuilder.normalizePath(basePath) == RemotePathBuilder.normalizePath(other.basePath) &&
                username == other.username &&
                (domain ?? "") == (other.domain ?? "")
        case .webdav:
            return webDAVParams?.scheme == other.webDAVParams?.scheme &&
                RemoteHostIdentity.canonical(host) == RemoteHostIdentity.canonical(other.host) &&
                port == other.port &&
                RemotePathBuilder.normalizePath(shareName) == RemotePathBuilder.normalizePath(other.shareName) &&
                RemotePathBuilder.normalizePath(basePath) == RemotePathBuilder.normalizePath(other.basePath) &&
                username == other.username
        case .s3:
            return s3Params?.scheme == other.s3Params?.scheme &&
                s3Params?.region == other.s3Params?.region &&
                s3Params?.usePathStyle == other.s3Params?.usePathStyle &&
                RemoteHostIdentity.canonical(host) == RemoteHostIdentity.canonical(other.host) &&
                port == other.port &&
                shareName == other.shareName &&
                RemotePathBuilder.normalizePath(basePath) == RemotePathBuilder.normalizePath(other.basePath) &&
                username == other.username
        case .sftp:
            return RemoteHostIdentity.canonical(host) == RemoteHostIdentity.canonical(other.host) &&
                port == other.port &&
                RemotePathBuilder.normalizePath(basePath) == RemotePathBuilder.normalizePath(other.basePath) &&
                username == other.username &&
                sftpParams?.hostKeyFingerprintSHA256 == other.sftpParams?.hostKeyFingerprintSHA256
        case .externalVolume:
            return externalVolumeParams?.displayPath == other.externalVolumeParams?.displayPath
        }
    }
}

struct SyncStateRecord: Codable, FetchableRecord, MutablePersistableRecord {
    static let databaseTableName = "sync_state"

    var stateKey: String
    var stateValue: String
    var updatedAt: Date
}

struct LocalAssetRecord: Codable, FetchableRecord, MutablePersistableRecord {
    static let databaseTableName = "local_assets"

    var assetLocalIdentifier: String
    var assetFingerprint: Data?
    var resourceCount: Int
    var totalFileSizeBytes: Int64
    var modificationDateMs: Int64?
    var updatedAt: Date
}

struct LocalAssetResourceRecord: Codable, FetchableRecord, MutablePersistableRecord {
    static let databaseTableName = "local_asset_resources"

    var assetLocalIdentifier: String
    var role: Int
    var slot: Int
    var contentHash: Data
    var fileSize: Int64
}
