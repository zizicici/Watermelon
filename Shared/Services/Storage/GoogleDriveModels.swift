import Foundation

nonisolated enum GoogleDriveConstants {
    static let apiBaseURL = URL(string: "https://www.googleapis.com/drive/v3")!
    static let uploadBaseURL = URL(string: "https://www.googleapis.com/upload/drive/v3")!
    static let folderMIMEType = "application/vnd.google-apps.folder"
    static let fileFields = "id,name,mimeType,size,md5Checksum,createdTime,modifiedTime,parents,trashed,appProperties"
    static let rootRole = "watermelonRoot"
    static let rootSchemaVersion = "1"
    static let lockRecordRole = "watermelonLockRecord"
    static let lockReleaseRole = "watermelonLockRelease"
    static let rootRoleKey = "wmRole"
    static let rootSchemaKey = "wmSchema"
    static let lockRootSlotKey = "wmLockRootSlot"
    static let lockSequenceKey = "wmLockSequence"
    static let lockNextSlotKey = "wmLockNextSlot"
    static let lockReleaseMarkerKey = "wmLockReleaseMarker"
    static let lockRecordIDKey = "wmLockRecordID"
    static let lockRepoRootIDKey = "wmRepoRootID"
}

nonisolated enum GoogleDriveSpace: String, Sendable {
    case drive
    case appDataFolder
}

nonisolated struct GoogleDriveFile: Decodable, Sendable {
    let id: String
    let name: String?
    let mimeType: String?
    let size: String?
    let md5Checksum: String?
    let createdTime: Date?
    let modifiedTime: Date?
    let parents: [String]?
    let trashed: Bool?
    let appProperties: [String: String]?

    var watermelonLockRootSlotID: String? {
        guard mimeType == GoogleDriveConstants.folderMIMEType,
              trashed != true,
              appProperties?[GoogleDriveConstants.rootRoleKey] == GoogleDriveConstants.rootRole,
              appProperties?[GoogleDriveConstants.rootSchemaKey] == GoogleDriveConstants.rootSchemaVersion,
              let slotID = appProperties?[GoogleDriveConstants.lockRootSlotKey],
              !slotID.isEmpty else { return nil }
        return slotID
    }

    func hasSameContentsAndBinding(as other: GoogleDriveFile) -> Bool {
        guard let ownMD5 = md5Checksum, let otherMD5 = other.md5Checksum else { return false }
        return id == other.id
            && name == other.name
            && parents == other.parents
            && size == other.size
            && ownMD5.caseInsensitiveCompare(otherMD5) == .orderedSame
            && mimeType == other.mimeType
            && trashed != true
    }
}

nonisolated func googleDriveFolderPrecedes(_ lhs: GoogleDriveFile, _ rhs: GoogleDriveFile) -> Bool {
    switch (lhs.createdTime, rhs.createdTime) {
    case let (left?, right?) where left != right:
        return left < right
    case (.some, nil):
        return true
    case (nil, .some):
        return false
    default:
        return lhs.id < rhs.id
    }
}

nonisolated struct GoogleDriveFileList: Decodable, Sendable {
    let files: [GoogleDriveFile]
    let nextPageToken: String?
}

nonisolated struct GoogleDriveGeneratedIDs: Decodable, Sendable {
    let ids: [String]
}

nonisolated struct GoogleDriveAbout: Decodable, Sendable {
    struct StorageQuota: Decodable, Sendable {
        let limit: String?
        let usage: String?
    }

    let storageQuota: StorageQuota?
}

nonisolated struct GoogleDriveLockRecord: Codable, Sendable, Equatable {
    static let currentSchemaVersion = 1

    let schemaVersion: Int
    let sequence: UInt64
    let virtualPath: String
    let lockBody: Data
    let nextSlotID: String
    let releaseMarkerID: String

    init(
        sequence: UInt64,
        virtualPath: String,
        lockBody: Data,
        nextSlotID: String,
        releaseMarkerID: String
    ) {
        schemaVersion = Self.currentSchemaVersion
        self.sequence = sequence
        self.virtualPath = virtualPath
        self.lockBody = lockBody
        self.nextSlotID = nextSlotID
        self.releaseMarkerID = releaseMarkerID
    }
}

nonisolated struct GoogleDriveLockRelease: Codable, Sendable, Equatable {
    static let currentSchemaVersion = 1

    let schemaVersion: Int
    let recordID: String
    let sequence: UInt64

    init(recordID: String, sequence: UInt64) {
        schemaVersion = Self.currentSchemaVersion
        self.recordID = recordID
        self.sequence = sequence
    }
}

nonisolated private struct GoogleDriveLockIdentity: Decodable {
    let sessionToken: String
    let lockToken: String
}

nonisolated enum GoogleDriveJSON {
    static func decode<T: Decodable>(_ type: T.Type, from data: Data) throws -> T {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try decoder.decode(type, from: data)
    }

    static func encode<T: Encodable>(_ value: T) throws -> Data {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        return try encoder.encode(value)
    }

    static func decodeResponse<T: Decodable>(_ type: T.Type, from data: Data) throws -> T {
        do {
            return try decode(type, from: data)
        } catch {
            throw GoogleDriveAuthenticationError.invalidResponse
        }
    }
}

nonisolated func googleDriveLockBodiesHaveSameIdentity(_ first: Data, _ second: Data) -> Bool {
    guard let firstIdentity = try? GoogleDriveJSON.decode(GoogleDriveLockIdentity.self, from: first),
          let secondIdentity = try? GoogleDriveJSON.decode(GoogleDriveLockIdentity.self, from: second) else {
        return false
    }
    return firstIdentity.sessionToken == secondIdentity.sessionToken
        && firstIdentity.lockToken == secondIdentity.lockToken
}
