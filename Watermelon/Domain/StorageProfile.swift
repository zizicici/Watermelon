import Foundation

struct ExternalVolumeConnectionParams: Codable {
    let rootBookmarkData: Data
    let displayPath: String
}

struct StorageProfile {
    let record: ServerProfileRecord

    var storageType: StorageType {
        record.resolvedStorageType
    }

    var requiresPassword: Bool {
        storageType == .smb
    }

    var displayTitle: String {
        switch storageType {
        case .smb:
            return "\(record.username)@\(record.name)"
        case .externalVolume:
            return record.name
        }
    }

    var displaySubtitle: String {
        switch storageType {
        case .smb:
            return "SMB://\(record.host)/\(record.shareName)\(record.basePath)"
        case .externalVolume:
            if let path = record.externalVolumeParams?.displayPath, !path.isEmpty {
                return "外接存储 · \(path)"
            }
            return "外接存储"
        }
    }

    var indicatorText: String {
        switch storageType {
        case .smb:
            return "\(record.username)@\(record.shareName)\(record.basePath)"
        case .externalVolume:
            if let path = record.externalVolumeParams?.displayPath, !path.isEmpty {
                return "外接存储: \(path)"
            }
            return "外接存储: \(record.name)"
        }
    }

    var identityKey: String {
        switch storageType {
        case .smb:
            return [
                storageType.rawValue,
                record.host,
                String(record.port),
                record.shareName,
                RemotePathBuilder.normalizePath(record.basePath),
                record.username,
                record.domain ?? ""
            ].joined(separator: "|")
        case .externalVolume:
            let bookmarkTag: String
            if let bookmarkData = record.externalVolumeParams?.rootBookmarkData {
                bookmarkTag = String(bookmarkData.base64EncodedString().prefix(24))
            } else {
                bookmarkTag = "missing_bookmark"
            }
            return [
                storageType.rawValue,
                bookmarkTag,
                RemotePathBuilder.normalizePath(record.basePath)
            ].joined(separator: "|")
        }
    }
}

extension ServerProfileRecord {
    var storageProfile: StorageProfile {
        StorageProfile(record: self)
    }

    var externalVolumeParams: ExternalVolumeConnectionParams? {
        decodedConnectionParams(as: ExternalVolumeConnectionParams.self)
    }

    func decodedConnectionParams<T: Decodable>(as type: T.Type) -> T? {
        guard let data = connectionParams else { return nil }
        return try? JSONDecoder().decode(T.self, from: data)
    }

    static func encodedConnectionParams<T: Encodable>(_ params: T) throws -> Data {
        try JSONEncoder().encode(params)
    }
}
