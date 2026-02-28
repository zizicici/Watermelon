import Foundation

struct ExternalVolumeConnectionParams: Codable {
    let rootBookmarkData: Data
    let displayPath: String
}

struct WebDAVConnectionParams: Codable {
    let endpointURLString: String
}

struct StorageProfile {
    let record: ServerProfileRecord

    var storageType: StorageType {
        record.resolvedStorageType
    }

    var requiresPassword: Bool {
        switch storageType {
        case .smb, .webdav:
            return true
        case .externalVolume:
            return false
        }
    }

    var displayTitle: String {
        switch storageType {
        case .smb:
            return "\(record.username)@\(record.name)"
        case .webdav:
            return "\(record.username)@\(record.name)"
        case .externalVolume:
            return record.name
        }
    }

    var displaySubtitle: String {
        switch storageType {
        case .smb:
            return "SMB://\(record.host)/\(record.shareName)\(record.basePath)"
        case .webdav:
            let endpoint = record.webDAVParams?.endpointURLString ?? "WebDAV"
            if record.basePath == "/" {
                return endpoint
            }
            return "\(endpoint)\(record.basePath)"
        case .externalVolume:
            if let path = record.externalVolumeParams?.displayPath, !path.isEmpty {
                return Self.relativeExternalPath(from: path)
            }
            return "外接存储"
        }
    }

    var indicatorText: String {
        switch storageType {
        case .smb:
            return "\(record.username)@\(record.shareName)\(record.basePath)"
        case .webdav:
            return "\(record.username)@\(record.name)"
        case .externalVolume:
            return record.name
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
        case .webdav:
            return [
                storageType.rawValue,
                record.webDAVParams?.endpointURLString ?? "missing_endpoint",
                RemotePathBuilder.normalizePath(record.basePath),
                record.username
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

    private static func relativeExternalPath(from absolutePath: String) -> String {
        let normalized = absolutePath.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
        guard !normalized.isEmpty else { return "/" }

        let components = normalized.split(separator: "/").map(String.init)
        if let uuidIndex = components.firstIndex(where: { UUID(uuidString: $0) != nil }),
           uuidIndex + 1 < components.count {
            return components[(uuidIndex + 1)...].joined(separator: "/")
        }

        if components.count >= 2 {
            return components.suffix(2).joined(separator: "/")
        }

        return components.last ?? "/"
    }
}

extension ServerProfileRecord {
    var storageProfile: StorageProfile {
        StorageProfile(record: self)
    }

    var externalVolumeParams: ExternalVolumeConnectionParams? {
        decodedConnectionParams(as: ExternalVolumeConnectionParams.self)
    }

    var webDAVParams: WebDAVConnectionParams? {
        decodedConnectionParams(as: WebDAVConnectionParams.self)
    }

    func decodedConnectionParams<T: Decodable>(as type: T.Type) -> T? {
        guard let data = connectionParams else { return nil }
        return try? JSONDecoder().decode(T.self, from: data)
    }

    static func encodedConnectionParams<T: Encodable>(_ params: T) throws -> Data {
        try JSONEncoder().encode(params)
    }

    func isExternalStorageUnavailableError(_ error: Error) -> Bool {
        resolvedStorageType == .externalVolume && RemoteStorageClientError.isLikelyExternalStorageUnavailable(error)
    }

    func userFacingStorageErrorMessage(_ error: Error) -> String {
        if isExternalStorageUnavailableError(error) {
            return "外接存储不可用，可能已拔出。请重新连接硬盘后再试。"
        }
        return error.localizedDescription
    }
}
