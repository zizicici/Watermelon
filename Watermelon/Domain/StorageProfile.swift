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
            return String(localized: "storage.error.externalFallback")
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

    func isConnectionUnavailableError(_ error: Error) -> Bool {
        switch resolvedStorageType {
        case .externalVolume:
            return RemoteStorageClientError.isLikelyExternalStorageUnavailable(error)
        case .smb:
            return SMBErrorClassifier.isConnectionUnavailable(error)
        case .webdav:
            return false
        }
    }

    func userFacingStorageErrorMessage(_ error: Error) -> String {
        if isExternalStorageUnavailableError(error) {
            return String(localized: "storage.error.externalUnavailable")
        }
        if resolvedStorageType == .smb, SMBErrorClassifier.isConnectionUnavailable(error) {
            return String(localized: "storage.error.smbUnavailable")
        }
        if resolvedStorageType == .webdav {
            if let statusCode = Self.webDAVErrorCode(from: error), statusCode == 401 {
                return String(localized: "storage.error.webdav401")
            }
            if let statusCode = Self.webDAVErrorCode(from: error), statusCode == 403 {
                return String(localized: "storage.error.webdav403")
            }
        }
        return error.localizedDescription
    }

    private static func webDAVErrorCode(from error: Error) -> Int? {
        if let storageError = error as? RemoteStorageClientError {
            switch storageError {
            case .underlying(let underlying):
                return webDAVErrorCode(from: underlying)
            default:
                return nil
            }
        }

        let nsError = error as NSError
        if nsError.domain == "WebDAVClient" {
            return nsError.code
        }
        if let underlying = nsError.userInfo[NSUnderlyingErrorKey] as? Error {
            return webDAVErrorCode(from: underlying)
        }
        return nil
    }

    func resolvedSessionPassword(from session: AppSession) -> String? {
        if storageProfile.requiresPassword {
            guard let password = session.activePassword, !password.isEmpty else { return nil }
            return password
        }
        return session.activePassword ?? ""
    }
}
