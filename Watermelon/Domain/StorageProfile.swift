import Foundation

struct ExternalVolumeConnectionParams: Codable {
    let rootBookmarkData: Data
    let displayPath: String
}

struct WebDAVConnectionParams: Codable {
    let scheme: String

    init(scheme: String) {
        self.scheme = scheme.lowercased()
    }

    private enum CodingKeys: String, CodingKey {
        case scheme
        case endpointURLString
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        if let scheme = try container.decodeIfPresent(String.self, forKey: .scheme),
           !scheme.isEmpty {
            self.scheme = scheme.lowercased()
            return
        }
        if let legacy = try container.decodeIfPresent(String.self, forKey: .endpointURLString),
           let urlScheme = URL(string: legacy)?.scheme?.lowercased(),
           !urlScheme.isEmpty {
            self.scheme = urlScheme
            return
        }
        throw DecodingError.dataCorruptedError(
            forKey: .scheme,
            in: container,
            debugDescription: "Missing WebDAV scheme and cannot derive it from legacy endpointURLString"
        )
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(scheme, forKey: .scheme)
    }
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
            guard let endpoint = record.webDAVEndpointURLString else { return "WebDAV" }
            if record.basePath == "/" {
                return endpoint
            }
            let trimmed = endpoint.hasSuffix("/") ? String(endpoint.dropLast()) : endpoint
            return "\(trimmed)\(record.basePath)"
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

    /// Canonical WebDAV endpoint built from the structured fields.
    /// Returns nil when the profile lacks the minimum shape (scheme + host).
    var webDAVEndpointURL: URL? {
        guard resolvedStorageType == .webdav,
              let scheme = webDAVParams?.scheme else { return nil }
        return Self.buildWebDAVEndpointURL(
            scheme: scheme,
            host: host,
            port: port,
            mountPath: shareName
        )
    }

    var webDAVEndpointURLString: String? {
        webDAVEndpointURL?.absoluteString
    }

    static func buildWebDAVEndpointURL(
        scheme: String,
        host: String,
        port: Int,
        mountPath: String
    ) -> URL? {
        guard !host.isEmpty else { return nil }

        var components = URLComponents()
        components.scheme = scheme
        components.host = host

        let defaultPort = scheme == "https" ? 443 : 80
        if port != 0, port != defaultPort {
            components.port = port
        }

        components.path = RemotePathBuilder.normalizePath(mountPath)
        return components.url
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

    static func webDAVErrorCode(from error: Error) -> Int? {
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
