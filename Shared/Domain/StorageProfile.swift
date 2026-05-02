import Foundation

struct ExternalVolumeConnectionParams: Codable {
    let rootBookmarkData: Data
    let displayPath: String
}

struct S3ConnectionParams: Codable {
    let scheme: String
    let region: String
    let usePathStyle: Bool

    init(scheme: String, region: String, usePathStyle: Bool) {
        self.scheme = scheme.lowercased()
        self.region = region
        self.usePathStyle = usePathStyle
    }
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
        case .smb, .webdav, .s3:
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
        case .s3:
            return "\(record.username)@\(record.name)"
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
        case .s3:
            return record.s3DisplayURLString ?? "S3"
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
        case .s3:
            return "\(record.username)@\(record.shareName)\(record.basePath)"
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

    var s3Params: S3ConnectionParams? {
        decodedConnectionParams(as: S3ConnectionParams.self)
    }

    var s3DisplayURLString: String? {
        guard resolvedStorageType == .s3, let params = s3Params, !host.isEmpty, !shareName.isEmpty else { return nil }
        let scheme = params.scheme.isEmpty ? "https" : params.scheme
        let defaultPort = scheme == "https" ? 443 : 80
        let portSuffix = (port == 0 || port == defaultPort) ? "" : ":\(port)"
        let trimmedBase = basePath == "/" ? "" : basePath
        if params.usePathStyle {
            return "\(scheme)://\(host)\(portSuffix)/\(shareName)\(trimmedBase)"
        }
        return "\(scheme)://\(shareName).\(host)\(portSuffix)\(trimmedBase)"
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
        case .s3:
            return S3ErrorClassifier.isConnectionUnavailable(error)
        }
    }

    func userFacingStorageErrorMessage(_ error: Error) -> String {
        if let compat = error as? BackupCompatibilityError {
            return compat.errorDescription ?? error.localizedDescription
        }
        if isExternalStorageUnavailableError(error) {
            return String(localized: "storage.error.externalUnavailable")
        }
        if resolvedStorageType == .smb, SMBErrorClassifier.isConnectionUnavailable(error) {
            return String(localized: "storage.error.smbUnavailable")
        }
        if resolvedStorageType == .webdav {
            return WebDAVErrorClassifier.describe(error)
        }
        if resolvedStorageType == .s3 {
            return S3ErrorClassifier.describe(error)
        }
        return error.localizedDescription
    }

    func resolvedSessionPassword(from session: AppSession) -> String? {
        if storageProfile.requiresPassword {
            guard let password = session.activePassword, !password.isEmpty else { return nil }
            return password
        }
        return session.activePassword ?? ""
    }
}
