import Foundation

enum HTTPTransportScheme: String, Sendable {
    case http
    case https

    var defaultPort: Int {
        switch self {
        case .http: return 80
        case .https: return 443
        }
    }

    static func parse(_ rawValue: String) -> HTTPTransportScheme? {
        HTTPTransportScheme(rawValue: rawValue.trimmingCharacters(in: .whitespacesAndNewlines).lowercased())
    }

    static func parseS3Compatible(_ rawValue: String) -> HTTPTransportScheme? {
        let trimmed = rawValue.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? .https : parse(trimmed)
    }
}

struct CanonicalHost: Equatable, Sendable {
    let socketHost: String
    let urlAuthority: String
    let isIPLiteral: Bool

    init?(_ rawValue: String, strippingSMBScheme: Bool = false) {
        guard let representation = RemoteHostEndpoint.representation(
            rawValue,
            strippingSMBScheme: strippingSMBScheme
        ) else { return nil }
        socketHost = representation.socketHost
        urlAuthority = representation.urlAuthority
        isIPLiteral = representation.isIPLiteral
    }
}

struct CanonicalPort: Equatable, Sendable {
    let value: Int

    init?(rawValue: Int, defaultValue: Int) {
        let effective = rawValue == 0 ? defaultValue : rawValue
        guard (1 ... 65535).contains(effective) else { return nil }
        value = effective
    }
}

struct CanonicalSMBConnection: Equatable, Sendable {
    let host: CanonicalHost
    let port: CanonicalPort
    let shareName: String
    let basePath: String
    let username: String
    let domain: String?
    let publishedV2IdentityComponents: [String]

    init(
        host: String,
        port: Int,
        shareName: String,
        basePath: String,
        username: String,
        domain: String?
    ) throws {
        guard let canonicalHost = CanonicalHost(host, strippingSMBScheme: true),
              let canonicalPort = CanonicalPort(rawValue: port, defaultValue: SMBEndpoint.defaultPort) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        self.host = canonicalHost
        self.port = canonicalPort
        self.shareName = try SMBPathCanonicalizer.canonicalShareName(shareName)
        self.basePath = try SMBPathCanonicalizer.canonicalRawPath(basePath)
        self.username = username
        self.domain = domain
        publishedV2IdentityComponents = [
            canonicalHost.socketHost,
            String(canonicalPort.value),
            shareName.trimmingCharacters(in: CharacterSet(charactersIn: "/")).lowercased(),
            RemotePathBuilder.normalizePath(basePath),
            username,
            (domain ?? "").lowercased()
        ]
    }
}

struct CanonicalWebDAVConnection: Equatable, Sendable {
    let scheme: HTTPTransportScheme
    let host: CanonicalHost
    let port: CanonicalPort
    let mountPath: String
    let basePath: String
    let effectiveRoot: String
    let username: String
    let publishedV2IdentityComponents: [String]

    init(
        scheme: String,
        host: String,
        port: Int,
        mountPath: String,
        basePath: String,
        username: String
    ) throws {
        guard let canonicalScheme = HTTPTransportScheme.parse(scheme),
              let canonicalHost = CanonicalHost(host),
              let canonicalPort = CanonicalPort(rawValue: port, defaultValue: canonicalScheme.defaultPort),
              !username.isEmpty else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let canonicalMount = try WebDAVPathCanonicalizer.canonicalRawPath(mountPath)
        let canonicalBase = try WebDAVPathCanonicalizer.canonicalRawPath(basePath)
        self.scheme = canonicalScheme
        self.host = canonicalHost
        self.port = canonicalPort
        self.mountPath = canonicalMount
        self.basePath = canonicalBase
        self.effectiveRoot = try WebDAVPathCanonicalizer.effectiveRootRawPath(
            mountPath: canonicalMount,
            basePath: canonicalBase
        )
        self.username = username
        publishedV2IdentityComponents = [
            scheme.lowercased(),
            canonicalHost.socketHost,
            String(port == 0 ? canonicalScheme.defaultPort : port),
            RemotePathBuilder.normalizePath(mountPath),
            canonicalBase,
            username
        ]
        guard endpointURL != nil else { throw RemoteStorageClientError.invalidConfiguration }
    }

    var endpointURL: URL? {
        var components = URLComponents()
        components.scheme = scheme.rawValue
        components.percentEncodedHost = host.urlAuthority
        if port.value != scheme.defaultPort {
            components.port = port.value
        }
        components.path = mountPath
        return components.url
    }
}

struct CanonicalS3Endpoint: Equatable, Sendable {
    let scheme: HTTPTransportScheme
    let host: CanonicalHost
    let port: CanonicalPort
}

enum S3Canonicalization {
    static func parseEndpoint(_ rawValue: String) -> CanonicalS3Endpoint? {
        let trimmed = rawValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }
        if !trimmed.contains("://"),
           RemoteHostIdentity.canonicalIPv6(trimmed) != nil,
           let host = CanonicalHost(trimmed),
           let port = CanonicalPort(rawValue: 0, defaultValue: HTTPTransportScheme.https.defaultPort) {
            return CanonicalS3Endpoint(scheme: .https, host: host, port: port)
        }
        let normalized = trimmed.contains("://") ? trimmed : "https://" + trimmed
        guard !hasExplicitEmptyPort(normalized),
              let components = URLComponents(string: normalized),
              components.user == nil,
              components.password == nil,
              components.query == nil,
              components.fragment == nil,
              components.percentEncodedPath.isEmpty || components.percentEncodedPath == "/",
              let parsedHost = components.host,
              let host = CanonicalHost(parsedHost),
              let scheme = HTTPTransportScheme.parse(components.scheme ?? "https") else {
            return nil
        }
        if let explicitPort = components.port, !(1 ... 65535).contains(explicitPort) { return nil }
        guard let port = CanonicalPort(rawValue: components.port ?? 0, defaultValue: scheme.defaultPort) else {
            return nil
        }
        return CanonicalS3Endpoint(scheme: scheme, host: host, port: port)
    }

    static func parseEndpoint(scheme rawScheme: String, host rawHost: String, port rawPort: Int) -> CanonicalS3Endpoint? {
        guard let scheme = HTTPTransportScheme.parseS3Compatible(rawScheme),
              let host = CanonicalHost(rawHost),
              let port = CanonicalPort(rawValue: rawPort, defaultValue: scheme.defaultPort) else { return nil }
        return CanonicalS3Endpoint(scheme: scheme, host: host, port: port)
    }

    static func defaultPathStyle(forHost host: String) -> Bool {
        let canonicalHost = RemoteHostIdentity.canonical(host)
        if canonicalHost.hasSuffix(".amazonaws.com") { return false }
        if canonicalHost.hasSuffix(".cloudflarestorage.com") { return false }
        if canonicalHost.hasSuffix(".backblazeb2.com") { return false }
        if canonicalHost.hasSuffix(".digitaloceanspaces.com") { return false }
        if canonicalHost.hasSuffix(".wasabisys.com") { return false }
        return true
    }

    static func resolveRegion(userInput: String, host: String) -> String {
        let trimmed = userInput.trimmingCharacters(in: .whitespacesAndNewlines)
        if !trimmed.isEmpty { return trimmed }
        return defaultRegion(forHost: host) ?? ""
    }

    static func effectiveSigningRegion(userInput: String, host: String) -> String {
        let resolved = resolveRegion(userInput: userInput, host: host)
        return resolved.isEmpty ? "us-east-1" : resolved
    }

    static func defaultRegion(forHost host: String) -> String? {
        let canonicalHost = RemoteHostIdentity.canonical(host)
        if canonicalHost.hasSuffix(".r2.cloudflarestorage.com") { return "auto" }
        if let region = extractMiddleSegment(host: canonicalHost, prefix: "s3.", suffix: ".amazonaws.com") {
            return region
        }
        if let region = extractMiddleSegment(host: canonicalHost, prefix: "s3.", suffix: ".backblazeb2.com") {
            return region
        }
        if let region = extractMiddleSegment(host: canonicalHost, prefix: "s3.", suffix: ".wasabisys.com") {
            return region
        }
        if canonicalHost.hasSuffix(".digitaloceanspaces.com") {
            let trimmed = String(canonicalHost.dropLast(".digitaloceanspaces.com".count))
            if !trimmed.isEmpty, !trimmed.contains(".") { return trimmed }
        }
        return nil
    }

    private static func hasExplicitEmptyPort(_ endpoint: String) -> Bool {
        guard let schemeDelimiter = endpoint.range(of: "://") else { return false }
        let remainder = endpoint[schemeDelimiter.upperBound...]
        let authority = remainder.prefix { $0 != "/" && $0 != "?" && $0 != "#" }
        let start = authority.lastIndex(of: "@").map { authority.index(after: $0) } ?? authority.startIndex
        return authority.suffix(from: start).hasSuffix(":")
    }

    private static func extractMiddleSegment(host: String, prefix: String, suffix: String) -> String? {
        guard host.hasPrefix(prefix), host.hasSuffix(suffix), host.count > prefix.count + suffix.count else {
            return nil
        }
        let middle = host.dropFirst(prefix.count).dropLast(suffix.count)
        if middle.isEmpty || middle.contains(".") { return nil }
        return String(middle)
    }
}

struct CanonicalS3Connection: Equatable, Sendable {
    let endpoint: CanonicalS3Endpoint
    let resolvedRegion: String
    let effectiveSigningRegion: String
    let usePathStyle: Bool
    let bucket: String
    let basePrefix: String
    let accessKeyID: String
    let publishedV2IdentityComponents: [String]

    init(
        scheme: String,
        host: String,
        port: Int,
        region: String,
        usePathStyle: Bool,
        bucket: String,
        basePath: String,
        accessKeyID: String
    ) throws {
        guard let endpoint = S3Canonicalization.parseEndpoint(scheme: scheme, host: host, port: port) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        try self.init(
            endpoint: endpoint,
            region: region,
            usePathStyle: usePathStyle,
            bucket: bucket,
            basePath: basePath,
            accessKeyID: accessKeyID,
            publishedScheme: scheme.lowercased(),
            publishedPort: port == 0 ? (scheme.lowercased() == "http" ? 80 : 443) : port
        )
    }

    init(
        endpoint: CanonicalS3Endpoint,
        region: String,
        usePathStyle: Bool,
        bucket: String,
        basePath: String,
        accessKeyID: String
    ) throws {
        try self.init(
            endpoint: endpoint,
            region: region,
            usePathStyle: usePathStyle,
            bucket: bucket,
            basePath: basePath,
            accessKeyID: accessKeyID,
            publishedScheme: endpoint.scheme.rawValue,
            publishedPort: endpoint.port.value
        )
    }

    private init(
        endpoint: CanonicalS3Endpoint,
        region: String,
        usePathStyle: Bool,
        bucket: String,
        basePath: String,
        accessKeyID: String,
        publishedScheme: String,
        publishedPort: Int
    ) throws {
        guard !bucket.isEmpty, !accessKeyID.isEmpty else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        self.endpoint = endpoint
        self.resolvedRegion = S3Canonicalization.resolveRegion(userInput: region, host: endpoint.host.socketHost)
        self.effectiveSigningRegion = S3Canonicalization.effectiveSigningRegion(
            userInput: region,
            host: endpoint.host.socketHost
        )
        self.usePathStyle = usePathStyle
        self.bucket = bucket
        self.basePrefix = RemotePathBuilder.normalizePath(basePath)
        self.accessKeyID = accessKeyID
        publishedV2IdentityComponents = [
            publishedScheme,
            endpoint.host.socketHost,
            String(publishedPort),
            self.effectiveSigningRegion,
            usePathStyle ? "path" : "virtual",
            bucket,
            self.basePrefix,
            accessKeyID
        ]
    }
}

struct CanonicalSFTPConnection: Equatable, Sendable {
    let host: CanonicalHost
    let port: CanonicalPort
    let basePath: String
    let username: String
    let authMethod: SFTPConnectionParams.AuthMethod
    let hostKeyFingerprintSHA256: String
    let publishedV2IdentityComponents: [String]

    init(
        host: String,
        port: Int,
        basePath: String,
        username: String,
        authMethod: SFTPConnectionParams.AuthMethod,
        hostKeyFingerprintSHA256: String
    ) throws {
        guard let canonicalHost = CanonicalHost(host),
              let canonicalPort = CanonicalPort(rawValue: port, defaultValue: SFTPEndpoint.defaultPort),
              !username.isEmpty else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        self.host = canonicalHost
        self.port = canonicalPort
        self.basePath = try SFTPPathCanonicalizer.canonicalRawPath(basePath)
        self.username = username
        self.authMethod = authMethod
        self.hostKeyFingerprintSHA256 = hostKeyFingerprintSHA256
        publishedV2IdentityComponents = [
            canonicalHost.socketHost,
            String(canonicalPort.value),
            self.basePath,
            username
        ]
    }
}

struct CanonicalConnectionComparisonKey: Equatable, Sendable {
    let storageType: StorageType
    let components: [String]
}

nonisolated struct CanonicalOneDriveConnection: Equatable, Sendable {
    let cloudEnvironment: OneDriveCloudEnvironment
    let accountType: OneDriveAccountType
    let driveID: String
    let rootItemID: String
    let displayRootPath: String
    let publishedV2IdentityComponents: [String]

    init(params: OneDriveConnectionParams) throws {
        let driveID = params.driveID.trimmingCharacters(in: .whitespacesAndNewlines)
        let rootItemID = params.rootItemID.trimmingCharacters(in: .whitespacesAndNewlines)
        guard params.schemaVersion == OneDriveConnectionParams.currentSchemaVersion,
              !driveID.isEmpty,
              !rootItemID.isEmpty else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        cloudEnvironment = params.cloudEnvironment
        accountType = params.accountType
        self.driveID = driveID
        self.rootItemID = rootItemID
        displayRootPath = params.displayRootPath
        publishedV2IdentityComponents = [
            params.cloudEnvironment.rawValue,
            driveID,
            rootItemID
        ]
    }
}

enum CanonicalProfileConnection: Equatable, Sendable {
    case smb(CanonicalSMBConnection)
    case webDAV(CanonicalWebDAVConnection)
    case s3(CanonicalS3Connection)
    case sftp(CanonicalSFTPConnection)
    case oneDrive(CanonicalOneDriveConnection)

    var storageType: StorageType {
        switch self {
        case .smb: return .smb
        case .webDAV: return .webdav
        case .s3: return .s3
        case .sftp: return .sftp
        case .oneDrive: return .onedrive
        }
    }

    var publishedV2IdentityComponents: [String] {
        switch self {
        case .smb(let value): return value.publishedV2IdentityComponents
        case .webDAV(let value): return value.publishedV2IdentityComponents
        case .s3(let value): return value.publishedV2IdentityComponents
        case .sftp(let value): return value.publishedV2IdentityComponents
        case .oneDrive(let value): return value.publishedV2IdentityComponents
        }
    }

    var publishedV2RemoteIdentityComponents: [String] {
        switch self {
        case .sftp(let value):
            return publishedV2IdentityComponents + [value.hostKeyFingerprintSHA256]
        case .smb, .webDAV, .s3, .oneDrive:
            return publishedV2IdentityComponents
        }
    }

    var canonicalComparisonKey: CanonicalConnectionComparisonKey {
        CanonicalConnectionComparisonKey(storageType: storageType, components: canonicalComparisonComponents)
    }

    private var canonicalComparisonComponents: [String] {
        switch self {
        case .smb(let value):
            return [
                value.host.socketHost,
                String(value.port.value),
                value.shareName.lowercased(),
                value.basePath,
                value.username,
                (value.domain ?? "").lowercased()
            ]
        case .webDAV(let value):
            return [
                value.scheme.rawValue,
                value.host.socketHost,
                String(value.port.value),
                value.effectiveRoot,
                value.username
            ]
        case .s3(let value):
            return [
                value.endpoint.scheme.rawValue,
                value.endpoint.host.socketHost,
                String(value.endpoint.port.value),
                value.effectiveSigningRegion,
                value.usePathStyle ? "path" : "virtual",
                value.bucket,
                value.basePrefix,
                value.accessKeyID
            ]
        case .sftp(let value):
            return [
                value.host.socketHost,
                String(value.port.value),
                value.basePath,
                value.username
            ]
        case .oneDrive(let value):
            return value.publishedV2IdentityComponents
        }
    }

    var displaySubtitle: String? {
        switch self {
        case .smb(let value):
            let port = value.port.value == SMBEndpoint.defaultPort ? "" : ":\(value.port.value)"
            return "SMB://\(value.host.urlAuthority)\(port)/\(value.shareName)\(value.basePath)"
        case .webDAV(let value):
            guard let endpoint = value.endpointURL?.absoluteString else { return nil }
            guard value.basePath != "/" else { return endpoint }
            return (endpoint.hasSuffix("/") ? String(endpoint.dropLast()) : endpoint) + value.basePath
        case .s3(let value):
            let port = value.endpoint.port.value == value.endpoint.scheme.defaultPort
                ? ""
                : ":\(value.endpoint.port.value)"
            let prefix = value.basePrefix == "/" ? "" : value.basePrefix
            if value.usePathStyle {
                return "\(value.endpoint.scheme.rawValue)://\(value.endpoint.host.urlAuthority)\(port)/\(value.bucket)\(prefix)"
            }
            guard !value.endpoint.host.isIPLiteral,
                  let virtualHost = CanonicalHost("\(value.bucket).\(value.endpoint.host.socketHost)") else { return nil }
            return "\(value.endpoint.scheme.rawValue)://\(virtualHost.urlAuthority)\(port)\(prefix)"
        case .sftp(let value):
            let port = value.port.value == SFTPEndpoint.defaultPort ? "" : ":\(value.port.value)"
            let path = value.basePath == "/" ? "" : value.basePath
            return "sftp://\(value.username)@\(value.host.urlAuthority)\(port)\(path)"
        case .oneDrive(let value):
            return value.displayRootPath.isEmpty ? "OneDrive" : value.displayRootPath
        }
    }
}
