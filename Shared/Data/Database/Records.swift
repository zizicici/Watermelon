import Foundation
import GRDB
import Darwin

enum StorageType: String, Codable, Sendable {
    case smb
    case webdav
    case externalVolume
    case s3
    case sftp
    case onedrive
}

enum SMBEndpoint {
    static let defaultPort = 445

    static func effectivePort(_ port: Int) -> Int {
        port == 0 ? defaultPort : port
    }

    static func url(host: String, port: Int) -> URL? {
        RemoteHostEndpoint.url(
            scheme: "smb",
            host: host,
            port: effectivePort(port),
            strippingSMBScheme: true
        )
    }
}

enum SFTPEndpoint {
    nonisolated static let defaultPort = 22

    nonisolated static func effectivePort(_ port: Int) -> Int {
        port == 0 ? defaultPort : port
    }
}

enum RemoteHostIdentity {
    nonisolated static func canonical(_ host: String) -> String {
        let trimmed = host.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return "" }
        if let ipv6 = canonicalIPv6(trimmed) {
            return ipv6
        }
        if let ipv4 = canonicalIPv4(trimmed) {
            return ipv4
        }

        var dnsHost = trimmed
        if dnsHost.count > 1, dnsHost.hasSuffix("."), !dnsHost.dropLast().hasSuffix(".") {
            dnsHost.removeLast()
        }
        var components = URLComponents()
        components.scheme = "https"
        components.host = dnsHost
        return components.url?.host?.lowercased() ?? dnsHost.lowercased()
    }

    nonisolated static func canonicalSMB(_ host: String) -> String {
        var rawHost = host.trimmingCharacters(in: .whitespacesAndNewlines)
        if rawHost.range(of: "smb://", options: [.anchored, .caseInsensitive]) != nil {
            rawHost.removeFirst("smb://".count)
        }
        return canonical(rawHost.trimmingCharacters(in: CharacterSet(charactersIn: "/")))
    }

    nonisolated static func canonicalIPv4(_ host: String) -> String? {
        var binaryAddress = in_addr()
        guard host.withCString({ inet_pton(AF_INET, $0, &binaryAddress) }) == 1 else { return nil }
        var buffer = [CChar](repeating: 0, count: Int(INET_ADDRSTRLEN))
        guard inet_ntop(AF_INET, &binaryAddress, &buffer, socklen_t(INET_ADDRSTRLEN)) != nil else {
            return nil
        }
        return String(cString: buffer)
    }

    nonisolated static func canonicalIPv6(_ host: String) -> String? {
        var candidate = host
        if candidate.hasPrefix("["), candidate.hasSuffix("]") {
            candidate = String(candidate.dropFirst().dropLast())
        }

        let address: String
        let zone: String?
        if let encodedZone = candidate.range(of: "%25") {
            address = String(candidate[..<encodedZone.lowerBound])
            zone = String(candidate[encodedZone.upperBound...])
        } else if let zoneDelimiter = candidate.lastIndex(of: "%") {
            address = String(candidate[..<zoneDelimiter])
            zone = String(candidate[candidate.index(after: zoneDelimiter)...])
        } else {
            address = candidate
            zone = nil
        }
        guard zone?.isEmpty != true else { return nil }

        var binaryAddress = in6_addr()
        let parsed = address.withCString { inet_pton(AF_INET6, $0, &binaryAddress) }
        guard parsed == 1 else { return nil }
        var buffer = [CChar](repeating: 0, count: Int(INET6_ADDRSTRLEN))
        guard inet_ntop(AF_INET6, &binaryAddress, &buffer, socklen_t(INET6_ADDRSTRLEN)) != nil else {
            return nil
        }
        let canonicalAddress = String(cString: buffer).lowercased()
        return zone.map { canonicalAddress + "%" + $0 } ?? canonicalAddress
    }
}

enum RemoteHostEndpoint {
    struct Representation: Equatable, Sendable {
        let socketHost: String
        let urlAuthority: String
        let isIPLiteral: Bool
    }

    static func representation(_ host: String, strippingSMBScheme: Bool = false) -> Representation? {
        var rawHost = host.trimmingCharacters(in: .whitespacesAndNewlines)
        if strippingSMBScheme,
           rawHost.range(of: "smb://", options: [.anchored, .caseInsensitive]) != nil {
            rawHost.removeFirst("smb://".count)
        }
        if strippingSMBScheme {
            rawHost = rawHost.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
        }
        guard !rawHost.isEmpty else { return nil }

        if let canonicalIPv6 = RemoteHostIdentity.canonicalIPv6(rawHost) {
            let address: String
            let zone: String?
            if let delimiter = canonicalIPv6.lastIndex(of: "%") {
                address = String(canonicalIPv6[..<delimiter])
                zone = String(canonicalIPv6[canonicalIPv6.index(after: delimiter)...])
            } else {
                address = canonicalIPv6
                zone = nil
            }
            let encodedZone = zone.flatMap {
                $0.addingPercentEncoding(withAllowedCharacters: zoneIdentifierAllowedCharacters)
            }
            guard zone == nil || encodedZone != nil else { return nil }
            let authority = "[" + address + (encodedZone.map { "%25" + $0 } ?? "") + "]"
            return Representation(socketHost: canonicalIPv6, urlAuthority: authority, isIPLiteral: true)
        }

        if let address = RemoteHostIdentity.canonicalIPv4(rawHost) {
            return Representation(socketHost: address, urlAuthority: address, isIPLiteral: true)
        }

        let canonicalHost = RemoteHostIdentity.canonical(rawHost)
        guard !canonicalHost.isEmpty else { return nil }
        var components = URLComponents()
        components.scheme = "https"
        components.host = canonicalHost
        guard let dnsHost = components.url?.host, !dnsHost.isEmpty else { return nil }
        return Representation(socketHost: dnsHost, urlAuthority: dnsHost, isIPLiteral: false)
    }

    static func socketHost(_ host: String, strippingSMBScheme: Bool = false) -> String? {
        representation(host, strippingSMBScheme: strippingSMBScheme)?.socketHost
    }

    static func urlAuthority(_ host: String, strippingSMBScheme: Bool = false) -> String? {
        representation(host, strippingSMBScheme: strippingSMBScheme)?.urlAuthority
    }

    static func url(
        scheme: String,
        host: String,
        port: Int?,
        strippingSMBScheme: Bool = false
    ) -> URL? {
        guard let authority = urlAuthority(host, strippingSMBScheme: strippingSMBScheme) else { return nil }
        var components = URLComponents()
        components.scheme = scheme
        components.percentEncodedHost = authority
        components.port = port
        return components.url
    }

    private static let zoneIdentifierAllowedCharacters: CharacterSet = {
        var allowed = CharacterSet.alphanumerics
        allowed.insert(charactersIn: "-._~")
        return allowed
    }()
}

struct RemoteDestinationIdentity: Equatable, Sendable {
    let storageType: StorageType
    let components: [String]

    var cacheKeyComponent: String {
        let values = [storageType.rawValue] + components
        let data = (try? JSONEncoder().encode(values)) ?? Data()
        return data.base64EncodedString()
    }

    static func smb(
        host: String,
        port: Int,
        shareName: String,
        basePath: String,
        username: String,
        domain: String?
    ) -> RemoteDestinationIdentity? {
        guard let connection = try? CanonicalSMBConnection(
            host: host,
            port: port,
            shareName: shareName,
            basePath: basePath,
            username: username,
            domain: domain
        ) else { return nil }
        return CanonicalProfileConnection.smb(connection).remoteDestinationIdentity
    }
}

struct ProfileDuplicateIdentity: Equatable, Sendable {
    let storageType: StorageType
    let components: [String]

    static func smb(
        host: String,
        port: Int,
        shareName: String,
        basePath: String,
        username: String,
        domain: String?
    ) -> ProfileDuplicateIdentity? {
        guard let connection = try? CanonicalSMBConnection(
            host: host,
            port: port,
            shareName: shareName,
            basePath: basePath,
            username: username,
            domain: domain
        ) else { return nil }
        return CanonicalProfileConnection.smb(connection).duplicateIdentity
    }

    static func webDAV(
        scheme: String,
        host: String,
        port: Int,
        mountPath: String,
        basePath: String,
        username: String
    ) -> ProfileDuplicateIdentity? {
        guard let connection = try? CanonicalWebDAVConnection(
            scheme: scheme,
            host: host,
            port: port,
            mountPath: mountPath,
            basePath: basePath,
            username: username
        ) else { return nil }
        return CanonicalProfileConnection.webDAV(connection).duplicateIdentity
    }

    static func s3(
        scheme: String,
        host: String,
        port: Int,
        region: String,
        usePathStyle: Bool,
        bucket: String,
        basePath: String,
        accessKeyID: String
    ) -> ProfileDuplicateIdentity? {
        guard let connection = try? CanonicalS3Connection(
            scheme: scheme,
            host: host,
            port: port,
            region: region,
            usePathStyle: usePathStyle,
            bucket: bucket,
            basePath: basePath,
            accessKeyID: accessKeyID
        ) else { return nil }
        return CanonicalProfileConnection.s3(connection).duplicateIdentity
    }

    static func sftp(
        host: String,
        port: Int,
        basePath: String,
        username: String
    ) -> ProfileDuplicateIdentity? {
        guard let connection = try? CanonicalSFTPConnection(
            host: host,
            port: port,
            basePath: basePath,
            username: username,
            authMethod: .password,
            hostKeyFingerprintSHA256: ""
        ) else { return nil }
        return CanonicalProfileConnection.sftp(connection).duplicateIdentity
    }
}

extension CanonicalProfileConnection {
    var duplicateIdentity: ProfileDuplicateIdentity {
        ProfileDuplicateIdentity(storageType: storageType, components: publishedV2IdentityComponents)
    }

    var remoteDestinationIdentity: RemoteDestinationIdentity {
        RemoteDestinationIdentity(storageType: storageType, components: publishedV2RemoteIdentityComponents)
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
        return remoteDestinationIdentity == other.remoteDestinationIdentity
    }

    var canonicalConnection: CanonicalProfileConnection? {
        switch resolvedStorageType {
        case .smb:
            guard let connection = try? CanonicalSMBConnection(
                host: host,
                port: port,
                shareName: shareName,
                basePath: basePath,
                username: username,
                domain: domain
            ) else { return nil }
            return .smb(connection)
        case .webdav:
            guard let params = webDAVParams else { return nil }
            guard let connection = try? CanonicalWebDAVConnection(
                scheme: params.scheme,
                host: host,
                port: port,
                mountPath: shareName,
                basePath: basePath,
                username: username
            ) else { return nil }
            return .webDAV(connection)
        case .s3:
            guard let params = s3Params else { return nil }
            guard let connection = try? CanonicalS3Connection(
                scheme: params.scheme,
                host: host,
                port: port,
                region: params.region,
                usePathStyle: params.usePathStyle,
                bucket: shareName,
                basePath: basePath,
                accessKeyID: username
            ) else { return nil }
            return .s3(connection)
        case .sftp:
            guard let params = sftpParams,
                  let connection = try? CanonicalSFTPConnection(
                    host: host,
                    port: port,
                    basePath: basePath,
                    username: username,
                    authMethod: params.authMethod,
                    hostKeyFingerprintSHA256: params.hostKeyFingerprintSHA256
                  ) else { return nil }
            return .sftp(connection)
        case .onedrive:
            guard let params = oneDriveParams,
                  let connection = try? CanonicalOneDriveConnection(params: params) else { return nil }
            return .oneDrive(connection)
        case .externalVolume:
            return nil
        }
    }

    var duplicateIdentity: ProfileDuplicateIdentity? {
        canonicalConnection?.duplicateIdentity
    }

    var remoteDestinationIdentity: RemoteDestinationIdentity {
        if resolvedStorageType == .externalVolume {
            let token = shareName.isEmpty ? "legacy-profile-\(id ?? 0)" : shareName
            return RemoteDestinationIdentity(storageType: .externalVolume, components: [token])
        }
        return canonicalConnection?.remoteDestinationIdentity
            ?? invalidRemoteDestinationIdentity(type: resolvedStorageType)
    }

    private func invalidRemoteDestinationIdentity(type: StorageType) -> RemoteDestinationIdentity {
        RemoteDestinationIdentity(storageType: type, components: [
            "invalid",
            String(id ?? 0),
            RemoteHostIdentity.canonical(host),
            String(port),
            shareName,
            RemotePathBuilder.normalizePath(basePath),
            username,
            domain ?? "",
            connectionParams?.base64EncodedString() ?? "missing"
        ])
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
