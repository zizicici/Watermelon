import Foundation
import GRDB
import Darwin

enum StorageType: String, Codable, Sendable {
    case smb
    case webdav
    case externalVolume
    case s3
    case sftp
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
    static let defaultPort = 22

    static func effectivePort(_ port: Int) -> Int {
        port == 0 ? defaultPort : port
    }
}

enum RemoteHostIdentity {
    static func canonical(_ host: String) -> String {
        let trimmed = host.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return "" }
        if let ipv6 = canonicalIPv6(trimmed) {
            return ipv6
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

    static func canonicalSMB(_ host: String) -> String {
        var rawHost = host.trimmingCharacters(in: .whitespacesAndNewlines)
        if rawHost.range(of: "smb://", options: [.anchored, .caseInsensitive]) != nil {
            rawHost.removeFirst("smb://".count)
        }
        return canonical(rawHost.trimmingCharacters(in: CharacterSet(charactersIn: "/")))
    }

    static func canonicalIPv6(_ host: String) -> String? {
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

        var ipv4Address = in_addr()
        if rawHost.withCString({ inet_pton(AF_INET, $0, &ipv4Address) }) == 1 {
            var buffer = [CChar](repeating: 0, count: Int(INET_ADDRSTRLEN))
            guard inet_ntop(AF_INET, &ipv4Address, &buffer, socklen_t(INET_ADDRSTRLEN)) != nil else {
                return nil
            }
            let address = String(cString: buffer)
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
    ) -> RemoteDestinationIdentity {
        RemoteDestinationIdentity(storageType: .smb, components: [
            RemoteHostIdentity.canonicalSMB(host),
            String(SMBEndpoint.effectivePort(port)),
            shareName.trimmingCharacters(in: CharacterSet(charactersIn: "/")).lowercased(),
            RemotePathBuilder.normalizePath(basePath),
            username,
            (domain ?? "").lowercased()
        ])
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
    ) -> ProfileDuplicateIdentity {
        ProfileDuplicateIdentity(storageType: .smb, components: [
            RemoteHostIdentity.canonicalSMB(host),
            String(SMBEndpoint.effectivePort(port)),
            shareName.trimmingCharacters(in: CharacterSet(charactersIn: "/")).lowercased(),
            RemotePathBuilder.normalizePath(basePath),
            username,
            (domain ?? "").lowercased()
        ])
    }

    static func webDAV(
        scheme: String,
        host: String,
        port: Int,
        mountPath: String,
        basePath: String,
        username: String
    ) -> ProfileDuplicateIdentity? {
        guard let canonicalBasePath = try? WebDAVPathCanonicalizer.canonicalRawPath(basePath) else {
            return nil
        }
        let canonicalScheme = scheme.lowercased()
        return ProfileDuplicateIdentity(storageType: .webdav, components: [
            canonicalScheme,
            RemoteHostIdentity.canonical(host),
            String(effectivePort(port, scheme: canonicalScheme)),
            RemotePathBuilder.normalizePath(mountPath),
            canonicalBasePath,
            username
        ])
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
    ) -> ProfileDuplicateIdentity {
        let canonicalScheme = scheme.lowercased()
        let canonicalHost = RemoteHostIdentity.canonical(host)
        return ProfileDuplicateIdentity(storageType: .s3, components: [
            canonicalScheme,
            canonicalHost,
            String(effectivePort(port, scheme: canonicalScheme)),
            S3Client.effectiveSigningRegion(userInput: region, host: canonicalHost),
            usePathStyle ? "path" : "virtual",
            bucket,
            RemotePathBuilder.normalizePath(basePath),
            accessKeyID
        ])
    }

    static func sftp(
        host: String,
        port: Int,
        basePath: String,
        username: String
    ) -> ProfileDuplicateIdentity {
        ProfileDuplicateIdentity(storageType: .sftp, components: [
            RemoteHostIdentity.canonical(host),
            String(SFTPEndpoint.effectivePort(port)),
            RemotePathBuilder.normalizePath(basePath),
            username
        ])
    }

    private static func effectivePort(_ port: Int, scheme: String) -> Int {
        if port != 0 { return port }
        return scheme == "http" ? 80 : 443
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

    var duplicateIdentity: ProfileDuplicateIdentity? {
        switch resolvedStorageType {
        case .smb:
            return .smb(
                host: host,
                port: port,
                shareName: shareName,
                basePath: basePath,
                username: username,
                domain: domain
            )
        case .webdav:
            guard let params = webDAVParams else { return nil }
            return .webDAV(
                scheme: params.scheme,
                host: host,
                port: port,
                mountPath: shareName,
                basePath: basePath,
                username: username
            )
        case .s3:
            guard let params = s3Params else { return nil }
            return .s3(
                scheme: params.scheme,
                host: host,
                port: port,
                region: params.region,
                usePathStyle: params.usePathStyle,
                bucket: shareName,
                basePath: basePath,
                accessKeyID: username
            )
        case .sftp:
            guard sftpParams != nil else { return nil }
            return .sftp(host: host, port: port, basePath: basePath, username: username)
        case .externalVolume:
            return nil
        }
    }

    var remoteDestinationIdentity: RemoteDestinationIdentity {
        switch resolvedStorageType {
        case .smb:
            return .smb(
                host: host,
                port: port,
                shareName: shareName,
                basePath: basePath,
                username: username,
                domain: domain
            )
        case .webdav:
            guard let params = webDAVParams else {
                return invalidRemoteDestinationIdentity(type: .webdav)
            }
            guard let canonicalBasePath = try? WebDAVPathCanonicalizer.canonicalRawPath(basePath) else {
                return invalidRemoteDestinationIdentity(type: .webdav)
            }
            let scheme = params.scheme.lowercased()
            return RemoteDestinationIdentity(storageType: .webdav, components: [
                scheme,
                RemoteHostIdentity.canonical(host),
                String(effectivePort(scheme: scheme)),
                RemotePathBuilder.normalizePath(shareName),
                canonicalBasePath,
                username
            ])
        case .s3:
            guard let params = s3Params else {
                return invalidRemoteDestinationIdentity(type: .s3)
            }
            let scheme = params.scheme.lowercased()
            let canonicalHost = RemoteHostIdentity.canonical(host)
            return RemoteDestinationIdentity(storageType: .s3, components: [
                scheme,
                canonicalHost,
                String(effectivePort(scheme: scheme)),
                S3Client.effectiveSigningRegion(userInput: params.region, host: canonicalHost),
                params.usePathStyle ? "path" : "virtual",
                shareName,
                RemotePathBuilder.normalizePath(basePath),
                username
            ])
        case .sftp:
            guard let params = sftpParams else {
                return invalidRemoteDestinationIdentity(type: .sftp)
            }
            return RemoteDestinationIdentity(storageType: .sftp, components: [
                RemoteHostIdentity.canonical(host),
                String(SFTPEndpoint.effectivePort(port)),
                RemotePathBuilder.normalizePath(basePath),
                username,
                params.hostKeyFingerprintSHA256
            ])
        case .externalVolume:
            let token = shareName.isEmpty ? "legacy-profile-\(id ?? 0)" : shareName
            return RemoteDestinationIdentity(storageType: .externalVolume, components: [token])
        }
    }

    private func effectivePort(scheme: String) -> Int {
        if port != 0 { return port }
        return scheme == "http" ? 80 : 443
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
