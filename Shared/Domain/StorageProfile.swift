import Foundation

struct ExternalVolumeConnectionParams: Codable {
    let rootBookmarkData: Data
    let displayPath: String

    init(rootBookmarkData: Data, displayPath: String) {
        self.rootBookmarkData = rootBookmarkData
        self.displayPath = displayPath
    }
}

struct ExternalVolumeCurrentLocation: Equatable, Sendable {
    let fullIdentity: Data?
    let volumePathIdentity: Data?
    let standardizedURL: URL
}

enum ExternalVolumeLocationPolicy {
    static func representsPotentialDuplicate(
        _ first: ExternalVolumeCurrentLocation,
        _ second: ExternalVolumeCurrentLocation
    ) -> Bool {
        if let firstFullIdentity = first.fullIdentity,
           let secondFullIdentity = second.fullIdentity,
           firstFullIdentity == secondFullIdentity {
            return true
        }
        if let firstVolumePathIdentity = first.volumePathIdentity,
           let secondVolumePathIdentity = second.volumePathIdentity {
            return firstVolumePathIdentity == secondVolumePathIdentity
        }
        return first.standardizedURL == second.standardizedURL
    }

    static func canReuseRemoteState(
        _ first: ExternalVolumeCurrentLocation,
        _ second: ExternalVolumeCurrentLocation
    ) -> Bool {
        guard let firstIdentity = first.fullIdentity,
              let secondIdentity = second.fullIdentity else { return false }
        return firstIdentity == secondIdentity
    }

    static func locationToken(
        existingToken: String?,
        selectedNewLocation: Bool,
        existingLocation: ExternalVolumeCurrentLocation?,
        candidateLocation: ExternalVolumeCurrentLocation,
        makeToken: () -> String
    ) -> String {
        guard selectedNewLocation else { return existingToken ?? makeToken() }
        guard let existingLocation,
              canReuseRemoteState(existingLocation, candidateLocation) else {
            return makeToken()
        }
        return existingToken ?? makeToken()
    }

    static func containsDuplicate(
        candidate: ExternalVolumeCurrentLocation,
        existingLocations: [ExternalVolumeCurrentLocation]
    ) -> Bool {
        existingLocations.contains { representsPotentialDuplicate($0, candidate) }
    }
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

extension StorageType {
    var symbolName: String {
        switch self {
        case .smb: return "server.rack"
        case .webdav: return "network"
        case .s3: return "cloud"
        case .sftp: return "arrow.up.folder"
        case .externalVolume: return "externaldrive"
        }
    }

    var sectionHeaderText: String {
        switch self {
        case .smb: return "SMB"
        case .webdav: return "WebDAV"
        case .externalVolume: return String(localized: "home.menu.externalStorage")
        case .s3: return "S3"
        case .sftp: return "SFTP"
        }
    }

    static let sectionDisplayOrder: [StorageType] = [.externalVolume, .smb, .webdav, .sftp, .s3]
}

struct StorageProfileSection {
    let type: StorageType
    var profiles: [ServerProfileRecord]
}

nonisolated enum SFTPHostKeyPromptPolicy {
    enum Decision: Equatable {
        case none
        case firstTrust
        case changedKey(expected: String)
    }

    static func decision(
        existingHost: String?,
        existingPort: Int?,
        expectedFingerprint: String?,
        proposedHost: String,
        proposedPort: Int,
        actualFingerprint: String
    ) -> Decision {
        guard actualFingerprint != expectedFingerprint else { return .none }
        let sameEndpoint: Bool
        if let existingHost, let existingPort {
            sameEndpoint = RemoteHostIdentity.canonical(existingHost) == RemoteHostIdentity.canonical(proposedHost)
                && SFTPEndpoint.effectivePort(existingPort) == SFTPEndpoint.effectivePort(proposedPort)
        } else {
            sameEndpoint = false
        }
        if sameEndpoint, let expectedFingerprint, !expectedFingerprint.isEmpty {
            return .changedKey(expected: expectedFingerprint)
        }
        return .firstTrust
    }

    static func retainedFingerprint(
        existingProfile: ServerProfileRecord?,
        proposedHost: String,
        proposedPort: Int
    ) -> String {
        guard let existingProfile,
              RemoteHostIdentity.canonical(existingProfile.host) == RemoteHostIdentity.canonical(proposedHost),
              SFTPEndpoint.effectivePort(existingProfile.port) == SFTPEndpoint.effectivePort(proposedPort) else {
            return ""
        }
        return existingProfile.sftpParams?.hostKeyFingerprintSHA256 ?? ""
    }

    static func fingerprintForSave(
        liveProfile: ServerProfileRecord?,
        proposedHost: String,
        proposedPort: Int,
        testedHost: String?,
        testedPort: Int?,
        testedFingerprint: String?
    ) -> String {
        if let testedHost, let testedPort, let testedFingerprint,
           RemoteHostIdentity.canonical(testedHost) == RemoteHostIdentity.canonical(proposedHost),
           SFTPEndpoint.effectivePort(testedPort) == SFTPEndpoint.effectivePort(proposedPort) {
            return testedFingerprint
        }
        return retainedFingerprint(
            existingProfile: liveProfile,
            proposedHost: proposedHost,
            proposedPort: proposedPort
        )
    }
}

extension Sequence where Element == ServerProfileRecord {
    func groupedByStorageType(excluding skip: Set<StorageType> = []) -> [StorageProfileSection] {
        StorageType.sectionDisplayOrder.compactMap { type in
            guard !skip.contains(type) else { return nil }
            let group = filter { $0.resolvedStorageType == type }
            return group.isEmpty ? nil : StorageProfileSection(type: type, profiles: group)
        }
    }
}

nonisolated struct SFTPConnectionParams: Codable {
    enum AuthMethod: String, Codable {
        case password
        case privateKey
    }

    let authMethod: AuthMethod
    let hostKeyFingerprintSHA256: String
}

nonisolated enum SFTPCredentialBlob: Codable, Equatable {
    case password(String)
    case privateKey(pem: String, passphrase: String?)

    private enum CodingKeys: String, CodingKey {
        case kind
        case password
        case pem
        case passphrase
    }

    private enum Kind: String, Codable {
        case password
        case privateKey
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        switch try container.decode(Kind.self, forKey: .kind) {
        case .password:
            self = .password(try container.decode(String.self, forKey: .password))
        case .privateKey:
            let passphrase = try container.decodeIfPresent(String.self, forKey: .passphrase)
            self = .privateKey(
                pem: try container.decode(String.self, forKey: .pem),
                passphrase: passphrase?.isEmpty == true ? nil : passphrase
            )
        }
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .password(let value):
            try container.encode(Kind.password, forKey: .kind)
            try container.encode(value, forKey: .password)
        case .privateKey(let pem, let passphrase):
            try container.encode(Kind.privateKey, forKey: .kind)
            try container.encode(pem, forKey: .pem)
            try container.encodeIfPresent(passphrase, forKey: .passphrase)
        }
    }

    func encodedJSONString() throws -> String {
        String(decoding: try JSONEncoder().encode(self), as: UTF8.self)
    }

    static func decode(from string: String) throws -> SFTPCredentialBlob {
        try JSONDecoder().decode(SFTPCredentialBlob.self, from: Data(string.utf8))
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
        case .smb, .webdav, .s3, .sftp:
            return true
        case .externalVolume:
            return false
        }
    }

    // SFTP credentials are multi-field, so the single-string prompt can't reconstruct them.
    var supportsPasswordPrompt: Bool {
        switch storageType {
        case .smb, .webdav, .s3:
            return true
        case .externalVolume, .sftp:
            return false
        }
    }

    var displaySubtitle: String {
        switch storageType {
        case .externalVolume:
            if let path = record.externalVolumeParams?.displayPath, !path.isEmpty {
                return Self.relativeExternalPath(from: path)
            }
            return String(localized: "storage.error.externalFallback")
        case .smb, .webdav, .s3, .sftp:
            return record.canonicalConnection?.displaySubtitle ?? storageType.sectionHeaderText
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
    static let browserLinkCredentialPrefix = "browser-link:"

    var storageProfile: StorageProfile {
        StorageProfile(record: self)
    }

    static func browserLinkCredentialRef(sessionID: String) -> String {
        browserLinkCredentialPrefix + sessionID
    }

    var browserLinkSessionID: String? {
        guard credentialRef.hasPrefix(Self.browserLinkCredentialPrefix) else { return nil }
        let sessionID = String(credentialRef.dropFirst(Self.browserLinkCredentialPrefix.count))
        return sessionID.isEmpty ? nil : sessionID
    }

    var isBrowserLinkProfile: Bool {
        browserLinkSessionID != nil
    }

    var runtimeConnectionIdentity: String {
        isBrowserLinkProfile ? credentialRef : "saved:\(id.map(String.init) ?? credentialRef)"
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

    var sftpParams: SFTPConnectionParams? {
        decodedConnectionParams(as: SFTPConnectionParams.self)
    }

    var sftpDisplayURLString: String? {
        guard let connection = canonicalConnection,
              case .sftp = connection else { return nil }
        return connection.displaySubtitle
    }

    var s3DisplayURLString: String? {
        guard let connection = canonicalConnection,
              case .s3 = connection else { return nil }
        return connection.displaySubtitle
    }

    /// Canonical WebDAV endpoint built from the structured fields.
    /// Returns nil when the profile lacks the minimum shape (scheme + host).
    var webDAVEndpointURL: URL? {
        guard case .webDAV(let connection) = canonicalConnection else { return nil }
        return connection.endpointURL
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
        (try? CanonicalWebDAVConnection(
            scheme: scheme,
            host: host,
            port: port,
            mountPath: mountPath,
            basePath: "/",
            username: "_"
        ))?.endpointURL
    }

    func decodedConnectionParams<T: Decodable>(as type: T.Type) -> T? {
        guard let data = connectionParams else { return nil }
        return try? JSONDecoder().decode(T.self, from: data)
    }

    static func encodedConnectionParams<T: Encodable>(_ params: T) throws -> Data {
        try JSONEncoder().encode(params)
    }

    func isExternalStorageUnavailableError(_ error: Error) -> Bool {
        !isBrowserLinkProfile && resolvedStorageType == .externalVolume
            && RemoteStorageClientError.isLikelyExternalStorageUnavailable(error)
    }

    func isConnectionUnavailableError(_ error: Error) -> Bool {
        if isBrowserLinkProfile {
            return RemoteStorageClientError.isConnectionUnavailable(error)
        }
        switch resolvedStorageType {
        case .externalVolume:
            return RemoteStorageClientError.isLikelyExternalStorageUnavailable(error)
        case .smb:
            return SMBErrorClassifier.isConnectionUnavailable(error)
        case .webdav:
            return false
        case .s3:
            return S3ErrorClassifier.isConnectionUnavailable(error)
        case .sftp:
            return SFTPErrorClassifier.isConnectionUnavailable(error)
        }
    }

    func userFacingStorageErrorMessage(_ error: Error) -> String {
        if error is LiteRepoError {
            return error.localizedDescription
        }
        if isBrowserLinkProfile {
            return error.localizedDescription
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
        if resolvedStorageType == .sftp {
            return SFTPErrorClassifier.describe(error)
        }
        return error.localizedDescription
    }

    func resolvedSessionPassword(from session: AppSession) -> String? {
        if storageProfile.requiresPassword {
            guard let password = session.activePassword else { return nil }
            return password
        }
        return session.activePassword ?? ""
    }
}
