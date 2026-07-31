import Combine
import Foundation

#if DEBUG
enum MacDemoDestinationPolicy {
    static func shouldProvideDestination(
        arguments: [String]
    ) -> Bool {
        arguments.contains("--demo-destination")
            || arguments.contains("--demo-connecting")
            || arguments.contains("--demo-connected")
    }
}
#endif

@MainActor
final class ProfileStore: ObservableObject {
    @Published private(set) var profiles: [ServerProfileRecord] = []
    @Published var loadError: Error?

    private let databaseManager: DatabaseManager
    private let keychainService: KeychainService
    private let appRuntimeFlags: AppRuntimeFlags
    private let oneDriveCredentialLifecycleService:
        OneDriveCredentialLifecycleService?

    init(
        databaseManager: DatabaseManager,
        keychainService: KeychainService,
        appRuntimeFlags: AppRuntimeFlags,
        oneDriveCredentialLifecycleService:
            OneDriveCredentialLifecycleService? = nil
    ) {
        self.databaseManager = databaseManager
        self.keychainService = keychainService
        self.appRuntimeFlags = appRuntimeFlags
        self.oneDriveCredentialLifecycleService =
            oneDriveCredentialLifecycleService
        reload()
    }

    func reload() {
        #if DEBUG
        if MacDemoDestinationPolicy.shouldProvideDestination(
            arguments: ProcessInfo.processInfo.arguments
        ) {
            profiles = [.macDemoDestination]
            loadError = nil
            return
        }
        #endif
        do {
            profiles = try databaseManager.fetchServerProfiles().filter {
                !$0.isBrowserLinkProfile
            }
            loadError = nil
        } catch {
            profiles = []
            loadError = error
        }
    }

    func saveLocalProfile(
        folderURL: URL
    ) throws -> ServerProfileRecord {
        let record = try ExternalStorageProfileSaveWorker.save(
            intent: .init(
                editingProfile: nil,
                selectedDirectoryURL: folderURL,
                name: ""
            ),
            databaseManager: databaseManager,
            runtimeFlags: appRuntimeFlags
        )
        reload()
        return record
    }

    func deleteProfile(id: Int64) throws {
        let profile = profiles.first { $0.id == id }
        let credentialRef = profile?.credentialRef
        let oneDriveCredential = profile?.resolvedStorageType == .onedrive
            ? credentialRef.flatMap {
                try? keychainService.readPassword(account: $0)
            }
            : nil
        try databaseManager.deleteServerProfile(id: id)
        if let credentialRef, !credentialRef.isEmpty {
            let stillUsed = try databaseManager.fetchServerProfiles().contains {
                $0.credentialRef == credentialRef
            }
            if !stillUsed {
                try? keychainService.delete(account: credentialRef)
            }
        }
        if let oneDriveCredential {
            oneDriveCredentialLifecycleService?
                .removeCachedAccountIfUnused(
                    credentialJSONString: oneDriveCredential
                )
        }
        reload()
    }

    @discardableResult
    func saveSMBProfile(
        name: String,
        host: String,
        port: Int,
        shareName: String,
        basePath: String,
        username: String,
        domain: String?,
        password: String
    ) throws -> ServerProfileRecord {
        let connection = try CanonicalSMBConnection(
            host: host,
            port: port,
            shareName: shareName,
            basePath: basePath,
            username: username,
            domain: domain
        )
        let trimmedName = name.trimmingCharacters(
            in: .whitespacesAndNewlines
        )
        let identity = CanonicalProfileConnection
            .smb(connection)
            .duplicateIdentity

        var record = ServerProfileRecord(
            id: nil,
            name: trimmedName.isEmpty
                ? connection.host.socketHost
                : trimmedName,
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: connection.host.socketHost,
            port: connection.port.value,
            shareName: connection.shareName,
            basePath: connection.basePath,
            username: connection.username,
            domain: connection.domain?.isEmpty == true
                ? nil
                : connection.domain,
            credentialRef:
                MacProfileCredentialReference.make(for: identity),
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date()
        )
        return try persistRemoteProfile(
            &record,
            credential: password
        )
    }

    @discardableResult
    func saveWebDAVProfile(
        name: String,
        scheme: String,
        host: String,
        port: Int,
        mountPath: String,
        basePath: String,
        username: String,
        password: String
    ) throws -> ServerProfileRecord {
        let connection = try CanonicalWebDAVConnection(
            scheme: scheme,
            host: host,
            port: port,
            mountPath: mountPath,
            basePath: basePath,
            username: username
        )
        let trimmedName = name.trimmingCharacters(
            in: .whitespacesAndNewlines
        )
        let identity = CanonicalProfileConnection
            .webDAV(connection)
            .duplicateIdentity
        let params = WebDAVConnectionParams(
            scheme: connection.scheme.rawValue
        )

        var record = ServerProfileRecord(
            id: nil,
            name: trimmedName.isEmpty
                ? connection.host.socketHost
                : trimmedName,
            storageType: StorageType.webdav.rawValue,
            connectionParams: try ServerProfileRecord
                .encodedConnectionParams(params),
            sortOrder: 0,
            host: connection.host.socketHost,
            port: connection.port.value,
            shareName: connection.mountPath,
            basePath: connection.basePath,
            username: connection.username,
            domain: nil,
            credentialRef:
                MacProfileCredentialReference.make(for: identity),
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date()
        )
        return try persistRemoteProfile(
            &record,
            credential: password
        )
    }

    @discardableResult
    func saveS3Profile(
        name: String,
        scheme: String,
        host: String,
        port: Int,
        region: String,
        bucket: String,
        basePath: String,
        usePathStyle: Bool,
        accessKeyID: String,
        secretAccessKey: String
    ) throws -> ServerProfileRecord {
        let connection = try CanonicalS3Connection(
            scheme: scheme,
            host: host,
            port: port,
            region: region,
            usePathStyle: usePathStyle,
            bucket: bucket,
            basePath: basePath,
            accessKeyID: accessKeyID
        )
        let trimmedName = name.trimmingCharacters(
            in: .whitespacesAndNewlines
        )
        let identity = CanonicalProfileConnection
            .s3(connection)
            .duplicateIdentity
        let params = S3ConnectionParams(
            scheme: connection.endpoint.scheme.rawValue,
            region: connection.resolvedRegion,
            usePathStyle: connection.usePathStyle
        )

        var record = ServerProfileRecord(
            id: nil,
            name: trimmedName.isEmpty
                ? connection.bucket
                : trimmedName,
            storageType: StorageType.s3.rawValue,
            connectionParams: try ServerProfileRecord
                .encodedConnectionParams(params),
            sortOrder: 0,
            host: connection.endpoint.host.socketHost,
            port: connection.endpoint.port.value,
            shareName: connection.bucket,
            basePath: connection.basePrefix,
            username: connection.accessKeyID,
            domain: nil,
            credentialRef:
                MacProfileCredentialReference.make(for: identity),
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date()
        )
        return try persistRemoteProfile(
            &record,
            credential: secretAccessKey
        )
    }

    @discardableResult
    func saveSFTPProfile(
        name: String,
        host: String,
        port: Int,
        basePath: String,
        username: String,
        credential: SFTPCredentialBlob,
        hostKeyFingerprintSHA256: String
    ) throws -> ServerProfileRecord {
        let canonical = try CanonicalSFTPConnection(
            host: host,
            port: port,
            basePath: basePath,
            username: username,
            authMethod: credential.authMethod,
            hostKeyFingerprintSHA256: hostKeyFingerprintSHA256
        )
        let params = SFTPConnectionParams(
            authMethod: credential.authMethod,
            hostKeyFingerprintSHA256: hostKeyFingerprintSHA256
        )
        let identity = CanonicalProfileConnection
            .sftp(canonical)
            .duplicateIdentity
        var record = ServerProfileRecord(
            id: nil,
            name: name.trimmingCharacters(
                in: .whitespacesAndNewlines
            ).isEmpty ? canonical.host.socketHost : name.trimmingCharacters(
                in: .whitespacesAndNewlines
            ),
            storageType: StorageType.sftp.rawValue,
            connectionParams: try ServerProfileRecord
                .encodedConnectionParams(params),
            sortOrder: 0,
            host: canonical.host.socketHost,
            port: canonical.port.value,
            shareName: "",
            basePath: canonical.basePath,
            username: canonical.username,
            domain: nil,
            credentialRef:
                MacProfileCredentialReference.make(for: identity),
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date()
        )
        return try persistRemoteProfile(
            &record,
            credential: try credential.encodedJSONString()
        )
    }

    @discardableResult
    func saveOneDriveProfile(
        name: String,
        connectionParams: OneDriveConnectionParams,
        credentialJSONString: String,
        username: String?
    ) throws -> ServerProfileRecord {
        let canonical = try CanonicalOneDriveConnection(
            params: connectionParams
        )
        let trimmedName = name.trimmingCharacters(
            in: .whitespacesAndNewlines
        )
        let trimmedUsername = username?.trimmingCharacters(
            in: .whitespacesAndNewlines
        )
        var record = ServerProfileRecord(
            id: nil,
            name: trimmedName.isEmpty
                ? String(localized: "auth.onedrive.defaultName")
                : trimmedName,
            storageType: StorageType.onedrive.rawValue,
            connectionParams: try ServerProfileRecord
                .encodedConnectionParams(connectionParams),
            sortOrder: 0,
            host: "graph.microsoft.com",
            port: 443,
            shareName: canonical.rootItemID,
            basePath: "/",
            username: trimmedUsername.flatMap {
                $0.isEmpty ? nil : $0
            } ?? String(
                localized: "auth.onedrive.accountFallback"
            ),
            domain: nil,
            credentialRef:
                MacProfileCredentialReference.make(
                    for: CanonicalProfileConnection
                        .oneDrive(canonical)
                        .duplicateIdentity
                ),
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date()
        )
        return try persistRemoteProfile(
            &record,
            credential: credentialJSONString
        )
    }

    func updateLocalProfile(
        _ profile: ServerProfileRecord,
        folderURL: URL
    ) throws -> ServerProfileRecord {
        let updated = try ExternalStorageProfileSaveWorker.save(
            intent: .init(
                editingProfile: profile,
                selectedDirectoryURL: folderURL,
                name: profile.name
            ),
            databaseManager: databaseManager,
            runtimeFlags: appRuntimeFlags
        )
        reload()
        return updated
    }

    func updateSMBProfile(
        _ profile: ServerProfileRecord,
        context: SMBServerPathContext
    ) throws -> ServerProfileRecord {
        let canonical = try CanonicalSMBConnection(
            host: context.auth.host,
            port: context.auth.port,
            shareName: context.shareName,
            basePath: context.basePath,
            username: context.auth.username,
            domain: context.auth.domain
        )
        var updated = profile
        updated.connectionParams = nil
        updated.host = canonical.host.socketHost
        updated.port = canonical.port.value
        updated.shareName = canonical.shareName
        updated.basePath = canonical.basePath
        updated.username = canonical.username
        updated.domain = canonical.domain?.isEmpty == true
            ? nil
            : canonical.domain
        updated.credentialRef =
            MacProfileCredentialReference.make(
                for: CanonicalProfileConnection
                    .smb(canonical)
                    .duplicateIdentity
            )
        return try persistRemoteProfile(
            &updated,
            credential: context.auth.password,
            replacing: profile
        )
    }

    func updateWebDAVProfile(
        _ profile: ServerProfileRecord,
        snapshot: WebDAVProfileSnapshot
    ) throws -> ServerProfileRecord {
        let canonical = try CanonicalWebDAVConnection(
            scheme: snapshot.scheme,
            host: snapshot.host,
            port: snapshot.port,
            mountPath: snapshot.mountPath,
            basePath: snapshot.basePath,
            username: snapshot.username
        )
        var updated = profile
        updated.connectionParams = try ServerProfileRecord
            .encodedConnectionParams(
                WebDAVConnectionParams(
                    scheme: canonical.scheme.rawValue
                )
            )
        updated.host = canonical.host.socketHost
        updated.port = canonical.port.value
        updated.shareName = canonical.mountPath
        updated.basePath = canonical.basePath
        updated.username = canonical.username
        updated.domain = nil
        updated.credentialRef =
            MacProfileCredentialReference.make(
                for: CanonicalProfileConnection
                    .webDAV(canonical)
                    .duplicateIdentity
            )
        return try persistRemoteProfile(
            &updated,
            credential: snapshot.password,
            replacing: profile
        )
    }

    func updateS3Profile(
        _ profile: ServerProfileRecord,
        snapshot: S3ProfileSnapshot
    ) throws -> ServerProfileRecord {
        let canonical = try CanonicalS3Connection(
            scheme: snapshot.scheme,
            host: snapshot.host,
            port: snapshot.port,
            region: snapshot.region,
            usePathStyle: snapshot.usePathStyle,
            bucket: snapshot.bucket,
            basePath: snapshot.basePath,
            accessKeyID: snapshot.accessKeyID
        )
        var updated = profile
        updated.connectionParams = try ServerProfileRecord
            .encodedConnectionParams(
                S3ConnectionParams(
                    scheme: canonical.endpoint.scheme.rawValue,
                    region: canonical.resolvedRegion,
                    usePathStyle: canonical.usePathStyle
                )
            )
        updated.host = canonical.endpoint.host.socketHost
        updated.port = canonical.endpoint.port.value
        updated.shareName = canonical.bucket
        updated.basePath = canonical.basePrefix
        updated.username = canonical.accessKeyID
        updated.domain = nil
        updated.credentialRef =
            MacProfileCredentialReference.make(
                for: CanonicalProfileConnection
                    .s3(canonical)
                    .duplicateIdentity
            )
        return try persistRemoteProfile(
            &updated,
            credential: snapshot.secretAccessKey,
            replacing: profile
        )
    }

    func updateSFTPProfile(
        _ profile: ServerProfileRecord,
        host: String,
        port: Int,
        basePath: String,
        username: String,
        credential: SFTPCredentialBlob,
        hostKeyFingerprintSHA256: String
    ) throws -> ServerProfileRecord {
        let canonical = try CanonicalSFTPConnection(
            host: host,
            port: port,
            basePath: basePath,
            username: username,
            authMethod: credential.authMethod,
            hostKeyFingerprintSHA256: hostKeyFingerprintSHA256
        )
        var updated = profile
        updated.connectionParams = try ServerProfileRecord
            .encodedConnectionParams(
                SFTPConnectionParams(
                    authMethod: credential.authMethod,
                    hostKeyFingerprintSHA256:
                        hostKeyFingerprintSHA256
                )
            )
        updated.host = canonical.host.socketHost
        updated.port = canonical.port.value
        updated.shareName = ""
        updated.basePath = canonical.basePath
        updated.username = canonical.username
        updated.domain = nil
        updated.credentialRef =
            MacProfileCredentialReference.make(
                for: CanonicalProfileConnection
                    .sftp(canonical)
                    .duplicateIdentity
            )
        return try persistRemoteProfile(
            &updated,
            credential: try credential.encodedJSONString(),
            replacing: profile
        )
    }

    func updateOneDriveProfile(
        _ profile: ServerProfileRecord,
        connectionParams: OneDriveConnectionParams,
        credentialJSONString: String,
        username: String?
    ) throws -> ServerProfileRecord {
        let canonical = try CanonicalOneDriveConnection(
            params: connectionParams
        )
        let originalCredential = try? keychainService.readPassword(
            account: profile.credentialRef
        )
        let trimmedUsername = username?.trimmingCharacters(
            in: .whitespacesAndNewlines
        )
        var updated = profile
        updated.connectionParams = try ServerProfileRecord
            .encodedConnectionParams(connectionParams)
        updated.host = "graph.microsoft.com"
        updated.port = 443
        updated.shareName = canonical.rootItemID
        updated.basePath = "/"
        updated.username = trimmedUsername.flatMap {
            $0.isEmpty ? nil : $0
        } ?? profile.username
        updated.domain = nil
        updated.credentialRef =
            MacProfileCredentialReference.make(
                for: CanonicalProfileConnection
                    .oneDrive(canonical)
                    .duplicateIdentity
            )
        let saved = try persistRemoteProfile(
            &updated,
            credential: credentialJSONString,
            replacing: profile
        )
        if let originalCredential,
           originalCredential != credentialJSONString {
            oneDriveCredentialLifecycleService?
                .removeCachedAccountIfUnused(
                    credentialJSONString: originalCredential
                )
        }
        return saved
    }

    func renameProfile(id: Int64, newName: String) throws {
        guard let index = profiles.firstIndex(where: { $0.id == id }) else { return }
        var record = profiles[index]
        let trimmed = newName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }
        record.name = trimmed
        try databaseManager.saveServerProfile(&record)
        reload()
    }

    func password(for profile: ServerProfileRecord) throws -> String {
        guard !profile.credentialRef.isEmpty else { return "" }
        return try keychainService.readPassword(account: profile.credentialRef)
    }

    private func persistRemoteProfile(
        _ profile: inout ServerProfileRecord,
        credential: String
    ) throws -> ServerProfileRecord {
        try persistRemoteProfile(
            &profile,
            credential: credential,
            replacing: nil
        )
    }

    private func persistRemoteProfile(
        _ profile: inout ServerProfileRecord,
        credential: String,
        replacing oldProfile: ServerProfileRecord?
    ) throws -> ServerProfileRecord {
        try databaseManager.validateConnectionProfileSave(
            profile,
            editingProfileID: oldProfile?.id
        )
        let previousCredential = try? keychainService.readPassword(
            account: profile.credentialRef
        )
        try keychainService.save(
            password: credential,
            account: profile.credentialRef
        )
        do {
            try databaseManager.saveConnectionProfile(
                &profile,
                editingProfileID: oldProfile?.id
            )
        } catch {
            if let previousCredential {
                try? keychainService.save(
                    password: previousCredential,
                    account: profile.credentialRef
                )
            } else {
                try? keychainService.delete(
                    account: profile.credentialRef
                )
            }
            throw error
        }
        if let oldCredentialRef = oldProfile?.credentialRef,
           oldCredentialRef != profile.credentialRef {
            let stillUsed = try databaseManager.fetchServerProfiles()
                .contains {
                    $0.credentialRef == oldCredentialRef
                }
            if !stillUsed {
                try? keychainService.delete(
                    account: oldCredentialRef
                )
            }
        }
        reload()
        return profile
    }

    // MARK: - Legacy folder path per profile (sync_state key-value, no schema change)

    private func legacyFolderPathKey(profileID: Int64) -> String {
        "mac.legacyFolderPath.\(profileID)"
    }

    func saveLegacyFolderPath(profileID: Int64, path: String) throws {
        try databaseManager.setSyncState(key: legacyFolderPathKey(profileID: profileID), value: path)
    }

    func clearLegacyFolderPath(profileID: Int64) {
        try? databaseManager.setSyncState(key: legacyFolderPathKey(profileID: profileID), value: "")
    }

    func loadLegacyFolderPath(profileID: Int64) -> String? {
        guard let value = try? databaseManager.syncStateValue(for: legacyFolderPathKey(profileID: profileID)),
              !value.isEmpty else { return nil }
        return value
    }
}

private extension SFTPCredentialBlob {
    var authMethod: SFTPConnectionParams.AuthMethod {
        switch self {
        case .password:
            return .password
        case .privateKey:
            return .privateKey
        }
    }
}

#if DEBUG
private extension ServerProfileRecord {
    static let macDemoDestination: ServerProfileRecord = {
        let params = S3ConnectionParams(
            scheme: "https",
            region: "us-east-1",
            usePathStyle: true
        )
        return ServerProfileRecord(
            id: -9_001,
            name: "Studio Archive",
            storageType: StorageType.s3.rawValue,
            connectionParams: try? encodedConnectionParams(params),
            sortOrder: 0,
            host: "photos.example.net",
            port: 443,
            shareName: "watermelon-backup",
            basePath: "Family Photos",
            username: "WM-DEMO-ACCESS",
            domain: nil,
            credentialRef: "mac-demo-destination",
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date()
        )
    }()
}
#endif
