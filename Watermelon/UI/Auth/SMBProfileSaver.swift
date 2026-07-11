import Foundation

enum SMBProfileSaver {
    static func save(
        dependencies: DependencyContainer,
        context: SMBServerPathContext,
        editingProfile: ServerProfileRecord?,
        name: String
    ) throws -> (ServerProfileRecord, String) {
        let finalName = name.trimmingCharacters(in: .whitespacesAndNewlines)
        let profileName = editingProfile?.name ?? (finalName.isEmpty ? context.auth.host : finalName)
        guard let host = RemoteHostEndpoint.socketHost(context.auth.host, strippingSMBScheme: true) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let port = SMBEndpoint.effectivePort(context.auth.port)
        let normalizedPath = RemotePathBuilder.normalizePath(context.basePath)
        try ensureNoDuplicate(
            dependencies: dependencies,
            context: context,
            editingProfile: editingProfile
        )
        let credentialRef = StorageProfilePersistence.credentialRef(
            for: ProfileDuplicateIdentity.smb(
                host: host,
                port: port,
                shareName: context.shareName,
                basePath: normalizedPath,
                username: context.auth.username,
                domain: context.auth.domain
            )
        )

        var profile = ServerProfileRecord(
            id: editingProfile?.id,
            name: profileName,
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: editingProfile?.sortOrder ?? 0,
            host: host,
            port: port,
            shareName: context.shareName,
            basePath: normalizedPath,
            username: context.auth.username,
            domain: context.auth.domain,
            credentialRef: credentialRef,
            backgroundBackupEnabled: editingProfile?.backgroundBackupEnabled ?? false,
            backgroundBackupMinIntervalMinutes: editingProfile?.backgroundBackupMinIntervalMinutes ?? BackgroundBackupInterval.default.minutes,
            backgroundBackupRequiresWiFi: editingProfile?.backgroundBackupRequiresWiFi ?? true,
            generateRemoteThumbnails: editingProfile?.generateRemoteThumbnails ?? false,
            createdAt: editingProfile?.createdAt ?? Date(),
            updatedAt: Date()
        )

        try StorageProfilePersistence.saveRemoteProfile(
            dependencies: dependencies,
            profile: &profile,
            credential: context.auth.password,
            replacing: editingProfile
        )
        return (profile, context.auth.password)
    }

    static func ensureNoDuplicate(
        dependencies: DependencyContainer,
        context: SMBServerPathContext,
        editingProfile: ServerProfileRecord?
    ) throws {
        let expected = ProfileDuplicateIdentity.smb(
            host: context.auth.host,
            port: context.auth.port,
            shareName: context.shareName,
            basePath: context.basePath,
            username: context.auth.username,
            domain: context.auth.domain
        )
        let existing = try dependencies.databaseManager.fetchServerProfiles().first {
            $0.id != editingProfile?.id && $0.duplicateIdentity == expected
        }

        if let duplicate = existing,
           editingProfile == nil || duplicate.id != editingProfile?.id {
            throw NSError(
                domain: "SMBProfileSaver",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.smb.save.duplicateConfig")]
            )
        }
    }
}
