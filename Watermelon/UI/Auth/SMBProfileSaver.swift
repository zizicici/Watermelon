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
        let host = RemoteHostIdentity.canonicalSMB(context.auth.host)
        let normalizedPath = RemotePathBuilder.normalizePath(context.basePath)
        try ensureNoDuplicate(
            dependencies: dependencies,
            context: context,
            editingProfile: editingProfile
        )
        let credentialRef = StorageProfilePersistence.credentialRef(
            storageType: .smb,
            identityFields: [
            host,
            String(context.auth.port),
            context.shareName,
            context.auth.domain ?? "",
            context.auth.username,
            normalizedPath
            ]
        )

        var profile = ServerProfileRecord(
            id: editingProfile?.id,
            name: profileName,
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: editingProfile?.sortOrder ?? 0,
            host: host,
            port: context.auth.port,
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
        let host = RemoteHostIdentity.canonicalSMB(context.auth.host)
        let normalizedPath = RemotePathBuilder.normalizePath(context.basePath)
        let existing = try dependencies.databaseManager.findServerProfile(
            host: host,
            port: context.auth.port,
            shareName: context.shareName,
            basePath: normalizedPath,
            username: context.auth.username,
            domain: context.auth.domain
        )

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
