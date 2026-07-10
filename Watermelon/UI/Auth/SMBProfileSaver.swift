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
        let normalizedPath = RemotePathBuilder.normalizePath(context.basePath)
        let existing = try dependencies.databaseManager.findServerProfile(
            host: context.auth.host,
            port: context.auth.port,
            shareName: context.shareName,
            basePath: normalizedPath,
            username: context.auth.username,
            domain: context.auth.domain
        )

        if let editingProfile,
           let duplicate = existing,
           duplicate.id != editingProfile.id {
            throw NSError(
                domain: "SMBProfileSaver",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.smb.save.duplicateConfig")]
            )
        }

        let credentialRef = [
            "smb",
            context.auth.host,
            String(context.auth.port),
            context.shareName,
            context.auth.domain ?? "",
            context.auth.username
        ].joined(separator: "|")
        let baseProfile = editingProfile ?? existing

        var profile = ServerProfileRecord(
            id: baseProfile?.id,
            name: profileName,
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: baseProfile?.sortOrder ?? 0,
            host: context.auth.host,
            port: context.auth.port,
            shareName: context.shareName,
            basePath: normalizedPath,
            username: context.auth.username,
            domain: context.auth.domain,
            credentialRef: credentialRef,
            backgroundBackupEnabled: baseProfile?.backgroundBackupEnabled ?? false,
            backgroundBackupMinIntervalMinutes: baseProfile?.backgroundBackupMinIntervalMinutes ?? BackgroundBackupInterval.default.minutes,
            backgroundBackupRequiresWiFi: baseProfile?.backgroundBackupRequiresWiFi ?? true,
            generateRemoteThumbnails: baseProfile?.generateRemoteThumbnails ?? false,
            createdAt: baseProfile?.createdAt ?? Date(),
            updatedAt: Date()
        )

        try dependencies.databaseManager.saveServerProfile(&profile)
        try dependencies.keychainService.save(password: context.auth.password, account: credentialRef)
        if let oldRef = baseProfile?.credentialRef,
           oldRef != credentialRef {
            try? dependencies.keychainService.delete(account: oldRef)
        }
        return (profile, context.auth.password)
    }
}
