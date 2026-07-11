import Foundation

enum SMBProfileSaver {
    static func makeProfile(
        context: SMBServerPathContext,
        editingProfile: ServerProfileRecord?,
        name: String
    ) throws -> ServerProfileRecord {
        let finalName = name.trimmingCharacters(in: .whitespacesAndNewlines)
        let profileName = editingProfile?.name ?? (finalName.isEmpty ? context.auth.host : finalName)
        let connection = try CanonicalSMBConnection(
            host: context.auth.host,
            port: context.auth.port,
            shareName: context.shareName,
            basePath: context.basePath,
            username: context.auth.username,
            domain: context.auth.domain
        )
        let identity = CanonicalProfileConnection.smb(connection).duplicateIdentity
        let credentialRef = StorageProfilePersistence.credentialRef(for: identity)

        return ServerProfileRecord(
            id: editingProfile?.id,
            name: profileName,
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: editingProfile?.sortOrder ?? 0,
            host: connection.host.socketHost,
            port: connection.port.value,
            shareName: connection.shareName,
            basePath: connection.basePath,
            username: connection.username,
            domain: connection.domain,
            credentialRef: credentialRef,
            backgroundBackupEnabled: editingProfile?.backgroundBackupEnabled ?? false,
            backgroundBackupMinIntervalMinutes: editingProfile?.backgroundBackupMinIntervalMinutes ?? BackgroundBackupInterval.default.minutes,
            backgroundBackupRequiresWiFi: editingProfile?.backgroundBackupRequiresWiFi ?? true,
            generateRemoteThumbnails: editingProfile?.generateRemoteThumbnails ?? false,
            createdAt: editingProfile?.createdAt ?? Date(),
            updatedAt: Date()
        )

    }
}
