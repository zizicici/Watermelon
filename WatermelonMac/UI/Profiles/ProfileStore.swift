import Combine
import Foundation

@MainActor
final class ProfileStore: ObservableObject {
    @Published private(set) var profiles: [ServerProfileRecord] = []
    @Published var loadError: Error?

    private let databaseManager: DatabaseManager
    private let keychainService: KeychainService
    private let bookmarkStore = SecurityScopedBookmarkStore()

    init(databaseManager: DatabaseManager, keychainService: KeychainService) {
        self.databaseManager = databaseManager
        self.keychainService = keychainService
        reload()
    }

    func reload() {
        do {
            profiles = try databaseManager.fetchServerProfiles()
            loadError = nil
        } catch {
            profiles = []
            loadError = error
        }
    }

    func saveLocalProfile(name: String, folderURL: URL) throws -> ServerProfileRecord {
        let trimmedName = name.trimmingCharacters(in: .whitespacesAndNewlines)
        let resolvedName = trimmedName.isEmpty ? folderURL.lastPathComponent : trimmedName

        let bookmark = try bookmarkStore.makeBookmarkData(for: folderURL)
        let params = ExternalVolumeConnectionParams(
            rootBookmarkData: bookmark,
            displayPath: folderURL.path
        )
        let connectionParams = try JSONEncoder().encode(params)

        var record = ServerProfileRecord(
            id: nil,
            name: resolvedName,
            storageType: StorageType.externalVolume.rawValue,
            connectionParams: connectionParams,
            sortOrder: 0,
            host: "",
            port: 0,
            shareName: "",
            basePath: folderURL.path,
            username: "",
            domain: nil,
            credentialRef: "",
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date()
        )
        try databaseManager.saveServerProfile(&record)
        reload()
        return record
    }

    func deleteProfile(id: Int64) throws {
        if let profile = profiles.first(where: { $0.id == id }),
           !profile.credentialRef.isEmpty {
            try? keychainService.delete(account: profile.credentialRef)
        }
        try databaseManager.deleteServerProfile(id: id)
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
        let normalizedBase = RemotePathBuilder.normalizePath(basePath)
        let resolvedName = name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            ? host
            : name.trimmingCharacters(in: .whitespacesAndNewlines)

        let credentialRef = [
            "smb",
            host,
            String(port),
            shareName,
            domain ?? "",
            username
        ].joined(separator: "|")

        var record = ServerProfileRecord(
            id: nil,
            name: resolvedName,
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: host,
            port: port,
            shareName: shareName,
            basePath: normalizedBase,
            username: username,
            domain: domain?.isEmpty == true ? nil : domain,
            credentialRef: credentialRef,
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date()
        )
        try databaseManager.saveServerProfile(&record)
        try keychainService.save(password: password, account: credentialRef)
        reload()
        return record
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
        let normalizedScheme = scheme.lowercased()
        let normalizedMount = RemotePathBuilder.normalizePath(mountPath)
        let normalizedBase = RemotePathBuilder.normalizePath(basePath)
        let resolvedName = name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            ? host
            : name.trimmingCharacters(in: .whitespacesAndNewlines)

        guard let endpoint = ServerProfileRecord.buildWebDAVEndpointURL(
            scheme: normalizedScheme,
            host: host,
            port: port,
            mountPath: normalizedMount
        ) else {
            throw NSError(
                domain: "ProfileStore",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "Invalid WebDAV endpoint"]
            )
        }

        let credentialRef = "webdav|\(endpoint.absoluteString)|\(username)"
        let params = WebDAVConnectionParams(scheme: normalizedScheme)
        let connectionParams = try ServerProfileRecord.encodedConnectionParams(params)

        var record = ServerProfileRecord(
            id: nil,
            name: resolvedName,
            storageType: StorageType.webdav.rawValue,
            connectionParams: connectionParams,
            sortOrder: 0,
            host: host,
            port: port,
            shareName: normalizedMount,
            basePath: normalizedBase,
            username: username,
            domain: nil,
            credentialRef: credentialRef,
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date()
        )
        try databaseManager.saveServerProfile(&record)
        try keychainService.save(password: password, account: credentialRef)
        reload()
        return record
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

    // MARK: - Legacy source folder bookmark per profile

    private func legacySourceKey(profileID: Int64) -> String {
        "mac.legacySourceBookmark.\(profileID)"
    }

    func saveLegacySource(profileID: Int64, url: URL) throws {
        let bookmark = try bookmarkStore.makeBookmarkData(for: url)
        let encoded = bookmark.base64EncodedString()
        try databaseManager.setSyncState(key: legacySourceKey(profileID: profileID), value: encoded)
    }

    func clearLegacySource(profileID: Int64) {
        try? databaseManager.setSyncState(key: legacySourceKey(profileID: profileID), value: "")
    }

    func resolveLegacySource(profileID: Int64) -> URL? {
        guard let encoded = try? databaseManager.syncStateValue(for: legacySourceKey(profileID: profileID)),
              !encoded.isEmpty,
              let bookmark = Data(base64Encoded: encoded) else {
            return nil
        }
        do {
            let resolved = try bookmarkStore.resolveBookmarkData(bookmark)
            if let refreshed = resolved.refreshedBookmarkData {
                let encodedRefreshed = refreshed.base64EncodedString()
                try? databaseManager.setSyncState(key: legacySourceKey(profileID: profileID), value: encodedRefreshed)
            }
            return resolved.url
        } catch {
            return nil
        }
    }
}
