import Foundation
import Photos

private struct NodeBackupDataSource: Codable {
    var version: Int = 1
    var backup: LocalDataSource?
}

extension ServerProfileRecord {
    var backupDataSourceOverride: LocalDataSource? {
        guard let data = backgroundBackupDataSourceJSON else { return nil }
        let decoder = JSONDecoder()
        if let source = try? decoder.decode(NodeBackupDataSource.self, from: data), source.version == 1 {
            return source.backup
        }
        if let legacy = try? decoder.decode(LocalDataSource.self, from: data) {
            return legacy
        }
        // A damaged selection must never expand to the entire library.
        return LocalDataSource(kind: .albums)
    }

    func defaultBackupDataSource(
        appDefault: LocalDataSource = LocalDataSourceStore.shared.defaultSource
    ) -> LocalDataSource {
        backupDataSourceOverride ?? appDefault
    }

    func encodedBackupDataSource(selecting source: LocalDataSource?) throws -> Data? {
        guard let source else { return nil }
        var seen = Set<String>()
        let albums = source.kind == .albums
            ? source.albums.filter { seen.insert($0.id).inserted }
            : backupDataSourceOverride?.albums ?? []
        let kind = source.kind == .albums && albums.isEmpty ? LocalDataSource.Kind.all : source.kind
        let selection = LocalDataSource(kind: kind, albums: albums)
        let encoder = JSONEncoder()
        encoder.outputFormatting = .sortedKeys
        return try encoder.encode(NodeBackupDataSource(backup: selection))
    }
}

extension DependencyContainer {
    // Single writer for the node override so Home's album repair and the settings page cannot drift.
    @MainActor
    @discardableResult
    func saveNodeBackupDataSource(_ source: LocalDataSource?, profileID: Int64) throws -> ServerProfileRecord {
        let blocked = NSError(domain: "NodeDataSource", code: 1, userInfo: [
            NSLocalizedDescriptionKey: String(localized: "home.alert.maintenanceInProgress")
        ])
        guard !appRuntimeFlags.isExecuting,
              !remoteMaintenanceController.isBusy,
              !appRuntimeFlags.isConnecting(profileID: profileID) else { throw blocked }
        guard let saved = try appRuntimeFlags.withProfileMutationLease(profileID: profileID, {
            guard var latest = try databaseManager.fetchServerProfile(id: profileID) else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            let data = try latest.encodedBackupDataSource(selecting: source)
            try databaseManager.setNodeBackupDataSourceJSON(data, profileID: profileID)
            latest.backgroundBackupDataSourceJSON = data
            appSession.setActiveNodeBackupDataSourceJSON(data, profileID: profileID)
            return latest
        }) else { throw blocked }
        NotificationCenter.default.post(name: .NodeBackupDataSourceChanged, object: nil)
        NotificationCenter.default.post(name: .BackgroundBackupProfileChanged, object: nil)
        return saved
    }
}

extension PhotoLibraryService {
    func nodeDataSourceErrors(for profiles: [ServerProfileRecord]) -> [Int64: LocalDataSourceError] {
        let albumProfiles = profiles.filter { $0.defaultBackupDataSource().kind == .albums }
        guard !albumProfiles.isEmpty else { return [:] }
        let authorization = authorizationStatus()
        let allIDs = Set(albumProfiles.flatMap { $0.defaultBackupDataSource().albums.map(\.id) })
        let existing = authorization == .authorized ? existingUserAlbumIdentifiers(in: allIDs) : []
        var errors: [Int64: LocalDataSourceError] = [:]
        for profile in albumProfiles {
            guard let id = profile.id else { continue }
            let source = profile.defaultBackupDataSource()
            do {
                try LocalDataSourceError.validateAlbums(
                    source.scope.selectedAlbumIdentifiers,
                    authorization: authorization,
                    existing: { existing },
                    names: Dictionary(source.albums.map { ($0.id, $0.name) }, uniquingKeysWith: { _, new in new })
                )
            } catch let error as LocalDataSourceError {
                errors[id] = error
            } catch {}
        }
        return errors
    }
}

struct BackupDataSourceDefaultsTracker {
    private var profileIdentity: String?
    private var source: LocalDataSource

    init(profile: ServerProfileRecord? = nil, source: LocalDataSource) {
        profileIdentity = profile?.runtimeConnectionIdentity
        self.source = source
    }

    mutating func changedSource(profile: ServerProfileRecord?) -> LocalDataSource? {
        let next = profile?.defaultBackupDataSource() ?? LocalDataSourceStore.shared.defaultSource
        let identity = profile?.runtimeConnectionIdentity
        guard identity != profileIdentity || next != source else { return nil }
        profileIdentity = identity
        source = next
        return next
    }
}
