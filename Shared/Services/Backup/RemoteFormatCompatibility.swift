import Foundation
import os.log

private let remoteFormatLog = Logger(subsystem: "com.zizicici.watermelon", category: "RemoteFormatCompatibility")

enum WatermelonRemoteFormat {
    static let markerDirectoryName = ".watermelon"
    static let versionFileName = "version.json"
}

struct WatermelonRemoteVersionManifest: Decodable, Sendable {
    let formatVersion: Int?
    let minAppVersion: String?
    let createdAt: String?
    let createdBy: String?

    enum CodingKeys: String, CodingKey {
        case formatVersion = "format_version"
        case minAppVersion = "min_app_version"
        case createdAt = "created_at"
        case createdBy = "created_by"
    }
}

enum BackupCompatibilityError: LocalizedError {
    case remoteFormatUnsupported(minAppVersion: String?)
    case repoIdentityMismatch
    case requiresForegroundMigration
    case repoFormatRegression
    case damagedV2Repo

    var errorDescription: String? {
        switch self {
        case .remoteFormatUnsupported(let minAppVersion):
            if let minAppVersion {
                return String.localizedStringWithFormat(
                    String(localized: "compatibility.error.remoteFormatUnsupported.versioned"),
                    AppName.localized,
                    minAppVersion
                )
            }
            return String.localizedStringWithFormat(
                String(localized: "compatibility.error.remoteFormatUnsupported"),
                AppName.localized
            )
        case .repoIdentityMismatch:
            return String(localized: "compatibility.error.repoIdentityMismatch")
        case .requiresForegroundMigration:
            return String(localized: "compatibility.error.requiresForegroundMigration")
        case .repoFormatRegression:
            return String(localized: "compatibility.error.repoFormatRegression")
        case .damagedV2Repo:
            return String(localized: "compatibility.error.damagedV2Repo")
        }
    }
}

enum RemoteFormatInspection: Equatable, Sendable {
    case fresh
    case v1
    case v2(formatVersion: Int)
    case v2WithV1Manifests(formatVersion: Int)
    case v2WithPendingMigrationCleanup(formatVersion: Int, ownerWriterID: String)
    case unsupported(minAppVersion: String?)
}

struct RemoteFormatCompatibilityService: Sendable {
    init() {}

    func verify(client: any RemoteStorageClientProtocol, profile: ServerProfileRecord) async throws {
        let basePath = RemotePathBuilder.normalizePath(profile.basePath)
        let entries = try await client.list(path: basePath)
        let markerExists = entries.contains { entry in
            entry.isDirectory && entry.name == WatermelonRemoteFormat.markerDirectoryName
        }
        guard markerExists else { return }

        let detected = await readMinAppVersion(client: client, profile: profile)
        throw BackupCompatibilityError.remoteFormatUnsupported(minAppVersion: detected)
    }

    func inspectRemoteFormat(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord
    ) async throws -> RemoteFormatInspection {
        let basePath = RemotePathBuilder.normalizePath(profile.basePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: basePath)
        } catch {
            throw error
        }

        let markerExists = entries.contains { entry in
            entry.isDirectory && entry.name == WatermelonRemoteFormat.markerDirectoryName
        }

        if markerExists {
            let manifest = try await loadVersionManifestStrict(client: client, profile: profile)
            if case .found(let preCheck) = manifest {
                let preVersion = preCheck.formatVersion ?? 0
                if preVersion > RepoLayout.currentSupportedFormatVersion {
                    return .unsupported(minAppVersion: preCheck.minAppVersion)
                }
            }
            let migrationMarkers = try await listMigrationMarkers(client: client, basePath: basePath)
            let migrationInProgress = !migrationMarkers.isEmpty
            var cachedV1Manifests: Bool?
            func hasV1Manifests() async throws -> Bool {
                if let cachedV1Manifests { return cachedV1Manifests }
                let detected = try await detectV1Manifests(client: client, basePath: basePath, entries: entries)
                cachedV1Manifests = detected
                return detected
            }
            switch manifest {
            case .absent:
                let v1Manifests = try await hasV1Manifests()
                let hasV2Data = try await detectV2DataDirectories(client: client, basePath: basePath)
                if hasV2Data {
                    if v1Manifests {
                        return .v2WithV1Manifests(formatVersion: RepoLayout.formatVersion)
                    }
                    if migrationInProgress {
                        let markerStates = try await inspectMigrationMarkers(
                            client: client,
                            basePath: basePath,
                            markers: migrationMarkers
                        )
                        if let marker = markerStates.first {
                            return .v2WithPendingMigrationCleanup(
                                formatVersion: RepoLayout.formatVersion,
                                ownerWriterID: marker.writerID
                            )
                        }
                    }
                    throw BackupCompatibilityError.damagedV2Repo
                }
                if v1Manifests { return .v1 }
                if migrationInProgress {
                    return .fresh
                }
                return .fresh
            case .found(let manifest):
                let formatVersion = manifest.formatVersion ?? 0
                if formatVersion >= 2 && formatVersion <= RepoLayout.currentSupportedFormatVersion {
                    let markerStates = try await inspectMigrationMarkers(client: client, basePath: basePath, markers: migrationMarkers)
                    if try await hasV1Manifests() {
                        return .v2WithV1Manifests(formatVersion: formatVersion)
                    }
                    if let cleanup = markerStates.first(where: { $0.phase.isCleanupSafe }) {
                        return .v2WithPendingMigrationCleanup(formatVersion: formatVersion, ownerWriterID: cleanup.writerID)
                    }
                    if let residue = markerStates.first {
                        // With no V1 manifests left, any marker only represents cleanup residue.
                        return .v2WithPendingMigrationCleanup(formatVersion: formatVersion, ownerWriterID: residue.writerID)
                    }
                    return .v2(formatVersion: formatVersion)
                }
                return .unsupported(minAppVersion: manifest.minAppVersion)
            }
        }

        if try await detectV1Manifests(client: client, basePath: basePath, entries: entries) {
            return .v1
        }
        return .fresh
    }

    private func inspectMigrationMarkers(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        markers: [RemoteStorageEntry]
    ) async throws -> [ParsedMigrationMarker] {
        let dir = RepoLayout.migrationsDirectoryPath(base: basePath)
        var results: [ParsedMigrationMarker] = []
        for entry in markers where !entry.isDirectory && entry.name.hasSuffix(".json") {
            let path = RemotePathBuilder.absolutePath(basePath: dir, remoteRelativePath: entry.name)
            let temp = FileManager.default.temporaryDirectory
                .appendingPathComponent("migration-marker-detect-\(UUID().uuidString).json")
            defer { try? FileManager.default.removeItem(at: temp) }
            let data: Data
            do {
                try await client.download(remotePath: path, localURL: temp)
                data = try Data(contentsOf: temp)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                if !isNotFoundError(error) { throw error }
                continue
            }
            do {
                // Adopting a hijacked/malformed marker would loop cleanup at a foreign writerID forever.
                let parsed = try MigrationMarker.parse(filename: entry.name, bytes: data)
                results.append(parsed)
            } catch {
                remoteFormatLog.warning(
                    "skipping migration marker at \(path, privacy: .public): \(String(describing: error), privacy: .public)"
                )
                continue
            }
        }
        return results
    }

    /// Fail-closed: list errors throw so a network blip can't be misread as "no migration".
    private func listMigrationMarkers(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> [RemoteStorageEntry] {
        let dir = RepoLayout.migrationsDirectoryPath(base: basePath)
        do {
            return try await client.list(path: dir)
        } catch {
            if isNotFoundError(error) { return [] }
            throw error
        }
    }

    /// Bootstrap-side guard: minting a new repoID over existing V2 data would orphan it.
    func hasAnyV2CommitOrSnapshotData(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> Bool {
        try await detectV2DataDirectories(client: client, basePath: basePath)
    }

    /// Fail-closed: non-not-found errors throw so a 401 can't mint `.fresh` over a real V2 repo.
    private func detectV2DataDirectories(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> Bool {
        for subdir in [RepoLayout.commitsDirectory, RepoLayout.snapshotsDirectory] {
            let path = RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory, subdir])
            do {
                let entries = try await client.list(path: path)
                if !entries.isEmpty { return true }
            } catch {
                if isNotFoundError(error) { continue }
                throw error
            }
        }
        return false
    }

    private func isNotFoundError(_ error: Error) -> Bool {
        isStorageNotFoundError(error)
    }

    private func detectV1Manifests(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        entries: [RemoteStorageEntry]
    ) async throws -> Bool {
        let yearEntries = entries
            .filter { $0.isDirectory && $0.name.range(of: "^[0-9]{4}$", options: .regularExpression) != nil }
            .sorted(by: { $0.name > $1.name })
        for yearEntry in yearEntries {
            let yearPath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: yearEntry.name)
            let monthEntries = try await client.list(path: yearPath)
            for monthEntry in monthEntries where monthEntry.isDirectory && monthEntry.name.range(of: "^[0-9]{2}$", options: .regularExpression) != nil {
                let monthPath = RemotePathBuilder.absolutePath(basePath: yearPath, remoteRelativePath: monthEntry.name)
                let monthContents = try await client.list(path: monthPath)
                if monthContents.contains(where: { !$0.isDirectory && $0.name == MonthManifestStore.manifestFileName }) {
                    return true
                }
            }
        }
        return false
    }

    enum VersionManifestLoad: Sendable {
        case absent
        case found(WatermelonRemoteVersionManifest)
    }

    /// `.absent` only on confirmed not-found; transport/parse errors throw so callers can't conflate "missing" with "couldn't read".
    private func loadVersionManifestStrict(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord
    ) async throws -> VersionManifestLoad {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("watermelon-version-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: tempURL) }

        let absolutePath = RemotePathBuilder.absolutePath(
            basePath: profile.basePath,
            remoteRelativePath: "\(WatermelonRemoteFormat.markerDirectoryName)/\(WatermelonRemoteFormat.versionFileName)"
        )
        let entry: RemoteStorageEntry?
        do {
            entry = try await client.metadata(path: absolutePath)
        } catch {
            if isNotFoundError(error) { return .absent }
            throw error
        }
        guard let entry else {
            return .absent
        }
        guard !entry.isDirectory else {
            throw BackupCompatibilityError.damagedV2Repo
        }
        try await client.download(remotePath: absolutePath, localURL: tempURL)
        let data = try Data(contentsOf: tempURL)
        let manifest = try JSONDecoder().decode(WatermelonRemoteVersionManifest.self, from: data)
        return .found(manifest)
    }

    private func loadVersionManifest(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord
    ) async -> WatermelonRemoteVersionManifest? {
        do {
            switch try await loadVersionManifestStrict(client: client, profile: profile) {
            case .absent: return nil
            case .found(let manifest): return manifest
            }
        } catch {
            return nil
        }
    }

    private func readMinAppVersion(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord
    ) async -> String? {
        let manifest = await loadVersionManifest(client: client, profile: profile)
        if let version = manifest?.minAppVersion?.trimmingCharacters(in: .whitespacesAndNewlines),
           !version.isEmpty {
            return version
        }
        return nil
    }
}
