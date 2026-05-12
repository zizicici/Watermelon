import Foundation

enum WatermelonRemoteFormat {
    static let markerDirectoryName = ".watermelon"
    static let versionFileName = "version.json"
}

struct WatermelonRemoteVersionManifest: Decodable {
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
    case unsupported(minAppVersion: String?)
}

actor RemoteFormatCompatibilityService {
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
            // version.json is checked first so a stale migration marker on a peer-upgraded
            // V3+ repo can't trick us into writing V2 over it.
            if case .found(let preCheck) = try await loadVersionManifestStrict(client: client, profile: profile) {
                let preVersion = preCheck.formatVersion ?? 0
                if preVersion > RepoLayout.currentSupportedFormatVersion {
                    return .unsupported(minAppVersion: preCheck.minAppVersion)
                }
            }
            // Migration marker only forces `.v1` when V1 manifests still exist; stale marker on a healthy V2 falls through.
            if try await detectMigrationInProgress(client: client, basePath: basePath) {
                if try await detectV1Manifests(client: client, basePath: basePath, entries: entries) {
                    return .v1
                }
            }
            // Surface transport errors; `.unsupported` would lock the repo on a network blip.
            let manifest = try await loadVersionManifestStrict(client: client, profile: profile)
            switch manifest {
            case .absent:
                // Marker + no version.json: either fresh-bootstrap crashed between mkdir
                // and version.json (re-bootstrap heals), or V1-migration crashed between
                // phase1 and phase2 (must resume V1, not fresh-bootstrap, or data unmigrated).
                if try await detectV1Manifests(client: client, basePath: basePath, entries: entries) {
                    return .v1
                }
                // Distinguish empty marker (fresh-bootstrap retry) from damaged V2
                // (commits/snapshots survived but identity files vanished). Treating
                // the damaged case as fresh would mint a new repoID and orphan all
                // pre-existing commits.
                if try await detectV2DataDirectories(client: client, basePath: basePath) {
                    throw BackupCompatibilityError.damagedV2Repo
                }
                return .fresh
            case .found(let manifest):
                let formatVersion = manifest.formatVersion ?? 0
                // Higher than `currentSupportedFormatVersion` means a peer wrote
                // something we don't understand; refuse to overlay our writes
                // on it. v2 additive fields stay forward-compatible across
                // v2-development cycles without bumping the format version.
                if formatVersion >= 2 && formatVersion <= RepoLayout.currentSupportedFormatVersion {
                    // Older clients that don't understand V2 may still write V1 manifests
                    // into a repo that has .watermelon/version.json. Route .v1 so builder
                    // re-runs the idempotent migration phases over the new V1 data.
                    if try await detectV1Manifests(client: client, basePath: basePath, entries: entries) {
                        return .v1
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

    /// True if `migrations/<*>.json` exists. Fail-closed: list transport errors must
    /// throw, not be swallowed as "no migration", or a network blip would let us
    /// route a half-migrated repo through the wrong path.
    private func detectMigrationInProgress(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> Bool {
        let dir = RepoLayout.migrationsDirectoryPath(base: basePath)
        do {
            let entries = try await client.list(path: dir)
            return entries.contains { !$0.isDirectory && $0.name.hasSuffix(".json") }
        } catch {
            if isNotFoundError(error) { return false }
            throw error
        }
    }

    /// True if `commits/` or `snapshots/` has any entries. Fail-closed: non-not-found
    /// errors throw — silently treating a 401 as "no V2 data" would let inspect mint
    /// `.fresh` over an existing V2 repo with intact commits, orphaning them.
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
        // Surface list errors — a transient failure misclassified as fresh would bootstrap V2 over a real V1 repo.
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

    /// `.absent` only on confirmed not-found; transport/parse errors throw so callers
    /// don't conflate "missing" with "couldn't read".
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
        guard let entry = try await client.metadata(path: absolutePath), !entry.isDirectory else {
            return .absent
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
