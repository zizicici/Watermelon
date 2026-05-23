import Foundation

enum BackupCompatibilityError: LocalizedError {
    case remoteFormatUnsupported(minAppVersion: String?)
    case repoIdentityMismatch(stored: String?, observed: String?)
    case requiresForegroundMigration
    case repoFormatRegression(repoID: String?)
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
            entry.isDirectory && entry.name == RepoLayout.watermelonDirectory
        }
        guard markerExists else { return }

        let detected = await readMinAppVersion(client: client, basePath: basePath)
        throw BackupCompatibilityError.remoteFormatUnsupported(minAppVersion: detected)
    }

    func inspectRemoteFormat(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord
    ) async throws -> RemoteFormatInspection {
        try await RepoBootstrapInspectionFSM().inspect(client: client, profile: profile)
    }

    /// Bootstrap-side guard: minting a new repoID over existing V2 data would orphan it.
    func hasAnyV2CommitOrSnapshotData(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> Bool {
        try await RepoBootstrapInspectionFSM.hasAnyV2CommitOrSnapshotData(client: client, basePath: basePath)
    }

    private func readMinAppVersion(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async -> String? {
        let manifest = await VersionManifestStore(client: client, basePath: basePath).loadOrNil()
        if let version = manifest?.minAppVersion?.trimmingCharacters(in: .whitespacesAndNewlines),
           !version.isEmpty {
            return version
        }
        return nil
    }
}
