import Foundation

struct RepoBootstrapInspectionFSM: Sendable {
    init() {}

    func inspect(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord
    ) async throws -> RemoteFormatInspection {
        let basePath = RemotePathBuilder.normalizePath(profile.basePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: basePath)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }
        let markerExists = entries.contains { entry in
            entry.isDirectory && entry.name == RepoLayout.watermelonDirectory
        }

        guard markerExists else {
            let hasV1 = try await Self.detectV1Manifests(client: client, basePath: basePath, entries: entries)
            return hasV1 ? .v1 : .fresh
        }

        return try await inspectMarkerPresent(
            client: client,
            basePath: basePath,
            entries: entries
        )
    }

    static func hasAnyV2CommitOrSnapshotData(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> Bool {
        try await detectV2DataDirectories(client: client, basePath: basePath)
    }

    private func inspectMarkerPresent(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        entries: [RemoteStorageEntry]
    ) async throws -> RemoteFormatInspection {
        let manifest: VersionManifestStore.Load
        do {
            manifest = try await VersionManifestStore(client: client, basePath: basePath).load()
        } catch is RepoBootstrap.VersionConflict {
            throw BackupCompatibilityError.damagedV2Repo
        } catch let bootstrap as RepoBootstrap.BootstrapError {
            if case .ioFailure = bootstrap { throw BackupCompatibilityError.damagedV2Repo }
            throw bootstrap
        }

        if case .found(let preCheck) = manifest,
           preCheck.formatVersion > RepoLayout.currentSupportedFormatVersion {
            return .unsupported(minAppVersion: preCheck.minAppVersion)
        }

        let markerStore = MigrationMarkerStore(client: client, basePath: basePath)
        let migrationDirEntries = try await markerStore.migrationsDirectoryEntries()
        let migrationInProgress = migrationDirEntries.contains { $0.name.hasSuffix(".json") }

        var cachedV1Manifests: Bool?
        func hasV1Manifests() async throws -> Bool {
            if let cachedV1Manifests { return cachedV1Manifests }
            let detected = try await Self.detectV1Manifests(client: client, basePath: basePath, entries: entries)
            cachedV1Manifests = detected
            return detected
        }

        switch manifest {
        case .absent:
            return try await inspectVersionAbsent(
                client: client,
                basePath: basePath,
                markerStore: markerStore,
                migrationDirEntries: migrationDirEntries,
                migrationInProgress: migrationInProgress,
                hasV1Manifests: hasV1Manifests
            )
        case .found(let manifest):
            return try await inspectVersionFound(
                manifest: manifest,
                markerStore: markerStore,
                migrationDirEntries: migrationDirEntries,
                hasV1Manifests: hasV1Manifests
            )
        }
    }

    private func inspectVersionAbsent(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        markerStore: MigrationMarkerStore,
        migrationDirEntries: [RemoteStorageEntry],
        migrationInProgress: Bool,
        hasV1Manifests: () async throws -> Bool
    ) async throws -> RemoteFormatInspection {
        let v1Manifests = try await hasV1Manifests()
        let hasV2Data = try await Self.detectV2DataDirectories(client: client, basePath: basePath)

        if hasV2Data {
            if v1Manifests {
                if migrationDirEntries.contains(where: { $0.isDirectory && $0.name.hasSuffix(".json") }) {
                    throw BackupCompatibilityError.damagedV2Repo
                }
                if migrationInProgress {
                    _ = try await parseMarkerEntriesFailingClosed(
                        markerStore: markerStore,
                        migrationDirEntries: migrationDirEntries
                    )
                }
                return .v2WithV1Manifests(formatVersion: RepoLayout.formatVersion)
            }
            if migrationInProgress {
                let markerStates = try await parseMarkerEntriesFailingClosed(
                    markerStore: markerStore,
                    migrationDirEntries: migrationDirEntries
                )
                let ordered = Self.sortedMarkers(markerStates)
                // Directory .json markers alongside parseable ones leave cleanup with
                // an incomplete marker set — fail closed.
                if migrationDirEntries.contains(where: { $0.isDirectory && $0.name.hasSuffix(".json") }) {
                    throw BackupCompatibilityError.damagedV2Repo
                }
                if let marker = ordered.first {
                    return .v2WithPendingMigrationCleanup(
                        formatVersion: RepoLayout.formatVersion,
                        ownerWriterID: marker.writerID
                    )
                }
            }
            throw BackupCompatibilityError.damagedV2Repo
        }
        if migrationDirEntries.contains(where: { $0.isDirectory && $0.name.hasSuffix(".json") }) {
            throw BackupCompatibilityError.damagedV2Repo
        }
        let fileMarkers = migrationDirEntries.filter {
            !$0.isDirectory && $0.name.hasSuffix(".json")
        }
        if !fileMarkers.isEmpty {
            do {
                _ = try await markerStore.parseEntries(migrationDirEntries)
            } catch is MigrationMarkerStore.InvalidMarker {
                throw BackupCompatibilityError.damagedV2Repo
            } catch is CancellationError {
                throw CancellationError()
            }
        }
        if v1Manifests { return .v1 }
        return .fresh
    }

    private func inspectVersionFound(
        manifest: VersionManifest,
        markerStore: MigrationMarkerStore,
        migrationDirEntries: [RemoteStorageEntry],
        hasV1Manifests: () async throws -> Bool
    ) async throws -> RemoteFormatInspection {
        let formatVersion = manifest.formatVersion
        guard formatVersion >= 2 && formatVersion <= RepoLayout.currentSupportedFormatVersion else {
            return .unsupported(minAppVersion: manifest.minAppVersion)
        }

        let markerStates = try await parseMarkerEntriesFailingClosed(
            markerStore: markerStore,
            migrationDirEntries: migrationDirEntries
        )
        let ordered = Self.sortedMarkers(markerStates)
        // parseEntries skips directories; any directory-shaped .json marker alongside
        // parseable ones leaves cleanup with an incomplete marker set — fail closed.
        if migrationDirEntries.contains(where: { $0.isDirectory && $0.name.hasSuffix(".json") }) {
            throw BackupCompatibilityError.damagedV2Repo
        }
        if try await hasV1Manifests() {
            return .v2WithV1Manifests(formatVersion: formatVersion)
        }
        if let cleanup = ordered.first(where: { $0.phase.isCleanupSafe }) {
            return .v2WithPendingMigrationCleanup(
                formatVersion: formatVersion,
                ownerWriterID: cleanup.writerID
            )
        }
        if let residue = ordered.first {
            return .v2WithPendingMigrationCleanup(
                formatVersion: formatVersion,
                ownerWriterID: residue.writerID
            )
        }
        return .v2(formatVersion: formatVersion)
    }

    private static func sortedMarkers(_ markers: [ParsedMigrationMarker]) -> [ParsedMigrationMarker] {
        markers.sorted { lhs, rhs in
            if lhs.phase.rawValue != rhs.phase.rawValue {
                return lhs.phase.rawValue > rhs.phase.rawValue
            }
            return lhs.writerID < rhs.writerID
        }
    }

    private func parseMarkerEntriesFailingClosed(
        markerStore: MigrationMarkerStore,
        migrationDirEntries: [RemoteStorageEntry]
    ) async throws -> [ParsedMigrationMarker] {
        do {
            return try await markerStore.parseEntries(migrationDirEntries)
        } catch is MigrationMarkerStore.InvalidMarker {
            throw BackupCompatibilityError.damagedV2Repo
        }
    }

    private static func detectV2DataDirectories(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> Bool {
        for subdir in [RepoLayout.commitsDirectory, RepoLayout.snapshotsDirectory] {
            let path = RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory, subdir])
            do {
                let entries = try await client.list(path: path)
                if !entries.isEmpty { return true }
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                if isStorageNotFoundError(error) { continue }
                throw error
            }
        }
        return false
    }

    private static func detectV1Manifests(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        entries: [RemoteStorageEntry]
    ) async throws -> Bool {
        let yearEntries = entries
            .filter { $0.isDirectory && $0.name.range(of: "^[0-9]{4}$", options: .regularExpression) != nil }
            .sorted(by: { $0.name > $1.name })
        for yearEntry in yearEntries {
            let yearPath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: yearEntry.name)
            let monthEntries: [RemoteStorageEntry]
            do {
                monthEntries = try await client.list(path: yearPath)
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                if isStorageNotFoundError(error) { continue }
                throw error
            }
            for monthEntry in monthEntries where monthEntry.isDirectory && monthEntry.name.range(of: "^[0-9]{2}$", options: .regularExpression) != nil {
                let monthPath = RemotePathBuilder.absolutePath(basePath: yearPath, remoteRelativePath: monthEntry.name)
                let monthContents: [RemoteStorageEntry]
                do {
                    monthContents = try await client.list(path: monthPath)
                } catch {
                    if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                    if isStorageNotFoundError(error) { continue }
                    throw error
                }
                if monthContents.contains(where: { !$0.isDirectory && $0.name == MonthManifestStore.manifestFileName }) {
                    return true
                }
            }
        }
        return false
    }
}
