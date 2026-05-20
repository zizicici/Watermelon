import Foundation

struct RepoBootstrapInspectionFSM: Sendable {
    init() {}

    func inspect(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord
    ) async throws -> RemoteFormatInspection {
        let basePath = RemotePathBuilder.normalizePath(profile.basePath)
        let machine = BootstrapInspectionMachine()
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
        machine.apply(.listedBase(markerExists: markerExists))

        guard markerExists else {
            let hasV1 = try await Self.detectV1Manifests(client: client, basePath: basePath, entries: entries)
            machine.apply(.detectedV1Manifests(hasV1))
            return machine.finish(hasV1 ? .v1 : .fresh)
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
        let machine = BootstrapInspectionMachine()
        machine.apply(.listedBase(markerExists: true))
        let manifest: VersionManifestStore.Load
        do {
            manifest = try await VersionManifestStore(client: client, basePath: basePath).load()
        } catch is RepoBootstrap.VersionConflict {
            try machine.failDamaged()
        } catch let bootstrap as RepoBootstrap.BootstrapError {
            if case .ioFailure = bootstrap { try machine.failDamaged() }
            throw bootstrap
        }

        let versionSignal = VersionSignal(manifest)
        machine.apply(.loadedVersion(versionSignal))

        if case .found(let preCheck) = manifest,
           preCheck.formatVersion > RepoLayout.currentSupportedFormatVersion {
            return machine.finish(.unsupported(minAppVersion: preCheck.minAppVersion))
        }

        let markerStore = MigrationMarkerStore(client: client, basePath: basePath)
        let migrationDirEntries = try await markerStore.migrationsDirectoryEntries()
        let migrationInProgress = migrationDirEntries.contains { $0.name.hasSuffix(".json") }
        machine.apply(.listedMigrationDirectory(rawMigrationMarkerExists: migrationInProgress))

        var cachedV1Manifests: Bool?
        func hasV1Manifests() async throws -> Bool {
            if let cachedV1Manifests { return cachedV1Manifests }
            let detected = try await Self.detectV1Manifests(client: client, basePath: basePath, entries: entries)
            cachedV1Manifests = detected
            machine.apply(.detectedV1Manifests(detected))
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
                hasV1Manifests: hasV1Manifests,
                machine: machine
            )
        case .found(let manifest):
            return try await inspectVersionFound(
                manifest: manifest,
                markerStore: markerStore,
                migrationDirEntries: migrationDirEntries,
                hasV1Manifests: hasV1Manifests,
                machine: machine
            )
        }
    }

    private func inspectVersionAbsent(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        markerStore: MigrationMarkerStore,
        migrationDirEntries: [RemoteStorageEntry],
        migrationInProgress: Bool,
        hasV1Manifests: () async throws -> Bool,
        machine: BootstrapInspectionMachine
    ) async throws -> RemoteFormatInspection {
        let v1Manifests = try await hasV1Manifests()
        let hasV2Data = try await Self.detectV2DataDirectories(client: client, basePath: basePath)
        machine.apply(.detectedV2Data(hasV2Data))
        machine.apply(.versionAbsent(
            v1Manifests: v1Manifests,
            v2Data: hasV2Data,
            rawMigrationMarkerExists: migrationInProgress
        ))

        if hasV2Data {
            if v1Manifests {
                if migrationDirEntries.contains(where: { $0.isDirectory && $0.name.hasSuffix(".json") }) {
                    try machine.failDamaged()
                }
                if migrationInProgress {
                    _ = try await parseMarkerEntriesFailingClosed(
                        markerStore: markerStore,
                        migrationDirEntries: migrationDirEntries,
                        machine: machine
                    )
                }
                return machine.finish(.v2WithV1Manifests(formatVersion: RepoLayout.formatVersion))
            }
            if migrationInProgress {
                let markerStates = try await parseMarkerEntriesFailingClosed(
                    markerStore: markerStore,
                    migrationDirEntries: migrationDirEntries,
                    machine: machine
                )
                let ordered = Self.sortedMarkers(markerStates)
                machine.apply(.parsedMarkers(ordered.map(MigrationMarkerRoute.init)))
                // Directory .json markers alongside parseable ones leave cleanup with
                // an incomplete marker set — fail closed.
                if migrationDirEntries.contains(where: { $0.isDirectory && $0.name.hasSuffix(".json") }) {
                    try machine.failDamaged()
                }
                if let marker = ordered.first {
                    return machine.finish(.v2WithPendingMigrationCleanup(
                        formatVersion: RepoLayout.formatVersion,
                        ownerWriterID: marker.writerID
                    ))
                }
            }
            try machine.failDamaged()
        }
        if migrationDirEntries.contains(where: { $0.isDirectory && $0.name.hasSuffix(".json") }) {
            try machine.failDamaged()
        }
        let fileMarkers = migrationDirEntries.filter {
            !$0.isDirectory && $0.name.hasSuffix(".json")
        }
        if !fileMarkers.isEmpty {
            do {
                _ = try await markerStore.parseEntries(migrationDirEntries)
            } catch is MigrationMarkerStore.InvalidMarker {
                try machine.failDamaged()
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                // Transport errors in !hasV2Data path: can't validate, proceed
            }
        }
        if v1Manifests { return machine.finish(.v1) }
        return machine.finish(.fresh)
    }

    private func inspectVersionFound(
        manifest: VersionManifest,
        markerStore: MigrationMarkerStore,
        migrationDirEntries: [RemoteStorageEntry],
        hasV1Manifests: () async throws -> Bool,
        machine: BootstrapInspectionMachine
    ) async throws -> RemoteFormatInspection {
        let formatVersion = manifest.formatVersion
        guard formatVersion >= 2 && formatVersion <= RepoLayout.currentSupportedFormatVersion else {
            return machine.finish(.unsupported(minAppVersion: manifest.minAppVersion))
        }

        let markerStates = try await parseMarkerEntriesFailingClosed(
            markerStore: markerStore,
            migrationDirEntries: migrationDirEntries,
            machine: machine
        )
        let ordered = Self.sortedMarkers(markerStates)
        machine.apply(.parsedMarkers(ordered.map(MigrationMarkerRoute.init)))
        // parseEntries skips directories; any directory-shaped .json marker alongside
        // parseable ones leaves cleanup with an incomplete marker set — fail closed.
        if migrationDirEntries.contains(where: { $0.isDirectory && $0.name.hasSuffix(".json") }) {
            try machine.failDamaged()
        }
        if try await hasV1Manifests() {
            machine.apply(.versionSupported(
                formatVersion: formatVersion,
                v1Manifests: true,
                markers: ordered.map(MigrationMarkerRoute.init)
            ))
            return machine.finish(.v2WithV1Manifests(formatVersion: formatVersion))
        }
        let markerRoutes = ordered.map(MigrationMarkerRoute.init)
        machine.apply(.versionSupported(
            formatVersion: formatVersion,
            v1Manifests: false,
            markers: markerRoutes
        ))
        if let cleanup = ordered.first(where: { $0.phase.isCleanupSafe }) {
            return machine.finish(.v2WithPendingMigrationCleanup(
                formatVersion: formatVersion,
                ownerWriterID: cleanup.writerID
            ))
        }
        if let residue = ordered.first {
            return machine.finish(.v2WithPendingMigrationCleanup(
                formatVersion: formatVersion,
                ownerWriterID: residue.writerID
            ))
        }
        return machine.finish(.v2(formatVersion: formatVersion))
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
        migrationDirEntries: [RemoteStorageEntry],
        machine: BootstrapInspectionMachine
    ) async throws -> [ParsedMigrationMarker] {
        do {
            return try await markerStore.parseEntries(migrationDirEntries)
        } catch is MigrationMarkerStore.InvalidMarker {
            try machine.failDamaged()
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
                throw error
            }
            for monthEntry in monthEntries where monthEntry.isDirectory && monthEntry.name.range(of: "^[0-9]{2}$", options: .regularExpression) != nil {
                let monthPath = RemotePathBuilder.absolutePath(basePath: yearPath, remoteRelativePath: monthEntry.name)
                let monthContents: [RemoteStorageEntry]
                do {
                    monthContents = try await client.list(path: monthPath)
                } catch {
                    if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
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

private enum BootstrapInspectionState: Equatable, Sendable {
    case start
    case baseListed(markerExists: Bool)
    case noMarker(v1Manifests: Bool)
    case markerPresent(version: VersionSignal, rawMigrationMarkerExists: Bool?)
    case versionAbsent(v1Manifests: Bool, v2Data: Bool, rawMigrationMarkerExists: Bool)
    case versionSupported(formatVersion: Int, v1Manifests: Bool, markers: [MigrationMarkerRoute])
    case terminal(RemoteFormatInspection)
    case damagedV2Repo
}

private enum BootstrapInspectionEvent: Sendable {
    case listedBase(markerExists: Bool)
    case loadedVersion(VersionSignal)
    case listedMigrationDirectory(rawMigrationMarkerExists: Bool)
    case detectedV1Manifests(Bool)
    case detectedV2Data(Bool)
    case parsedMarkers([MigrationMarkerRoute])
    case versionAbsent(v1Manifests: Bool, v2Data: Bool, rawMigrationMarkerExists: Bool)
    case versionSupported(formatVersion: Int, v1Manifests: Bool, markers: [MigrationMarkerRoute])
}

private final class BootstrapInspectionMachine {
    private(set) var state: BootstrapInspectionState = .start

    func apply(_ event: BootstrapInspectionEvent) {
        switch event {
        case .listedBase(let markerExists):
            state = .baseListed(markerExists: markerExists)
        case .loadedVersion(let signal):
            state = .markerPresent(version: signal, rawMigrationMarkerExists: nil)
        case .listedMigrationDirectory(let rawMigrationMarkerExists):
            if case .markerPresent(let signal, _) = state {
                state = .markerPresent(version: signal, rawMigrationMarkerExists: rawMigrationMarkerExists)
            }
        case .detectedV1Manifests(let detected):
            if case .baseListed(false) = state {
                state = .noMarker(v1Manifests: detected)
            }
        case .detectedV2Data:
            break
        case .parsedMarkers:
            break
        case .versionAbsent(let v1Manifests, let v2Data, let rawMigrationMarkerExists):
            state = .versionAbsent(
                v1Manifests: v1Manifests,
                v2Data: v2Data,
                rawMigrationMarkerExists: rawMigrationMarkerExists
            )
        case .versionSupported(let formatVersion, let v1Manifests, let markers):
            state = .versionSupported(formatVersion: formatVersion, v1Manifests: v1Manifests, markers: markers)
        }
    }

    func finish(_ inspection: RemoteFormatInspection) -> RemoteFormatInspection {
        state = .terminal(inspection)
        return inspection
    }

    func failDamaged() throws -> Never {
        state = .damagedV2Repo
        throw BackupCompatibilityError.damagedV2Repo
    }
}

private enum VersionSignal: Equatable, Sendable {
    case absent
    case supported(formatVersion: Int)
    case unsupported(minAppVersion: String?)

    init(_ load: VersionManifestStore.Load) {
        switch load {
        case .absent:
            self = .absent
        case .found(let manifest):
            if manifest.formatVersion >= 2 && manifest.formatVersion <= RepoLayout.currentSupportedFormatVersion {
                self = .supported(formatVersion: manifest.formatVersion)
            } else {
                self = .unsupported(minAppVersion: manifest.minAppVersion)
            }
        }
    }
}

private struct MigrationMarkerRoute: Equatable, Sendable {
    let writerID: String
    let phase: MigrationMarkerPhase

    init(_ marker: ParsedMigrationMarker) {
        self.writerID = marker.writerID
        self.phase = marker.phase
    }
}
