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
            // A just-written `.watermelon/` can be omitted from one base LIST on a grace backend while
            // version.json is already readable (direct object reads lead parent-prefix listings). A
            // V2-bound syncIndex treats the resulting `.fresh`/`.v1` as a deterministic format regression
            // and clears the live committed view, so reconfirm marker absence within grace before routing.
            if let reconfirmed = try await reconfirmMarkerAbsentWithinGrace(client: client, basePath: basePath) {
                return try await inspectMarkerPresent(
                    client: client,
                    basePath: basePath,
                    entries: reconfirmed
                )
            }
            let hasV1 = try await Self.detectV1Manifests(client: client, basePath: basePath, entries: entries)
            return hasV1 ? .v1 : .fresh
        }

        return try await inspectMarkerPresent(
            client: client,
            basePath: basePath,
            entries: entries
        )
    }

    /// A marker-absent base LIST on a fresh/V1 remote is the common case, so its reconfirm must not pay
    /// the full read-after-write ceiling: when `version.json` metadata is genuinely absent there is no
    /// pending write to wait on. Cap the metadata-visibility flap retry at a few quick reads instead.
    private static let negativeMarkerReconfirmRetryCount = 4

    /// On a grace backend, re-prove a marker-absent base LIST against the more read-after-write-consistent
    /// version.json object before letting `.absent` drive `.fresh`/`.v1`. Returns a fresh base listing to
    /// route as marker-present once the manifest reappears; nil (genuinely fresh, or zero-grace) keeps the
    /// single authoritative listing. The tolerant loader also reconfirms a metadata-visible / download-404
    /// version read within grace (no early abort). A genuinely-absent version.json only triggers a small
    /// bounded retry — not the full grace ceiling — so empty/legacy opens stay fast; transport errors
    /// propagate (fail closed) and never silently demote to `.fresh`.
    private func reconfirmMarkerAbsentWithinGrace(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> [RemoteStorageEntry]? {
        guard client.readAfterWriteGraceSeconds > 0 else { return nil }
        let versionStore = VersionManifestStore(client: client, basePath: basePath)
        if case .found = try await versionStore.loadToleratingDownloadVisibilityLag() {
            return try await listBase(client: client, basePath: basePath)
        }
        var attempt = 0
        while attempt < Self.negativeMarkerReconfirmRetryCount {
            try await Task.sleep(nanoseconds: UInt64(200 * (1 << min(attempt, 3))) * 1_000_000)
            attempt += 1
            if case .found = try await versionStore.loadToleratingDownloadVisibilityLag() {
                return try await listBase(client: client, basePath: basePath)
            }
        }
        return nil
    }

    private func listBase(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> [RemoteStorageEntry] {
        do {
            return try await client.list(path: basePath)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }
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
            manifest = try await loadVersionReconfirmingAbsenceWithinGrace(
                store: VersionManifestStore(client: client, basePath: basePath),
                client: client
            )
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

    // `.watermelon/` is present, so a not-found version.json on a backend advertising metadata
    // read-after-write lag is not stable evidence of a fresh endpoint — it can be a just-written
    // manifest still propagating. Re-read within the grace window before letting `.absent` drive
    // the `.fresh` route; zero-grace backends keep their single authoritative read. The per-read
    // `loadToleratingDownloadVisibilityLag` also covers the metadata-visible / download-404 race so
    // a lagging data-path GET reconfirms to `.found` rather than aborting inspection with a raw error.
    private func loadVersionReconfirmingAbsenceWithinGrace(
        store: VersionManifestStore,
        client: any RemoteStorageClientProtocol
    ) async throws -> VersionManifestStore.Load {
        let manifest = try await GracefulRead.retryWithinGrace(
            client: client,
            floorSeconds: 1,
            backoff: .exponential(baseMs: 200, maxShift: 3)
        ) {
            if case .found(let manifest) = try await store.loadToleratingDownloadVisibilityLag() {
                return manifest
            }
            return nil
        }
        if let manifest { return .found(manifest) }
        // Zero-grace or persistent absence past the deadline keeps the absent verdict.
        return .absent
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
        let probes: [(subdir: String, isFinalDataName: (String) -> Bool)] = [
            (RepoLayout.commitsDirectory, { RepoLayout.parseCommitFilename($0) != nil }),
            (RepoLayout.snapshotsDirectory, { RepoLayout.parseSnapshotFilename($0) != nil })
        ]
        for probe in probes {
            let path = RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory, probe.subdir])
            do {
                let entries = try await client.list(path: path)
                // A `<commit|snapshot>.staging-<uuid>` is our own incomplete write that maintenance sweeps
                // only after open; counting it as committed data fails admission closed as damaged before
                // the sweep can run. Other junk/corrupt-named files still count (fail closed).
                if entries.contains(where: { !isOwnStagingArtifact($0.name, ofFinalDataName: probe.isFinalDataName) }) {
                    return true
                }
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                if isStorageNotFoundError(error) { continue }
                throw error
            }
        }
        return false
    }

    private static func isOwnStagingArtifact(
        _ name: String,
        ofFinalDataName isFinalDataName: (String) -> Bool
    ) -> Bool {
        guard let range = name.range(of: ".staging-") else { return false }
        return isFinalDataName(String(name[..<range.lowerBound]))
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
                // Mirror scanV1Months' 01-12 domain: an out-of-range month flagged here never clears (migration's scan + verifyFinalState skip it), looping admission forever.
                guard let month = Int(monthEntry.name), (1...12).contains(month) else { continue }
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
