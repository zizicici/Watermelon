import Foundation

/// Evidence the analyzer reads to decide a remote's format. Every method is an orchestrated remote
/// read living behind this boundary so `RepoFormatAnalyzer` stays a pure, no-mutation decision authority
/// that can be exercised with canned evidence. Marker visibility-lag grace, version-load grace, and the
/// V1/V2 traversals are the evidence implementation's job; the analyzer only maps their outcomes to a route.
nonisolated protocol RepoFormatEvidenceProviding: Sendable {
    /// Whether `.watermelon/` is effectively present, after reconfirming a marker-absent base listing
    /// within read-after-write grace.
    func markerPresent() async throws -> Bool
    /// Version manifest load, tolerating download/metadata visibility lag. May throw `VersionConflict`
    /// or `BootstrapError` which the analyzer maps to a damaged verdict.
    func loadVersion() async throws -> VersionManifestStore.Load
    func migrationDirectoryEntries() async throws -> [RemoteStorageEntry]
    /// Parses listed migration markers; throws `MigrationMarkerStore.InvalidMarker` on a malformed marker.
    func parseMigrationMarkers(_ entries: [RemoteStorageEntry]) async throws -> [ParsedMigrationMarker]
    func hasV1Manifests() async throws -> Bool
    func hasV2DataDirectories() async throws -> Bool
}

/// Deterministic mapping from observed repository evidence to a `RemoteFormatInspection` route. Does no
/// remote I/O and mutates nothing — all reads are delegated to `RepoFormatEvidenceProviding`. The branch
/// structure mirrors the prior inspection flow exactly, preserving every route and fail-closed boundary.
nonisolated struct RepoFormatAnalyzer: Sendable {
    init() {}

    func analyze(evidence: any RepoFormatEvidenceProviding) async throws -> RemoteFormatInspection {
        guard try await evidence.markerPresent() else {
            return try await evidence.hasV1Manifests() ? .v1 : .fresh
        }
        return try await analyzeMarkerPresent(evidence: evidence)
    }

    private func analyzeMarkerPresent(
        evidence: any RepoFormatEvidenceProviding
    ) async throws -> RemoteFormatInspection {
        let manifest: VersionManifestStore.Load
        do {
            manifest = try await evidence.loadVersion()
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

        let migrationDirEntries = try await evidence.migrationDirectoryEntries()
        let migrationInProgress = migrationDirEntries.contains { $0.name.hasSuffix(".json") }

        switch manifest {
        case .absent:
            return try await analyzeVersionAbsent(
                evidence: evidence,
                migrationDirEntries: migrationDirEntries,
                migrationInProgress: migrationInProgress
            )
        case .found(let manifest):
            return try await analyzeVersionFound(
                manifest: manifest,
                evidence: evidence,
                migrationDirEntries: migrationDirEntries
            )
        }
    }

    private func analyzeVersionAbsent(
        evidence: any RepoFormatEvidenceProviding,
        migrationDirEntries: [RemoteStorageEntry],
        migrationInProgress: Bool
    ) async throws -> RemoteFormatInspection {
        let v1Manifests = try await evidence.hasV1Manifests()
        let hasV2Data = try await evidence.hasV2DataDirectories()

        if hasV2Data {
            if v1Manifests {
                if migrationDirEntries.contains(where: { $0.isDirectory && $0.name.hasSuffix(".json") }) {
                    throw BackupCompatibilityError.damagedV2Repo
                }
                if migrationInProgress {
                    _ = try await parseMarkersFailingClosed(
                        evidence: evidence,
                        migrationDirEntries: migrationDirEntries
                    )
                }
                return .v2WithV1Manifests(formatVersion: RepoLayout.formatVersion)
            }
            if migrationInProgress {
                let markerStates = try await parseMarkersFailingClosed(
                    evidence: evidence,
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
            _ = try await parseMarkersFailingClosed(
                evidence: evidence,
                migrationDirEntries: migrationDirEntries
            )
        }
        if v1Manifests { return .v1 }
        return .fresh
    }

    private func analyzeVersionFound(
        manifest: VersionManifest,
        evidence: any RepoFormatEvidenceProviding,
        migrationDirEntries: [RemoteStorageEntry]
    ) async throws -> RemoteFormatInspection {
        let formatVersion = manifest.formatVersion
        guard formatVersion >= 2 && formatVersion <= RepoLayout.currentSupportedFormatVersion else {
            return .unsupported(minAppVersion: manifest.minAppVersion)
        }

        let markerStates = try await parseMarkersFailingClosed(
            evidence: evidence,
            migrationDirEntries: migrationDirEntries
        )
        let ordered = Self.sortedMarkers(markerStates)
        // parseEntries skips directories; any directory-shaped .json marker alongside
        // parseable ones leaves cleanup with an incomplete marker set — fail closed.
        if migrationDirEntries.contains(where: { $0.isDirectory && $0.name.hasSuffix(".json") }) {
            throw BackupCompatibilityError.damagedV2Repo
        }
        if try await evidence.hasV1Manifests() {
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

    private func parseMarkersFailingClosed(
        evidence: any RepoFormatEvidenceProviding,
        migrationDirEntries: [RemoteStorageEntry]
    ) async throws -> [ParsedMigrationMarker] {
        do {
            return try await evidence.parseMigrationMarkers(migrationDirEntries)
        } catch is MigrationMarkerStore.InvalidMarker {
            throw BackupCompatibilityError.damagedV2Repo
        }
    }

    private static func sortedMarkers(_ markers: [ParsedMigrationMarker]) -> [ParsedMigrationMarker] {
        markers.sorted { lhs, rhs in
            if lhs.phase.rawValue != rhs.phase.rawValue {
                return lhs.phase.rawValue > rhs.phase.rawValue
            }
            return lhs.writerID < rhs.writerID
        }
    }
}

/// Production evidence: every method is an orchestrated remote read against the live client. Marker-absent
/// reconfirmation and the version-load grace loop live here so the analyzer never touches the client. The
/// one piece of retained state is the marker-probe base listing, reused as the V1-detection seed.
/// `@unchecked Sendable`: the analyzer drives the queries sequentially within one inspection, so the
/// single cached base listing is never accessed concurrently.
nonisolated final class RepoFormatRemoteEvidence: RepoFormatEvidenceProviding, @unchecked Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String
    /// The base listing that drove the marker-present/absent verdict. Reused verbatim as the
    /// year-directory seed for V1 detection so a base LIST that diverges mid-inspection (eventual
    /// consistency dropping/404ing a year dir) can't re-route a V1 / V2-with-V1-residue remote off a
    /// second, different listing — matching the old single-base-list inspection contract.
    private var markerProbeBaseEntries: [RemoteStorageEntry]?

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = client
        self.basePath = basePath
    }

    /// A marker-absent base LIST on a fresh/V1 remote is the common case, so its reconfirm must not pay
    /// the full read-after-write ceiling: when `version.json` metadata is genuinely absent there is no
    /// pending write to wait on. Cap the metadata-visibility flap retry at a few quick reads instead.
    private static let negativeMarkerReconfirmRetryCount = 4

    func markerPresent() async throws -> Bool {
        let entries = try await listBase()
        markerProbeBaseEntries = entries
        if entries.contains(where: { $0.isDirectory && $0.name == RepoLayout.watermelonDirectory }) {
            return true
        }
        // A just-written `.watermelon/` can be omitted from one base LIST on a grace backend while
        // version.json is already readable (direct object reads lead parent-prefix listings). A
        // V2-bound syncIndex treats the resulting `.fresh`/`.v1` as a deterministic format regression
        // and clears the live committed view, so reconfirm marker absence within grace before routing.
        return try await reconfirmMarkerAbsentWithinGrace()
    }

    func loadVersion() async throws -> VersionManifestStore.Load {
        // `.watermelon/` is present, so a not-found version.json on a backend advertising metadata
        // read-after-write lag is not stable evidence of a fresh endpoint — it can be a just-written
        // manifest still propagating. Re-read within the grace window before letting `.absent` drive
        // the `.fresh` route; zero-grace backends keep their single authoritative read. The per-read
        // `loadToleratingDownloadVisibilityLag` also covers the metadata-visible / download-404 race so
        // a lagging data-path GET reconfirms to `.found` rather than aborting inspection with a raw error.
        let store = VersionManifestStore(client: client, basePath: basePath)
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
        return .absent
    }

    func migrationDirectoryEntries() async throws -> [RemoteStorageEntry] {
        try await MigrationMarkerStore(client: client, basePath: basePath).migrationsDirectoryEntries()
    }

    func parseMigrationMarkers(_ entries: [RemoteStorageEntry]) async throws -> [ParsedMigrationMarker] {
        try await MigrationMarkerStore(client: client, basePath: basePath).parseEntries(entries)
    }

    func hasV1Manifests() async throws -> Bool {
        var found = false
        try await V1MonthIterator.forEachMonth(
            client: client,
            basePath: basePath,
            options: .init(listFailurePolicy: .skipMissing, yearOrder: .descending),
            baseEntries: markerProbeBaseEntries
        ) { _, _, monthPath in
            if try await V1MonthIterator.monthContainsManifest(
                client: client,
                monthPath: monthPath,
                listFailurePolicy: .skipMissing
            ) {
                found = true
                return .stop
            }
            return .continue
        }
        return found
    }

    func hasV2DataDirectories() async throws -> Bool {
        try await RepoV2DataProbe.hasAnyCommitOrSnapshotData(client: client, basePath: basePath)
    }

    private func listBase() async throws -> [RemoteStorageEntry] {
        do {
            return try await client.list(path: basePath)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }
    }

    /// On a grace backend, re-prove a marker-absent base LIST against the more read-after-write-consistent
    /// version.json object before letting `.absent` drive `.fresh`/`.v1`. Returns true once the manifest
    /// reappears (route as marker-present); false (genuinely fresh, or zero-grace) keeps the single
    /// authoritative listing. A genuinely-absent version.json only triggers a small bounded retry — not
    /// the full grace ceiling — so empty/legacy opens stay fast; transport errors propagate (fail closed).
    private func reconfirmMarkerAbsentWithinGrace() async throws -> Bool {
        guard client.readAfterWriteGraceSeconds > 0 else { return false }
        let versionStore = VersionManifestStore(client: client, basePath: basePath)
        if case .found = try await versionStore.loadToleratingDownloadVisibilityLag() {
            return try await markerReconfirmed()
        }
        var attempt = 0
        while attempt < Self.negativeMarkerReconfirmRetryCount {
            try await Task.sleep(nanoseconds: UInt64(200 * (1 << min(attempt, 3))) * 1_000_000)
            attempt += 1
            if case .found = try await versionStore.loadToleratingDownloadVisibilityLag() {
                return try await markerReconfirmed()
            }
        }
        return false
    }

    /// On reconfirm the marker is routed as present; reseed V1 detection from a fresh base listing,
    /// mirroring the old inspection's re-list-then-detect-V1 path.
    private func markerReconfirmed() async throws -> Bool {
        markerProbeBaseEntries = try await listBase()
        return true
    }
}
