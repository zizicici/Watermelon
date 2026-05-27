import Foundation
import os.log

private let v1MigrationLog = Logger(subsystem: "com.zizicici.watermelon", category: "V1Migration")

actor V1MigrationService {
    enum MigrationError: Error {
        case ioFailure(Error)
        case noProfileID
        case verifyFailed(reason: String)
    }

    struct ScannedV1Month: Sendable {
        let year: Int
        let month: Int
        let manifestAbsolutePath: String
    }

    struct MigrationOutcome: Sendable {
        let migratedMonthCount: Int
    }

    private let client: any RemoteStorageClientProtocol
    private let basePath: String
    private let database: DatabaseManager
    private let identity: RepoIdentity
    private let bootstrap: RepoBootstrap
    private let residueQuarantine: V1MigrationResidueQuarantine
    private let markerStore: MigrationMarkerStore

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        database: DatabaseManager,
        identity: RepoIdentity,
        bootstrap: RepoBootstrap
    ) {
        self.client = client
        self.basePath = basePath
        self.database = database
        self.identity = identity
        self.bootstrap = bootstrap
        self.residueQuarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        self.markerStore = MigrationMarkerStore(client: client, basePath: basePath)
    }

    func scanV1Months() async throws -> [ScannedV1Month] {
        let entries = try await client.list(path: basePath)
        var results: [ScannedV1Month] = []
        let yearEntries = entries.filter { $0.isDirectory && $0.name.range(of: "^[0-9]{4}$", options: .regularExpression) != nil }
        for yearEntry in yearEntries {
            guard let year = Int(yearEntry.name) else { continue }
            let yearPath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: yearEntry.name)
            // Silent skip + phase3 retry-scan would DELETE V1 manifests phase1 never processed; surface list failures.
            let monthEntries = try await client.list(path: yearPath)
            for monthEntry in monthEntries where monthEntry.isDirectory && monthEntry.name.range(of: "^[0-9]{2}$", options: .regularExpression) != nil {
                guard let month = Int(monthEntry.name), (1...12).contains(month) else { continue }
                let monthPath = RemotePathBuilder.absolutePath(basePath: yearPath, remoteRelativePath: monthEntry.name)
                let monthFiles = try await client.list(path: monthPath)
                if monthFiles.contains(where: { !$0.isDirectory && $0.name == MonthManifestStore.manifestFileName }) {
                    let absPath = RemotePathBuilder.absolutePath(basePath: monthPath, remoteRelativePath: MonthManifestStore.manifestFileName)
                    results.append(ScannedV1Month(year: year, month: month, manifestAbsolutePath: absPath))
                }
            }
        }
        return results
    }

    @discardableResult
    func runPhase1(profileID: Int64, repoID: String, writerID: String, runID: String) async throws -> Int {
        // WebDAV/SMB/SFTP don't auto-create parents on PUT.
        try await bootstrap.ensureSubdirectories()
        // Marker before any commit keeps interrupted migration visible to inspect.
        try await markerStore.writePhase(writerID: writerID, phase: .phase1, runID: runID)

        let allocator = SeqAllocator(database: database, profileID: profileID, repoID: repoID, initial: try await initialSeq(repoID: repoID, profileID: profileID))
        let clock = PersistedLamportClock(database: database, profileID: profileID, repoID: repoID, initial: try await initialClock(repoID: repoID, profileID: profileID))
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        let months = try await scanV1Months()
        var processed = 0
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        var existingV2Output: RepoMaterializer.MaterializeOutput?

        for scanned in months {
            try Task.checkCancellation()
            let storeOrNil = try await MonthManifestStore.loadManifestDirect(
                client: client,
                basePath: basePath,
                year: scanned.year,
                month: scanned.month,
                manifestAbsolutePath: scanned.manifestAbsolutePath,
                pushSchemaUpgrade: false
            )
            guard let store = storeOrNil else {
                // Quarantine corrupt residue so detectV1Manifests stops looping it as .v1.
                v1MigrationLog.warning("V1 manifest at \(scanned.manifestAbsolutePath, privacy: .public) unreadable — quarantining as legacy residue")
                try await residueQuarantine.quarantine(year: scanned.year, month: scanned.month, sourcePath: scanned.manifestAbsolutePath)
                continue
            }

            let snapshot = store.unsortedSnapshot()
            if snapshot.assets.isEmpty {
                // No assets to migrate; remove or quarantine so the V1 scan stops finding it.
                if snapshot.resources.isEmpty && snapshot.links.isEmpty {
                    try await deleteIfPresent(path: scanned.manifestAbsolutePath)
                } else {
                    v1MigrationLog.warning("V1 manifest for \(scanned.year, privacy: .public)-\(scanned.month, privacy: .public) has \(snapshot.resources.count, privacy: .public) resources / \(snapshot.links.count, privacy: .public) links but no assets — quarantining as legacy residue")
                    try await residueQuarantine.quarantine(year: scanned.year, month: scanned.month, sourcePath: scanned.manifestAbsolutePath)
                }
                continue
            }

            let monthKey = LibraryMonthKey(year: scanned.year, month: scanned.month)
            let plan = V1ManifestMigrationPlanner.plan(
                assets: snapshot.assets,
                resources: snapshot.resources,
                links: snapshot.links
            )
            var migrableAssets = plan.migrable
            let skippedAssetFailures = plan.skippedFailures
            if !migrableAssets.isEmpty {
                if existingV2Output == nil {
                    let output = try await materializer.materialize(expectedRepoID: repoID)
                    // Local DB high-water can lag remote when a peer already commits at this writerID
                    // (e.g. fresh install pulling existing repo): allocator would collide on same-writer
                    // seq, clock would emit values LWW-stale vs. existing V2 state.
                    try await RepoStateAuthority.observeSameWriterSeq(
                        writerID: writerID,
                        observedSeqByWriter: output.observedSeqByWriter,
                        allocator: allocator
                    )
                    try await clock.observe(output.state.observedClock)
                    existingV2Output = output
                }
                let existingState = existingV2Output?.state.months[monthKey]
                var existingFingerprints: Set<AssetFingerprint> = []
                if let existingAssets = existingState?.assets {
                    existingFingerprints.formUnion(existingAssets.keys)
                }
                if let deleted = existingState?.deletedAssetStamps.keys {
                    existingFingerprints.formUnion(deleted)
                }
                if !existingFingerprints.isEmpty {
                    migrableAssets.removeAll { existingFingerprints.contains($0.asset.assetFingerprint) }
                }
            }
            if migrableAssets.isEmpty {
                if let firstSkipped = skippedAssetFailures.first {
                    v1MigrationLog.warning("V1 manifest for \(scanned.year, privacy: .public)-\(scanned.month, privacy: .public) has \(skippedAssetFailures.count, privacy: .public) asset integrity issue(s) and no migrable assets; quarantining as legacy residue: \(firstSkipped, privacy: .public)")
                    try await writePartialMigrationMarker(
                        year: scanned.year,
                        month: scanned.month,
                        runID: runID,
                        migratedAssetCount: 0,
                        totalAssetCount: snapshot.assets.count,
                        failures: skippedAssetFailures
                    )
                } else {
                    v1MigrationLog.warning("V1 manifest for \(scanned.year, privacy: .public)-\(scanned.month, privacy: .public) has no migrable assets; quarantining as legacy residue")
                }
                try await residueQuarantine.quarantine(year: scanned.year, month: scanned.month, sourcePath: scanned.manifestAbsolutePath)
                continue
            }
            if let firstSkipped = skippedAssetFailures.first {
                v1MigrationLog.warning("V1 manifest for \(scanned.year, privacy: .public)-\(scanned.month, privacy: .public) migrating \(migrableAssets.count, privacy: .public) asset(s) and leaving \(skippedAssetFailures.count, privacy: .public) non-migrable asset(s) in residue: \(firstSkipped, privacy: .public)")
                try await writePartialMigrationMarker(
                    year: scanned.year,
                    month: scanned.month,
                    runID: runID,
                    migratedAssetCount: migrableAssets.count,
                    totalAssetCount: snapshot.assets.count,
                    failures: skippedAssetFailures
                )
            }
            let migrationMaxRetries = 4
            var migrationAttempt = 0
            var migrationHeader: CommitHeader
            var ops: [CommitOp]
            // Retry must re-tick clocks; reusing them with a new seq corrupts LWW ordering.
            while true {
                let clockRange = try await clock.tickRange(count: migrableAssets.count)
                var clockCursor = clockRange.low
                var pendingOps: [CommitOp] = []
                pendingOps.reserveCapacity(migrableAssets.count)
                for (index, migrable) in migrableAssets.enumerated() {
                    let asset = migrable.asset
                    let body = CommitAddAssetBody(
                        assetFingerprint: asset.assetFingerprint,
                        creationDateMs: asset.creationDateMs,
                        backedUpAtMs: asset.backedUpAtMs,
                        resources: migrable.resources
                    )
                    pendingOps.append(CommitOp(opSeq: index, clock: clockCursor, body: .addAsset(body)))
                    if index + 1 < migrableAssets.count {
                        clockCursor += 1
                    }
                }
                let seq = try await allocator.allocate()
                let header = CommitHeader(
                    version: CommitHeader.currentVersion,
                    repoID: repoID,
                    writerID: writerID,
                    seq: seq,
                    runID: runID,
                    scope: CommitHeader.monthScope(monthKey),
                    clockMin: clockRange.low,
                    clockMax: clockRange.high,
                    bodyKind: CommitHeader.bodyKindPlain
                )
                do {
                    _ = try await commitWriter.write(header: header, ops: pendingOps, month: monthKey, respectTaskCancellation: false)
                    migrationHeader = header
                    ops = pendingOps
                    break
                } catch CommitLogWriter.WriteError.alreadyExists {
                    migrationAttempt += 1
                    if migrationAttempt >= migrationMaxRetries { throw CommitLogWriter.WriteError.alreadyExists }
                    continue
                } catch {
                    migrationAttempt += 1
                    if migrationAttempt >= migrationMaxRetries || !Self.shouldRetryMigrationCommitWrite(error) {
                        throw error
                    }
                    v1MigrationLog.warning("V1 migration commit write failed for \(monthKey.text, privacy: .public), retrying with fresh clock/seq: \(error.localizedDescription, privacy: .public)")
                    continue
                }
            }

            let finalSeq = migrationHeader.seq
            var state = existingV2Output?.state.months[monthKey] ?? .empty
            for op in ops {
                guard case .addAsset(let body) = op.body else { continue }
                let stamp = OpStamp(writerID: writerID, seq: finalSeq, clock: op.clock)
                let totalSize = body.resources.reduce(Int64(0)) { $0 + $1.fileSize }
                state.assets[body.assetFingerprint] = SnapshotAssetRow(
                    assetFingerprint: body.assetFingerprint,
                    creationDateMs: body.creationDateMs,
                    backedUpAtMs: body.backedUpAtMs,
                    resourceCount: body.resources.count,
                    totalFileSizeBytes: totalSize,
                    stamp: stamp
                )
                for resource in body.resources {
                    state.resources[resource.physicalRemotePath] = SnapshotResourceRow(
                        physicalRemotePath: resource.physicalRemotePath,
                        contentHash: resource.contentHash,
                        fileSize: resource.fileSize,
                        resourceType: resource.resourceType,
                        creationDateMs: body.creationDateMs,
                        backedUpAtMs: body.backedUpAtMs,
                        crypto: resource.crypto,
                        stamp: stamp
                    )
                    state.assetResources[
                        AssetResourceKey(
                            assetFingerprint: body.assetFingerprint,
                            role: resource.role,
                            slot: resource.slot
                        )
                    ] = SnapshotAssetResourceRow(
                        assetFingerprint: body.assetFingerprint,
                        role: resource.role,
                        slot: resource.slot,
                        resourceHash: resource.contentHash,
                        logicalName: resource.logicalName
                    )
                }
            }
            var covered = existingV2Output?.coveredByMonth[monthKey] ?? .empty
            covered.add(writerID: writerID, range: ClosedSeqRange(low: finalSeq, high: finalSeq))
            let snapshotHeader = SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(monthKey),
                writerID: writerID,
                repoID: repoID,
                covered: covered
            )
            let parts = RepoSnapshotBuilder.build(header: snapshotHeader, state: state)
            let lamport = await clock.value()
            _ = try await snapshotWriter.write(
                header: snapshotHeader,
                assets: parts.assets,
                resources: parts.resources,
                assetResources: parts.assetResources,
                deletedKeys: parts.deletedKeys,
                month: monthKey,
                lamport: lamport,
                runID: runID,
                respectTaskCancellation: false
            )
            try await residueQuarantine.quarantine(
                year: scanned.year,
                month: scanned.month,
                sourcePath: scanned.manifestAbsolutePath
            )
            processed += 1
        }
        return processed
    }

    /// FSM step: publish version (infrastructure). Idempotent — both full
    /// migration and cleanup paths call this; cleanup keeps `migrationCompleted`
    /// at its current value (see `markProfileMigrated`).
    func ensureVersionPublished(writerID: String) async throws {
        try await VersionManifestStore(client: client, basePath: basePath).writeIfAbsent(writerID: writerID)
        try await bootstrap.ensureSubdirectories()
    }

    /// FSM step: mark this profile migrated. Full-migration path only —
    /// cleanup path intentionally leaves `migrationCompleted=0` (preserves
    /// pre-refactor behavior where a peer's cleanup run never set the flag).
    func markProfileMigrated(profileID: Int64, repoID: String, writerID: String, runID: String) async throws {
        try await markerStore.writePhase(writerID: writerID, phase: .phase2, runID: runID)
        try await identity.setMigrationCompleted(profileID: profileID, repoID: repoID)
    }

    func runPhase3(writerID: String, runID: String) async throws {
        try await markerStore.writePhase(writerID: writerID, phase: .phase3, runID: runID)
        try await residueQuarantine.sweepResidueManifests()
        try await markerStore.deleteAll(writerID: writerID)
    }

    /// FSM step: verify final state. Post-condition guard — re-list base and
    /// assert (a) no V1 manifest visible, (b) the writer we just cleaned has
    /// no surviving marker. Intentionally does NOT check `anyMigrationMarkerExists`
    /// (peer markers from prior aborted runs are the next inspection's job) nor
    /// `migrationCompleted` (cleanup-only path doesn't set it).
    func verifyFinalState(cleanedWriterID: String) async throws {
        let lingering = try await scanV1Months()
        if !lingering.isEmpty {
            throw MigrationError.verifyFailed(reason: "V1 manifest still visible at \(lingering.count) month(s)")
        }
        if try await ownsMigrationMarker(writerID: cleanedWriterID) {
            throw MigrationError.verifyFailed(reason: "migration marker for \(cleanedWriterID) still visible")
        }
    }

    func runFullMigration(
        profileID: Int64,
        repoID: String,
        writerID: String,
        runID: String,
        onMigrationStart: (() async -> Void)? = nil,
        onMigrationComplete: ((Int) async -> Void)? = nil
    ) async throws -> MigrationOutcome {
        var migratedCount = 0
        let state = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)
        let needsMigration: Bool
        if state.migrationCompleted != 1 {
            needsMigration = true
        } else if try await ownsMigrationMarker(writerID: writerID) {
            needsMigration = true
        } else {
            needsMigration = !(try await scanV1Months()).isEmpty
        }
        if needsMigration {
            await onMigrationStart?()
            try await ensureVersionPublished(writerID: writerID)
            migratedCount = try await runPhase1(profileID: profileID, repoID: repoID, writerID: writerID, runID: runID)
            try await markProfileMigrated(profileID: profileID, repoID: repoID, writerID: writerID, runID: runID)
            try await runPhase3(writerID: writerID, runID: runID)
            await onMigrationComplete?(migratedCount)
        }
        try await verifyFinalState(cleanedWriterID: writerID)
        return MigrationOutcome(migratedMonthCount: migratedCount)
    }

    func runCleanupOnly(
        ownerWriterID: String,
        writerID: String,
        runID: String
    ) async throws {
        try await ensureVersionPublished(writerID: writerID)
        try await runPhase3(writerID: ownerWriterID, runID: runID)
        try await verifyFinalState(cleanedWriterID: ownerWriterID)
    }

    func currentPhase(writerID: String) async throws -> MigrationMarkerPhase? {
        try await markerStore.currentPhase(writerID: writerID)
    }

    func ownsMigrationMarker(writerID: String) async throws -> Bool {
        try await markerStore.existsFor(writerID: writerID)
    }

    func anyMigrationMarkerExists() async throws -> Bool {
        try await markerStore.existsAny()
    }

    private func metadataIfPresent(path: String) async throws -> RemoteStorageEntry? {
        do {
            return try await client.metadata(path: path)
        } catch {
            if isStorageNotFoundError(error) { return nil }
            throw error
        }
    }

    private func deleteIfPresent(path: String) async throws {
        guard try await metadataIfPresent(path: path) != nil else { return }
        do {
            try await client.delete(path: path)
        } catch {
            // Peer racing the same cleanup can remove the file between metadata and delete;
            // non-idempotent backends surface that as an error. Treat not-found as success.
            if !isStorageNotFoundError(error) { throw error }
        }
    }

    private func writePartialMigrationMarker(
        year: Int,
        month: Int,
        runID: String,
        migratedAssetCount: Int,
        totalAssetCount: Int,
        failures: [String]
    ) async throws {
        let monthRel = String(format: "%04d/%02d", year, month)
        let markerPath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: monthRel + "/" + V1MigrationResidueFileNames.partialMigrationMarkerFileName
        )
        let dict: [String: Any] = [
            "v": 1,
            "run_id": runID,
            "year": year,
            "month": month,
            "total_asset_count": totalAssetCount,
            "migrated_asset_count": migratedAssetCount,
            "skipped_asset_count": failures.count,
            "failures": failures
        ]
        let data = try JSONSerialization.data(withJSONObject: dict, options: [.sortedKeys])
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("v1-partial-migration-\(UUID().uuidString).json")
        try data.write(to: temp, options: .atomic)
        defer { try? FileManager.default.removeItem(at: temp) }
        // Marker is per-(month, writer-in-migration) — no concurrent writer can race the same
        // path during this run, and the gate still verifies bytes post-write. Requiring
        // exclusive move-if-absent here would abort migration on SFTP and non-exclusive
        // WebDAV/S3 backends even though residue quarantine could proceed.
        _ = try await MetadataCreateGate.createWithStagingFallback(
            client: client,
            localURL: temp,
            remotePath: markerPath,
            respectTaskCancellation: false,
            finalizationPolicy: .allowBestEffort
        )
    }

    private func initialSeq(repoID: String, profileID: Int64) async throws -> UInt64 {
        guard let row = try await identity.loadRepoState(profileID: profileID, repoID: repoID) else {
            return 0
        }
        return RepoStateAuthority.counters(from: row).lastSeq
    }

    private static func shouldRetryMigrationCommitWrite(_ error: Error) -> Bool {
        if RemoteWriteClassifier.isCancellation(error) { return false }
        if case CommitLogWriter.WriteError.alreadyExists = error { return true }
        if case CommitLogWriter.WriteError.encodingFailed(_) = error { return false }
        if case CommitLogWriter.WriteError.ioFailure(let underlying) = error {
            return RemoteWriteClassifier.classifyVerifyFailure(underlying) == .transient
        }
        return RemoteWriteClassifier.classifyVerifyFailure(error) == .transient
    }

    private func initialClock(repoID: String, profileID: Int64) async throws -> UInt64 {
        guard let row = try await identity.loadRepoState(profileID: profileID, repoID: repoID) else {
            return 0
        }
        return RepoStateAuthority.counters(from: row).lastClock
    }
}
