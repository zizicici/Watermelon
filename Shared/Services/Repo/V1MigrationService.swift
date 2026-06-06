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
    private let journalStore: MigrationJournalStore
    // Months this run wrote a partial-migration marker for; phase3 must keep their residue even if
    // the freshly-written marker lags the listing/metadata within read-after-write grace.
    private var partialMarkerMonthRelPaths: Set<String> = []
    // Every month this run's phase1 scanned (had a live V1 manifest) and processed with authoritative
    // in-memory knowledge. Their residue is safe to sweep without the cross-run grace probe; only
    // prior-run residue (a month NOT scanned this run — e.g. an interrupted earlier migration's
    // partial-marker month resumed via runFullMigration) needs the marker-visibility wait.
    private var sameRunScannedMonthRelPaths: Set<String> = []

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
        self.journalStore = MigrationJournalStore(client: client, basePath: basePath)
    }

    private func recordMigrationJournal(
        repoID: String,
        writerID: String,
        runID: String,
        year: Int,
        month: Int,
        outcome: MigrationJournalOutcome,
        migratedAssetCount: Int,
        totalAssetCount: Int,
        skippedAssetCount: Int,
        reason: String?
    ) async throws {
        try await journalStore.record(MigrationJournalRecord(
            repoID: repoID,
            writerID: writerID,
            runID: runID,
            year: year,
            month: month,
            outcome: outcome,
            createdAtMs: Int64(Date().timeIntervalSince1970 * 1000),
            migratedAssetCount: migratedAssetCount,
            totalAssetCount: totalAssetCount,
            skippedAssetCount: skippedAssetCount,
            reason: reason
        ))
    }

    func scanV1Months() async throws -> [ScannedV1Month] {
        var results: [ScannedV1Month] = []
        // Strict policy: silent skip + phase3 retry-scan would DELETE V1 manifests phase1 never processed,
        // so every list failure surfaces.
        try await V1MonthIterator.forEachMonth(
            client: client,
            basePath: basePath,
            options: .init(listFailurePolicy: .propagate)
        ) { year, month, monthPath in
            if try await V1MonthIterator.monthContainsManifest(
                client: client,
                monthPath: monthPath,
                listFailurePolicy: .propagate
            ) {
                let absPath = RemotePathBuilder.absolutePath(basePath: monthPath, remoteRelativePath: MonthManifestStore.manifestFileName)
                results.append(ScannedV1Month(year: year, month: month, manifestAbsolutePath: absPath))
            }
            return .continue
        }
        return results
    }

    @discardableResult
    func runPhase1(profileID: Int64, repoID: String, writerID: String, runID: String) async throws -> Int {
        partialMarkerMonthRelPaths.removeAll()
        sameRunScannedMonthRelPaths.removeAll()
        // WebDAV/SMB/SFTP don't auto-create parents on PUT.
        try await bootstrap.ensureSubdirectories()
        // Marker before any commit keeps interrupted migration visible to inspect.
        try await markerStore.writePhase(writerID: writerID, phase: .phase1, runID: runID)

        let allocator = SeqAllocator(database: database, profileID: profileID, repoID: repoID, initial: try await initialSeq(repoID: repoID, profileID: profileID))
        let clock = PersistedLamportClock(database: database, profileID: profileID, repoID: repoID, initial: try await initialClock(repoID: repoID, profileID: profileID))
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        let months = try await scanV1Months()
        sameRunScannedMonthRelPaths = Set(months.map { String(format: "%04d/%02d", $0.year, $0.month) })
        var processed = 0
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        var existingV2Output: RepoMaterializer.MaterializeOutput?

        for scanned in months {
            try Task.checkCancellation()
            // Journal the per-month terminal decision before residue moves aside; an uncaught
            // per-month failure (non-cancellation) records `failed` best-effort and rethrows the
            // original error. `monthJournaled` keeps a quarantine/commit failure after a recorded
            // decision from double-journaling.
            var monthJournaled = false
            var monthTotalAssetCount = 0
            var monthMigratedAssetCount = 0
            do {
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
                try await recordMigrationJournal(repoID: repoID, writerID: writerID, runID: runID, year: scanned.year, month: scanned.month, outcome: .quarantined, migratedAssetCount: 0, totalAssetCount: 0, skippedAssetCount: 0, reason: "V1 manifest unreadable")
                monthJournaled = true
                try await residueQuarantine.quarantine(year: scanned.year, month: scanned.month, sourcePath: scanned.manifestAbsolutePath)
                continue
            }

            let snapshot = store.unsortedSnapshot()
            monthTotalAssetCount = snapshot.assets.count
            if snapshot.assets.isEmpty {
                // No assets to migrate; remove or quarantine so the V1 scan stops finding it.
                if snapshot.resources.isEmpty && snapshot.links.isEmpty {
                    // Record before the destructive delete so a processed empty month is never left
                    // without a journal entry; the guard stops a later delete failure double-recording.
                    try await recordMigrationJournal(repoID: repoID, writerID: writerID, runID: runID, year: scanned.year, month: scanned.month, outcome: .quarantined, migratedAssetCount: 0, totalAssetCount: 0, skippedAssetCount: 0, reason: "empty V1 manifest deleted")
                    monthJournaled = true
                    try await deleteIfPresent(path: scanned.manifestAbsolutePath)
                } else {
                    v1MigrationLog.warning("V1 manifest for \(scanned.year, privacy: .public)-\(scanned.month, privacy: .public) has \(snapshot.resources.count, privacy: .public) resources / \(snapshot.links.count, privacy: .public) links but no assets — quarantining as legacy residue")
                    try await recordMigrationJournal(repoID: repoID, writerID: writerID, runID: runID, year: scanned.year, month: scanned.month, outcome: .quarantined, migratedAssetCount: 0, totalAssetCount: 0, skippedAssetCount: 0, reason: "inconsistent V1 manifest: \(snapshot.resources.count) resources / \(snapshot.links.count) links / 0 assets")
                    monthJournaled = true
                    // Write partial-migration marker before quarantine so phase-3 sweep preserves
                    // the residue: a structurally inconsistent V1 manifest (resources/links without
                    // assets) is forensic evidence later repair tooling needs, not orphan data.
                    try await writePartialMigrationMarker(
                        year: scanned.year,
                        month: scanned.month,
                        runID: runID,
                        migratedAssetCount: 0,
                        totalAssetCount: 0,
                        failures: ["inconsistent V1 manifest: \(snapshot.resources.count) resources / \(snapshot.links.count) links / 0 assets"]
                    )
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
                // A non-clean V2 month folds to a best-effort/partial baseline whose asset set is not
                // authoritative: deduping/publishing from it can re-add stale V1 rows over trusted V2
                // resource shape and resurrect fingerprints present only in the non-selected/rejected
                // state. Like every other V2 write/maintenance consumer, fail closed and defer.
                if let monthOutcome = existingV2Output?.outcomeByMonth[monthKey],
                   monthOutcome == .ambiguous || monthOutcome == .corrupt {
                    v1MigrationLog.warning("V1 manifest for \(scanned.year, privacy: .public)-\(scanned.month, privacy: .public) overlaps a non-clean V2 month (\(String(describing: monthOutcome), privacy: .public)); deferring migration and quarantining as legacy residue")
                    try await recordMigrationJournal(repoID: repoID, writerID: writerID, runID: runID, year: scanned.year, month: scanned.month, outcome: .quarantined, migratedAssetCount: 0, totalAssetCount: snapshot.assets.count, skippedAssetCount: 0, reason: "existing V2 month outcome \(String(describing: monthOutcome)) not clean; migration deferred to avoid clobbering trusted V2 state")
                    monthJournaled = true
                    try await writePartialMigrationMarker(
                        year: scanned.year,
                        month: scanned.month,
                        runID: runID,
                        migratedAssetCount: 0,
                        totalAssetCount: snapshot.assets.count,
                        failures: ["existing V2 month outcome \(String(describing: monthOutcome)) not clean; migration deferred to avoid clobbering trusted V2 state"]
                    )
                    try await residueQuarantine.quarantine(
                        year: scanned.year,
                        month: scanned.month,
                        sourcePath: scanned.manifestAbsolutePath
                    )
                    continue
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
                try await recordMigrationJournal(repoID: repoID, writerID: writerID, runID: runID, year: scanned.year, month: scanned.month, outcome: .quarantined, migratedAssetCount: 0, totalAssetCount: snapshot.assets.count, skippedAssetCount: skippedAssetFailures.count, reason: skippedAssetFailures.first ?? "no migrable assets")
                monthJournaled = true
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
                    state.resources[RemotePhysicalPathKey(resource.physicalRemotePath)] = SnapshotResourceRow(
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
                covered: covered,
                createdAtMs: nil
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
            monthMigratedAssetCount = migrableAssets.count
            // Record `imported` after commit/snapshot publish and before V1 manifest quarantine.
            try await recordMigrationJournal(repoID: repoID, writerID: writerID, runID: runID, year: scanned.year, month: scanned.month, outcome: .imported, migratedAssetCount: migrableAssets.count, totalAssetCount: snapshot.assets.count, skippedAssetCount: skippedAssetFailures.count, reason: nil)
            monthJournaled = true
            try await residueQuarantine.quarantine(
                year: scanned.year,
                month: scanned.month,
                sourcePath: scanned.manifestAbsolutePath
            )
            processed += 1
            } catch {
                // Cancellation is not a migration failure: rethrow without journaling or remote writes.
                if error is CancellationError || RemoteWriteClassifier.isCancellation(error) { throw error }
                if !monthJournaled {
                    // Best-effort: a failing failed-record write must not mask the original error.
                    try? await recordMigrationJournal(
                        repoID: repoID,
                        writerID: writerID,
                        runID: runID,
                        year: scanned.year,
                        month: scanned.month,
                        outcome: .failed,
                        migratedAssetCount: monthMigratedAssetCount,
                        totalAssetCount: monthTotalAssetCount,
                        skippedAssetCount: 0,
                        reason: String(describing: error)
                    )
                }
                throw error
            }
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

    func runPhase3(
        writerID: String,
        runID: String,
        crossRunMarkerVisibilityDeadline: Date? = nil
    ) async throws {
        try await markerStore.writePhase(writerID: writerID, phase: .phase3, runID: runID)
        try await residueQuarantine.sweepResidueManifests(
            preserveMonthRelPaths: partialMarkerMonthRelPaths,
            sameRunProcessedMonthRelPaths: sameRunScannedMonthRelPaths,
            crossRunMarkerVisibilityDeadline: crossRunMarkerVisibilityDeadline
        )
        try await markerStore.deleteAll(writerID: writerID)
    }

    /// FSM step: verify final state. Post-condition guard — re-list base and
    /// assert (a) no journal-unresolved V1 manifest visible, (b) the writer we
    /// just cleaned has no surviving marker. Intentionally does NOT check
    /// `anyMigrationMarkerExists` (peer markers from prior aborted runs are the
    /// next inspection's job) nor `migrationCompleted` (cleanup-only path doesn't set it).
    func verifyFinalState(repoID: String, cleanedWriterID: String) async throws {
        let lingering = try await scanV1Months()
        if !lingering.isEmpty {
            // A month a safe terminal journal record resolved has durable commit+snapshot data; only
            // its physical manifest cleanup lagged an interrupt. Inspection's journal-suppressed routing
            // (hasUnresolvedV1Manifests) admits exactly that state to cleanup-only, so the post-condition
            // must suppress it too rather than fail a fully-migrated repo. Scoped to this repo's ID so a
            // foreign/planted record can't mask a genuinely-unresolved manifest.
            let resolved = try await MigrationJournalStore(client: client, basePath: basePath)
                .loadSummary()
                .safelyResolvedMonths(forRepoID: repoID)
            let unresolved = lingering.filter { !resolved.contains(LibraryMonthKey(year: $0.year, month: $0.month)) }
            if !unresolved.isEmpty {
                throw MigrationError.verifyFailed(reason: "V1 manifest still visible at \(unresolved.count) month(s)")
            }
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
            // runFullMigration is also the resume route for an interrupted multi-month migration: when a
            // later month still has a live V1 manifest, inspection routes here (not cleanup-only) while a
            // prior-run partial-marker month survives only as residue — not in the in-memory scanned set.
            // Pass the cross-run deadline so that prior-run residue gets the marker-visibility window; the
            // same-run scanned set short-circuits this run's own months so the healthy migration pays nothing.
            try await runPhase3(
                writerID: writerID,
                runID: runID,
                crossRunMarkerVisibilityDeadline: computeCrossRunMarkerVisibilityDeadline()
            )
            await onMigrationComplete?(migratedCount)
        }
        try await verifyFinalState(repoID: repoID, cleanedWriterID: writerID)
        return MigrationOutcome(migratedMonthCount: migratedCount)
    }

    func runCleanupOnly(
        repoID: String,
        ownerWriterID: String,
        writerID: String,
        runID: String
    ) async throws {
        try await ensureVersionPublished(writerID: writerID)
        // Cross-run cleanup has no in-memory partial-marker set; on grace backends a marker written
        // just before the interrupt can still lag the sweep's listing/metadata, so give it the
        // read-after-write window to surface before its residue can be deleted.
        try await runPhase3(
            writerID: ownerWriterID,
            runID: runID,
            crossRunMarkerVisibilityDeadline: computeCrossRunMarkerVisibilityDeadline()
        )
        try await verifyFinalState(repoID: repoID, cleanedWriterID: ownerWriterID)
    }

    /// Shared marker-visibility deadline for both cross-run entrypoints (cleanup-only and
    /// interrupted runFullMigration resume). Factored into one place so a future caller can't
    /// reintroduce the asymmetry where one entrypoint swept prior-run residue on a single
    /// non-grace probe. nil on zero-grace backends (single-probe behavior, no added latency).
    private func computeCrossRunMarkerVisibilityDeadline() -> Date? {
        client.readAfterWriteGraceSeconds > 0
            ? client.metadataReadAfterWriteDeadline(floorSeconds: 1)
            : nil
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
        partialMarkerMonthRelPaths.insert(monthRel)
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
