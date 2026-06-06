import Foundation
import os.log

private let compactionLog = Logger(subsystem: "com.zizicici.watermelon", category: "RepoCompactionService")

enum RepoCompactionSnapshotGCMode: Sendable, Equatable {
    case disabled
    case reportOnly
    case thresholdGated
    case always
}

struct RepoCompactionMonthContext: Sendable {
    let month: LibraryMonthKey
    let materialized: RepoMaterializer.MaterializeOutput
    let monthReport: RepoCompactionMonthReport
}

struct RepoCompactionService: Sendable {
    let services: BackupV2RuntimeServices
    let nowMs: @Sendable () -> Int64

    init(
        services: BackupV2RuntimeServices,
        nowMs: @escaping @Sendable () -> Int64 = {
            Int64(Date().timeIntervalSince1970 * 1000)
        }
    ) {
        self.services = services
        self.nowMs = nowMs
    }

    // MARK: - Public Entry Points

    func compactMonth(
        _ month: LibraryMonthKey,
        snapshotGCMode: RepoCompactionSnapshotGCMode = .disabled
    ) async throws -> RepoMaintenanceMonthResult {
        try Task.checkCancellation()

        let materialized = try await RepoMaterializer(
            client: services.metadataClient,
            basePath: services.basePath
        ).materializeMonth(month, expectedRepoID: services.repoID)

        guard materialized.outcomeByMonth[month] == .clean else {
            compactionLog.info("compaction skip \(month.text, privacy: .public): outcome not clean")
            return skippedResult(month: month)
        }

        let covered = materialized.coveredByMonth[month, default: .empty]
        let monthState = materialized.state.months[month] ?? .empty
        let hasContent = !covered.rangesByWriter.values.allSatisfy(\.isEmpty) || !monthState.assets.isEmpty
            || !monthState.resources.isEmpty
            || !monthState.assetResources.isEmpty
            || !monthState.deletedAssetStamps.isEmpty
        guard hasContent else {
            return skippedResult(month: month)
        }

        let report: RepoCompactionReport?
        do {
            report = try await RepoCompactionPlanner(
                client: services.metadataClient,
                basePath: services.basePath,
                policy: services.compactionPolicy
            ).makeReport(expectedRepoID: services.repoID, preMaterialized: materialized)
        } catch is CancellationError {
            // `try?` would swallow this, letting a last-month cancel bypass the runner's cancel-path shutdown.
            throw CancellationError()
        } catch {
            report = nil
        }
        guard let monthReport = report?.months.first(where: { $0.month == month }) else {
            return skippedResult(month: month)
        }

        let checkpointResult: RepoCheckpointResult
        do {
            let context = RepoCompactionMonthContext(
                month: month,
                materialized: materialized,
                monthReport: monthReport
            )
            checkpointResult = try await RepoCheckpointService(
                client: services.metadataClient,
                basePath: services.basePath,
                repoID: services.repoID,
                writerID: services.writerID,
                runID: services.runID,
                clock: services.lamport,
                policy: services.compactionPolicy
            ).checkpointMonth(month, mode: .whenRecommended, respectTaskCancellation: true, context: context)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            compactionLog.error("compaction checkpoint failed for \(month.text, privacy: .public): \(String(describing: error), privacy: .public)")
            return skippedResult(month: month)
        }

        let checkpointPhaseResult = mapCheckpointToPhaseResult(checkpointResult)

        // Commit GC is driven by the materialize-accepted baseline, not by this run's checkpoint,
        // so a residual deletable prefix self-heals even when no fresh checkpoint is recommended.
        let commitCleanup: RepoRetentionCommitDeleteResult
        do {
            commitCleanup = try await runCommitGC(
                month: month,
                postCheckpointAccepted: checkpointResult.acceptedSnapshot,
                monthReport: monthReport
            )
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            compactionLog.error("compaction commit GC failed for \(month.text, privacy: .public): \(String(describing: error), privacy: .public)")
            commitCleanup = .preflightBlocked(blockers: [], report: emptyCommitReport(month: month))
        }

        let snapshotGC: RepoMaintenanceSnapshotGCDisposition
        if let resolved = resolveSnapshotGC(
            month: month,
            monthReport: monthReport,
            snapshotGCMode: snapshotGCMode,
            commitCleanup: commitCleanup
        ) {
            snapshotGC = resolved
        } else {
            do {
                let gcResult = try await runSnapshotGC(month: month)
                snapshotGC = .ran(gcResult)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                compactionLog.error("compaction snapshot GC failed for \(month.text, privacy: .public): \(String(describing: error), privacy: .public)")
                snapshotGC = .skipped(.skippedAfterCommitCleanupStopped)
            }
        }

        // resolveSnapshotGC maps a cancelled-but-completed/blocked commit phase to
        // `.skipped(.skippedCancellation)` rather than throwing; propagate it like checkpoint and
        // commit GC do so a last-month cancellation still reaches the caller's cancel-path shutdown.
        try Task.checkCancellation()

        return RepoMaintenanceMonthResult(
            month: month,
            checkpoint: checkpointPhaseResult,
            commitCleanup: commitCleanup,
            snapshotGC: snapshotGC
        )
    }

    func compactStartupMonths() async throws -> RepoMaintenanceStartupResult {
        let months = try await candidateStartupMonths()
        var results: [LibraryMonthKey: RepoMaintenanceMonthResult] = [:]
        for month in months {
            try Task.checkCancellation()
            do {
                results[month] = try await compactMonth(month, snapshotGCMode: .thresholdGated)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                compactionLog.error("compaction startup month \(month.text, privacy: .public) failed: \(String(describing: error), privacy: .public)")
            }
        }
        return RepoMaintenanceStartupResult(monthResults: results)
    }

    func compactMonthForUserMaintenance(_ month: LibraryMonthKey) async throws -> RepoMaintenanceMonthResult {
        try await compactMonth(month, snapshotGCMode: .always)
    }

    func reportSnapshotGC() async throws -> RepoMaintenanceStartupResult {
        let months = try await candidateStartupMonths()
        var results: [LibraryMonthKey: RepoMaintenanceMonthResult] = [:]
        for month in months {
            try Task.checkCancellation()
            do {
                results[month] = try await compactMonth(month, snapshotGCMode: .reportOnly)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                compactionLog.error("compaction report month \(month.text, privacy: .public) failed: \(String(describing: error), privacy: .public)")
            }
        }
        return RepoMaintenanceStartupResult(monthResults: results)
    }

    /// Recover months stuck `.corrupt` solely because every snapshot baseline was unreadable while
    /// commit replay rebuilt complete state: write a fresh verified baseline from that replay state so
    /// the month re-materializes `.clean` and normal GC can reclaim the corrupt file. Without this the
    /// `.clean` gate on every compaction/checkpoint entry leaves the month permanently un-writable and
    /// un-maintainable. Additive only — rejected/foreign uncovered commits are not repaired here, since
    /// recovering those requires removing the offending finalized file (out of scope for a baseline write).
    /// Completeness is enforced, not assumed: a corrupt snapshot can be the sole record of a GC'd commit
    /// prefix, so months whose surviving replay is provably incomplete are left corrupt rather than
    /// laundered into a verified-clean baseline that silently drops the lost assets.
    @discardableResult
    func repairCorruptSnapshotBaselines() async throws -> Int {
        try Task.checkCancellation()
        let materialized: RepoMaterializer.MaterializeOutput
        if let preMaterialized = await services.initialMaterializeOutput.peek() {
            materialized = preMaterialized
        } else {
            materialized = try await RepoMaterializer(
                client: services.metadataClient,
                basePath: services.basePath
            ).materialize(expectedRepoID: services.repoID)
        }
        let months = materialized.corruptedSnapshotMonths
            .filter { materialized.outcomeByMonth[$0] == .corrupt }
            .sorted()
        guard !months.isEmpty else { return 0 }

        var repaired = 0
        for month in months {
            try Task.checkCancellation()
            // A1a: repair only with authenticated proof of completeness. The corrupt snapshot's covered must
            // be recoverable from its filename-bound coverage attestation (unknown ⇒ fail closed, e.g. a
            // legacy unattested corrupt snapshot), and everything it attested to cover must still be recorded
            // by surviving materialized coverage — otherwise it may have been the sole record of a GC'd prefix
            // and repairing would launder that loss into a verified-clean baseline.
            guard let authenticatedCoverage = materialized.authenticatedCorruptCoverageByMonth[month] else {
                compactionLog.warning("corrupt-snapshot baseline repair skip \(month.text, privacy: .public): corrupt snapshot coverage unknown (no authenticated coverage attestation)")
                continue
            }
            guard Self.authenticatedCoverageGloballyRecorded(authenticatedCoverage, materialized: materialized) else {
                compactionLog.warning("corrupt-snapshot baseline repair skip \(month.text, privacy: .public): authenticated corrupt coverage not globally recorded by surviving coverage")
                continue
            }
            // Belt-and-suspenders: the surviving commit replay must also be provably complete. If a GC'd
            // commit prefix lost its sole baseline (this month's now-unreadable snapshot), repairing
            // would finalize that data loss and erase the non-clean signal, so leave the month corrupt.
            guard Self.corruptMonthReplayIsProvablyComplete(month, materialized: materialized) else {
                compactionLog.warning("corrupt-snapshot baseline repair skip \(month.text, privacy: .public): surviving replay incomplete (GC'd commit prefix lost with its only snapshot)")
                continue
            }
            do {
                // The corrupt snapshot lingers after repair; advance the fresh baseline beyond its filename
                // lamport so any later cleanup sees the repaired snapshot as the newest valid authority.
                if let corruptLamport = materialized.corruptSnapshotMaxLamportByMonth[month] {
                    try await services.lamport.observe(corruptLamport)
                }
                // checkpointMonth re-materializes and re-gates on corruptedSnapshotMonths, so a month
                // that healed since the open-time materialize is a no-op rather than a forced write.
                let result = try await RepoCheckpointService(
                    client: services.metadataClient,
                    basePath: services.basePath,
                    repoID: services.repoID,
                    writerID: services.writerID,
                    runID: services.runID,
                    clock: services.lamport,
                    policy: services.compactionPolicy
                ).checkpointMonth(month, mode: .repairCorruptBaseline, respectTaskCancellation: true)
                if result.outcome == .writtenAccepted {
                    repaired += 1
                    compactionLog.info("corrupt-snapshot baseline repaired for \(month.text, privacy: .public)")
                }
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                compactionLog.error("corrupt-snapshot baseline repair failed for \(month.text, privacy: .public): \(String(describing: error), privacy: .public)")
            }
        }
        return repaired
    }

    // MARK: - Commit GC

    private func runCommitGC(
        month: LibraryMonthKey,
        postCheckpointAccepted: RepoMaterializer.AcceptedSnapshotBaselineInfo?,
        monthReport: RepoCompactionMonthReport
    ) async throws -> RepoRetentionCommitDeleteResult {
        try Task.checkCancellation()

        let materialized = try await RepoMaterializer(
            client: services.metadataClient,
            basePath: services.basePath
        ).materializeMonth(month, expectedRepoID: services.repoID)

        guard materialized.outcomeByMonth[month] == .clean else {
            compactionLog.info("compaction commit GC skip \(month.text, privacy: .public): outcome not clean after checkpoint")
            return .preflightBlocked(blockers: [], report: emptyCommitReport(month: month))
        }

        if let blocker = try await commitGCMigrationMarkerBlocker() {
            compactionLog.info("compaction commit GC skip \(month.text, privacy: .public): \(String(describing: blocker), privacy: .public)")
            return .preflightBlocked(blockers: [blocker], report: emptyCommitReport(month: month))
        }

        guard let accepted = materialized.acceptedSnapshotBaselinesByMonth[month] else {
            return .preflightBlocked(blockers: [.noAcceptedSnapshot(month: month)], report: emptyCommitReport(month: month))
        }

        if let postCheckpointAccepted, !accepted.covered.superset(of: postCheckpointAccepted.covered) {
            compactionLog.info("compaction commit GC skip \(month.text, privacy: .public): post-materialize accepted covered regressed below checkpoint accepted")
            return .preflightBlocked(blockers: [], report: emptyCommitReport(month: month))
        }

        let coveredPrefix = services.compactionPolicy.conservativeDeletePrefixByWriter(covered: accepted.covered)
        let plannerPrefix = monthReport.deletePrefixByWriter
        let deletePrefix = Self.computeDeletePrefix(acceptedPrefix: coveredPrefix, plannerPrefix: plannerPrefix)
        guard !deletePrefix.isEmpty else {
            return .preflightBlocked(blockers: [.noDeleteCandidates], report: emptyCommitReport(month: month))
        }

        let scan = try await RepoRetentionDeleteCandidateScanner(
            client: services.metadataClient,
            basePath: services.basePath
        ).scan(month: month, expectedRepoID: services.repoID, deletePrefixByWriter: deletePrefix)

        if !scan.blockers.isEmpty {
            return .preflightBlocked(blockers: scan.blockers, report: emptyCommitReport(month: month))
        }
        guard !scan.candidates.isEmpty else {
            return .preflightBlocked(blockers: [.noDeleteCandidates], report: emptyCommitReport(month: month))
        }

        let acceptedFile: SnapshotFile
        do {
            acceptedFile = try await SnapshotReader(client: services.metadataClient, basePath: services.basePath)
                .read(filename: accepted.filename)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            return .preflightBlocked(blockers: [.materializerReadFailed], report: emptyCommitReport(month: month))
        }

        let covered = materialized.coveredByMonth[month, default: .empty]
        let evidence = RepoRetentionPreDeleteEvidence(
            materializedState: materialized.state,
            materializedCovered: covered,
            observedSeqByWriter: materialized.observedSeqByWriter,
            acceptedSnapshot: accepted,
            postDeleteEquivalenceContract: RepoRetentionPostDeleteEquivalenceContract(
                mode: .retentionSuperset,
                acceptedSnapshotFilename: accepted.filename,
                acceptedSnapshotSHA256Hex: acceptedFile.sha256Hex.lowercased(),
                acceptedSnapshotCovered: accepted.covered,
                requiredObservedSeqByWriter: materialized.observedSeqByWriter,
                expectedDeletePrefixByWriter: deletePrefix,
                preDeleteCovered: covered,
                preDeleteState: materialized.state
            )
        )

        let plan = RepoRetentionDeletePreflightPlan(
            month: month,
            repoID: services.repoID,
            acceptedSnapshot: accepted,
            deletePrefixByWriter: deletePrefix,
            commitFiles: scan.candidates,
            protectedSummary: scan.protectedSummary,
            preDeleteEvidence: evidence
        )

        var preflightReport = emptyCommitReport(month: month)
        preflightReport.acceptedSnapshot = accepted
        preflightReport.materializedCovered = covered
        preflightReport.observedSeqByWriter = materialized.observedSeqByWriter
        preflightReport.deletePrefixByWriter = deletePrefix
        preflightReport.candidateScan = scan
        preflightReport.compactionMonthReport = monthReport

        let result = try await RepoRetentionCommitDeleteExecutor(
            client: services.metadataClient,
            basePath: services.basePath,
            policy: services.compactionPolicy,
            isLocalVolume: services.isLocalVolume
        ).execute(plan: plan, report: preflightReport)

        if containsCancellation(result) {
            throw CancellationError()
        }
        return result
    }

    // MARK: - Snapshot GC dispatch

    private func resolveSnapshotGC(
        month: LibraryMonthKey,
        monthReport: RepoCompactionMonthReport,
        snapshotGCMode: RepoCompactionSnapshotGCMode,
        commitCleanup: RepoRetentionCommitDeleteResult
    ) -> RepoMaintenanceSnapshotGCDisposition? {
        switch snapshotGCMode {
        case .disabled:
            compactionLog.info("compaction snapshot GC skip \(month.text, privacy: .public): disabled for this run (snapshots=\(monthReport.snapshotFileCount, privacy: .public), snapshotBytes=\(monthReport.snapshotBytes, privacy: .public))")
            return .skipped(.skippedDisabled)
        case .reportOnly:
            compactionLog.info("compaction snapshot GC report \(month.text, privacy: .public): snapshots=\(monthReport.snapshotFileCount, privacy: .public), parseable=\(monthReport.parseableSnapshotFileCount, privacy: .public), unparseable=\(monthReport.unparseableSnapshotFileCount, privacy: .public), snapshotBytes=\(monthReport.snapshotBytes, privacy: .public)")
            return .skipped(.skippedReportOnly)
        case .thresholdGated:
            // Gate on the already-listed count so below-threshold months skip before a fresh materialize.
            guard services.compactionPolicy.shouldRunSnapshotGC(snapshotFileCount: monthReport.snapshotFileCount) else {
                compactionLog.info("compaction snapshot GC skip \(month.text, privacy: .public): below threshold (snapshots=\(monthReport.snapshotFileCount, privacy: .public), trigger=\(services.compactionPolicy.snapshotGCTriggerFileCount, privacy: .public))")
                return .skipped(.skippedBelowThreshold)
            }
        case .always:
            break
        }
        switch commitCleanup {
        case .preflightBlocked, .completed:
            if Task.isCancelled {
                return .skipped(.skippedCancellation)
            }
            return nil
        case .stopped:
            return .skipped(.skippedAfterCommitCleanupStopped)
        case .verificationFailed:
            return .skipped(.skippedAfterCommitCleanupVerificationFailed)
        case .verificationInconclusive:
            return .skipped(.skippedAfterCommitCleanupVerificationInconclusive)
        }
    }

    // MARK: - Snapshot GC

    private func runSnapshotGC(month: LibraryMonthKey) async throws -> RepoSnapshotGCResult {
        try Task.checkCancellation()

        let materialized = try await RepoMaterializer(
            client: services.metadataClient,
            basePath: services.basePath
        ).materializeMonth(month, expectedRepoID: services.repoID)

        guard materialized.outcomeByMonth[month] == .clean else {
            compactionLog.info("compaction snapshot GC skip \(month.text, privacy: .public): outcome not clean after commit GC")
            return .preflightBlocked(blockers: [], report: emptySnapshotReport(month: month))
        }

        if let blocker = try await snapshotGCMigrationMarkerBlocker() {
            compactionLog.info("compaction snapshot GC skip \(month.text, privacy: .public): \(String(describing: blocker), privacy: .public)")
            return .preflightBlocked(blockers: [blocker], report: emptySnapshotReport(month: month))
        }

        guard let accepted = materialized.acceptedSnapshotBaselinesByMonth[month] else {
            return .preflightBlocked(
                blockers: [.noAcceptedPerMonthSnapshot(month: month)],
                report: emptySnapshotReport(month: month)
            )
        }

        let scan = try await SnapshotDeleteCandidateScanner(
            client: services.metadataClient,
            basePath: services.basePath,
            policy: services.compactionPolicy
        ).scan(
            month: month,
            expectedRepoID: services.repoID,
            acceptedBaseline: accepted
        )

        if !scan.blockers.isEmpty {
            return .preflightBlocked(blockers: scan.blockers, report: emptySnapshotReport(month: month))
        }
        if !scan.acceptedBaselineListed {
            return .preflightBlocked(
                blockers: [.acceptedBaselineNotListed(filename: accepted.filename)],
                report: emptySnapshotReport(month: month)
            )
        }
        if scan.candidates.isEmpty {
            return .preflightBlocked(blockers: [.noDeleteCandidates], report: emptySnapshotReport(month: month))
        }

        // Domination + keepN live only in the protection set; the scanner just validates and lists.
        let protection = RepoSnapshotProtectionSet.compute(.init(
            acceptedBaselineFilename: accepted.filename,
            acceptedBaselineCovered: accepted.covered,
            parseableSnapshotsForMonth: scan.parseableSnapshots.map {
                .init(filename: $0.filename, lamport: $0.lamport, writerID: $0.writerID, covered: $0.covered)
            },
            snapshotKeepCount: services.compactionPolicy.snapshotFallbackKeepCount
        ))

        let deleteFilenames = Set(protection.deleteCandidateFilenames)
        let deleteCandidates = scan.candidates.filter { deleteFilenames.contains($0.filename) }
        guard !deleteCandidates.isEmpty else {
            return .preflightBlocked(blockers: [.noDeleteCandidates], report: emptySnapshotReport(month: month))
        }

        let acceptedFile: SnapshotFile
        do {
            acceptedFile = try await SnapshotReader(client: services.metadataClient, basePath: services.basePath)
                .read(filename: accepted.filename)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            return .preflightBlocked(blockers: [.materializerReadFailed], report: emptySnapshotReport(month: month))
        }

        var additionalProtectedSHAByFilename: [String: String] = [:]
        let candidateSHAByFilename = Dictionary(uniqueKeysWithValues: scan.candidates.map {
            ($0.filename, $0.sha256Hex.lowercased())
        })
        let snapshotReader = SnapshotReader(client: services.metadataClient, basePath: services.basePath)
        for filename in protection.protectedFilenames.sorted() {
            if filename == accepted.filename { continue }
            if let sha = candidateSHAByFilename[filename] {
                additionalProtectedSHAByFilename[filename] = sha
                continue
            }
            do {
                let file = try await snapshotReader.read(filename: filename)
                additionalProtectedSHAByFilename[filename] = file.sha256Hex.lowercased()
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                return .preflightBlocked(blockers: [.materializerReadFailed], report: emptySnapshotReport(month: month))
            }
        }

        let covered = materialized.coveredByMonth[month, default: .empty]
        let contract = RepoSnapshotPostDeleteEquivalenceContract(
            acceptedSnapshotFilename: accepted.filename,
            acceptedSnapshotLamport: accepted.lamport,
            acceptedSnapshotSHA256Hex: acceptedFile.sha256Hex.lowercased(),
            acceptedSnapshotCovered: accepted.covered,
            additionalProtectedSnapshotSHA256ByFilename: additionalProtectedSHAByFilename,
            requiredObservedSeqByWriter: materialized.observedSeqByWriter,
            preDeleteCovered: covered,
            preDeleteState: materialized.state,
            preDeleteObservedClock: materialized.state.observedClock
        )

        let plan = RepoSnapshotDeletePreflightPlan(
            month: month,
            repoID: services.repoID,
            acceptedSnapshot: accepted,
            acceptedSnapshotSHA256Hex: acceptedFile.sha256Hex.lowercased(),
            protectedFilenames: protection.protectedFilenames,
            snapshotsToDelete: deleteCandidates,
            protectedSummary: scan.protectedSummary,
            postDeleteContract: contract
        )

        var preflightReport = emptySnapshotReport(month: month)
        preflightReport.acceptedSnapshot = accepted
        preflightReport.materializedCovered = covered
        preflightReport.observedSeqByWriter = materialized.observedSeqByWriter
        preflightReport.protectedFilenames = protection.protectedFilenames
        preflightReport.candidateScan = scan

        let result = try await RepoSnapshotDeleteExecutor(
            client: services.metadataClient,
            basePath: services.basePath,
            policy: services.compactionPolicy,
            isLocalVolume: services.isLocalVolume
        ).execute(plan: plan, report: preflightReport)

        if snapshotGCContainsCancellation(result) {
            throw CancellationError()
        }
        return result
    }

    // MARK: - Migration-marker delete barrier

    // Migration-marker state is unresolved V2 authority. Cleanup-only open clears only the selected
    // owner's marker (V1MigrationService.verifyFinalState intentionally leaves peer markers for the
    // next inspection), and startup/user maintenance can run before that next inspection — so a fresh
    // marker probe must gate every destructive delete. Presence ⇒ block; an inconclusive read fails
    // closed (skip, never delete). This is the "migration markers should skip deletion" invariant and
    // turns the declared-but-unemitted migration blocker cases into real signals. Safe direction only:
    // a spurious block (e.g. a just-deleted marker still listing within read-after-write grace) merely
    // defers compaction to the next run.
    private func anyMigrationMarkerPresent() async throws -> Bool {
        try await MigrationMarkerStore(client: services.metadataClient, basePath: services.basePath).existsAny()
    }

    private func commitGCMigrationMarkerBlocker() async throws -> RepoRetentionDeletePreflightBlocker? {
        do {
            return try await anyMigrationMarkerPresent() ? .migrationInProgress : nil
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            return .migrationCheckFailed
        }
    }

    private func snapshotGCMigrationMarkerBlocker() async throws -> RepoSnapshotDeletePreflightBlocker? {
        do {
            return try await anyMigrationMarkerPresent() ? .migrationInProgress : nil
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            return .migrationCheckFailed
        }
    }

    // MARK: - Helpers

    private func candidateStartupMonths() async throws -> [LibraryMonthKey] {
        // Reuse the open-time full materialize so the planner does not re-fold the whole repo.
        let preMaterialized = await services.initialMaterializeOutput.peek()
        let report = try await RepoCompactionPlanner(
            client: services.metadataClient,
            basePath: services.basePath,
            policy: services.compactionPolicy
        ).makeReport(expectedRepoID: services.repoID, preMaterialized: preMaterialized)
        return report.months
            .filter {
                $0.checkpointRecommended
                    || $0.legacyBaselineUpgradeRecommended
                    || $0.checkpointCoveredPrefixCandidateCount > 0
                    || services.compactionPolicy.shouldRunSnapshotGC(snapshotFileCount: $0.snapshotFileCount)
            }
            .map(\.month)
            .sorted()
    }

    // A corrupt-snapshot month is safe to re-bless `.clean` only if every commit the unreadable
    // snapshot could have covered is still accounted for. Commit GC only deletes a writer's contiguous
    // prefix starting at seq 1 (conservativeContiguousPrefixByWriter), so each writer's surviving range
    // must be covered from seq 1 through its highest surviving seq by some month's commits or an accepted
    // baseline (the global covered union). A gap there may be a GC'd commit whose sole record was this
    // month's now-unreadable snapshot — repairing would silently drop those assets.
    /// Every (writer, seq) the body-corrupt snapshot authenticated as covered must still be recorded by the
    /// surviving materialized coverage (the global union across months). If so, the corrupt snapshot was not
    /// the sole record of anything, so a fresh baseline from surviving replay drops nothing.
    private static func authenticatedCoverageGloballyRecorded(
        _ authenticatedCoverage: CoveredRanges,
        materialized: RepoMaterializer.MaterializeOutput
    ) -> Bool {
        var globalCovered = CoveredRanges.empty
        for (_, covered) in materialized.coveredByMonth {
            globalCovered = globalCovered.merging(covered)
        }
        return globalCovered.superset(of: authenticatedCoverage)
    }

    private static func corruptMonthReplayIsProvablyComplete(
        _ month: LibraryMonthKey,
        materialized: RepoMaterializer.MaterializeOutput
    ) -> Bool {
        let monthCovered = materialized.coveredByMonth[month, default: .empty]
        var globalCovered = CoveredRanges.empty
        for (_, covered) in materialized.coveredByMonth {
            globalCovered = globalCovered.merging(covered)
        }
        for (writerID, ranges) in monthCovered.rangesByWriter {
            guard let maxHigh = ranges.last?.high else { continue }
            let requiredPrefix = CoveredRanges(
                rangesByWriter: [writerID: [ClosedSeqRange(low: 1, high: maxHigh)]]
            )
            guard globalCovered.superset(of: requiredPrefix) else { return false }
        }
        return true
    }

    private static func computeDeletePrefix(
        acceptedPrefix: [String: UInt64],
        plannerPrefix: [String: UInt64]
    ) -> [String: UInt64] {
        let writers = Set(acceptedPrefix.keys).union(plannerPrefix.keys)
        var result: [String: UInt64] = [:]
        for writerID in writers {
            guard let accepted = acceptedPrefix[writerID],
                  let planner = plannerPrefix[writerID] else { continue }
            let prefix = min(accepted, planner)
            if prefix > 0 {
                result[writerID] = prefix
            }
        }
        return result
    }

    private func skippedResult(month: LibraryMonthKey) -> RepoMaintenanceMonthResult {
        RepoMaintenanceMonthResult(
            month: month,
            checkpoint: RepoCheckpointPhaseResult(
                outcome: .skippedEmptyFold,
                checkpoint: RepoCheckpointResult(
                    outcome: .skippedEmptyFold,
                    month: month,
                    snapshotName: nil,
                    lamport: nil,
                    covered: .empty,
                    beforeReport: nil,
                    afterReport: nil,
                    acceptedSnapshot: nil
                )
            ),
            commitCleanup: nil,
            snapshotGC: .skipped(.skippedMaintenanceFrozen)
        )
    }

    private func mapCheckpointToPhaseResult(_ checkpoint: RepoCheckpointResult) -> RepoCheckpointPhaseResult {
        let outcome: RepoCheckpointPhaseOutcome
        switch checkpoint.outcome {
        case .writtenAccepted:
            outcome = .checkpointWritten
        case .skippedEmptyFold:
            outcome = .skippedEmptyFold
        case .skippedBelowThreshold:
            outcome = .skippedBelowThreshold
        }
        return RepoCheckpointPhaseResult(outcome: outcome, checkpoint: checkpoint)
    }

    private func emptyCommitReport(month: LibraryMonthKey) -> RepoRetentionDeletePreflightReport {
        RepoRetentionDeletePreflightReport(
            month: month,
            repoID: services.repoID,
            mode: .dryRun,
            evaluatedAtMs: nowMs()
        )
    }

    private func emptySnapshotReport(month: LibraryMonthKey) -> RepoSnapshotDeletePreflightReport {
        RepoSnapshotDeletePreflightReport(
            month: month,
            repoID: services.repoID,
            evaluatedAtMs: nowMs()
        )
    }
}

private func containsCancellation(_ result: RepoRetentionCommitDeleteResult) -> Bool {
    switch result {
    case .preflightBlocked(_, _),
         .completed(_, _, _):
        return false
    case .stopped(_, let reason, _, let verification):
        return reason.containsCancellation || verification?.containsCancellation == true
    case .verificationFailed(_, let stopReason, _, let verification):
        return stopReason?.containsCancellation == true || verification.containsCancellation
    case .verificationInconclusive(_, let stopReason, _, let verification):
        return stopReason?.containsCancellation == true || verification.containsCancellation
    }
}

private extension RepoRetentionCommitDeleteStopReason {
    var containsCancellation: Bool {
        switch self {
        case .cancelled(_):
            return true
        case .deleteFailed(_, .cancelled):
            return true
        case .deleteFailed(_, _),
             .preDeleteRevalidationFailed(_, _):
            return false
        }
    }
}

private extension RepoRetentionPostDeleteVerificationResult {
    var containsCancellation: Bool {
        if case .inconclusive(reason: .cancelled) = self {
            return true
        }
        return false
    }
}

private func snapshotGCContainsCancellation(_ result: RepoSnapshotGCResult) -> Bool {
    switch result {
    case .preflightBlocked, .completed:
        return false
    case .stopped(_, let reason, _, let verification):
        switch reason {
        case .cancelled: return true
        case .deleteFailed(_, .cancelled): return true
        default: break
        }
        if let verification, case .inconclusive(reason: .cancelled) = verification { return true }
        return false
    case .verificationFailed(_, let stopReason, _, let verification):
        if let stopReason {
            switch stopReason {
            case .cancelled: return true
            case .deleteFailed(_, .cancelled): return true
            default: break
            }
        }
        if case .inconclusive(reason: .cancelled) = verification { return true }
        return false
    case .verificationInconclusive(_, let stopReason, _, let verification):
        if let stopReason {
            switch stopReason {
            case .cancelled: return true
            case .deleteFailed(_, .cancelled): return true
            default: break
            }
        }
        if case .inconclusive(reason: .cancelled) = verification { return true }
        return false
    }
}
