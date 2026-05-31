import Foundation
import os.log

private let compactionLog = Logger(subsystem: "com.zizicici.watermelon", category: "RepoCompactionService")

/// Independent compaction coordinator that owns checkpoint writing, commit GC, and
/// snapshot GC. Entry points are explicit maintenance paths (startup, run-end,
/// threshold), never the per-flush backup hot path.
///
/// Gating contract:
/// - Materializes target month; outcome must be clean before any destructive work.
/// - Commit count/bytes must meet threshold before checkpoint + GC proceeds.
/// - Commit GC uses accepted covered-bound + planner prefix (no barrier dependency).
/// - Snapshot GC uses covered-dominance candidate selection with keepN protection.
/// - Ambiguous / post-verify inconclusive outcomes skip and report.
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

    /// Compact a single month: materialize → clean check → threshold → checkpoint →
    /// commit GC → snapshot GC.
    func compactMonth(_ month: LibraryMonthKey) async throws -> RepoMaintenanceMonthResult {
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

        let report = try? await RepoCompactionPlanner(
            client: services.metadataClient,
            basePath: services.basePath,
            policy: services.compactionPolicy
        ).makeReport(expectedRepoID: services.repoID, preMaterialized: materialized)
        let monthReport = report?.months.first { $0.month == month }

        guard let monthReport, monthReport.checkpointRecommended else {
            return skippedResult(month: month)
        }

        // Phase A — checkpoint
        let checkpointResult: RepoCheckpointResult
        do {
            checkpointResult = try await RepoCheckpointService(
                client: services.metadataClient,
                basePath: services.basePath,
                repoID: services.repoID,
                writerID: services.writerID,
                runID: services.runID,
                clock: services.lamport,
                policy: services.compactionPolicy
            ).checkpointMonth(month, mode: .whenRecommended, respectTaskCancellation: true)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            compactionLog.error("compaction checkpoint failed for \(month.text, privacy: .public): \(String(describing: error), privacy: .public)")
            return skippedResult(month: month)
        }

        let checkpointPhaseResult = mapCheckpointToPhaseResult(checkpointResult)

        // Phase B — commit GC
        let commitCleanup: RepoRetentionCommitDeleteResult
        do {
            commitCleanup = try await runCommitGC(month: month, preMaterialized: materialized, monthReport: monthReport)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            compactionLog.error("compaction commit GC failed for \(month.text, privacy: .public): \(String(describing: error), privacy: .public)")
            commitCleanup = .preflightBlocked(blockers: [], report: emptyCommitReport(month: month))
        }

        // Phase C — snapshot GC, gated on commit GC outcome
        let snapshotGC: RepoMaintenanceSnapshotGCDisposition
        switch commitCleanup {
        case .preflightBlocked, .completed:
            if Task.isCancelled {
                snapshotGC = .skipped(.skippedCancellation)
            } else {
                do {
                    let gcResult = try await runSnapshotGC(month: month)
                    snapshotGC = .ran(gcResult)
                } catch is CancellationError {
                    snapshotGC = .skipped(.skippedCancellation)
                } catch {
                    compactionLog.error("compaction snapshot GC failed for \(month.text, privacy: .public): \(String(describing: error), privacy: .public)")
                    snapshotGC = .skipped(.skippedAfterCommitCleanupStopped)
                }
            }
        case .stopped:
            snapshotGC = .skipped(.skippedAfterCommitCleanupStopped)
        case .verificationFailed:
            snapshotGC = .skipped(.skippedAfterCommitCleanupVerificationFailed)
        case .verificationInconclusive:
            snapshotGC = .skipped(.skippedAfterCommitCleanupVerificationInconclusive)
        }

        return RepoMaintenanceMonthResult(
            month: month,
            checkpoint: checkpointPhaseResult,
            commitCleanup: commitCleanup,
            snapshotGC: snapshotGC
        )
    }

    /// Multi-month startup sweep. Scans for months meeting the compaction threshold
    /// and compacts each one sequentially.
    func compactStartupMonths() async throws -> RepoMaintenanceStartupResult {
        let months = try await candidateStartupMonths()
        var results: [LibraryMonthKey: RepoMaintenanceMonthResult] = [:]
        for month in months {
            try Task.checkCancellation()
            do {
                results[month] = try await compactMonth(month)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                compactionLog.error("compaction startup month \(month.text, privacy: .public) failed: \(String(describing: error), privacy: .public)")
            }
        }
        return RepoMaintenanceStartupResult(monthResults: results)
    }

    // MARK: - Phase B — Commit GC

    private func runCommitGC(
        month: LibraryMonthKey,
        preMaterialized: RepoMaterializer.MaterializeOutput,
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

        guard let accepted = materialized.acceptedSnapshotBaselinesByMonth[month] else {
            return .preflightBlocked(blockers: [.noAcceptedSnapshot(month: month)], report: emptyCommitReport(month: month))
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
            retainedBarrierUnionCovered: .empty,
            postDeleteEquivalenceContract: RepoRetentionPostDeleteEquivalenceContract(
                mode: .retentionSuperset,
                acceptedSnapshotFilename: accepted.filename,
                acceptedSnapshotSHA256Hex: acceptedFile.sha256Hex.lowercased(),
                acceptedSnapshotCovered: accepted.covered,
                retainedBarrierUnionCovered: .empty,
                requiredObservedSeqByWriter: materialized.observedSeqByWriter,
                expectedDeletePrefixByWriter: deletePrefix,
                retainedBarrierCheckpointSHA256ByFilename: [:],
                preDeleteCovered: covered,
                preDeleteState: materialized.state
            )
        )

        let plan = RepoRetentionDeletePreflightPlan(
            month: month,
            repoID: services.repoID,
            acceptedSnapshot: accepted,
            barrierSet: RetentionBarrierSet(unsuperseded: [], unionCovered: .empty),
            composedLivenessGate: RetentionLivenessGate(
                requiredCompleteView: false,
                requiredNoActiveNonSelfWriters: false,
                legacyClientGraceMs: 0
            ),
            livenessDecision: RepoRetentionDeletePreflightService.LivenessDecision(
                blockers: [], evaluatedAtMs: nowMs()
            ),
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

        if RetentionMaintenanceOrchestrator.containsCancellation(result) {
            throw CancellationError()
        }
        return result
    }

    // MARK: - Phase C — Snapshot GC

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

        guard let accepted = materialized.acceptedSnapshotBaselinesByMonth[month] else {
            return .preflightBlocked(
                blockers: [.noAcceptedPerMonthSnapshot(month: month)],
                report: emptySnapshotReport(month: month)
            )
        }

        let barrierReferenced = await loadBarrierReferencedFilenames(month: month)

        let scan = try await SnapshotDeleteCandidateScanner(
            client: services.metadataClient,
            basePath: services.basePath,
            policy: services.compactionPolicy
        ).scan(
            month: month,
            expectedRepoID: services.repoID,
            acceptedBaseline: accepted,
            barrierReferencedFilenames: barrierReferenced
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

        let protection = RepoSnapshotProtectionSet.compute(.init(
            acceptedBaselineFilename: accepted.filename,
            acceptedBaselineCovered: accepted.covered,
            barrierReferencedFilenames: barrierReferenced,
            parseableSnapshotsForMonth: scan.parseableSnapshots.map {
                .init(filename: $0.filename, lamport: $0.lamport, writerID: $0.writerID, covered: $0.covered)
            },
            snapshotKeepCount: services.compactionPolicy.snapshotFallbackKeepCount
        ))

        let deleteCandidates = scan.candidates.filter { !protection.protectedFilenames.contains($0.filename) }
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
            if barrierReferenced.contains(filename) { continue }
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
            retainedBarrierUnionCovered: .empty,
            retainedManifestCheckpointSHA256ByFilename: [:],
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
            barrierSet: RetentionBarrierSet(unsuperseded: [], unionCovered: .empty),
            composedLivenessGate: RetentionLivenessGate(
                requiredCompleteView: false,
                requiredNoActiveNonSelfWriters: false,
                legacyClientGraceMs: 0
            ),
            livenessDecision: RepoSnapshotDeletePreflightService.LivenessDecision(
                blockers: [], evaluatedAtMs: nowMs()
            ),
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

    // MARK: - Helpers

    private func candidateStartupMonths() async throws -> [LibraryMonthKey] {
        let report = try await RepoCompactionPlanner(
            client: services.metadataClient,
            basePath: services.basePath,
            policy: services.compactionPolicy
        ).makeReport(expectedRepoID: services.repoID)
        return report.months.filter(\.checkpointRecommended).map(\.month).sorted()
    }

    private func loadBarrierReferencedFilenames(month: LibraryMonthKey) async -> Set<String> {
        do {
            let load = try await RetentionManifestRemoteStore(
                client: services.metadataClient,
                basePath: services.basePath
            ).loadBarrierSet(expectedRepoID: services.repoID, month: month)
            return Set(load.barrierSet.unsuperseded.map(\.checkpointSnapshotName))
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { return [] }
            return []
        }
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
                ),
                barrier: nil
            ),
            commitCleanup: nil,
            snapshotGC: .skipped(.skippedMaintenanceFrozen)
        )
    }

    private func mapCheckpointToPhaseResult(_ checkpoint: RepoCheckpointResult) -> RepoCheckpointPhaseResult {
        let outcome: RepoCheckpointPhaseOutcome
        switch checkpoint.outcome {
        case .writtenAccepted:
            outcome = .checkpointWrittenBarrierPublished
        case .skippedEmptyFold:
            outcome = .skippedEmptyFold
        case .skippedBelowThreshold:
            outcome = .skippedBelowThreshold
        }
        return RepoCheckpointPhaseResult(outcome: outcome, checkpoint: checkpoint, barrier: nil)
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
