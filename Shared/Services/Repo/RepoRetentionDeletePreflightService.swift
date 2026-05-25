import Foundation

enum RepoRetentionDeletePreflightMode: Equatable, Sendable {
    case dryRun
}

enum RepoRetentionCandidateHeaderMismatchReason: Equatable, Sendable {
    case repoID(expected: String, actual: String)
    case month(expected: LibraryMonthKey, actual: LibraryMonthKey?)
    case writerID(expected: String, actual: String)
    case seq(expected: UInt64, actual: UInt64)
}

enum RepoRetentionBarrierCheckpointMismatchReason: Equatable, Sendable {
    case sha256(expected: String, actual: String)
    case repoID(expected: String, actual: String)
    case month(expected: LibraryMonthKey, actual: LibraryMonthKey?)
    case writerID(expected: String, actual: String)
    case coveredRanges
}

enum RepoRetentionDeletePreflightError: Error, Equatable, Sendable {
    case livenessSnapshotUnavailable
}

enum RepoRetentionDeletePreflightBlocker: Equatable, Sendable {
    case missingVersion
    case unreadableVersion
    case unsupportedVersion(formatVersion: Int)
    case repoIdentityMismatch(expected: String, observed: String)
    case migrationInProgress
    case migrationCheckFailed
    case invalidBarrierSet([InvalidRetentionManifestEntry])
    case barrierSetReadFailed
    case emptyBarrierSet
    case barrierCheckpointReadFailed(filename: String)
    case barrierCheckpointMismatch(filename: String, reason: RepoRetentionBarrierCheckpointMismatchReason)
    case retentionLivenessBlocked([RetentionDeletionSafetyBlocker])
    case barrierTooYoung(filename: String, createdAtMs: Int64)
    case barrierCreatedInFuture(filename: String, createdAtMs: Int64)
    case materializerReadRace
    case materializerReadFailed
    case noAcceptedSnapshot(month: LibraryMonthKey)
    case acceptedSnapshotMissingBarrierCoverage
    case barrierObservedSeqRegression(writerID: String, expectedAtLeast: UInt64, observed: UInt64)
    case noDeleteCandidates
    case candidateListFailed
    case candidateReadFailed(filename: String)
    case candidateHeaderMismatch(filename: String, reason: RepoRetentionCandidateHeaderMismatchReason)
    case candidateCorruptOrUntrusted(filename: String)
    case plannerReadFailed
    case plannerCrossCheckFailed(plannedCount: Int, plannerCount: Int, plannedBytes: Int64, plannerBytes: Int64)
}

enum RepoRetentionPreflightVersionStatus: Equatable, Sendable {
    case compatible(formatVersion: Int)
    case missing
    case unreadable
    case unsupported(formatVersion: Int)
}

struct RepoRetentionProtectedSummary: Equatable, Sendable {
    var targetMonthUnparseableFilenameCount: Int = 0
    var crossMonthCommitFileCount: Int = 0
    var outOfPrefixCommitFileCount: Int = 0
    var ignoredNonCommitEntryCount: Int = 0
    var headerMismatchCandidateCount: Int = 0
    var corruptOrUntrustedCandidateCount: Int = 0
    var readFailedCandidateCount: Int = 0
    var protectedBytes: Int64 = 0
}

struct RepoRetentionDeleteCandidate: Equatable, Sendable {
    let filename: String
    let path: String
    let month: LibraryMonthKey
    let writerID: String
    let seq: UInt64
    let size: Int64
    let sha256Hex: String
    let rowCount: Int
}

struct RepoRetentionDeleteCandidateScanResult: Equatable, Sendable {
    let candidates: [RepoRetentionDeleteCandidate]
    let protectedSummary: RepoRetentionProtectedSummary
    let blockers: [RepoRetentionDeletePreflightBlocker]
    let readConcurrencyLimit: Int
}

enum RepoRetentionPostDeleteEquivalenceMode: Equatable, Sendable {
    case retentionSuperset
}

struct RepoRetentionPostDeleteEquivalenceContract: Equatable, Sendable {
    let mode: RepoRetentionPostDeleteEquivalenceMode
    let acceptedSnapshotFilename: String
    let acceptedSnapshotCovered: CoveredRanges
    let retainedBarrierUnionCovered: CoveredRanges
    let requiredObservedSeqByWriter: [String: UInt64]
    let expectedDeletePrefixByWriter: [String: UInt64]
    let preDeleteCovered: CoveredRanges
    let preDeleteState: RepoSnapshotState
}

struct RepoRetentionPreDeleteEvidence: Equatable, Sendable {
    let materializedState: RepoSnapshotState
    let materializedCovered: CoveredRanges
    let observedSeqByWriter: [String: UInt64]
    let acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo
    let retainedBarrierUnionCovered: CoveredRanges
    let postDeleteEquivalenceContract: RepoRetentionPostDeleteEquivalenceContract
}

struct RepoRetentionDeletePreflightPlan: Equatable, Sendable {
    let month: LibraryMonthKey
    let repoID: String
    let acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo
    let barrierSet: RetentionBarrierSet
    let composedLivenessGate: RetentionLivenessGate
    let livenessDecision: RetentionDeletionSafetyDecision
    let deletePrefixByWriter: [String: UInt64]
    let commitFiles: [RepoRetentionDeleteCandidate]
    let protectedSummary: RepoRetentionProtectedSummary
    let preDeleteEvidence: RepoRetentionPreDeleteEvidence
}

struct RepoRetentionDeletePreflightReport: Equatable, Sendable {
    let month: LibraryMonthKey
    let repoID: String
    let mode: RepoRetentionDeletePreflightMode
    let evaluatedAtMs: Int64
    var versionStatus: RepoRetentionPreflightVersionStatus?
    var remoteRepoID: String?
    var migrationMarkerPresent: Bool?
    var barrierLoad: RetentionManifestBarrierLoadResult?
    var composedLivenessGate: RetentionLivenessGate?
    var livenessDecision: RetentionDeletionSafetyDecision?
    var acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo?
    var materializedCovered: CoveredRanges?
    var observedSeqByWriter: [String: UInt64] = [:]
    var deletePrefixByWriter: [String: UInt64] = [:]
    var candidateScan: RepoRetentionDeleteCandidateScanResult?
    var compactionMonthReport: RepoCompactionMonthReport?
}

enum RepoRetentionDeletePreflightResult: Equatable, Sendable {
    case blocked(blockers: [RepoRetentionDeletePreflightBlocker], report: RepoRetentionDeletePreflightReport)
    case planned(plan: RepoRetentionDeletePreflightPlan, report: RepoRetentionDeletePreflightReport)
}

struct RepoRetentionDeletePreflightService: Sendable {
    typealias PeerStatusProvider = @Sendable () async throws -> RetentionPeerStatusView

    let client: any RemoteStorageClientProtocol
    let basePath: String
    let policy: RepoCompactionPolicy
    let isLocalVolume: Bool
    let barrierClockSkewToleranceMs: Int64
    private let peerStatusProvider: PeerStatusProvider

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        policy: RepoCompactionPolicy = .default,
        isLocalVolume: Bool,
        barrierClockSkewToleranceMs: Int64 = 5 * 60 * 1000,
        peerStatusProvider: @escaping PeerStatusProvider = {
            throw RepoRetentionDeletePreflightError.livenessSnapshotUnavailable
        }
    ) {
        self.client = wrapIfSerial(client)
        self.basePath = basePath
        self.policy = policy
        self.isLocalVolume = isLocalVolume
        self.barrierClockSkewToleranceMs = barrierClockSkewToleranceMs
        self.peerStatusProvider = peerStatusProvider
    }

    func makePlan(
        month: LibraryMonthKey,
        expectedRepoID: String,
        mode: RepoRetentionDeletePreflightMode,
        nowMs: Int64
    ) async throws -> RepoRetentionDeletePreflightResult {
        let repoID = RepoCanonicalIdentity.normalizeLossy(expectedRepoID)
        var report = RepoRetentionDeletePreflightReport(
            month: month,
            repoID: repoID,
            mode: mode,
            evaluatedAtMs: nowMs
        )
        var blockers: [RepoRetentionDeletePreflightBlocker] = []

        blockers.append(contentsOf: try await checkVersion(report: &report))
        blockers.append(contentsOf: try await checkMigrationMarkers(report: &report))
        if !blockers.isEmpty {
            return .blocked(blockers: blockers, report: report)
        }

        let barrierLoad: RetentionManifestBarrierLoadResult
        do {
            barrierLoad = try await RetentionManifestRemoteStore(client: client, basePath: basePath)
                .loadBarrierSet(expectedRepoID: repoID, month: month)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            report.barrierLoad = nil
            return .blocked(blockers: [.barrierSetReadFailed], report: report)
        }
        report.barrierLoad = barrierLoad
        if !barrierLoad.isComplete {
            blockers.append(.invalidBarrierSet(barrierLoad.invalid))
        }
        let allBarrierSet = barrierLoad.barrierSet
        if allBarrierSet.unsuperseded.isEmpty {
            blockers.append(.emptyBarrierSet)
        }
        blockers.append(contentsOf: barrierFutureBlockers(barrierSet: allBarrierSet, nowMs: nowMs))
        let barrierSet = deletionEligibleBarrierSet(validManifests: barrierLoad.valid, nowMs: nowMs)
        if barrierSet.unsuperseded.isEmpty {
            blockers.append(contentsOf: barrierTooYoungBlockers(barrierSet: allBarrierSet, nowMs: nowMs))
        }
        if !blockers.isEmpty {
            return .blocked(blockers: blockers, report: report)
        }

        let composedLivenessGate = barrierSet.composedLivenessGate
        report.composedLivenessGate = composedLivenessGate
        blockers.append(contentsOf: try await barrierCheckpointEvidenceBlockers(barrierSet: barrierSet))
        blockers.append(contentsOf: try await livenessBlockers(
            composedGate: composedLivenessGate,
            nowMs: nowMs,
            report: &report
        ))
        if !blockers.isEmpty {
            return .blocked(blockers: blockers, report: report)
        }

        // Authoritative identity check against repo.json, not the materializer echo.
        do {
            switch try await RepoCanonicalIdentityReader(client: client, basePath: basePath).loadCanonical() {
            case .absent:
                return .blocked(blockers: [.repoIdentityMismatch(expected: repoID, observed: "(absent)")], report: report)
            case .found(let remoteID):
                let canonical = RepoCanonicalIdentity.normalizeLossy(remoteID)
                report.remoteRepoID = canonical
                guard canonical == repoID else {
                    return .blocked(blockers: [.repoIdentityMismatch(expected: repoID, observed: canonical)], report: report)
                }
            }
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            return .blocked(blockers: [.materializerReadFailed], report: report)
        }

        let materialized: RepoMaterializer.MaterializeOutput
        do {
            materialized = try await RepoMaterializer(client: client, basePath: basePath)
                .materializeMonth(month, expectedRepoID: repoID)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            if error is RepoMaterializer.MetadataReadRaceError {
                return .blocked(blockers: [.materializerReadRace], report: report)
            }
            return .blocked(blockers: [.materializerReadFailed], report: report)
        }

        guard let acceptedSnapshot = materialized.acceptedSnapshotBaselinesByMonth[month] else {
            return .blocked(blockers: [.noAcceptedSnapshot(month: month)], report: report)
        }
        let materializedCovered = materialized.coveredByMonth[month, default: .empty]
        report.acceptedSnapshot = acceptedSnapshot
        report.materializedCovered = materializedCovered
        report.observedSeqByWriter = materialized.observedSeqByWriter
        guard acceptedSnapshot.covered.superset(of: barrierSet.unionCovered) else {
            return .blocked(blockers: [.acceptedSnapshotMissingBarrierCoverage], report: report)
        }
        let barrierObservedSeq = barrierObservedSeqHighByWriter(barrierSet: barrierSet)
        let observedRegressionBlockers = observedSeqRegressionBlockers(
            required: barrierObservedSeq,
            observed: materialized.observedSeqByWriter
        )
        if !observedRegressionBlockers.isEmpty {
            return .blocked(blockers: observedRegressionBlockers, report: report)
        }
        let requiredObservedSeqByWriter = mergedObservedSeqRequirements(
            materialized: materialized.observedSeqByWriter,
            barrierObserved: barrierObservedSeq
        )

        let compactionReport: RepoCompactionReport
        do {
            compactionReport = try await RepoCompactionPlanner(client: client, basePath: basePath, policy: policy)
                .makeReport(expectedRepoID: repoID, preMaterialized: materialized)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            return .blocked(blockers: [.plannerReadFailed], report: report)
        }
        let monthReport = compactionReport.months.first { $0.month == month }
        report.compactionMonthReport = monthReport

        let deletePrefix = deletePrefixByWriter(
            acceptedSnapshot: acceptedSnapshot,
            barrierSet: barrierSet,
            plannerPrefix: monthReport?.deletePrefixByWriter ?? [:]
        )
        report.deletePrefixByWriter = deletePrefix
        if deletePrefix.isEmpty {
            return .blocked(blockers: [.noDeleteCandidates], report: report)
        }

        let candidateScan = try await RepoRetentionDeleteCandidateScanner(
            client: client,
            basePath: basePath
        ).scan(month: month, expectedRepoID: repoID, deletePrefixByWriter: deletePrefix)
        report.candidateScan = candidateScan
        if !candidateScan.blockers.isEmpty {
            return .blocked(blockers: candidateScan.blockers, report: report)
        }
        if let monthReport {
            let plannedBytes = candidateScan.candidates.reduce(Int64(0)) { $0 + $1.size }
            if candidateScan.candidates.count > monthReport.checkpointCoveredPrefixCandidateCount ||
                plannedBytes > monthReport.checkpointCoveredPrefixCandidateBytes {
                return .blocked(blockers: [
                    .plannerCrossCheckFailed(
                        plannedCount: candidateScan.candidates.count,
                        plannerCount: monthReport.checkpointCoveredPrefixCandidateCount,
                        plannedBytes: plannedBytes,
                        plannerBytes: monthReport.checkpointCoveredPrefixCandidateBytes
                    )
                ], report: report)
            }
        }
        guard !candidateScan.candidates.isEmpty else {
            return .blocked(blockers: [.noDeleteCandidates], report: report)
        }

        let evidence = RepoRetentionPreDeleteEvidence(
            materializedState: materialized.state,
            materializedCovered: materializedCovered,
            observedSeqByWriter: materialized.observedSeqByWriter,
            acceptedSnapshot: acceptedSnapshot,
            retainedBarrierUnionCovered: barrierSet.unionCovered,
            postDeleteEquivalenceContract: RepoRetentionPostDeleteEquivalenceContract(
                mode: .retentionSuperset,
                acceptedSnapshotFilename: acceptedSnapshot.filename,
                acceptedSnapshotCovered: acceptedSnapshot.covered,
                retainedBarrierUnionCovered: barrierSet.unionCovered,
                requiredObservedSeqByWriter: requiredObservedSeqByWriter,
                expectedDeletePrefixByWriter: deletePrefix,
                preDeleteCovered: materializedCovered,
                preDeleteState: materialized.state
            )
        )
        let plan = RepoRetentionDeletePreflightPlan(
            month: month,
            repoID: repoID,
            acceptedSnapshot: acceptedSnapshot,
            barrierSet: barrierSet,
            composedLivenessGate: composedLivenessGate,
            livenessDecision: report.livenessDecision ?? RetentionDeletionSafetyDecision(blockers: [], evaluatedAtMs: nowMs),
            deletePrefixByWriter: deletePrefix,
            commitFiles: candidateScan.candidates,
            protectedSummary: candidateScan.protectedSummary,
            preDeleteEvidence: evidence
        )
        return .planned(plan: plan, report: report)
    }

    private func deletionEligibleBarrierSet(
        validManifests: [RetentionManifest],
        nowMs: Int64
    ) -> RetentionBarrierSet {
        let minAgeMs = Int64(policy.retentionStalenessThresholdSeconds) * 1000
        return RetentionBarrierSet.unsuperseded(manifests: validManifests.filter { manifest in
            nowMs - manifest.createdAtMs >= minAgeMs
        })
    }

    private func barrierCheckpointEvidenceBlockers(
        barrierSet: RetentionBarrierSet
    ) async throws -> [RepoRetentionDeletePreflightBlocker] {
        let reader = SnapshotReader(client: client, basePath: basePath)
        var blockers: [RepoRetentionDeletePreflightBlocker] = []
        for manifest in barrierSet.unsuperseded.sorted(by: { lhs, rhs in
            RetentionManifestStore.filename(for: lhs.ref) < RetentionManifestStore.filename(for: rhs.ref)
        }) {
            let filename = manifest.checkpointSnapshotName
            let snapshot: SnapshotFile
            do {
                snapshot = try await reader.read(filename: filename)
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                blockers.append(.barrierCheckpointReadFailed(filename: filename))
                continue
            }
            if let mismatch = barrierCheckpointMismatch(manifest: manifest, snapshot: snapshot) {
                blockers.append(.barrierCheckpointMismatch(filename: filename, reason: mismatch))
            }
        }
        return blockers
    }

    private func barrierCheckpointMismatch(
        manifest: RetentionManifest,
        snapshot: SnapshotFile
    ) -> RepoRetentionBarrierCheckpointMismatchReason? {
        if snapshot.sha256Hex.lowercased() != manifest.checkpointSHA256Hex {
            return .sha256(expected: manifest.checkpointSHA256Hex, actual: snapshot.sha256Hex.lowercased())
        }
        if RepoCanonicalIdentity.normalizeLossy(snapshot.header.repoID) != manifest.repoID {
            return .repoID(expected: manifest.repoID, actual: RepoCanonicalIdentity.normalizeLossy(snapshot.header.repoID))
        }
        let snapshotMonth = CommitHeader.parseMonthScope(snapshot.header.scope)
        if snapshotMonth != manifest.month {
            return .month(expected: manifest.month, actual: snapshotMonth)
        }
        if snapshot.header.writerID != manifest.createdByWriterID {
            return .writerID(expected: manifest.createdByWriterID, actual: snapshot.header.writerID)
        }
        if snapshot.header.covered != manifest.coveredRanges {
            return .coveredRanges
        }
        return nil
    }

    private func checkVersion(report: inout RepoRetentionDeletePreflightReport) async throws -> [RepoRetentionDeletePreflightBlocker] {
        do {
            switch try await VersionManifestStore(client: client, basePath: basePath).load() {
            case .absent:
                report.versionStatus = .missing
                return [.missingVersion]
            case .found(let manifest):
                do {
                    try VersionManifestStore.classify(
                        remoteFormat: manifest.formatVersion,
                        minAppVersion: manifest.minAppVersion
                    )
                    report.versionStatus = .compatible(formatVersion: manifest.formatVersion)
                    return []
                } catch RepoBootstrap.VersionConflict.higherFormatVersion(let remote, _, _) {
                    report.versionStatus = .unsupported(formatVersion: remote)
                    return [.unsupportedVersion(formatVersion: remote)]
                } catch RepoBootstrap.VersionConflict.mismatchedFormatVersion(let remote, _, _) {
                    report.versionStatus = .unsupported(formatVersion: remote)
                    return [.unsupportedVersion(formatVersion: remote)]
                } catch {
                    report.versionStatus = .unreadable
                    return [.unreadableVersion]
                }
            }
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            report.versionStatus = .unreadable
            return [.unreadableVersion]
        }
    }

    private func checkMigrationMarkers(report: inout RepoRetentionDeletePreflightReport) async throws -> [RepoRetentionDeletePreflightBlocker] {
        do {
            let exists = try await MigrationMarkerStore(client: client, basePath: basePath).existsAny()
            report.migrationMarkerPresent = exists
            return exists ? [.migrationInProgress] : []
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            report.migrationMarkerPresent = nil
            return [.migrationCheckFailed]
        }
    }

    private func barrierFutureBlockers(
        barrierSet: RetentionBarrierSet,
        nowMs: Int64
    ) -> [RepoRetentionDeletePreflightBlocker] {
        var blockers: [RepoRetentionDeletePreflightBlocker] = []
        for manifest in barrierSet.unsuperseded {
            let filename = RetentionManifestStore.filename(for: manifest.ref)
            if manifest.createdAtMs > nowMs + barrierClockSkewToleranceMs {
                blockers.append(.barrierCreatedInFuture(filename: filename, createdAtMs: manifest.createdAtMs))
            }
        }
        return blockers
    }

    private func barrierTooYoungBlockers(
        barrierSet: RetentionBarrierSet,
        nowMs: Int64
    ) -> [RepoRetentionDeletePreflightBlocker] {
        let minAgeMs = Int64(policy.retentionStalenessThresholdSeconds) * 1000
        var blockers: [RepoRetentionDeletePreflightBlocker] = []
        for manifest in barrierSet.unsuperseded {
            let filename = RetentionManifestStore.filename(for: manifest.ref)
            if manifest.createdAtMs <= nowMs + barrierClockSkewToleranceMs,
               nowMs - manifest.createdAtMs < minAgeMs {
                blockers.append(.barrierTooYoung(filename: filename, createdAtMs: manifest.createdAtMs))
            }
        }
        return blockers
    }

    private func livenessBlockers(
        composedGate: RetentionLivenessGate,
        nowMs: Int64,
        report: inout RepoRetentionDeletePreflightReport
    ) async throws -> [RepoRetentionDeletePreflightBlocker] {
        do {
            let view = isLocalVolume ? RetentionPeerStatusView.empty : try await peerStatusProvider()
            let decision = RetentionDeletionSafetyGate.evaluate(
                peerStatusView: view,
                policy: policy,
                manifestGate: composedGate,
                nowMs: nowMs,
                isLocalVolume: isLocalVolume
            )
            report.livenessDecision = decision
            return decision.allowed ? [] : [.retentionLivenessBlocked(decision.blockers)]
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw RepoRetentionDeletePreflightError.livenessSnapshotUnavailable
        }
    }

    private func barrierObservedSeqHighByWriter(barrierSet: RetentionBarrierSet) -> [String: UInt64] {
        var result: [String: UInt64] = [:]
        for manifest in barrierSet.unsuperseded {
            for (writerID, seq) in manifest.observedSeqHighByWriter {
                result[writerID] = max(result[writerID] ?? 0, seq)
            }
        }
        return result
    }

    private func observedSeqRegressionBlockers(
        required: [String: UInt64],
        observed: [String: UInt64]
    ) -> [RepoRetentionDeletePreflightBlocker] {
        required.keys.sorted().compactMap { writerID in
            let expected = required[writerID] ?? 0
            let actual = observed[writerID] ?? 0
            guard actual < expected else { return nil }
            return .barrierObservedSeqRegression(
                writerID: writerID,
                expectedAtLeast: expected,
                observed: actual
            )
        }
    }

    private func mergedObservedSeqRequirements(
        materialized: [String: UInt64],
        barrierObserved: [String: UInt64]
    ) -> [String: UInt64] {
        var result = materialized
        for (writerID, seq) in barrierObserved {
            result[writerID] = max(result[writerID] ?? 0, seq)
        }
        return result
    }

    private func deletePrefixByWriter(
        acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo,
        barrierSet: RetentionBarrierSet,
        plannerPrefix: [String: UInt64]
    ) -> [String: UInt64] {
        let acceptedPrefix = policy.conservativeDeletePrefixByWriter(covered: acceptedSnapshot.covered)
        let barrierPrefix = barrierSet.authorizedDeletePrefixByWriter(policy: policy)
        let writers = Set(acceptedPrefix.keys).union(barrierPrefix.keys).union(plannerPrefix.keys)
        var result: [String: UInt64] = [:]
        for writerID in writers {
            guard let accepted = acceptedPrefix[writerID],
                  let barrier = barrierPrefix[writerID],
                  let planner = plannerPrefix[writerID] else { continue }
            let prefix = min(accepted, barrier, planner)
            if prefix > 0 {
                result[writerID] = prefix
            }
        }
        return result
    }
}

private struct RepoRetentionDeleteCandidateScanner: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String

    func scan(
        month: LibraryMonthKey,
        expectedRepoID: String,
        deletePrefixByWriter: [String: UInt64]
    ) async throws -> RepoRetentionDeleteCandidateScanResult {
        let dir = RepoLayout.commitsDirectoryPath(base: basePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: dir)
        } catch {
            if isStorageNotFoundError(error) {
                return RepoRetentionDeleteCandidateScanResult(
                    candidates: [],
                    protectedSummary: RepoRetentionProtectedSummary(),
                    blockers: [],
                    readConcurrencyLimit: 1
                )
            }
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            return RepoRetentionDeleteCandidateScanResult(
                candidates: [],
                protectedSummary: RepoRetentionProtectedSummary(),
                blockers: [.candidateListFailed],
                readConcurrencyLimit: 1
            )
        }

        let reader = CommitLogReader(client: client, basePath: basePath)
        var candidates: [RepoRetentionDeleteCandidate] = []
        var protectedSummary = RepoRetentionProtectedSummary()
        var blockers: [RepoRetentionDeletePreflightBlocker] = []

        for entry in entries.sorted(by: { $0.name < $1.name }) {
            guard !entry.isDirectory, entry.name.hasSuffix(".jsonl") else {
                protectedSummary.ignoredNonCommitEntryCount += 1
                continue
            }
            guard let parsed = RepoLayout.parseCommitFilename(entry.name) else {
                if monthPrefix(from: entry.name) == month {
                    protectedSummary.targetMonthUnparseableFilenameCount += 1
                    protectedSummary.protectedBytes += entry.size
                } else {
                    protectedSummary.ignoredNonCommitEntryCount += 1
                }
                continue
            }
            guard parsed.month == month else {
                protectedSummary.crossMonthCommitFileCount += 1
                protectedSummary.protectedBytes += entry.size
                continue
            }
            guard parsed.seq > 0 else {
                protectedSummary.outOfPrefixCommitFileCount += 1
                protectedSummary.protectedBytes += entry.size
                continue
            }
            guard entry.name == RepoLayout.commitFileName(
                month: parsed.month,
                writerID: parsed.writerID,
                seq: parsed.seq
            ) else {
                protectedSummary.targetMonthUnparseableFilenameCount += 1
                protectedSummary.protectedBytes += entry.size
                continue
            }
            guard let prefix = deletePrefixByWriter[parsed.writerID], parsed.seq <= prefix else {
                protectedSummary.outOfPrefixCommitFileCount += 1
                protectedSummary.protectedBytes += entry.size
                continue
            }
            let listedPath = RemotePathBuilder.absolutePath(basePath: dir, remoteRelativePath: entry.name)

            let commit: CommitFile
            do {
                commit = try await reader.read(remotePath: listedPath)
            } catch let error as RepoJSONLReadError {
                switch error {
                case .notFound:
                    protectedSummary.readFailedCandidateCount += 1
                    protectedSummary.protectedBytes += entry.size
                    blockers.append(.candidateReadFailed(filename: entry.name))
                case .missingHeader, .missingEnd, .integrityMismatch(_), .decodeFailure(_):
                    protectedSummary.corruptOrUntrustedCandidateCount += 1
                    protectedSummary.protectedBytes += entry.size
                    blockers.append(.candidateCorruptOrUntrusted(filename: entry.name))
                }
                continue
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                protectedSummary.readFailedCandidateCount += 1
                protectedSummary.protectedBytes += entry.size
                blockers.append(.candidateReadFailed(filename: entry.name))
                continue
            }

            if let mismatch = headerMismatch(
                parsed: parsed,
                header: commit.header,
                expectedRepoID: expectedRepoID
            ) {
                protectedSummary.headerMismatchCandidateCount += 1
                protectedSummary.protectedBytes += entry.size
                blockers.append(.candidateHeaderMismatch(filename: entry.name, reason: mismatch))
                continue
            }

            candidates.append(RepoRetentionDeleteCandidate(
                filename: entry.name,
                path: listedPath,
                month: parsed.month,
                writerID: parsed.writerID,
                seq: parsed.seq,
                size: entry.size,
                sha256Hex: commit.sha256Hex.lowercased(),
                rowCount: commit.rowCount
            ))
        }

        candidates.sort {
            if $0.writerID != $1.writerID { return $0.writerID < $1.writerID }
            return $0.seq < $1.seq
        }
        return RepoRetentionDeleteCandidateScanResult(
            candidates: candidates,
            protectedSummary: protectedSummary,
            blockers: blockers,
            readConcurrencyLimit: 1
        )
    }

    private func headerMismatch(
        parsed: RepoLayout.ParsedCommitFilename,
        header: CommitHeader,
        expectedRepoID: String
    ) -> RepoRetentionCandidateHeaderMismatchReason? {
        if RepoCanonicalIdentity.normalizeLossy(header.repoID) != expectedRepoID {
            return .repoID(expected: expectedRepoID, actual: RepoCanonicalIdentity.normalizeLossy(header.repoID))
        }
        if header.writerID != parsed.writerID {
            return .writerID(expected: parsed.writerID, actual: header.writerID)
        }
        if header.seq != parsed.seq {
            return .seq(expected: parsed.seq, actual: header.seq)
        }
        let scopeMonth = CommitHeader.parseMonthScope(header.scope)
        if scopeMonth != parsed.month {
            return .month(expected: parsed.month, actual: scopeMonth)
        }
        return nil
    }
}

private func monthPrefix(from filename: String) -> LibraryMonthKey? {
    guard filename.count >= 7 else { return nil }
    let prefix = String(filename.prefix(7))
    let parts = prefix.split(separator: "-")
    guard parts.count == 2,
          let year = Int(parts[0]),
          let month = Int(parts[1]),
          (1...12).contains(month) else {
        return nil
    }
    return LibraryMonthKey(year: year, month: month)
}
