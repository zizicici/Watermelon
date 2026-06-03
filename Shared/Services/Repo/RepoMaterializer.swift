import Foundation
import os.log

private let materializerLog = Logger(subsystem: "com.zizicici.watermelon", category: "RepoMaterializer")

actor RepoMaterializer {
    private let client: any RemoteStorageClientProtocol
    private let snapshotReader: SnapshotReader
    private let commitReader: CommitLogReader

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        // Internal TaskGroups fan out N concurrent read ops; `.serialOnly` backends need serialization.
        let effective = wrapIfSerial(client)
        self.client = effective
        self.snapshotReader = SnapshotReader(client: effective, basePath: basePath)
        self.commitReader = CommitLogReader(client: effective, basePath: basePath)
    }

    struct AcceptedSnapshotBaselineInfo: Sendable, Equatable {
        let filename: String
        let month: LibraryMonthKey
        let lamport: UInt64
        let writerID: String
        let runIDPrefix: String
        let covered: CoveredRanges
    }

    enum MonthOutcome: Sendable, Equatable {
        /// Covered-max baseline accepted and commit replay applied.
        case clean
        /// No trusted baseline (state rebuilt from commit replay), an uncovered rejected commit, an
        /// accepted commit whose addAsset resource lay outside the month, or a surviving baseline demoted
        /// because a corrupt sibling of unknown coverage may have covered more.
        case corrupt
        /// Trusted candidates exist but no single covered superset; read path uses best-effort
        /// state, write/maintenance consumers must skip.
        case ambiguous
    }

    struct MaterializeOutput: Sendable {
        let state: RepoSnapshotState
        let observedSeqByWriter: [String: UInt64]
        /// Final fold coverage after accepted snapshot baseline plus replayed commits.
        let coveredByMonth: [LibraryMonthKey: CoveredRanges]
        let acceptedSnapshotBaselinesByMonth: [LibraryMonthKey: AcceptedSnapshotBaselineInfo]
        /// Per-month materialization outcome: clean = covered-max baseline, corrupt = all snapshots
        /// bad (commit replay rebuild), an uncovered rejected commit, an out-of-month accepted commit op,
        /// or a baseline demoted below a corrupt sibling of unknown coverage (same-writer at/above its
        /// lamport, or any cross-writer), ambiguous = incomparable trusted coverage.
        let outcomeByMonth: [LibraryMonthKey: MonthOutcome]
        /// The subset of `.corrupt` months where every snapshot candidate was unreadable but commit
        /// replay rebuilt complete state — excludes the rejected-commit cause (self-protecting) and the
        /// out-of-month-op cause (explicitly subtracted: its dropped asset means replay is incomplete).
        /// `RepoCompactionService.repairCorruptSnapshotBaselines` re-checkpoints these so they
        /// re-materialize `.clean`; an out-of-month or rejected-commit month stays corrupt until fixed.
        let corruptedSnapshotMonths: Set<LibraryMonthKey>
        /// Per-month max filename-lamport among snapshot candidates that read as corrupt. Lets repair
        /// advance the clock above a lingering corrupt snapshot so its fresh baseline dominates it.
        var corruptSnapshotMaxLamportByMonth: [LibraryMonthKey: UInt64] = [:]
        let repoID: String?
    }

    enum MetadataReadRaceError: Error, Equatable {
        case requiredCommitVanished(filename: String, month: LibraryMonthKey, writerID: String, seq: UInt64)
        case snapshotVanishedWithoutRecovery(filename: String, month: LibraryMonthKey, lamport: UInt64, writerID: String, runIDPrefix: String)
        case metadataChangedAgainAfterRetry
    }

    func materialize(expectedRepoID: String) async throws -> MaterializeOutput {
        try await materialize(filterMonth: nil, expectedRepoID: expectedRepoID)
    }

    func materializeMonth(_ month: LibraryMonthKey, expectedRepoID: String) async throws -> MaterializeOutput {
        try await materialize(filterMonth: month, expectedRepoID: expectedRepoID)
    }

    private func materialize(filterMonth: LibraryMonthKey?, expectedRepoID: String) async throws -> MaterializeOutput {
        do {
            return try await materializeOnce(filterMonth: filterMonth, expectedRepoID: expectedRepoID)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            guard let race = error as? InternalMetadataReadRace else { throw error }
            // A listed commit/snapshot can 404 on GET inside the backend read-after-write window.
            // Zero-grace backends keep the single immediate retry (concurrent-delete guard); grace
            // backends keep retrying within the grace budget before treating the listed file as a
            // genuine metadata race. A successful read that still lacks coverage means the file was
            // truly deleted, so validateRetry fails closed without further retry.
            let deadline: Date? = client.readAfterWriteGraceSeconds > 0
                ? client.metadataReadAfterWriteDeadline(floorSeconds: 1)
                : nil
            var attempt = 0
            while true {
                do {
                    let retry = try await materializeOnce(filterMonth: filterMonth, expectedRepoID: expectedRepoID)
                    try validateRetry(retry, recovers: race)
                    return retry
                } catch {
                    if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                    if error is InternalMetadataReadRace {
                        guard let deadline, Date() < deadline else {
                            throw MetadataReadRaceError.metadataChangedAgainAfterRetry
                        }
                        try await Task.sleep(nanoseconds: UInt64(200 * (1 << min(attempt, 3))) * 1_000_000)
                        attempt += 1
                        continue
                    }
                    // A grace-backend retry listing can omit the same lagging file, so materializeOnce
                    // succeeds without it and validateRetry reports the original race's coverage still
                    // missing. Inside the grace window that is list visibility lag, not a confirmed
                    // delete — keep retrying. Zero-grace backends and the post-deadline case keep the
                    // immediate fail-closed propagation of the original race error.
                    if let deadline, Date() < deadline, Self.isRecoverableRetryRaceFailure(error) {
                        try await Task.sleep(nanoseconds: UInt64(200 * (1 << min(attempt, 3))) * 1_000_000)
                        attempt += 1
                        continue
                    }
                    throw error
                }
            }
        }
    }

    private static func isRecoverableRetryRaceFailure(_ error: Error) -> Bool {
        guard let race = error as? MetadataReadRaceError else { return false }
        switch race {
        case .requiredCommitVanished, .snapshotVanishedWithoutRecovery:
            return true
        case .metadataChangedAgainAfterRetry:
            return false
        }
    }

    private func validateRetry(_ output: MaterializeOutput, recovers race: InternalMetadataReadRace) throws {
        let ceiling = RepoStateAuthority.maxPersistableSeq
        switch race {
        case .requiredCommitVanished(let filename, let month, let writerID, let seq):
            let covered = output.coveredByMonth[month] ?? .empty
            guard covered.contains(writerID: writerID, seq: seq) else {
                throw MetadataReadRaceError.requiredCommitVanished(
                    filename: filename,
                    month: month,
                    writerID: writerID,
                    seq: seq
                )
            }
            if seq < ceiling {
                guard (output.observedSeqByWriter[writerID] ?? 0) >= seq else {
                    throw MetadataReadRaceError.requiredCommitVanished(
                        filename: filename,
                        month: month,
                        writerID: writerID,
                        seq: seq
                    )
                }
            }
        case .snapshotVanished(let filename, let month, let lamport, let writerID, let runIDPrefix):
            guard let accepted = output.acceptedSnapshotBaselinesByMonth[month],
                  Self.snapshotBaseline(accepted, recoversLamport: lamport, writerID: writerID, runIDPrefix: runIDPrefix) else {
                throw MetadataReadRaceError.snapshotVanishedWithoutRecovery(
                    filename: filename,
                    month: month,
                    lamport: lamport,
                    writerID: writerID,
                    runIDPrefix: runIDPrefix
                )
            }
        }
    }

    private static func snapshotBaseline(
        _ accepted: AcceptedSnapshotBaselineInfo,
        recoversLamport lamport: UInt64,
        writerID: String,
        runIDPrefix: String
    ) -> Bool {
        if accepted.lamport != lamport { return accepted.lamport > lamport }
        if accepted.writerID != writerID { return accepted.writerID > writerID }
        return accepted.runIDPrefix >= runIDPrefix
    }


    private func materializeOnce(filterMonth: LibraryMonthKey?, expectedRepoID: String) async throws -> MaterializeOutput {
        async let snapshotFilenames = snapshotReader.listSnapshotFilenames()
        async let commitFilenames = commitReader.listCommitFilenames()
        let snapshots = try await snapshotFilenames
        let commits = try await commitFilenames

        var snapshotReferences: [MaterializerSnapshotReference] = []
        var filenameRejectedMonths: Set<LibraryMonthKey> = []
        for filename in snapshots {
            guard let parsed = RepoLayout.parseSnapshotFilename(filename) else { continue }
            if let filterMonth, parsed.month != filterMonth { continue }
            guard parsed.lamport < LamportClock.maxAdoptableValue else {
                materializerLog.warning("skip snapshot with unworkable lamport in filename: \(filename, privacy: .public)")
                filenameRejectedMonths.insert(parsed.month)
                continue
            }
            snapshotReferences.append(MaterializerSnapshotReference(
                month: parsed.month,
                filename: filename,
                lamport: parsed.lamport,
                writerID: parsed.writerID,
                runIDPrefix: parsed.runIDPrefix
            ))
        }

        let snapshotTrust = try await SnapshotTrustPipeline(reader: snapshotReader).accept(
            references: snapshotReferences,
            expectedRepoID: expectedRepoID
        )

        var commitReferences: [MaterializerCommitReference] = []
        for filename in commits {
            guard let parsed = RepoLayout.parseCommitFilename(filename) else { continue }
            if let filterMonth, parsed.month != filterMonth { continue }
            commitReferences.append(MaterializerCommitReference(
                month: parsed.month,
                filename: filename,
                writerID: parsed.writerID,
                seq: parsed.seq
            ))
        }

        var baselineCoveredByMonth = snapshotTrust.acceptedBaselinesByMonth.mapValues(\.covered)
        for month in snapshotTrust.emptyBaselineMonths where baselineCoveredByMonth[month] == nil {
            baselineCoveredByMonth[month] = .empty
        }
        let commitTrust = try await CommitTrustPipeline(reader: commitReader).accept(
            references: commitReferences,
            coveredByMonth: baselineCoveredByMonth,
            expectedRepoID: expectedRepoID
        )
        let projection = MaterializerReplayProjector().project(
            baselinesByMonth: snapshotTrust.acceptedBaselinesByMonth,
            emptyBaselineMonths: snapshotTrust.emptyBaselineMonths,
            acceptedCommits: commitTrust.acceptedCommits,
            maxTrustedLamportByMonth: snapshotTrust.maxTrustedLamportByMonth
        )
        let state = projection.state
        let monthsWithOutOfMonthReplayOps = projection.monthsWithOutOfMonthOps

        var corruptedSnapshotMonths = snapshotTrust.corruptedSnapshotMonths
        for month in filenameRejectedMonths where snapshotTrust.acceptedSnapshotLamportByMonth[month] == nil {
            corruptedSnapshotMonths.insert(month)
        }
        // An out-of-month replay op lives on a COVERED commit, so a repair baseline would absorb its seq
        // and silence the out-of-month signal on the next materialize, laundering the dropped asset to
        // clean. Unlike rejected commits (uncovered → re-flagged every materialize), this signal is not
        // self-protecting. Keep these months out of corruptedSnapshotMonths so repair leaves them
        // terminal-.corrupt, honoring the set's "complete state" contract; the .corrupt outcome still
        // holds via monthsWithOutOfMonthReplayOps below.
        corruptedSnapshotMonths.subtract(monthsWithOutOfMonthReplayOps)
        let ceiling = RepoStateAuthority.maxPersistableSeq
        var observedSeqByWriter: [String: UInt64] = [:]
        for (writer, seq) in commitTrust.observedSeqByWriter where seq < ceiling {
            observedSeqByWriter[writer] = seq
        }
        for (_, covered) in commitTrust.coveredByMonth {
            for (writer, ranges) in covered.rangesByWriter {
                let high = ranges.compactMap { range -> UInt64? in
                    guard range.low < ceiling else { return nil }
                    return min(range.high, ceiling &- 1)
                }.max() ?? 0
                guard high > 0 else { continue }
                let prior = observedSeqByWriter[writer] ?? 0
                if high > prior {
                    observedSeqByWriter[writer] = high
                }
            }
        }
        for reference in commitReferences {
            guard reference.seq < ceiling else { continue }
            let prior = observedSeqByWriter[reference.writerID] ?? 0
            if reference.seq > prior {
                observedSeqByWriter[reference.writerID] = reference.seq
            }
        }

        var monthsWithRejectedCommits: Set<LibraryMonthKey> = []
        for reference in commitReferences {
            let wasCoveredByBaseline = (baselineCoveredByMonth[reference.month] ?? .empty)
                .contains(writerID: reference.writerID, seq: reference.seq)
            let wasAcceptedByPipeline = (commitTrust.coveredByMonth[reference.month] ?? .empty)
                .contains(writerID: reference.writerID, seq: reference.seq)
            if !wasCoveredByBaseline && !wasAcceptedByPipeline {
                monthsWithRejectedCommits.insert(reference.month)
            }
        }

        // Build per-month outcome. Every materialized month gets an entry so downstream
        // write/maintenance consumers can reliably gate on clean vs ambiguous/corrupt.
        let coverageRegressedMonths = snapshotTrust.coverageRegressedMonths
        var outcomeByMonth: [LibraryMonthKey: MonthOutcome] = [:]
        let allMonths = Set(state.months.keys)
            .union(commitTrust.coveredByMonth.keys)
            .union(corruptedSnapshotMonths)
            .union(coverageRegressedMonths)
            .union(snapshotTrust.ambiguousMonths)
            .union(monthsWithRejectedCommits)
            .union(monthsWithOutOfMonthReplayOps)
        for month in allMonths {
            if snapshotTrust.ambiguousMonths.contains(month) {
                outcomeByMonth[month] = .ambiguous
            } else if corruptedSnapshotMonths.contains(month)
                || monthsWithRejectedCommits.contains(month)
                || coverageRegressedMonths.contains(month)
                || monthsWithOutOfMonthReplayOps.contains(month) {
                outcomeByMonth[month] = .corrupt
            } else {
                outcomeByMonth[month] = .clean
            }
        }

        if !corruptedSnapshotMonths.isEmpty {
            materializerLog.warning("materialize: \(corruptedSnapshotMonths.count, privacy: .public) month(s) had all snapshots corrupt; commit replay rebuilt state — caller should force a fresh baseline on next flush")
        }
        if !coverageRegressedMonths.isEmpty {
            materializerLog.warning("materialize: \(coverageRegressedMonths.count, privacy: .public) month(s) fell back below a corrupt snapshot at or above the accepted lamport whose coverage is unreadable; flagged non-clean to avoid attesting a possibly-truncated state")
        }
        if !monthsWithRejectedCommits.isEmpty {
            materializerLog.warning("materialize: \(monthsWithRejectedCommits.count, privacy: .public) month(s) had rejected uncovered commits")
        }
        if !monthsWithOutOfMonthReplayOps.isEmpty {
            materializerLog.warning("materialize: \(monthsWithOutOfMonthReplayOps.count, privacy: .public) month(s) had an accepted commit whose addAsset resource lay outside the month; flagged non-clean")
        }
        if !snapshotTrust.ambiguousMonths.isEmpty {
            materializerLog.warning("materialize: \(snapshotTrust.ambiguousMonths.count, privacy: .public) month(s) have ambiguous snapshot coverage")
        }

        return MaterializeOutput(
            state: state,
            observedSeqByWriter: observedSeqByWriter,
            coveredByMonth: commitTrust.coveredByMonth,
            acceptedSnapshotBaselinesByMonth: snapshotTrust.acceptedBaselinesByMonth.mapValues(\.info),
            outcomeByMonth: outcomeByMonth,
            corruptedSnapshotMonths: corruptedSnapshotMonths,
            corruptSnapshotMaxLamportByMonth: snapshotTrust.maxCorruptCandidateLamportByMonth,
            repoID: expectedRepoID
        )
    }
}

private enum InternalMetadataReadRace: Error {
    case requiredCommitVanished(filename: String, month: LibraryMonthKey, writerID: String, seq: UInt64)
    case snapshotVanished(filename: String, month: LibraryMonthKey, lamport: UInt64, writerID: String, runIDPrefix: String)
}

private struct MaterializerSnapshotReference: Sendable {
    let month: LibraryMonthKey
    let filename: String
    let lamport: UInt64
    let writerID: String
    let runIDPrefix: String
}

private struct MaterializerCommitReference: Sendable {
    let month: LibraryMonthKey
    let filename: String
    let writerID: String
    let seq: UInt64
}

private struct AcceptedSnapshotBaseline: Sendable {
    let state: RepoMonthState
    let covered: CoveredRanges
    let baselineStamps: [AssetFingerprint: OpStamp]
    let info: RepoMaterializer.AcceptedSnapshotBaselineInfo
    let lamport: UInt64
}

private struct SnapshotTrustResult: Sendable {
    let acceptedBaselinesByMonth: [LibraryMonthKey: AcceptedSnapshotBaseline]
    let emptyBaselineMonths: Set<LibraryMonthKey>
    let corruptedSnapshotMonths: Set<LibraryMonthKey>
    /// Months with an accepted baseline but a corrupt sibling that may have covered more (whose commits
    /// could already be GC'd): a same-writer corrupt at or above the survivor's lamport, or any
    /// cross-writer corrupt (lamport is not a cross-writer coverage proxy). Non-clean, never repair-eligible.
    let coverageRegressedMonths: Set<LibraryMonthKey>
    let ambiguousMonths: Set<LibraryMonthKey>
    /// Max lamport across ALL trusted snapshot candidates (not just accepted), for observedClock.
    let maxTrustedLamportByMonth: [LibraryMonthKey: UInt64]
    let acceptedSnapshotLamportByMonth: [LibraryMonthKey: UInt64]
    /// Per-month max filename-lamport among candidates that read as corrupt.
    let maxCorruptCandidateLamportByMonth: [LibraryMonthKey: UInt64]
}

private struct SnapshotTrustPipeline {
    let reader: SnapshotReader

    func accept(
        references: [MaterializerSnapshotReference],
        expectedRepoID: String
    ) async throws -> SnapshotTrustResult {
        var snapshotsByMonth: [LibraryMonthKey: [MaterializerSnapshotReference]] = [:]
        for reference in references {
            snapshotsByMonth[reference.month, default: []].append(reference)
        }
        for month in snapshotsByMonth.keys {
            snapshotsByMonth[month]?.sort { lhs, rhs in
                if lhs.lamport != rhs.lamport { return lhs.lamport > rhs.lamport }
                if lhs.writerID != rhs.writerID { return lhs.writerID > rhs.writerID }
                return lhs.runIDPrefix > rhs.runIDPrefix
            }
        }

        return try await withThrowingTaskGroup(of: SnapshotMonthTaskResult.self) { group in
            for (month, candidates) in snapshotsByMonth {
                let reader = self.reader
                let expected = expectedRepoID
                group.addTask {
                    var trustedBaselines: [AcceptedSnapshotBaseline] = []
                    var sawCandidate = false
                    var corruptCandidateMaxLamportByWriter: [String: UInt64] = [:]
                    for candidate in candidates {
                        do {
                            let file = try await reader.read(filename: candidate.filename)
                            // Any successfully-read snapshot file counts as a candidate for corrupt
                            // detection, even if subsequent validation rejects it — the month had
                            // listed parseable snapshots but produced no trusted baseline.
                            sawCandidate = true
                            guard Self.fileMatchesReference(file, reference: candidate) else {
                                materializerLog.warning("skip snapshot whose filename disagrees with header: \(candidate.filename, privacy: .public)")
                                continue
                            }
                            guard file.header.repoID == expected else {
                                materializerLog.warning("skip foreign-repo snapshot \(candidate.filename, privacy: .public) header=\(file.header.repoID, privacy: .public) expected=\(expected, privacy: .public)")
                                continue
                            }
                            if snapshotHasUnworkableRowStamp(file, filenameLamport: candidate.lamport) {
                                materializerLog.warning("skip snapshot with poisoned row stamp: \(candidate.filename, privacy: .public)")
                                continue
                            }
                            if let baseline = Self.makeBaseline(file: file, reference: candidate) {
                                trustedBaselines.append(baseline)
                            }
                        } catch let error as RepoJSONLReadError {
                            switch error {
                            case .integrityMismatch, .missingHeader, .missingEnd, .decodeFailure:
                                materializerLog.warning("skip corrupt snapshot \(candidate.filename, privacy: .public): \(String(describing: error), privacy: .public)")
                                sawCandidate = true
                                corruptCandidateMaxLamportByWriter[candidate.writerID] = max(
                                    corruptCandidateMaxLamportByWriter[candidate.writerID] ?? 0,
                                    candidate.lamport
                                )
                                continue
                            case .notFound:
                                throw InternalMetadataReadRace.snapshotVanished(
                                    filename: candidate.filename,
                                    month: candidate.month,
                                    lamport: candidate.lamport,
                                    writerID: candidate.writerID,
                                    runIDPrefix: candidate.runIDPrefix
                                )
                            }
                        }
                    }
                    return SnapshotMonthTaskResult(
                        month: month,
                        trustedBaselines: trustedBaselines,
                        sawCandidate: sawCandidate,
                        corruptCandidateMaxLamportByWriter: corruptCandidateMaxLamportByWriter
                    )
                }
            }

            var acceptedBaselinesByMonth: [LibraryMonthKey: AcceptedSnapshotBaseline] = [:]
            var emptyBaselineMonths: Set<LibraryMonthKey> = []
            var corruptedSnapshotMonths: Set<LibraryMonthKey> = []
            var coverageRegressedMonths: Set<LibraryMonthKey> = []
            var ambiguousMonths: Set<LibraryMonthKey> = []
            var maxTrustedLamportByMonth: [LibraryMonthKey: UInt64] = [:]
            var acceptedSnapshotLamportByMonth: [LibraryMonthKey: UInt64] = [:]
            var maxCorruptCandidateLamportByMonth: [LibraryMonthKey: UInt64] = [:]
            for try await result in group {
                let month = result.month
                let trusted = result.trustedBaselines
                let maxLamport = trusted.map(\.lamport).max() ?? 0
                if maxLamport > 0 {
                    maxTrustedLamportByMonth[month] = maxLamport
                }
                let corruptByWriter = result.corruptCandidateMaxLamportByWriter
                let maxCorruptLamport = corruptByWriter.values.max() ?? 0
                if maxCorruptLamport > 0 {
                    maxCorruptCandidateLamportByMonth[month] = maxCorruptLamport
                }

                if trusted.isEmpty {
                    if result.sawCandidate {
                        corruptedSnapshotMonths.insert(month)
                    }
                    emptyBaselineMonths.insert(month)
                    continue
                }

                let selected = Self.selectCoveredMaxBaseline(trusted, month: month)
                if let selected {
                    acceptedBaselinesByMonth[month] = selected
                    acceptedSnapshotLamportByMonth[month] = selected.lamport
                    // A corrupt sibling may have been the covered-max whose commits commit-GC already
                    // deleted, leaving the survivor a strict subset; its coverage is unreadable, so fail
                    // closed. Lamport tracks coverage monotonically only inside one writer's lineage (the
                    // clock is one global per-repo counter), so a same-writer corrupt at or above the
                    // survivor's lamport demotes, while any cross-writer corrupt demotes regardless of
                    // lamport — cross-writer lamport ordering is meaningless as a coverage proxy. (Stays
                    // out of corruptedSnapshotMonths so repair can't re-bless the lossy fallback as clean.)
                    let sameWriterMaxCorrupt = corruptByWriter[selected.info.writerID] ?? 0
                    let hasCrossWriterCorrupt = corruptByWriter.keys.contains { $0 != selected.info.writerID }
                    if (sameWriterMaxCorrupt > 0 && sameWriterMaxCorrupt >= selected.lamport) || hasCrossWriterCorrupt {
                        coverageRegressedMonths.insert(month)
                    }
                } else {
                    // Incomparable trusted coverage — pick best-effort for reads, mark ambiguous.
                    let bestEffort = trusted.max(by: { lhs, rhs in
                        let lhsCount = lhs.covered.totalCoveredSeqs()
                        let rhsCount = rhs.covered.totalCoveredSeqs()
                        if lhsCount != rhsCount { return lhsCount < rhsCount }
                        if lhs.lamport != rhs.lamport { return lhs.lamport < rhs.lamport }
                        if lhs.info.writerID != rhs.info.writerID { return lhs.info.writerID < rhs.info.writerID }
                        return lhs.info.runIDPrefix < rhs.info.runIDPrefix
                    })
                    if let bestEffort {
                        acceptedBaselinesByMonth[month] = bestEffort
                        acceptedSnapshotLamportByMonth[month] = bestEffort.lamport
                    }
                    ambiguousMonths.insert(month)
                    materializerLog.warning("month \(month.text, privacy: .public) has \(trusted.count, privacy: .public) trusted snapshots with incomparable coverage; using best-effort baseline")
                }
            }
            return SnapshotTrustResult(
                acceptedBaselinesByMonth: acceptedBaselinesByMonth,
                emptyBaselineMonths: emptyBaselineMonths,
                corruptedSnapshotMonths: corruptedSnapshotMonths,
                coverageRegressedMonths: coverageRegressedMonths,
                ambiguousMonths: ambiguousMonths,
                maxTrustedLamportByMonth: maxTrustedLamportByMonth,
                acceptedSnapshotLamportByMonth: acceptedSnapshotLamportByMonth,
                maxCorruptCandidateLamportByMonth: maxCorruptCandidateLamportByMonth
            )
        }
    }

    /// Returns the trusted baseline whose `covered` is a superset of all other trusted
    /// candidates' `covered`, or nil if no single candidate dominates.
    private static func selectCoveredMaxBaseline(
        _ trusted: [AcceptedSnapshotBaseline],
        month: LibraryMonthKey
    ) -> AcceptedSnapshotBaseline? {
        guard !trusted.isEmpty else { return nil }
        if trusted.count == 1 { return trusted[0] }

        var coveredMax: AcceptedSnapshotBaseline?
        for (i, candidate) in trusted.enumerated() {
            let isSuperset = trusted.enumerated().allSatisfy { (j, other) in
                i == j || candidate.covered.superset(of: other.covered)
            }
            if isSuperset {
                if let existing = coveredMax {
                    // Multiple covered-max candidates — tiebreak by lamport/writer/run
                    if candidate.lamport != existing.lamport {
                        coveredMax = candidate.lamport > existing.lamport ? candidate : existing
                    } else if candidate.info.writerID != existing.info.writerID {
                        coveredMax = candidate.info.writerID > existing.info.writerID ? candidate : existing
                    } else {
                        coveredMax = candidate.info.runIDPrefix >= existing.info.runIDPrefix ? candidate : existing
                    }
                } else {
                    coveredMax = candidate
                }
            }
        }
        return coveredMax
    }

    private struct SnapshotMonthTaskResult: Sendable {
        let month: LibraryMonthKey
        let trustedBaselines: [AcceptedSnapshotBaseline]
        let sawCandidate: Bool
        /// Per-writer max filename-lamport among candidates that read as corrupt. Lamport tracks
        /// coverage monotonically only within one writer's lineage, so the demotion guard compares
        /// same-writer lamports but fails closed on any cross-writer corrupt sibling.
        let corruptCandidateMaxLamportByWriter: [String: UInt64]
    }

    private static func fileMatchesReference(_ file: SnapshotFile, reference: MaterializerSnapshotReference) -> Bool {
        file.header.writerID == reference.writerID
            && CommitHeader.parseMonthScope(file.header.scope) == reference.month
    }

    private static func makeBaseline(file: SnapshotFile, reference: MaterializerSnapshotReference) -> AcceptedSnapshotBaseline? {
        let month = reference.month
        var state = RepoMonthState.empty
        var baselineStamps: [AssetFingerprint: OpStamp] = [:]
        for asset in file.assets {
            state.assets[asset.assetFingerprint] = asset
            baselineStamps[asset.assetFingerprint] = asset.stamp
        }
        var resourceHashes: Set<Data> = []
        for resource in file.resources {
            guard materializerResourcePath(resource.physicalRemotePath, belongsTo: month) else {
                materializerLog.warning("reject snapshot with out-of-month resource month=\(month.text, privacy: .public) path=\(resource.physicalRemotePath, privacy: .public)")
                return nil
            }
            state.resources[RemotePhysicalPathKey(resource.physicalRemotePath)] = resource
            resourceHashes.insert(resource.contentHash)
        }
        for ar in file.assetResources {
            // A link to a resourceHash with no resource row in the snapshot is an inconsistent body: the
            // asset would materialize missing while the covered good commit (which carries the resource)
            // is skipped, and clean compaction could then delete that commit. Reject the baseline so the
            // covered commit replays or the month fails closed.
            guard resourceHashes.contains(ar.resourceHash) else {
                materializerLog.warning("reject snapshot with asset-resource link to absent resource month=\(month.text, privacy: .public)")
                return nil
            }
            let key = AssetResourceKey(assetFingerprint: ar.assetFingerprint, role: ar.role, slot: ar.slot)
            state.assetResources[key] = ar
        }
        for d in file.deletedKeys {
            guard d.keyType == .asset else {
                materializerLog.warning("reject snapshot with unsupported deletedKey.keyType=\(String(describing: d.keyType), privacy: .public) for \(month.text, privacy: .public)")
                return nil
            }
            let fp: AssetFingerprint
            do {
                fp = try RepoWireValidator.validateAssetFingerprint(d.keyValue, field: "keyValue")
            } catch {
                materializerLog.warning("reject snapshot with malformed deletedKey hash for \(month.text, privacy: .public): \(String(describing: error), privacy: .public)")
                return nil
            }
            state.deletedAssetStamps[fp] = d.stamp
        }
        return AcceptedSnapshotBaseline(
            state: state,
            covered: file.header.covered,
            baselineStamps: baselineStamps,
            info: RepoMaterializer.AcceptedSnapshotBaselineInfo(
                filename: reference.filename,
                month: reference.month,
                lamport: reference.lamport,
                writerID: reference.writerID,
                runIDPrefix: reference.runIDPrefix,
                covered: file.header.covered
            ),
            lamport: reference.lamport
        )
    }
}

private struct AcceptedCommit: Sendable {
    let month: LibraryMonthKey
    let header: CommitHeader
    let ops: [CommitOp]
}

private struct CommitTrustResult: Sendable {
    let observedSeqByWriter: [String: UInt64]
    let coveredByMonth: [LibraryMonthKey: CoveredRanges]
    let acceptedCommits: [AcceptedCommit]
}

private struct CommitTrustPipeline {
    let reader: CommitLogReader

    func accept(
        references: [MaterializerCommitReference],
        coveredByMonth initialCoveredByMonth: [LibraryMonthKey: CoveredRanges],
        expectedRepoID: String
    ) async throws -> CommitTrustResult {
        var observedSeqByWriter: [String: UInt64] = [:]
        var commitsToRead: [MaterializerCommitReference] = []
        for reference in references {
            let covered = initialCoveredByMonth[reference.month] ?? .empty
            if !covered.contains(writerID: reference.writerID, seq: reference.seq) {
                commitsToRead.append(reference)
            }
        }

        let readCommits = try await readAcceptedFiles(
            references: commitsToRead,
            expectedRepoID: expectedRepoID
        )

        var coveredByMonth = initialCoveredByMonth
        for commit in readCommits {
            let prior = observedSeqByWriter[commit.header.writerID] ?? 0
            if commit.header.seq > prior {
                observedSeqByWriter[commit.header.writerID] = commit.header.seq
            }
            var monthCovered = coveredByMonth[commit.month] ?? .empty
            monthCovered.add(writerID: commit.header.writerID, seq: commit.header.seq)
            coveredByMonth[commit.month] = monthCovered
        }

        return CommitTrustResult(
            observedSeqByWriter: observedSeqByWriter,
            coveredByMonth: coveredByMonth,
            acceptedCommits: readCommits
        )
    }

    private func readAcceptedFiles(
        references: [MaterializerCommitReference],
        expectedRepoID: String
    ) async throws -> [AcceptedCommit] {
        try await withThrowingTaskGroup(of: AcceptedCommit?.self) { group in
            for reference in references {
                let reader = self.reader
                let expected = expectedRepoID
                group.addTask {
                    do {
                        let file = try await reader.read(filename: reference.filename)
                        let scopeMonth = CommitHeader.parseMonthScope(file.header.scope)
                        if file.header.writerID != reference.writerID
                            || file.header.seq != reference.seq
                            || scopeMonth != reference.month {
                            materializerLog.warning("skip commit whose filename disagrees with header: \(reference.filename, privacy: .public)")
                            return nil
                        }
                        if file.header.repoID != expected {
                            materializerLog.warning("skip commit with mismatched repoID file=\(String(describing: reference.month), privacy: .public) header=\(file.header.repoID, privacy: .public) expected=\(expected, privacy: .public)")
                            return nil
                        }
                        var acceptedOps: [CommitOp] = []
                        var seenOpSeqs: Set<Int> = []
                        for op in file.ops {
                            guard op.clock < LamportClock.maxAdoptableValue else {
                                materializerLog.warning("reject commit with unworkable op clock=\(op.clock, privacy: .public) writerID=\(file.header.writerID, privacy: .public) seq=\(file.header.seq, privacy: .public)")
                                return nil
                            }
                            // Duplicate opSeq makes the replay sort key (clock, writerID, seq, opSeq)
                            // non-unique, so a same-clock add+tombstone pair would resolve by arbitrary
                            // sort order. Reject the whole commit fail-closed; it stays uncovered and the
                            // month folds non-clean via monthsWithRejectedCommits.
                            guard seenOpSeqs.insert(op.opSeq).inserted else {
                                materializerLog.warning("reject commit with duplicate opSeq=\(op.opSeq, privacy: .public) writerID=\(file.header.writerID, privacy: .public) seq=\(file.header.seq, privacy: .public)")
                                return nil
                            }
                            acceptedOps.append(op)
                        }
                        return AcceptedCommit(month: reference.month, header: file.header, ops: acceptedOps)
                    } catch let error as RepoJSONLReadError {
                        switch error {
                        case .integrityMismatch, .missingHeader, .missingEnd, .decodeFailure:
                            materializerLog.warning("skip corrupt commit \(reference.filename, privacy: .public): \(String(describing: error), privacy: .public)")
                            return nil
                        case .notFound:
                            throw InternalMetadataReadRace.requiredCommitVanished(
                                filename: reference.filename,
                                month: reference.month,
                                writerID: reference.writerID,
                                seq: reference.seq
                            )
                        }
                    }
                }
            }
            var result: [AcceptedCommit] = []
            for try await commit in group {
                if let commit { result.append(commit) }
            }
            return result
        }
    }
}

private struct MaterializerReplayProjector {
    struct ProjectionResult {
        let state: RepoSnapshotState
        /// Months where an accepted commit's addAsset op was skipped because a resource lay outside the
        /// month. The commit stays covered, so it never reaches `monthsWithRejectedCommits`; the caller
        /// folds these to `.corrupt` so the silently-dropped asset is not attested clean.
        let monthsWithOutOfMonthOps: Set<LibraryMonthKey>
    }

    func project(
        baselinesByMonth: [LibraryMonthKey: AcceptedSnapshotBaseline],
        emptyBaselineMonths: Set<LibraryMonthKey>,
        acceptedCommits: [AcceptedCommit],
        maxTrustedLamportByMonth: [LibraryMonthKey: UInt64]
    ) -> ProjectionResult {
        var monthStates = baselinesByMonth.mapValues(\.state)
        var monthsWithOutOfMonthOps: Set<LibraryMonthKey> = []
        for month in emptyBaselineMonths where monthStates[month] == nil {
            monthStates[month] = .empty
        }
        var baselineStampsByMonth: [LibraryMonthKey: [AssetFingerprint: OpStamp]] = [:]
        for (month, baseline) in baselinesByMonth where !baseline.baselineStamps.isEmpty {
            baselineStampsByMonth[month] = baseline.baselineStamps
        }

        // observedClock incorporates max lamport of ALL trusted snapshot candidates, not just
        // the accepted baseline, so subsequent lamport ticks stay above every trusted writer.
        var observedClock: UInt64 = maxTrustedLamportByMonth.values.max() ?? 0
        var sortedOps: [ReplayOp] = []
        for commit in acceptedCommits {
            for op in commit.ops {
                sortedOps.append(ReplayOp(month: commit.month, writerID: commit.header.writerID, seq: commit.header.seq, op: op))
                observedClock = max(observedClock, op.clock)
            }
        }
        sortedOps.sort { lhs, rhs in
            if lhs.op.clock != rhs.op.clock { return lhs.op.clock < rhs.op.clock }
            if lhs.writerID != rhs.writerID { return lhs.writerID < rhs.writerID }
            if lhs.seq != rhs.seq { return lhs.seq < rhs.seq }
            return lhs.op.opSeq < rhs.op.opSeq
        }

        var lastAddByMonthFP: [LibraryMonthKey: [AssetFingerprint: OpStamp]] = baselineStampsByMonth
        for sorted in sortedOps {
            var state = monthStates[sorted.month] ?? .empty
            switch sorted.op.body {
            case .addAsset(let body):
                guard body.resources.allSatisfy({ materializerResourcePath($0.physicalRemotePath, belongsTo: sorted.month) }) else {
                    materializerLog.warning("skip addAsset with resource outside month=\(sorted.month.text, privacy: .public)")
                    monthsWithOutOfMonthOps.insert(sorted.month)
                    continue
                }
                let incoming = OpStamp(writerID: sorted.writerID, seq: sorted.seq, clock: sorted.op.clock)
                if let baselineStamp = state.assets[body.assetFingerprint]?.stamp,
                   opStampPrecedes(incoming, baselineStamp) {
                    continue
                }
                if let deletedStamp = state.deletedAssetStamps[body.assetFingerprint],
                   opStampPrecedes(incoming, deletedStamp) {
                    continue
                }
                state.deletedAssetStamps.removeValue(forKey: body.assetFingerprint)
                state.assets[body.assetFingerprint] = SnapshotAssetRow(
                    assetFingerprint: body.assetFingerprint,
                    creationDateMs: body.creationDateMs,
                    backedUpAtMs: body.backedUpAtMs,
                    resourceCount: body.resources.count,
                    totalFileSizeBytes: body.resources.reduce(Int64(0)) { $0 + $1.fileSize },
                    stamp: incoming
                )
                for resource in body.resources {
                    let resourceKey = RemotePhysicalPathKey(resource.physicalRemotePath)
                    let keepExistingResource = state.resources[resourceKey]
                        .map { opStampPrecedes(incoming, $0.stamp) } ?? false
                    if !keepExistingResource {
                        state.resources[resourceKey] = SnapshotResourceRow(
                            physicalRemotePath: resource.physicalRemotePath,
                            contentHash: resource.contentHash,
                            fileSize: resource.fileSize,
                            resourceType: resource.resourceType,
                            creationDateMs: body.creationDateMs,
                            backedUpAtMs: body.backedUpAtMs,
                            crypto: resource.crypto,
                            stamp: incoming
                        )
                    }
                    let key = AssetResourceKey(
                        assetFingerprint: body.assetFingerprint,
                        role: resource.role,
                        slot: resource.slot
                    )
                    state.assetResources[key] = SnapshotAssetResourceRow(
                        assetFingerprint: body.assetFingerprint,
                        role: resource.role,
                        slot: resource.slot,
                        resourceHash: resource.contentHash,
                        logicalName: resource.logicalName
                    )
                }
                lastAddByMonthFP[sorted.month, default: [:]][body.assetFingerprint] = incoming
            case .tombstoneAsset(let body):
                let tombstoneStamp = OpStamp(writerID: sorted.writerID, seq: sorted.seq, clock: sorted.op.clock)
                if let existingStamp = state.assets[body.assetFingerprint]?.stamp,
                   opStampPrecedes(tombstoneStamp, existingStamp) {
                    materializerLog.info("skip tombstone superseded by newer addAsset stamp in baseline")
                } else if let lastAdd = lastAddByMonthFP[sorted.month]?[body.assetFingerprint],
                          materializerIsAfterBasis(lastAdd, basis: body.observedBasis) {
                    materializerLog.info("skip observation-tombstone for fp; healing add observed after basis")
                } else {
                    state.assets.removeValue(forKey: body.assetFingerprint)
                    state.assetResources = state.assetResources.filter { $0.key.assetFingerprint != body.assetFingerprint }
                    if state.deletedAssetStamps[body.assetFingerprint].map({ opStampPrecedes($0, tombstoneStamp) }) ?? true {
                        state.deletedAssetStamps[body.assetFingerprint] = tombstoneStamp
                    }
                }
            }
            monthStates[sorted.month] = state
        }

        return ProjectionResult(
            state: RepoSnapshotState(months: monthStates, observedClock: observedClock),
            monthsWithOutOfMonthOps: monthsWithOutOfMonthOps
        )
    }

    private struct ReplayOp {
        let month: LibraryMonthKey
        let writerID: String
        let seq: UInt64
        let op: CommitOp
    }
}

private func materializerIsAfterBasis(_ stamp: OpStamp, basis: TombstoneObservationBasis) -> Bool {
    if stamp.clock > basis.lamportWatermark { return true }
    let prevMax = basis.perWriterMaxSeq[stamp.writerID] ?? 0
    return stamp.seq > prevMax
}

private func materializerResourcePath(_ path: String, belongsTo month: LibraryMonthKey) -> Bool {
    let components = RemotePathBuilder.normalizeRelativePath(path)
        .split(separator: "/", omittingEmptySubsequences: false)
    guard components.count == 3, !components[2].isEmpty else { return false }
    let expectedYear = String(format: "%04d", month.year)
    let expectedMonth = String(format: "%02d", month.month)
    return String(components[0]) == expectedYear && String(components[1]) == expectedMonth
}

private func snapshotHasUnworkableRowStamp(_ file: SnapshotFile, filenameLamport: UInt64) -> Bool {
    let covered = file.header.covered
    for asset in file.assets {
        let stamp = asset.stamp
        if isUnworkableStampClock(stamp.clock, filenameLamport: filenameLamport) { return true }
        if !covered.contains(writerID: stamp.writerID, seq: stamp.seq) { return true }
    }
    for resource in file.resources {
        let stamp = resource.stamp
        if isUnworkableStampClock(stamp.clock, filenameLamport: filenameLamport) { return true }
        if !covered.contains(writerID: stamp.writerID, seq: stamp.seq) { return true }
    }
    for d in file.deletedKeys {
        let stamp = d.stamp
        if isUnworkableStampClock(stamp.clock, filenameLamport: filenameLamport) { return true }
        if !covered.contains(writerID: stamp.writerID, seq: stamp.seq) { return true }
    }
    return false
}

private func isUnworkableStampClock(_ clock: UInt64, filenameLamport: UInt64) -> Bool {
    if clock >= LamportClock.maxAdoptableValue { return true }
    if clock > filenameLamport { return true }
    return false
}
