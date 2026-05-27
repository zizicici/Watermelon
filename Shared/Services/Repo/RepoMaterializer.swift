import Foundation
import os.log

private let materializerLog = Logger(subsystem: "com.zizicici.watermelon", category: "RepoMaterializer")

actor RepoMaterializer {
    private let snapshotReader: SnapshotReader
    private let commitReader: CommitLogReader
    private let crossRepoIndexReader: RepoCrossRepoIndexReader

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        // Internal TaskGroups fan out N concurrent read ops; `.serialOnly` backends need serialization.
        let effective = wrapIfSerial(client)
        self.snapshotReader = SnapshotReader(client: effective, basePath: basePath)
        self.commitReader = CommitLogReader(client: effective, basePath: basePath)
        self.crossRepoIndexReader = RepoCrossRepoIndexReader(client: effective, basePath: basePath)
    }

    struct AcceptedSnapshotBaselineInfo: Sendable, Equatable {
        let filename: String
        let month: LibraryMonthKey
        let lamport: UInt64
        let writerID: String
        let runIDPrefix: String
        let covered: CoveredRanges
    }

    struct AcceptedCrossRepoIndexBaselineInfo: Sendable, Equatable {
        let filename: String
        let lamport: UInt64
        let writerID: String
        let runIDPrefix: String
        let coveredForMonth: CoveredRanges
    }

    struct MaterializeOutput: Sendable {
        let state: RepoSnapshotState
        let observedSeqByWriter: [String: UInt64]
        /// Final fold coverage after accepted snapshot baseline plus replayed commits.
        let coveredByMonth: [LibraryMonthKey: CoveredRanges]
        /// Months whose active baseline came from a per-month snapshot file. Mutually
        /// exclusive with `acceptedCrossRepoIndexBaselineByMonth` (exactly one is non-nil
        /// per month that has any baseline).
        let acceptedSnapshotBaselinesByMonth: [LibraryMonthKey: AcceptedSnapshotBaselineInfo]
        /// Months whose active baseline came from a cross-repo index file. See above for
        /// mutual exclusion. Both nil for months with no baseline.
        let acceptedCrossRepoIndexBaselineByMonth: [LibraryMonthKey: AcceptedCrossRepoIndexBaselineInfo]
        /// Months where every snapshot candidate was corrupt — commit replay rebuilt state
        /// from empty. Caller can flip these months' next flush to emit a fresh baseline
        /// even when `dirty == false`, preventing O(commit log) replay every materialize.
        let corruptedSnapshotMonths: Set<LibraryMonthKey>
        let repoID: String?

        init(
            state: RepoSnapshotState,
            observedSeqByWriter: [String: UInt64],
            coveredByMonth: [LibraryMonthKey: CoveredRanges],
            acceptedSnapshotBaselinesByMonth: [LibraryMonthKey: AcceptedSnapshotBaselineInfo],
            acceptedCrossRepoIndexBaselineByMonth: [LibraryMonthKey: AcceptedCrossRepoIndexBaselineInfo] = [:],
            corruptedSnapshotMonths: Set<LibraryMonthKey>,
            repoID: String?
        ) {
            self.state = state
            self.observedSeqByWriter = observedSeqByWriter
            self.coveredByMonth = coveredByMonth
            self.acceptedSnapshotBaselinesByMonth = acceptedSnapshotBaselinesByMonth
            self.acceptedCrossRepoIndexBaselineByMonth = acceptedCrossRepoIndexBaselineByMonth
            self.corruptedSnapshotMonths = corruptedSnapshotMonths
            self.repoID = repoID
        }
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
            do {
                let retry = try await materializeOnce(filterMonth: filterMonth, expectedRepoID: expectedRepoID)
                try validateRetry(retry, recovers: race)
                return retry
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                if error is InternalMetadataReadRace {
                    throw MetadataReadRaceError.metadataChangedAgainAfterRetry
                }
                throw error
            }
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
            // Unit 5 invariant: unreadable accepted snapshot candidates cannot downgrade.
            // Recovery accepted via EITHER a per-month snapshot OR a cross-repo index baseline
            // whose (lamport, writerID, runIDPrefix) is same-or-newer per the existing tiebreak.
            let perMonthRecovers: Bool = output.acceptedSnapshotBaselinesByMonth[month].map { baseline in
                Self.snapshotReferenceIsSameOrNewer(
                    lamport: baseline.lamport,
                    writerID: baseline.writerID,
                    runIDPrefix: baseline.runIDPrefix,
                    thanLamport: lamport,
                    writerID: writerID,
                    runIDPrefix: runIDPrefix
                )
            } ?? false
            let crossRepoRecovers: Bool = output.acceptedCrossRepoIndexBaselineByMonth[month].map { crossRepo in
                Self.snapshotReferenceIsSameOrNewer(
                    lamport: crossRepo.lamport,
                    writerID: crossRepo.writerID,
                    runIDPrefix: crossRepo.runIDPrefix,
                    thanLamport: lamport,
                    writerID: writerID,
                    runIDPrefix: runIDPrefix
                )
            } ?? false
            guard perMonthRecovers || crossRepoRecovers else {
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

    private static func snapshotReferenceIsSameOrNewer(
        lamport: UInt64,
        writerID: String,
        runIDPrefix: String,
        thanLamport otherLamport: UInt64,
        writerID otherWriterID: String,
        runIDPrefix otherRunIDPrefix: String
    ) -> Bool {
        if lamport != otherLamport { return lamport > otherLamport }
        if writerID != otherWriterID { return writerID > otherWriterID }
        return runIDPrefix >= otherRunIDPrefix
    }

    /// Lists cross-repo index filenames but treats non-cancellation storage errors as "index
    /// directory unavailable" (return `[]`) so per-month snapshot + commit fallback still
    /// runs. The cross-repo index is an OPTIONAL acceleration artifact — its LIST failing
    /// must never abort materialization. Cancellation is still propagated.
    private static func listCrossRepoIndexFilenamesBestEffort(
        reader: RepoCrossRepoIndexReader
    ) async throws -> [String] {
        do {
            return try await reader.listIndexFilenames()
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            materializerLog.warning("cross-repo index list failed; falling back to per-month snapshots: \(String(describing: error), privacy: .public)")
            return []
        }
    }

    /// Drops per-month snapshot references for months where the cross-repo baseline already
    /// holds (or shares) the lex-max tiebreak. Lex tiebreak is `(lamport desc, writerID desc,
    /// runIDPrefix desc)` per the existing `SnapshotTrustPipeline.accept` ordering. A
    /// per-month reference whose `(lamport, writerID, runIDPrefix)` is STRICTLY newer than
    /// the cross-repo baseline's still gets read — it might win the merge. Otherwise the
    /// cross-repo baseline wins regardless of whether we read the per-month file, so the
    /// download is elided. This is the U02 hot-path read-elision invariant.
    private static func filterSnapshotReferences(
        _ references: [MaterializerSnapshotReference],
        against crossRepoBaselinesByMonth: [LibraryMonthKey: AcceptedSnapshotBaseline]
    ) -> [MaterializerSnapshotReference] {
        guard !crossRepoBaselinesByMonth.isEmpty else { return references }
        return references.filter { ref in
            guard let crossRepo = crossRepoBaselinesByMonth[ref.month] else {
                return true
            }
            let crossLamport = crossRepo.lamport
            let crossWriter = crossRepo.info.writerID
            let crossRunPrefix = crossRepo.info.runIDPrefix
            if ref.lamport != crossLamport { return ref.lamport > crossLamport }
            if ref.writerID != crossWriter { return ref.writerID > crossWriter }
            return ref.runIDPrefix > crossRunPrefix
        }
    }

    private func materializeOnce(filterMonth: LibraryMonthKey?, expectedRepoID: String) async throws -> MaterializeOutput {
        async let snapshotFilenames = snapshotReader.listSnapshotFilenames()
        async let commitFilenames = commitReader.listCommitFilenames()
        async let indexFilenames = Self.listCrossRepoIndexFilenamesBestEffort(reader: crossRepoIndexReader)
        let snapshots = try await snapshotFilenames
        let commits = try await commitFilenames
        let indexes = try await indexFilenames

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

        var indexReferences: [MaterializerCrossRepoIndexReference] = []
        for filename in indexes {
            guard let parsed = RepoLayout.parseCrossRepoIndexFilename(filename) else { continue }
            guard parsed.lamport < LamportClock.maxAdoptableValue else {
                materializerLog.warning("skip cross-repo index with unworkable lamport in filename: \(filename, privacy: .public)")
                continue
            }
            indexReferences.append(MaterializerCrossRepoIndexReference(
                filename: filename,
                lamport: parsed.lamport,
                writerID: parsed.writerID,
                runIDPrefix: parsed.runIDPrefix
            ))
        }

        // Cross-repo trust runs FIRST so we can prune per-month snapshot reads for months whose
        // cross-repo baseline cannot lose the lex-max tiebreak. Per-month snapshot candidates
        // for those months are reference-only (filename + parsed lamport/writer/run prefix —
        // no read) until we know they could plausibly beat the cross-repo baseline.
        let indexRefs = indexReferences
        let crossRepoTrust = try await CrossRepoIndexTrustPipeline(reader: crossRepoIndexReader).accept(
            references: indexRefs,
            expectedRepoID: expectedRepoID,
            filterMonth: filterMonth
        )

        let filteredSnapshotRefs = Self.filterSnapshotReferences(
            snapshotReferences,
            against: crossRepoTrust.acceptedBaselinesByMonth
        )
        let snapshotTrust = try await SnapshotTrustPipeline(reader: snapshotReader).accept(
            references: filteredSnapshotRefs,
            expectedRepoID: expectedRepoID
        )

        // Merge per-month baselines: cross-repo and per-month compete lex-max via
        // (lamport desc, writerID desc, runIDPrefix desc).
        var mergedBaselinesByMonth: [LibraryMonthKey: AcceptedSnapshotBaseline] = [:]
        var crossRepoWinningMonths: Set<LibraryMonthKey> = []
        var emptyBaselineMonths = snapshotTrust.emptyBaselineMonths
        var acceptedLamportByMonth = snapshotTrust.acceptedSnapshotLamportByMonth

        let allMonthsWithCandidates = Set(snapshotTrust.acceptedBaselinesByMonth.keys)
            .union(crossRepoTrust.acceptedBaselinesByMonth.keys)
        for month in allMonthsWithCandidates {
            let perMonth = snapshotTrust.acceptedBaselinesByMonth[month]
            let crossRepo = crossRepoTrust.acceptedBaselinesByMonth[month]
            switch (perMonth, crossRepo) {
            case (let per?, nil):
                mergedBaselinesByMonth[month] = per
            case (nil, let cross?):
                mergedBaselinesByMonth[month] = cross
                crossRepoWinningMonths.insert(month)
                acceptedLamportByMonth[month] = cross.lamport
                emptyBaselineMonths.remove(month)
            case (let per?, let cross?):
                if Self.snapshotReferenceIsSameOrNewer(
                    lamport: cross.lamport,
                    writerID: cross.info.writerID,
                    runIDPrefix: cross.info.runIDPrefix,
                    thanLamport: per.lamport,
                    writerID: per.info.writerID,
                    runIDPrefix: per.info.runIDPrefix
                ) && !(cross.lamport == per.lamport && cross.info.writerID == per.info.writerID && cross.info.runIDPrefix == per.info.runIDPrefix) {
                    // strictly newer cross-repo wins; equal-keys is a degenerate tie, keep per-month
                    mergedBaselinesByMonth[month] = cross
                    crossRepoWinningMonths.insert(month)
                    acceptedLamportByMonth[month] = cross.lamport
                    emptyBaselineMonths.remove(month)
                } else {
                    mergedBaselinesByMonth[month] = per
                }
            case (nil, nil):
                break
            }
        }

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

        var baselineCoveredByMonth = mergedBaselinesByMonth.mapValues(\.covered)
        for month in emptyBaselineMonths where baselineCoveredByMonth[month] == nil {
            baselineCoveredByMonth[month] = .empty
        }
        let commitTrust = try await CommitTrustPipeline(reader: commitReader).accept(
            references: commitReferences,
            coveredByMonth: baselineCoveredByMonth,
            expectedRepoID: expectedRepoID
        )
        let state = MaterializerReplayProjector().project(
            baselinesByMonth: mergedBaselinesByMonth,
            emptyBaselineMonths: emptyBaselineMonths,
            acceptedCommits: commitTrust.acceptedCommits,
            acceptedSnapshotLamportByMonth: acceptedLamportByMonth
        )

        var corruptedSnapshotMonths = snapshotTrust.corruptedSnapshotMonths
        // A month whose per-month snapshots all failed but where cross-repo took over
        // is NOT corrupt-without-baseline anymore.
        for month in crossRepoWinningMonths {
            corruptedSnapshotMonths.remove(month)
        }
        for month in filenameRejectedMonths where acceptedLamportByMonth[month] == nil {
            corruptedSnapshotMonths.insert(month)
        }
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
        // Cross-repo index header carries observedSeqByWriter from the time the index was
        // written. Advance our return with any higher values it observed — same advance-only
        // policy as RepoStateAuthority.observeSameWriterSeq.
        if let crossRepoAccepted = crossRepoTrust.acceptedFile {
            for (writer, seq) in crossRepoAccepted.tail.observedSeqByWriter where seq < ceiling {
                let prior = observedSeqByWriter[writer] ?? 0
                if seq > prior {
                    observedSeqByWriter[writer] = seq
                }
            }
        }

        if !corruptedSnapshotMonths.isEmpty {
            materializerLog.warning("materialize: \(corruptedSnapshotMonths.count, privacy: .public) month(s) had all snapshots corrupt; commit replay rebuilt state — caller should force a fresh baseline on next flush")
        }

        var acceptedSnapshotBaselinesByMonth: [LibraryMonthKey: AcceptedSnapshotBaselineInfo] = [:]
        for (month, baseline) in snapshotTrust.acceptedBaselinesByMonth where !crossRepoWinningMonths.contains(month) {
            acceptedSnapshotBaselinesByMonth[month] = baseline.info
        }
        var acceptedCrossRepoIndexBaselineByMonth: [LibraryMonthKey: AcceptedCrossRepoIndexBaselineInfo] = [:]
        if let crossRepoAccepted = crossRepoTrust.acceptedFile {
            for month in crossRepoWinningMonths {
                guard let covered = crossRepoAccepted.header.coveredByMonth[month] else { continue }
                acceptedCrossRepoIndexBaselineByMonth[month] = AcceptedCrossRepoIndexBaselineInfo(
                    filename: crossRepoTrust.acceptedFilename ?? "",
                    lamport: crossRepoAccepted.header.lamport,
                    writerID: crossRepoAccepted.header.writerID,
                    runIDPrefix: crossRepoAccepted.header.runIDPrefix,
                    coveredForMonth: covered
                )
            }
        }

        return MaterializeOutput(
            state: state,
            observedSeqByWriter: observedSeqByWriter,
            coveredByMonth: commitTrust.coveredByMonth,
            acceptedSnapshotBaselinesByMonth: acceptedSnapshotBaselinesByMonth,
            acceptedCrossRepoIndexBaselineByMonth: acceptedCrossRepoIndexBaselineByMonth,
            corruptedSnapshotMonths: corruptedSnapshotMonths,
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
    let acceptedSnapshotLamportByMonth: [LibraryMonthKey: UInt64]
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

        return try await withThrowingTaskGroup(of: SnapshotTaskResult.self) { group in
            for (month, candidates) in snapshotsByMonth {
                let reader = self.reader
                let expected = expectedRepoID
                group.addTask {
                    for candidate in candidates {
                        do {
                            let file = try await reader.read(filename: candidate.filename)
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
                                return SnapshotTaskResult(month: month, baseline: baseline)
                            }
                        } catch let error as RepoJSONLReadError {
                            switch error {
                            case .integrityMismatch, .missingHeader, .missingEnd, .decodeFailure:
                                materializerLog.warning("skip corrupt snapshot \(candidate.filename, privacy: .public): \(String(describing: error), privacy: .public)")
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
                    return SnapshotTaskResult(month: month, baseline: nil)
                }
            }

            var acceptedBaselinesByMonth: [LibraryMonthKey: AcceptedSnapshotBaseline] = [:]
            var emptyBaselineMonths: Set<LibraryMonthKey> = []
            var corruptedSnapshotMonths: Set<LibraryMonthKey> = []
            var acceptedSnapshotLamportByMonth: [LibraryMonthKey: UInt64] = [:]
            for try await result in group {
                guard let baseline = result.baseline else {
                    if !(snapshotsByMonth[result.month]?.isEmpty ?? true) {
                        corruptedSnapshotMonths.insert(result.month)
                    }
                    emptyBaselineMonths.insert(result.month)
                    continue
                }
                acceptedBaselinesByMonth[result.month] = baseline
                acceptedSnapshotLamportByMonth[result.month] = baseline.lamport
            }
            return SnapshotTrustResult(
                acceptedBaselinesByMonth: acceptedBaselinesByMonth,
                emptyBaselineMonths: emptyBaselineMonths,
                corruptedSnapshotMonths: corruptedSnapshotMonths,
                acceptedSnapshotLamportByMonth: acceptedSnapshotLamportByMonth
            )
        }
    }

    private struct SnapshotTaskResult: Sendable {
        let month: LibraryMonthKey
        let baseline: AcceptedSnapshotBaseline?
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
        for resource in file.resources {
            guard materializerResourcePath(resource.physicalRemotePath, belongsTo: month) else {
                materializerLog.warning("reject snapshot with out-of-month resource month=\(month.text, privacy: .public) path=\(resource.physicalRemotePath, privacy: .public)")
                return nil
            }
            state.resources[resource.physicalRemotePath] = resource
        }
        for ar in file.assetResources {
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
                        for op in file.ops {
                            guard op.clock < LamportClock.maxAdoptableValue else {
                                materializerLog.warning("reject commit with unworkable op clock=\(op.clock, privacy: .public) writerID=\(file.header.writerID, privacy: .public) seq=\(file.header.seq, privacy: .public)")
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
    func project(
        baselinesByMonth: [LibraryMonthKey: AcceptedSnapshotBaseline],
        emptyBaselineMonths: Set<LibraryMonthKey>,
        acceptedCommits: [AcceptedCommit],
        acceptedSnapshotLamportByMonth: [LibraryMonthKey: UInt64]
    ) -> RepoSnapshotState {
        var monthStates = baselinesByMonth.mapValues(\.state)
        for month in emptyBaselineMonths where monthStates[month] == nil {
            monthStates[month] = .empty
        }
        var baselineStampsByMonth: [LibraryMonthKey: [AssetFingerprint: OpStamp]] = [:]
        for (month, baseline) in baselinesByMonth where !baseline.baselineStamps.isEmpty {
            baselineStampsByMonth[month] = baseline.baselineStamps
        }

        var observedClock: UInt64 = acceptedSnapshotLamportByMonth.values.max() ?? 0
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
                    let keepExistingResource = state.resources[resource.physicalRemotePath]
                        .map { opStampPrecedes(incoming, $0.stamp) } ?? false
                    if !keepExistingResource {
                        state.resources[resource.physicalRemotePath] = SnapshotResourceRow(
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

        return RepoSnapshotState(months: monthStates, observedClock: observedClock)
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

// MARK: - Cross-repo index trust pipeline

private struct MaterializerCrossRepoIndexReference: Sendable {
    let filename: String
    let lamport: UInt64
    let writerID: String
    let runIDPrefix: String
}

private struct CrossRepoIndexTrustResult: Sendable {
    let acceptedFile: RepoCrossRepoIndexFile?
    let acceptedFilename: String?
    let acceptedBaselinesByMonth: [LibraryMonthKey: AcceptedSnapshotBaseline]
}

private struct CrossRepoIndexTrustPipeline {
    let reader: RepoCrossRepoIndexReader

    func accept(
        references: [MaterializerCrossRepoIndexReference],
        expectedRepoID: String,
        filterMonth: LibraryMonthKey?
    ) async throws -> CrossRepoIndexTrustResult {
        // Lex-desc: same tiebreak as SnapshotTrustPipeline at the per-month level,
        // applied here to one whole-repo file.
        let sorted = references.sorted { lhs, rhs in
            if lhs.lamport != rhs.lamport { return lhs.lamport > rhs.lamport }
            if lhs.writerID != rhs.writerID { return lhs.writerID > rhs.writerID }
            return lhs.runIDPrefix > rhs.runIDPrefix
        }
        for candidate in sorted {
            do {
                let file = try await reader.read(filename: candidate.filename)
                guard Self.fileMatchesReference(file, reference: candidate) else {
                    materializerLog.warning("skip cross-repo index whose filename disagrees with header: \(candidate.filename, privacy: .public)")
                    continue
                }
                guard file.header.repoID == expectedRepoID else {
                    materializerLog.warning("skip foreign-repo cross-repo index \(candidate.filename, privacy: .public) header=\(file.header.repoID, privacy: .public) expected=\(expectedRepoID, privacy: .public)")
                    continue
                }
                guard file.header.schemaVersion == RepoCrossRepoIndexSchema.currentVersion else {
                    materializerLog.warning("skip cross-repo index with unsupported schemaVersion \(file.header.schemaVersion, privacy: .public): \(candidate.filename, privacy: .public)")
                    continue
                }
                if crossRepoIndexHasUnworkableRowStamp(file) {
                    materializerLog.warning("skip cross-repo index with poisoned row stamp or per-section month mismatch: \(candidate.filename, privacy: .public)")
                    continue
                }
                let baselines = Self.buildBaselines(from: file, filterMonth: filterMonth, candidate: candidate)
                return CrossRepoIndexTrustResult(
                    acceptedFile: file,
                    acceptedFilename: candidate.filename,
                    acceptedBaselinesByMonth: baselines
                )
            } catch is CancellationError {
                throw CancellationError()
            } catch let error as RepoJSONLReadError {
                switch error {
                case .integrityMismatch, .missingHeader, .missingEnd, .decodeFailure:
                    materializerLog.warning("skip corrupt cross-repo index \(candidate.filename, privacy: .public): \(String(describing: error), privacy: .public)")
                    continue
                case .notFound:
                    // Cross-repo index files are best-effort; a vanished candidate just falls
                    // through to the next lex-lower one. Unlike per-month snapshots, no retry
                    // race is needed because we don't claim invariant coverage from the index.
                    materializerLog.info("cross-repo index vanished mid-read; trying next candidate: \(candidate.filename, privacy: .public)")
                    continue
                }
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                // Non-cancellation download / local-temp-file errors during the OPTIONAL fast-path
                // read must not abort materialization. Treat this candidate as unavailable and try
                // the next lex-lower one; if all candidates fail, materializeOnce falls back to
                // per-month snapshots + commit replay exactly as today.
                materializerLog.warning("cross-repo index read failed (transport or local IO); skipping candidate: \(candidate.filename, privacy: .public): \(String(describing: error), privacy: .public)")
                continue
            }
        }
        return CrossRepoIndexTrustResult(acceptedFile: nil, acceptedFilename: nil, acceptedBaselinesByMonth: [:])
    }

    private static func fileMatchesReference(_ file: RepoCrossRepoIndexFile, reference: MaterializerCrossRepoIndexReference) -> Bool {
        file.header.lamport == reference.lamport
            && file.header.writerID == reference.writerID
            && file.header.runIDPrefix == reference.runIDPrefix
    }

    private static func buildBaselines(
        from file: RepoCrossRepoIndexFile,
        filterMonth: LibraryMonthKey?,
        candidate: MaterializerCrossRepoIndexReference
    ) -> [LibraryMonthKey: AcceptedSnapshotBaseline] {
        var sectionsByMonth: [LibraryMonthKey: RepoCrossRepoIndexMonthSection] = [:]
        for section in file.monthSections {
            sectionsByMonth[section.month] = section
        }

        var result: [LibraryMonthKey: AcceptedSnapshotBaseline] = [:]
        for (month, covered) in file.header.coveredByMonth {
            if let filterMonth, month != filterMonth { continue }
            let section = sectionsByMonth[month]
            var monthState = RepoMonthState.empty
            var baselineStamps: [AssetFingerprint: OpStamp] = [:]
            if let section {
                for asset in section.assets {
                    monthState.assets[asset.assetFingerprint] = asset
                    baselineStamps[asset.assetFingerprint] = asset.stamp
                }
                for resource in section.resources {
                    monthState.resources[resource.physicalRemotePath] = resource
                }
                for ar in section.assetResources {
                    let key = AssetResourceKey(assetFingerprint: ar.assetFingerprint, role: ar.role, slot: ar.slot)
                    monthState.assetResources[key] = ar
                }
                for d in section.deletedKeys {
                    guard d.keyType == .asset else { continue }
                    let fp: AssetFingerprint
                    do {
                        fp = try RepoWireValidator.validateAssetFingerprint(d.keyValue, field: "keyValue")
                    } catch {
                        continue
                    }
                    monthState.deletedAssetStamps[fp] = d.stamp
                }
            }
            let info = RepoMaterializer.AcceptedSnapshotBaselineInfo(
                filename: candidate.filename,
                month: month,
                lamport: candidate.lamport,
                writerID: candidate.writerID,
                runIDPrefix: candidate.runIDPrefix,
                covered: covered
            )
            result[month] = AcceptedSnapshotBaseline(
                state: monthState,
                covered: covered,
                baselineStamps: baselineStamps,
                info: info,
                lamport: candidate.lamport
            )
        }
        return result
    }
}

private func crossRepoIndexHasUnworkableRowStamp(_ file: RepoCrossRepoIndexFile) -> Bool {
    let filenameLamport = file.header.lamport
    for section in file.monthSections {
        guard let covered = file.header.coveredByMonth[section.month] else {
            return true
        }
        for asset in section.assets {
            let stamp = asset.stamp
            if isUnworkableStampClock(stamp.clock, filenameLamport: filenameLamport) { return true }
            if !covered.contains(writerID: stamp.writerID, seq: stamp.seq) { return true }
        }
        for resource in section.resources {
            let stamp = resource.stamp
            if isUnworkableStampClock(stamp.clock, filenameLamport: filenameLamport) { return true }
            if !covered.contains(writerID: stamp.writerID, seq: stamp.seq) { return true }
            if !materializerResourcePath(resource.physicalRemotePath, belongsTo: section.month) {
                return true
            }
        }
        for d in section.deletedKeys {
            let stamp = d.stamp
            if isUnworkableStampClock(stamp.clock, filenameLamport: filenameLamport) { return true }
            if !covered.contains(writerID: stamp.writerID, seq: stamp.seq) { return true }
            // Mirror SnapshotTrustPipeline.makeBaseline strictness: only `.asset` keyType is
            // a valid baseline entry, and the keyValue must parse as a 32-byte hash. Silently
            // skipping these would make the cross-repo trust contract weaker than per-month
            // snapshot trust — dropping legitimate tombstone state while still claiming the
            // commit range is covered.
            guard d.keyType == .asset else { return true }
            do {
                _ = try RepoWireValidator.validateHash(d.keyValue, field: "keyValue")
            } catch {
                return true
            }
        }
    }
    return false
}
