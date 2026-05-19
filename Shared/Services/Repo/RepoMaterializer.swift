import Foundation
import os.log

private let materializerLog = Logger(subsystem: "com.zizicici.watermelon", category: "RepoMaterializer")

actor RepoMaterializer {
    private let snapshotReader: SnapshotReader
    private let commitReader: CommitLogReader

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        // Internal TaskGroups fan out N concurrent read ops; `.serialOnly` backends need serialization.
        let effective = wrapIfSerial(client)
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

    struct MaterializeOutput: Sendable {
        let state: RepoSnapshotState
        let observedSeqByWriter: [String: UInt64]
        /// Final fold coverage after accepted snapshot baseline plus replayed commits.
        let coveredByMonth: [LibraryMonthKey: CoveredRanges]
        let acceptedSnapshotBaselinesByMonth: [LibraryMonthKey: AcceptedSnapshotBaselineInfo]
        /// Months where every snapshot candidate was corrupt — commit replay rebuilt state
        /// from empty. Caller can flip these months' next flush to emit a fresh baseline
        /// even when `dirty == false`, preventing O(commit log) replay every materialize.
        let corruptedSnapshotMonths: Set<LibraryMonthKey>
        let repoID: String?
    }

    enum MetadataReadRaceError: Error, Equatable {
        case requiredCommitVanished(filename: String, month: LibraryMonthKey, writerID: String, seq: UInt64)
        case snapshotVanishedWithoutRecovery(filename: String, month: LibraryMonthKey, lamport: UInt64, writerID: String, runIDPrefix: String)
        case metadataChangedAgainAfterRetry
    }

    func materialize(expectedRepoID: String? = nil) async throws -> MaterializeOutput {
        try await materialize(filterMonth: nil, expectedRepoID: expectedRepoID)
    }

    func materializeMonth(_ month: LibraryMonthKey, expectedRepoID: String? = nil) async throws -> MaterializeOutput {
        try await materialize(filterMonth: month, expectedRepoID: expectedRepoID)
    }

    private func materialize(filterMonth: LibraryMonthKey?, expectedRepoID: String?) async throws -> MaterializeOutput {
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
        switch race {
        case .requiredCommitVanished(let filename, let month, let writerID, let seq):
            let covered = output.coveredByMonth[month] ?? .empty
            guard covered.contains(writerID: writerID, seq: seq),
                  (output.observedSeqByWriter[writerID] ?? 0) >= seq else {
                throw MetadataReadRaceError.requiredCommitVanished(
                    filename: filename,
                    month: month,
                    writerID: writerID,
                    seq: seq
                )
            }
        case .snapshotVanished(let filename, let month, let lamport, let writerID, let runIDPrefix):
            // Unit 5 invariant: unreadable accepted snapshot candidates cannot downgrade.
            guard let baseline = output.acceptedSnapshotBaselinesByMonth[month],
                  Self.snapshotReferenceIsSameOrNewer(
                    lamport: baseline.lamport,
                    writerID: baseline.writerID,
                    runIDPrefix: baseline.runIDPrefix,
                    thanLamport: lamport,
                    writerID: writerID,
                    runIDPrefix: runIDPrefix
                  ) else {
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

    private func materializeOnce(filterMonth: LibraryMonthKey?, expectedRepoID: String?) async throws -> MaterializeOutput {
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
        let state = MaterializerReplayProjector().project(
            baselinesByMonth: snapshotTrust.acceptedBaselinesByMonth,
            emptyBaselineMonths: snapshotTrust.emptyBaselineMonths,
            acceptedCommits: commitTrust.acceptedCommits,
            acceptedSnapshotLamportByMonth: snapshotTrust.acceptedSnapshotLamportByMonth
        )

        var corruptedSnapshotMonths = snapshotTrust.corruptedSnapshotMonths
        for month in filenameRejectedMonths where snapshotTrust.acceptedSnapshotLamportByMonth[month] == nil {
            corruptedSnapshotMonths.insert(month)
        }
        var observedSeqByWriter = commitTrust.observedSeqByWriter
        for (_, covered) in commitTrust.coveredByMonth {
            for (writer, ranges) in covered.rangesByWriter {
                let high = ranges.map(\.high).max() ?? 0
                let prior = observedSeqByWriter[writer] ?? 0
                if high > prior {
                    observedSeqByWriter[writer] = high
                }
            }
        }

        if !corruptedSnapshotMonths.isEmpty {
            materializerLog.warning("materialize: \(corruptedSnapshotMonths.count, privacy: .public) month(s) had all snapshots corrupt; commit replay rebuilt state — caller should force a fresh baseline on next flush")
        }
        return MaterializeOutput(
            state: state,
            observedSeqByWriter: observedSeqByWriter,
            coveredByMonth: commitTrust.coveredByMonth,
            acceptedSnapshotBaselinesByMonth: snapshotTrust.acceptedBaselinesByMonth.mapValues(\.info),
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
    let baselineStamps: [Data: OpStamp]
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
        expectedRepoID: String?
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
                            if let expected {
                                guard !file.header.repoID.isEmpty else {
                                    materializerLog.warning("skip unstamped legacy snapshot \(candidate.filename, privacy: .public) while materializing repo=\(expected, privacy: .public)")
                                    continue
                                }
                                guard file.header.repoID == expected else {
                                    materializerLog.warning("skip foreign-repo snapshot \(candidate.filename, privacy: .public) header=\(file.header.repoID, privacy: .public) expected=\(expected, privacy: .public)")
                                    continue
                                }
                            }
                            if snapshotHasUnworkableRowStamp(file, filenameLamport: candidate.lamport) {
                                materializerLog.warning("skip snapshot with poisoned row stamp: \(candidate.filename, privacy: .public)")
                                continue
                            }
                            return SnapshotTaskResult(month: month, baseline: Self.makeBaseline(file: file, reference: candidate))
                        } catch let error as SnapshotReader.ReadError {
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

    private static func makeBaseline(file: SnapshotFile, reference: MaterializerSnapshotReference) -> AcceptedSnapshotBaseline {
        let month = reference.month
        var state = RepoMonthState.empty
        var baselineStamps: [Data: OpStamp] = [:]
        for asset in file.assets {
            state.assets[asset.assetFingerprint] = asset
            if let stamp = asset.stamp {
                baselineStamps[asset.assetFingerprint] = stamp
            }
        }
        for resource in file.resources {
            guard materializerResourcePath(resource.physicalRemotePath, belongsTo: month) else {
                materializerLog.warning("skip snapshot resource outside month=\(month.text, privacy: .public) path=\(resource.physicalRemotePath, privacy: .public)")
                continue
            }
            state.resources[resource.physicalRemotePath] = resource
        }
        for ar in file.assetResources {
            let key = AssetResourceKey(assetFingerprint: ar.assetFingerprint, role: ar.role, slot: ar.slot)
            state.assetResources[key] = ar
        }
        for d in file.deletedKeys {
            guard d.keyType == .asset else {
                materializerLog.warning("skip unsupported deletedKey.keyType=\(String(describing: d.keyType), privacy: .public) for \(month.text, privacy: .public)")
                continue
            }
            let fp: Data
            do {
                fp = try RepoWireValidator.validateHash(d.keyValue, field: "keyValue")
            } catch {
                materializerLog.warning("skip malformed deletedKey hash for \(month.text, privacy: .public): \(String(describing: error), privacy: .public)")
                continue
            }
            state.deletedAssetFingerprints.insert(fp)
            if let stamp = d.stamp {
                state.deletedAssetStamps[fp] = stamp
            }
        }
        if !state.assets.isEmpty && baselineStamps.count < state.assets.count {
            let stampless = state.assets.count - baselineStamps.count
            materializerLog.info(
                "baseline stamp coverage partial month=\(month.text, privacy: .public) total=\(state.assets.count, privacy: .public) stampless=\(stampless, privacy: .public) — LWW gate degrades for those assets"
            )
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
        expectedRepoID: String?
    ) async throws -> CommitTrustResult {
        var observedSeqByWriter: [String: UInt64] = [:]
        var commitsToRead: [MaterializerCommitReference] = []
        for reference in references {
            let prior = observedSeqByWriter[reference.writerID] ?? 0
            if reference.seq > prior {
                observedSeqByWriter[reference.writerID] = reference.seq
            }
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
        expectedRepoID: String?
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
                        if let expected, file.header.repoID != expected {
                            materializerLog.warning("skip commit with mismatched repoID file=\(String(describing: reference.month), privacy: .public) header=\(file.header.repoID, privacy: .public) expected=\(expected, privacy: .public)")
                            return nil
                        }
                        var acceptedOps: [CommitOp] = []
                        for op in file.ops {
                            guard op.clock < LamportClock.maxAdoptableValue else {
                                materializerLog.warning("skip op with unworkable clock=\(op.clock, privacy: .public) writerID=\(file.header.writerID, privacy: .public) seq=\(file.header.seq, privacy: .public)")
                                continue
                            }
                            acceptedOps.append(op)
                        }
                        return AcceptedCommit(month: reference.month, header: file.header, ops: acceptedOps)
                    } catch let error as CommitLogReader.ReadError {
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
        var baselineStampsByMonth: [LibraryMonthKey: [Data: OpStamp]] = [:]
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

        var lastAddByMonthFP: [LibraryMonthKey: [Data: OpStamp]] = baselineStampsByMonth
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
                state.deletedAssetFingerprints.remove(body.assetFingerprint)
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
                    let keepExistingResource = state.resources[resource.physicalRemotePath]?.stamp
                        .map { opStampPrecedes(incoming, $0) } ?? false
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
                } else if let basis = body.observedBasis,
                          let lastAdd = lastAddByMonthFP[sorted.month]?[body.assetFingerprint],
                          materializerIsAfterBasis(lastAdd, basis: basis) {
                    materializerLog.info("skip observation-tombstone for fp; healing add observed after basis")
                } else {
                    state.assets.removeValue(forKey: body.assetFingerprint)
                    state.assetResources = state.assetResources.filter { $0.key.assetFingerprint != body.assetFingerprint }
                    state.deletedAssetFingerprints.insert(body.assetFingerprint)
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
    for asset in file.assets {
        if let stamp = asset.stamp, isUnworkableStampClock(stamp.clock, filenameLamport: filenameLamport) {
            return true
        }
    }
    for resource in file.resources {
        if let stamp = resource.stamp, isUnworkableStampClock(stamp.clock, filenameLamport: filenameLamport) {
            return true
        }
    }
    for d in file.deletedKeys {
        if let stamp = d.stamp, isUnworkableStampClock(stamp.clock, filenameLamport: filenameLamport) {
            return true
        }
    }
    return false
}

private func isUnworkableStampClock(_ clock: UInt64, filenameLamport: UInt64) -> Bool {
    if clock >= LamportClock.maxAdoptableValue { return true }
    if clock > filenameLamport { return true }
    return false
}
