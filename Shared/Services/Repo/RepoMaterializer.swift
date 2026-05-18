import Foundation
import os.log

private let materializerLog = Logger(subsystem: "com.zizicici.watermelon", category: "RepoMaterializer")

actor RepoMaterializer {
    private let client: any RemoteStorageClientProtocol
    private let basePath: String
    private let snapshotReader: SnapshotReader
    private let commitReader: CommitLogReader

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        // Internal TaskGroups fan out N concurrent read ops; on `.serialOnly` SMB / SFTP
        // that violates the connection's serial contract. wrapIfSerial is idempotent
        // via `isSerialized` so caller's wrapper isn't double-serialized.
        let effective = wrapIfSerial(client)
        self.client = effective
        self.basePath = basePath
        self.snapshotReader = SnapshotReader(client: effective, basePath: basePath)
        self.commitReader = CommitLogReader(client: effective, basePath: basePath)
    }

    struct MaterializeOutput: Sendable {
        let state: RepoSnapshotState
        let observedSeqByWriter: [String: UInt64]
        /// Ops baked into `state.months[month]`. Skipped (corrupt) seqs excluded so the
        /// next snapshot writer doesn't blanket-cover commits whose effects aren't in state.
        let coveredByMonth: [LibraryMonthKey: CoveredRanges]
        /// Months where every snapshot candidate was corrupt — commit replay rebuilt state
        /// from empty. Caller can flip these months' next flush to emit a fresh baseline
        /// even when `dirty == false`, preventing O(commit log) replay every materialize.
        let corruptedSnapshotMonths: Set<LibraryMonthKey>
    }

    func materialize(expectedRepoID: String? = nil) async throws -> MaterializeOutput {
        try await materialize(filterMonth: nil, expectedRepoID: expectedRepoID)
    }

    func materializeMonth(_ month: LibraryMonthKey, expectedRepoID: String? = nil) async throws -> MaterializeOutput {
        try await materialize(filterMonth: month, expectedRepoID: expectedRepoID)
    }

    private func materialize(filterMonth: LibraryMonthKey?, expectedRepoID: String?) async throws -> MaterializeOutput {
        async let snapshotFilenames = snapshotReader.listSnapshotFilenames()
        async let commitFilenames = commitReader.listCommitFilenames()
        let snapshots = try await snapshotFilenames
        let commits = try await commitFilenames

        // Lamport desc + (writerID, runIDPrefix) tiebreak: stable order across sessions,
        // and corrupt top falls back to next-newest.
        struct SnapshotCandidate {
            let filename: String
            let lamport: UInt64
            let writerID: String
            let runIDPrefix: String
        }
        var snapshotsByMonth: [LibraryMonthKey: [SnapshotCandidate]] = [:]
        // Months where ANY snapshot filename was rejected at the outer
        // unworkable-lamport guard. If no other candidate gets accepted
        // for these months, mark them for rebaseline so the next flush
        // writes a fresh safe-lamport baseline rather than letting the
        // unworkable file linger forever and force a full commit-log
        // replay every session.
        var filenameRejectedMonths: Set<LibraryMonthKey> = []
        for filename in snapshots {
            guard let parsed = RepoLayout.parseSnapshotFilename(filename) else { continue }
            if let filterMonth, parsed.month != filterMonth { continue }
            guard parsed.lamport < LamportClock.maxAdoptableValue else {
                // A peer/corrupt snapshot whose filename lamport is at/above the
                // safe-adoption ceiling cannot come from a legitimate writer
                // (clock would have run out of headroom decades before reaching
                // it) and would either dead-end our clock via observe or win
                // baseline selection by pretending to be newest. `<
                // maxAdoptableValue` keeps headroom for at least one tick
                // after observe — without it a peer's snapshot at
                // `maxObservableValue - 1` was accepted, observed, and the
                // next `tickRange(1)` threw `advanceExhausted` with no path
                // to recover short of manually removing the file. The month
                // is tracked below so a fresh baseline gets emitted on the
                // next flush, otherwise replay would keep re-skipping the
                // same file every session.
                materializerLog.warning("skip snapshot with unworkable lamport in filename: \(filename, privacy: .public)")
                filenameRejectedMonths.insert(parsed.month)
                continue
            }
            snapshotsByMonth[parsed.month, default: []].append(SnapshotCandidate(
                filename: filename,
                lamport: parsed.lamport,
                writerID: parsed.writerID,
                runIDPrefix: parsed.runIDPrefix
            ))
        }
        for month in snapshotsByMonth.keys {
            snapshotsByMonth[month]?.sort { lhs, rhs in
                if lhs.lamport != rhs.lamport { return lhs.lamport > rhs.lamport }
                if lhs.writerID != rhs.writerID { return lhs.writerID > rhs.writerID }
                return lhs.runIDPrefix > rhs.runIDPrefix
            }
        }

        var monthStates: [LibraryMonthKey: RepoMonthState] = [:]
        var coveredByMonth: [LibraryMonthKey: CoveredRanges] = [:]
        var baselineStampsByMonth: [LibraryMonthKey: [Data: OpStamp]] = [:]
        var observedSeqByWriter: [String: UInt64] = [:]
        var observedClock: UInt64 = 0
        var corruptedSnapshotMonths: Set<LibraryMonthKey> = []
        // Only the lamports of snapshots that survived quarantine contribute to
        // observedClock. A corrupt / foreign / row-stamp-rejected candidate at
        // a safe-looking filename lamport must not advance the clock — its file
        // isn't part of the materialised state, so observing it would persist
        // headroom we haven't actually earned and could exhaust the next tick.
        var acceptedSnapshotLamportByMonth: [LibraryMonthKey: UInt64] = [:]

        try await withThrowingTaskGroup(of: (LibraryMonthKey, SnapshotFile?, UInt64?).self) { group in
            for (month, candidates) in snapshotsByMonth {
                let reader = self.snapshotReader
                let candidatesForTask = candidates
                let expected = expectedRepoID
                group.addTask {
                    // Corrupt / foreign-repo / misnamed snapshots all fall through to the
                    // next candidate; corrupt baselines can be rebuilt via replay, foreign
                    // would pollute state.
                    for candidate in candidatesForTask {
                        do {
                            let file = try await reader.read(filename: candidate.filename)
                            if let parsed = RepoLayout.parseSnapshotFilename(candidate.filename),
                               (parsed.writerID != file.header.writerID ||
                                CommitHeader.parseMonthScope(file.header.scope) != parsed.month) {
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
                            // Row-stamp quarantine: a syntactically valid snapshot with a
                            // safe-looking filename lamport can still smuggle a poisoned
                            // OpStamp.clock into baseline state via asset/resource/
                            // deletedKey rows. Once baked into `baselineStamps` /
                            // `state.deletedAssetStamps`, an above-ceiling stamp would
                            // dominate every legitimate replay through `opStampPrecedes`,
                            // suppressing valid add/tombstone ops. A stamp.clock above
                            // the candidate's own filename lamport is the same class of
                            // violation: legit writers tick the filename at-or-after
                            // every covered op's clock.
                            if Self.snapshotHasUnworkableRowStamp(file, filenameLamport: candidate.lamport) {
                                materializerLog.warning("skip snapshot with poisoned row stamp: \(candidate.filename, privacy: .public)")
                                continue
                            }
                            return (month, file, candidate.lamport)
                        } catch let error as SnapshotReader.ReadError {
                            switch error {
                            case .integrityMismatch, .missingHeader, .missingEnd, .decodeFailure:
                                materializerLog.warning("skip corrupt snapshot \(candidate.filename, privacy: .public): \(String(describing: error), privacy: .public)")
                                continue
                            }
                        }
                    }
                    return (month, nil, nil)
                }
            }
            for try await (month, fileOrNil, acceptedLamport) in group {
                guard let file = fileOrNil else {
                    // No usable snapshot — start from empty; commit replay rebuilds state.
                    // Had candidates → all corrupt; flag so caller can force a fresh baseline
                    // on the next flush instead of replaying the full commit log each session.
                    if !(snapshotsByMonth[month]?.isEmpty ?? true) {
                        corruptedSnapshotMonths.insert(month)
                    }
                    monthStates[month] = .empty
                    coveredByMonth[month] = .empty
                    continue
                }
                if let acceptedLamport {
                    acceptedSnapshotLamportByMonth[month] = acceptedLamport
                }
                var state = RepoMonthState.empty
                var baselineStamps: [Data: OpStamp] = [:]
                for asset in file.assets {
                    state.assets[asset.assetFingerprint] = asset
                    if let stamp = asset.stamp {
                        baselineStamps[asset.assetFingerprint] = stamp
                    }
                }
                for resource in file.resources {
                    guard Self.resourcePath(resource.physicalRemotePath, belongsTo: month) else {
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
                    // Non-asset keyType is wire-supported but no writer emits it today.
                    // Skip rather than abort so future V3 readers tolerate forward fields.
                    guard d.keyType == .asset else {
                        materializerLog.warning("skip unsupported deletedKey.keyType=\(String(describing: d.keyType), privacy: .public) for \(month.text, privacy: .public)")
                        continue
                    }
                    // A single malformed keyValue (corruption, hostile peer) used to abort
                    // the whole materialize — backup would be permanently stuck. Soft-skip
                    // the row and log; if the tombstone is legitimate, a future snapshot
                    // re-emits it.
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
                monthStates[month] = state
                coveredByMonth[month] = file.header.covered
                if !baselineStamps.isEmpty {
                    baselineStampsByMonth[month] = baselineStamps
                }
                if !state.assets.isEmpty && baselineStamps.count < state.assets.count {
                    let stampless = state.assets.count - baselineStamps.count
                    materializerLog.info(
                        "baseline stamp coverage partial month=\(month.text, privacy: .public) total=\(state.assets.count, privacy: .public) stampless=\(stampless, privacy: .public) — LWW gate degrades for those assets"
                    )
                }
            }
        }
        // Filename-only-rejected months never enter the task group (their
        // candidate list is empty), so the `fileOrNil == nil` branch above
        // wouldn't see them. Flag them here so the next flush emits a fresh
        // safe-lamport baseline — otherwise the unworkable filename lingers
        // and forces a full uncovered commit-log replay every materialize.
        for month in filenameRejectedMonths where acceptedSnapshotLamportByMonth[month] == nil {
            corruptedSnapshotMonths.insert(month)
        }

        var commitsToReplay: [(month: LibraryMonthKey, parsed: RepoLayout.ParsedCommitFilename, filename: String)] = []
        for filename in commits {
            guard let parsed = RepoLayout.parseCommitFilename(filename) else { continue }
            if let filterMonth, parsed.month != filterMonth { continue }
            // Advance observedSeq from filename even when read fails — a corrupt commit
            // still occupies its seq slot; allocator must skip it on next write.
            let prior = observedSeqByWriter[parsed.writerID] ?? 0
            if parsed.seq > prior {
                observedSeqByWriter[parsed.writerID] = parsed.seq
            }
            let covered = coveredByMonth[parsed.month] ?? CoveredRanges.empty
            if !covered.contains(writerID: parsed.writerID, seq: parsed.seq) {
                commitsToReplay.append((parsed.month, parsed, filename))
            }
        }

        // Skip corrupt commits with a warning; transport failures still abort.
        let parsedCommits: [(month: LibraryMonthKey, file: CommitFile)] = try await withThrowingTaskGroup(of: (LibraryMonthKey, CommitFile)?.self) { group in
            for entry in commitsToReplay {
                let filename = entry.filename
                let month = entry.month
                let parsed = entry.parsed
                let reader = self.commitReader
                group.addTask {
                    do {
                        let file = try await reader.read(filename: filename)
                        // Filename-vs-header check: a misnamed file replayed under the
                        // wrong (month, writer, seq) breaks covered-range bookkeeping.
                        let scopeMonth = CommitHeader.parseMonthScope(file.header.scope)
                        if file.header.writerID != parsed.writerID
                            || file.header.seq != parsed.seq
                            || scopeMonth != parsed.month {
                            materializerLog.warning("skip commit whose filename disagrees with header: \(filename, privacy: .public)")
                            return nil
                        }
                        return (month, file)
                    } catch let error as CommitLogReader.ReadError {
                        switch error {
                        case .integrityMismatch, .missingHeader, .missingEnd, .decodeFailure:
                            materializerLog.warning("skip corrupt commit \(filename, privacy: .public): \(String(describing: error), privacy: .public)")
                            return nil
                        }
                    }
                }
            }
            var result: [(LibraryMonthKey, CommitFile)] = []
            for try await item in group {
                if let item { result.append(item) }
            }
            return result
        }

        struct SortedOp {
            let month: LibraryMonthKey
            let writerID: String
            let seq: UInt64
            let op: CommitOp
        }
        var sortedOps: [SortedOp] = []
        for entry in parsedCommits {
            let header = entry.file.header
            // Foreign-repoID commits exist when a profile re-points or a wiped-and-rewritten
            // remote leaves stale files; never replay them against the current repo state.
            if let expectedRepoID, header.repoID != expectedRepoID {
                materializerLog.warning("skip commit with mismatched repoID file=\(String(describing: entry.month), privacy: .public) header=\(header.repoID, privacy: .public) expected=\(expectedRepoID, privacy: .public)")
                continue
            }
            for op in entry.file.ops {
                // Quarantine ops with unworkable clock values — a peer/corrupt commit
                // with clock at/near the safe ceiling would otherwise (a) poison our
                // PersistedLamportClock via observe and (b) always win LWW comparisons.
                // Strict `<` keeps one tick of headroom after observe.
                guard op.clock < LamportClock.maxAdoptableValue else {
                    materializerLog.warning("skip op with unworkable clock=\(op.clock, privacy: .public) writerID=\(header.writerID, privacy: .public) seq=\(header.seq, privacy: .public)")
                    continue
                }
                sortedOps.append(SortedOp(month: entry.month, writerID: header.writerID, seq: header.seq, op: op))
                observedClock = max(observedClock, op.clock)
            }
            let prior = observedSeqByWriter[header.writerID] ?? 0
            if header.seq > prior {
                observedSeqByWriter[header.writerID] = header.seq
            }
            var monthCovered = coveredByMonth[entry.month] ?? .empty
            monthCovered.add(writerID: header.writerID, seq: header.seq)
            coveredByMonth[entry.month] = monthCovered
        }
        // observedClock / observedSeq must include snapshot lamports + covered-range
        // highs; commits living entirely under a baseline aren't iterated above.
        // Use the lamports of snapshots that PASSED quarantine — a corrupt /
        // foreign / row-stamp-rejected candidate at a safe-looking filename
        // lamport was excluded from materialised state, so trusting its
        // filename here would persist headroom we haven't earned (a peer
        // planting a `maxObservableValue - 1` filename with a corrupt body
        // would otherwise exhaust the next tickRange).
        for (_, lamport) in acceptedSnapshotLamportByMonth {
            observedClock = max(observedClock, lamport)
        }
        for (_, covered) in coveredByMonth {
            for (writer, ranges) in covered.rangesByWriter {
                let high = ranges.map(\.high).max() ?? 0
                let prior = observedSeqByWriter[writer] ?? 0
                if high > prior {
                    observedSeqByWriter[writer] = high
                }
            }
        }
        sortedOps.sort { lhs, rhs in
            if lhs.op.clock != rhs.op.clock { return lhs.op.clock < rhs.op.clock }
            if lhs.writerID != rhs.writerID { return lhs.writerID < rhs.writerID }
            if lhs.seq != rhs.seq { return lhs.seq < rhs.seq }
            return lhs.op.opSeq < rhs.op.opSeq
        }

        // Most recent addAsset stamp per (month, fp). Seed from snapshot baseline
        // so an observation-tombstone whose basis predates a baked-in heal is
        // skipped, then update as ops replay.
        var lastAddByMonthFP: [LibraryMonthKey: [Data: OpStamp]] = baselineStampsByMonth

        for sorted in sortedOps {
            var state = monthStates[sorted.month] ?? .empty
            switch sorted.op.body {
            case .addAsset(let body):
                guard body.resources.allSatisfy({ Self.resourcePath($0.physicalRemotePath, belongsTo: sorted.month) }) else {
                    materializerLog.warning("skip addAsset with resource outside month=\(sorted.month.text, privacy: .public)")
                    continue
                }
                let incoming = OpStamp(writerID: sorted.writerID, seq: sorted.seq, clock: sorted.op.clock)
                if let baselineStamp = state.assets[body.assetFingerprint]?.stamp,
                   opStampPrecedes(incoming, baselineStamp) {
                    // Stale replay against newer add baseline.
                    continue
                }
                if let deletedStamp = state.deletedAssetStamps[body.assetFingerprint],
                   opStampPrecedes(incoming, deletedStamp) {
                    // Stale replay against newer tombstone — would resurrect a
                    // fp the LWW order said should stay deleted.
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
                    // Path-level LWW: a stale uncovered add (different fp) replaying against a
                    // newer covered/baseline row at the same path would otherwise unconditionally
                    // overwrite, leaving state.resources pointing at bytes that were already
                    // superseded by another writer.
                    if let existing = state.resources[resource.physicalRemotePath]?.stamp,
                       opStampPrecedes(incoming, existing) {
                        // Skip only the resource overwrite. Asset + assetResources rows
                        // still land pointing at the now-absent hash so verify reports
                        // this stale asset as `.fullyMissing` and tombstones it.
                    } else {
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
                // LWW: baseline addAsset is in `covered` and won't be replayed, so resolve here against its stamp.
                if let existingStamp = state.assets[body.assetFingerprint]?.stamp,
                   opStampPrecedes(tombstoneStamp, existingStamp) {
                    materializerLog.info("skip tombstone superseded by newer addAsset stamp in baseline")
                } else if let basis = body.observedBasis,
                   let lastAdd = lastAddByMonthFP[sorted.month]?[body.assetFingerprint],
                   Self.isAfterBasis(lastAdd, basis: basis) {
                    // Heal landed after verify observed — dropping the tombstone preserves a peer-resurrected fp.
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

        if !corruptedSnapshotMonths.isEmpty {
            materializerLog.warning("materialize: \(corruptedSnapshotMonths.count, privacy: .public) month(s) had all snapshots corrupt; commit replay rebuilt state — caller should force a fresh baseline on next flush")
        }
        return MaterializeOutput(
            state: RepoSnapshotState(months: monthStates, observedClock: observedClock),
            observedSeqByWriter: observedSeqByWriter,
            coveredByMonth: coveredByMonth,
            corruptedSnapshotMonths: corruptedSnapshotMonths
        )
    }

    /// True when the op stamp landed AFTER the tombstone's observation basis —
    /// either by global clock advance or by writer-specific seq advance. Either
    /// alone is sufficient: a writer not in the basis map (joined post-observation)
    /// is treated as "we never saw their state", so any op of theirs counts as new.
    private static func isAfterBasis(_ stamp: OpStamp, basis: TombstoneObservationBasis) -> Bool {
        if stamp.clock > basis.lamportWatermark { return true }
        let prevMax = basis.perWriterMaxSeq[stamp.writerID] ?? 0
        return stamp.seq > prevMax
    }

    private static func resourcePath(_ path: String, belongsTo month: LibraryMonthKey) -> Bool {
        let components = RemotePathBuilder.normalizeRelativePath(path)
            .split(separator: "/", omittingEmptySubsequences: false)
        guard components.count == 3, !components[2].isEmpty else { return false }
        let expectedYear = String(format: "%04d", month.year)
        let expectedMonth = String(format: "%02d", month.month)
        return String(components[0]) == expectedYear && String(components[1]) == expectedMonth
    }

    /// True if any asset/resource/deletedKey row stamp would either consume the
    /// last tick of clock headroom (`clock >= ceiling`) or violate the legitimate
    /// snapshot invariant that the filename lamport tracks the maximum covered op
    /// clock (`clock > filenameLamport`). Either way the snapshot must not become
    /// the baseline; replay over a poisoned baseline stamp would silently suppress
    /// later legitimate ops via LWW comparison.
    private static func snapshotHasUnworkableRowStamp(_ file: SnapshotFile, filenameLamport: UInt64) -> Bool {
        for asset in file.assets {
            if let stamp = asset.stamp, Self.isUnworkableStampClock(stamp.clock, filenameLamport: filenameLamport) {
                return true
            }
        }
        for resource in file.resources {
            if let stamp = resource.stamp, Self.isUnworkableStampClock(stamp.clock, filenameLamport: filenameLamport) {
                return true
            }
        }
        for d in file.deletedKeys {
            if let stamp = d.stamp, Self.isUnworkableStampClock(stamp.clock, filenameLamport: filenameLamport) {
                return true
            }
        }
        return false
    }

    private static func isUnworkableStampClock(_ clock: UInt64, filenameLamport: UInt64) -> Bool {
        if clock >= LamportClock.maxAdoptableValue { return true }
        if clock > filenameLamport { return true }
        return false
    }
}
