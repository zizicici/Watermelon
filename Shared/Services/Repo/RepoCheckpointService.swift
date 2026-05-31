import Foundation

protocol RepoCheckpointClock: Sendable {
    func observeForCheckpoint(_ external: UInt64) async throws
    func tickRangeForCheckpoint(count: Int) async throws -> LamportClock.Range
}

extension LamportClock: RepoCheckpointClock {
    func observeForCheckpoint(_ external: UInt64) async throws {
        observe(external)
    }

    func tickRangeForCheckpoint(count: Int) async throws -> LamportClock.Range {
        try tickRange(count: count)
    }
}

extension PersistedLamportClock: RepoCheckpointClock {
    func observeForCheckpoint(_ external: UInt64) async throws {
        try observe(external)
    }

    func tickRangeForCheckpoint(count: Int) async throws -> LamportClock.Range {
        try tickRange(count: count)
    }
}

enum RepoCheckpointMode: Sendable, Equatable {
    case whenRecommended
    case repairCorruptBaseline
    case force
}

struct RepoCheckpointResult: Sendable, Equatable {
    enum Outcome: Sendable, Equatable {
        case skippedEmptyFold
        case skippedBelowThreshold
        case writtenAccepted
    }

    let outcome: Outcome
    let month: LibraryMonthKey
    let snapshotName: String?
    let lamport: UInt64?
    let covered: CoveredRanges
    let beforeReport: RepoCompactionMonthReport?
    let afterReport: RepoCompactionMonthReport?
    let acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo?
}

enum RepoCheckpointError: Error, Equatable {
    case readbackMismatch(snapshotName: String, reason: String?)
    case notAcceptedAfterWrite(snapshotName: String)
    case acceptedCoverageMismatch(snapshotName: String)
    case materializeRegression(snapshotName: String)
}

struct RepoCheckpointService: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String
    let repoID: String
    let writerID: String
    let runID: String
    let clock: any RepoCheckpointClock
    let policy: RepoCompactionPolicy
    let nowMs: @Sendable () -> Int64

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        repoID: String,
        writerID: String,
        runID: String,
        clock: any RepoCheckpointClock,
        policy: RepoCompactionPolicy = .default,
        nowMs: @escaping @Sendable () -> Int64 = {
            Int64(Date().timeIntervalSince1970 * 1000)
        }
    ) {
        self.client = wrapIfSerial(client)
        self.basePath = basePath
        self.repoID = repoID
        self.writerID = writerID
        self.runID = runID
        self.clock = clock
        self.policy = policy
        self.nowMs = nowMs
    }

    func checkpointMonth(
        _ month: LibraryMonthKey,
        mode: RepoCheckpointMode,
        respectTaskCancellation: Bool
    ) async throws -> RepoCheckpointResult {
        let materialized = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(month, expectedRepoID: repoID)

        guard materialized.outcomeByMonth[month] == .clean else {
            return RepoCheckpointResult(
                outcome: .skippedBelowThreshold,
                month: month,
                snapshotName: nil,
                lamport: nil,
                covered: .empty,
                beforeReport: nil,
                afterReport: nil,
                acceptedSnapshot: nil
            )
        }

        let beforeReport = try await monthReport(for: month, materialized: materialized)
        let covered = materialized.coveredByMonth[month, default: .empty]
        let monthState = materialized.state.months[month] ?? .empty
        let hasFold = !covered.isEmpty || !monthState.isEmpty

        guard hasFold else {
            return RepoCheckpointResult(
                outcome: .skippedEmptyFold,
                month: month,
                snapshotName: nil,
                lamport: nil,
                covered: covered,
                beforeReport: beforeReport,
                afterReport: nil,
                acceptedSnapshot: nil
            )
        }

        guard shouldWrite(mode: mode, materialized: materialized, month: month, beforeReport: beforeReport) else {
            return RepoCheckpointResult(
                outcome: .skippedBelowThreshold,
                month: month,
                snapshotName: nil,
                lamport: nil,
                covered: covered,
                beforeReport: beforeReport,
                afterReport: nil,
                acceptedSnapshot: nil
            )
        }

        try await clock.observeForCheckpoint(materialized.state.observedClock)
        let range = try await clock.tickRangeForCheckpoint(count: 1)
        let lamport = range.high
        let header = SnapshotHeader(
            version: SnapshotHeader.checkpointVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerID,
            repoID: repoID,
            covered: covered,
            createdAtMs: nowMs()
        )
        let parts = RepoSnapshotBuilder.build(header: header, state: monthState)
        let writer = SnapshotWriter(client: client, basePath: basePath)
        let expected = try await writer.write(
            header: header,
            assets: parts.assets,
            resources: parts.resources,
            assetResources: parts.assetResources,
            deletedKeys: parts.deletedKeys,
            month: month,
            lamport: lamport,
            runID: runID,
            respectTaskCancellation: respectTaskCancellation
        )
        let snapshotName = RepoLayout.snapshotFileName(month: month, lamport: lamport, writerID: writerID, runID: runID)
        try await verifyReadback(expected: expected, snapshotName: snapshotName)

        let (after, accepted) = try await materializeUntilSnapshotAccepted(
            month: month,
            snapshotName: snapshotName,
            expectedCovered: covered,
            before: materialized
        )
        let afterReport = try await monthReport(for: month, materialized: after)
        return RepoCheckpointResult(
            outcome: .writtenAccepted,
            month: month,
            snapshotName: snapshotName,
            lamport: lamport,
            covered: covered,
            beforeReport: beforeReport,
            afterReport: afterReport,
            acceptedSnapshot: accepted
        )
    }

    private func shouldWrite(
        mode: RepoCheckpointMode,
        materialized: RepoMaterializer.MaterializeOutput,
        month: LibraryMonthKey,
        beforeReport: RepoCompactionMonthReport?
    ) -> Bool {
        let covered = materialized.coveredByMonth[month, default: .empty]
        let monthState = materialized.state.months[month] ?? .empty
        let hasFold = !covered.isEmpty || !monthState.isEmpty
        switch mode {
        case .force:
            return hasFold
        case .whenRecommended:
            return beforeReport?.checkpointRecommended == true
        case .repairCorruptBaseline:
            return materialized.corruptedSnapshotMonths.contains(month) && hasFold
        }
    }

    private func monthReport(
        for month: LibraryMonthKey,
        materialized: RepoMaterializer.MaterializeOutput
    ) async throws -> RepoCompactionMonthReport? {
        let report = try await RepoCompactionPlanner(client: client, basePath: basePath, policy: policy)
            .makeReport(expectedRepoID: repoID, preMaterialized: materialized)
        return report.months.first { $0.month == month }
    }

    private func verifyReadback(expected: SnapshotFile, snapshotName: String) async throws {
        let reader = SnapshotReader(client: client, basePath: basePath)
        let deadline = client.metadataReadAfterWriteDeadline(floorSeconds: 1)
        var attempt = 0
        var lastReason: String?
        while true {
            do {
                let actual = try await reader.read(filename: snapshotName)
                if actual == expected { return }
                lastReason = "snapshot bytes parsed but did not match expected rows"
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                lastReason = String(describing: error)
            }
            guard Date() < deadline else {
                throw RepoCheckpointError.readbackMismatch(snapshotName: snapshotName, reason: lastReason)
            }
            let millis = 200 * (1 << min(attempt, 3))
            attempt += 1
            try await Task.sleep(nanoseconds: UInt64(millis) * 1_000_000)
        }
    }

    // The just-written snapshot is readable by name (verifyReadback proved it) but the
    // snapshots-directory LIST that materialize relies on can still be stale inside the
    // backend read-after-write grace window. Retry the accept check until the deadline so
    // a recoverable visibility lag isn't reported as a checkpoint rejection. The coverage
    // and regression guards only fire once the snapshot IS accepted — those are logical, not
    // visibility, failures.
    private func materializeUntilSnapshotAccepted(
        month: LibraryMonthKey,
        snapshotName: String,
        expectedCovered: CoveredRanges,
        before: RepoMaterializer.MaterializeOutput
    ) async throws -> (after: RepoMaterializer.MaterializeOutput, accepted: RepoMaterializer.AcceptedSnapshotBaselineInfo) {
        let deadline = client.metadataReadAfterWriteDeadline(floorSeconds: 1)
        var attempt = 0
        while true {
            let after = try await RepoMaterializer(client: client, basePath: basePath)
                .materializeMonth(month, expectedRepoID: repoID)
            if after.outcomeByMonth[month] == .clean,
               let accepted = after.acceptedSnapshotBaselinesByMonth[month],
               accepted.covered.superset(of: expectedCovered) {
                guard isRetentionEquivalent(before: before, after: after, month: month) else {
                    throw RepoCheckpointError.materializeRegression(snapshotName: snapshotName)
                }
                return (after, accepted)
            }
            guard Date() < deadline else {
                throw RepoCheckpointError.notAcceptedAfterWrite(snapshotName: snapshotName)
            }
            let millis = 200 * (1 << min(attempt, 3))
            attempt += 1
            try await Task.sleep(nanoseconds: UInt64(millis) * 1_000_000)
        }
    }

    private func isRetentionEquivalent(
        before: RepoMaterializer.MaterializeOutput,
        after: RepoMaterializer.MaterializeOutput,
        month: LibraryMonthKey
    ) -> Bool {
        guard before.repoID == after.repoID else { return false }
        guard before.state.months[month] == after.state.months[month] else { return false }
        guard after.coveredByMonth[month, default: .empty].superset(of: before.coveredByMonth[month, default: .empty]) else {
            return false
        }
        guard after.state.observedClock >= before.state.observedClock else { return false }
        for (writer, seq) in before.observedSeqByWriter where (after.observedSeqByWriter[writer] ?? 0) < seq {
            return false
        }
        return true
    }
}

private extension CoveredRanges {
    var isEmpty: Bool {
        rangesByWriter.values.allSatisfy(\.isEmpty)
    }
}

private extension RepoMonthState {
    var isEmpty: Bool {
        assets.isEmpty
            && resources.isEmpty
            && assetResources.isEmpty
            && deletedAssetStamps.isEmpty
    }
}
