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
        respectTaskCancellation: Bool,
        context: RepoCompactionMonthContext? = nil
    ) async throws -> RepoCheckpointResult {
        let contextValid = context != nil && context!.month == month && context!.monthReport.month == month
        let materialized: RepoMaterializer.MaterializeOutput
        if contextValid {
            materialized = context!.materialized
        } else {
            materialized = try await RepoMaterializer(client: client, basePath: basePath)
                .materializeMonth(month, expectedRepoID: repoID)
        }

        let monthOutcome = materialized.outcomeByMonth[month]
        let corruptAllowed = monthOutcome == .corrupt && mode == .repairCorruptBaseline
        guard monthOutcome == nil || monthOutcome == .clean || corruptAllowed else {
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

        let beforeReport: RepoCompactionMonthReport?
        if contextValid {
            beforeReport = context!.monthReport
        } else {
            beforeReport = try await monthReport(for: month, materialized: materialized)
        }
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

        let accepted = try await performLightweightAcceptance(
            month: month,
            snapshotName: snapshotName,
            expectedCovered: covered,
            verifiedSnapshot: expected
        )
        return RepoCheckpointResult(
            outcome: .writtenAccepted,
            month: month,
            snapshotName: snapshotName,
            lamport: lamport,
            covered: covered,
            beforeReport: beforeReport,
            afterReport: nil,
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

    // Lightweight post-write acceptance: LIST same-month snapshots, read candidate bodies
    // (skip own if verifyReadback already confirmed), apply covered-max selection. Does not
    // replay commits — the covered-max snapshot baseline is sufficient evidence for subsequent GC.
    // The just-written snapshot is readable by name (verifyReadback proved it) but the
    // snapshots-directory LIST can still be stale inside the backend read-after-write grace
    // window. Retry until the deadline so a recoverable visibility lag isn't reported as
    // a checkpoint rejection.
    private func performLightweightAcceptance(
        month: LibraryMonthKey,
        snapshotName: String,
        expectedCovered: CoveredRanges,
        verifiedSnapshot: SnapshotFile
    ) async throws -> RepoMaterializer.AcceptedSnapshotBaselineInfo {
        let deadline = client.metadataReadAfterWriteDeadline(floorSeconds: 1)
        var attempt = 0
        while true {
            let result = try await listAndAccept(
                month: month,
                snapshotName: snapshotName,
                expectedCovered: expectedCovered,
                verifiedSnapshot: verifiedSnapshot
            )
            switch result {
            case .accepted(let info):
                return info
            case .retry:
                guard Date() < deadline else {
                    throw RepoCheckpointError.notAcceptedAfterWrite(snapshotName: snapshotName)
                }
                let millis = 200 * (1 << min(attempt, 3))
                attempt += 1
                try await Task.sleep(nanoseconds: UInt64(millis) * 1_000_000)
            }
        }
    }

    private enum LightweightAcceptanceResult {
        case accepted(RepoMaterializer.AcceptedSnapshotBaselineInfo)
        case retry
    }

    private struct TrustedCandidate {
        let filename: String
        let covered: CoveredRanges
        let info: RepoMaterializer.AcceptedSnapshotBaselineInfo
        let body: SnapshotFile
    }

    private func listAndAccept(
        month: LibraryMonthKey,
        snapshotName: String,
        expectedCovered: CoveredRanges,
        verifiedSnapshot: SnapshotFile
    ) async throws -> LightweightAcceptanceResult {
        let filenames = try await SnapshotReader(client: client, basePath: basePath).listSnapshotFilenames()
        let monthFilenames = filenames.filter { name in
            guard let parsed = RepoLayout.parseSnapshotFilename(name) else { return false }
            return parsed.month == month && parsed.lamport < LamportClock.maxAdoptableValue
        }

        guard monthFilenames.contains(snapshotName) else {
            return .retry
        }

        var trusted: [TrustedCandidate] = []
        let reader = SnapshotReader(client: client, basePath: basePath)

        for filename in monthFilenames {
            let file: SnapshotFile
            let parsed = RepoLayout.parseSnapshotFilename(filename)!

            if filename == snapshotName {
                file = verifiedSnapshot
            } else {
                do {
                    file = try await reader.read(filename: filename)
                } catch is CancellationError {
                    throw CancellationError()
                } catch {
                    continue
                }
            }

            guard file.header.repoID == repoID else { continue }
            guard CommitHeader.parseMonthScope(file.header.scope) == month else { continue }
            guard file.header.writerID == parsed.writerID else { continue }
            if snapshotBodyIsNotTrusted(file, month: month, filenameLamport: parsed.lamport) { continue }

            let info = RepoMaterializer.AcceptedSnapshotBaselineInfo(
                filename: filename,
                month: month,
                lamport: parsed.lamport,
                writerID: parsed.writerID,
                runIDPrefix: parsed.runIDPrefix,
                covered: file.header.covered
            )
            trusted.append(TrustedCandidate(filename: filename, covered: file.header.covered, info: info, body: file))
        }

        guard !trusted.isEmpty else { return .retry }

        // covered-max selection: find the candidate whose covered is a superset of all others.
        var coveredMaxIdx: Int? = nil
        for (i, candidate) in trusted.enumerated() {
            let isSuperset = trusted.enumerated().allSatisfy { (j, other) in
                i == j || candidate.covered.superset(of: other.covered)
            }
            if isSuperset {
                if let prev = coveredMaxIdx {
                    let prevL = trusted[prev].info.lamport
                    if candidate.info.lamport != prevL {
                        coveredMaxIdx = candidate.info.lamport > prevL ? i : prev
                    } else if candidate.info.writerID != trusted[prev].info.writerID {
                        coveredMaxIdx = candidate.info.writerID > trusted[prev].info.writerID ? i : prev
                    } else {
                        coveredMaxIdx = candidate.info.runIDPrefix >= trusted[prev].info.runIDPrefix ? i : prev
                    }
                } else {
                    coveredMaxIdx = i
                }
            }
        }

        guard let idx = coveredMaxIdx else {
            throw RepoCheckpointError.notAcceptedAfterWrite(snapshotName: snapshotName)
        }

        let accepted = trusted[idx]
        guard accepted.info.covered.superset(of: expectedCovered) else {
            throw RepoCheckpointError.acceptedCoverageMismatch(snapshotName: snapshotName)
        }

        // If a peer snapshot (not our own) is the accepted covered-max, its body must be a
        // retention superset of our verified snapshot: every asset/resource we wrote must
        // appear in the peer's body or be tombstoned, ensuring commit GC won't delete the
        // only evidence for rows we just materialized.
        if accepted.filename != snapshotName {
            guard peerBodyRetainsVerifiedState(peer: accepted.body, verified: verifiedSnapshot) else {
                throw RepoCheckpointError.notAcceptedAfterWrite(snapshotName: snapshotName)
            }
        }

        return .accepted(accepted.info)
    }

    // Strict fail-closed: a peer snapshot may replace our just-written checkpoint only if every
    // verified row exists in the peer body with identical content. No tombstone substitution —
    // that requires full materialize/replay proof outside the lightweight path. Duplicate peer
    // rows cause rejection rather than a runtime trap.
    private func peerBodyRetainsVerifiedState(peer: SnapshotFile, verified: SnapshotFile) -> Bool {
        var peerAssetsByFP: [AssetFingerprint: SnapshotAssetRow] = [:]
        for row in peer.assets {
            if peerAssetsByFP[row.assetFingerprint] != nil { return false }
            peerAssetsByFP[row.assetFingerprint] = row
        }
        var peerResourcesByPath: [RemotePhysicalPathKey: SnapshotResourceRow] = [:]
        for row in peer.resources {
            let key = RemotePhysicalPathKey(row.physicalRemotePath)
            if peerResourcesByPath[key] != nil { return false }
            peerResourcesByPath[key] = row
        }
        var peerARByKey: [AssetResourceKey: SnapshotAssetResourceRow] = [:]
        for row in peer.assetResources {
            let key = AssetResourceKey(assetFingerprint: row.assetFingerprint, role: row.role, slot: row.slot)
            if peerARByKey[key] != nil { return false }
            peerARByKey[key] = row
        }
        var peerDeletedByKey: [String: SnapshotDeletedKeyRow] = [:]
        for row in peer.deletedKeys {
            let key = "\(row.keyType.rawValue)|\(row.keyValue.lowercased())"
            if peerDeletedByKey[key] != nil { return false }
            peerDeletedByKey[key] = row
        }

        for asset in verified.assets {
            guard let peerAsset = peerAssetsByFP[asset.assetFingerprint], peerAsset == asset else {
                return false
            }
        }
        for resource in verified.resources {
            let key = RemotePhysicalPathKey(resource.physicalRemotePath)
            guard let peerResource = peerResourcesByPath[key], peerResource == resource else {
                return false
            }
        }
        for ar in verified.assetResources {
            let key = AssetResourceKey(assetFingerprint: ar.assetFingerprint, role: ar.role, slot: ar.slot)
            guard let peerAR = peerARByKey[key], peerAR == ar else {
                return false
            }
        }
        for dk in verified.deletedKeys {
            let key = "\(dk.keyType.rawValue)|\(dk.keyValue.lowercased())"
            guard let peerDK = peerDeletedByKey[key], peerDK.stamp == dk.stamp else {
                return false
            }
        }
        return true
    }

    private func snapshotBodyIsNotTrusted(
        _ file: SnapshotFile,
        month: LibraryMonthKey,
        filenameLamport: UInt64
    ) -> Bool {
        for resource in file.resources {
            let components = RemotePathBuilder.normalizeRelativePath(resource.physicalRemotePath)
                .split(separator: "/", omittingEmptySubsequences: false)
            guard components.count == 3, !components[2].isEmpty else { return true }
            let expectedYear = String(format: "%04d", month.year)
            let expectedMonth = String(format: "%02d", month.month)
            if String(components[0]) != expectedYear || String(components[1]) != expectedMonth { return true }
        }
        let covered = file.header.covered
        for asset in file.assets {
            if asset.stamp.clock >= LamportClock.maxAdoptableValue { return true }
            if asset.stamp.clock > filenameLamport { return true }
            if !covered.contains(writerID: asset.stamp.writerID, seq: asset.stamp.seq) { return true }
        }
        for resource in file.resources {
            if resource.stamp.clock >= LamportClock.maxAdoptableValue { return true }
            if resource.stamp.clock > filenameLamport { return true }
            if !covered.contains(writerID: resource.stamp.writerID, seq: resource.stamp.seq) { return true }
        }
        for d in file.deletedKeys {
            if d.stamp.clock >= LamportClock.maxAdoptableValue { return true }
            if d.stamp.clock > filenameLamport { return true }
            if !covered.contains(writerID: d.stamp.writerID, seq: d.stamp.seq) { return true }
            guard d.keyType == .asset else { return true }
            do { _ = try RepoWireValidator.validateHash(d.keyValue, field: "keyValue") } catch { return true }
        }
        return false
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
