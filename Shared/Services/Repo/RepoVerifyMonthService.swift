import Foundation

actor RepoVerifyMonthService {
    private static let contentTrustMaxVerifiedFilesPerMonth = 64
    private static let contentTrustMaxVerifiedBytesPerMonth: Int64 = 32 * 1024 * 1024

    private let client: any RemoteStorageClientProtocol
    private let basePath: String
    private let expectedRepoID: String

    private struct TombstonePlan {
        let tombstones: [(item: VerifyMonthReportItem, reason: CommitTombstoneBody.Reason)]
        let perWriterMaxSeq: [String: UInt64]
        let lamportWatermark: UInt64
    }

    private struct PresenceSnapshot: Sendable {
        let presenceByHash: [Data: RemoteResourcePresence]
        // Hashes whose remote bytes were downloaded and proven to differ from the recorded hash.
        let confirmedMismatchedHashes: Set<Data>

        func isPresent(_ hash: Data) -> Bool {
            switch presenceByHash[hash] {
            case .hashVerified, .listedSizeMatched: return true
            case .missing, .inconclusive, .none: return false
            }
        }

        func hasInconclusiveResource(in links: [SnapshotAssetResourceRow]) -> Bool {
            links.contains { link in
                if case .inconclusive = presenceByHash[link.resourceHash] { return true }
                return false
            }
        }

        func hasConfirmedMismatchedResource(in links: [SnapshotAssetResourceRow]) -> Bool {
            links.contains { confirmedMismatchedHashes.contains($0.resourceHash) }
        }

        /// Most-healthy assumption: an only-budget-inconclusive resource is treated present, so the
        /// classifier surfaces damage that is confirmed by a *budget-independent* missing/mismatched
        /// sibling instead of being masked behind the inconclusive one.
        func isPresentAssumingInconclusivePresent(_ hash: Data) -> Bool {
            switch presenceByHash[hash] {
            case .hashVerified, .listedSizeMatched, .inconclusive: return true
            case .missing, .none: return false
            }
        }
    }

    /// Collects content-mismatched hashes the probe confirms, so the caller can read them after probing.
    private final class ProbeMismatchSink: @unchecked Sendable {
        var hashes: Set<Data> = []
    }

    /// Read-after-write lag only hides a *recently* written file; future timestamps (peer clock skew) count as fresh.
    static func isWithinGraceWindow(backedUpAtMs: Int64, now: Date, graceSeconds: TimeInterval) -> Bool {
        now.timeIntervalSince1970 - Double(backedUpAtMs) / 1000.0 <= graceSeconds
    }

    init(client: any RemoteStorageClientProtocol, basePath: String, expectedRepoID: String) {
        self.client = client
        self.basePath = basePath
        self.expectedRepoID = expectedRepoID
    }

    func verify(month: LibraryMonthKey) async throws -> VerifyMonthReport {
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materializeMonth(month, expectedRepoID: expectedRepoID)
        let outcome = output.outcomeByMonth[month]
        guard outcome == .clean || outcome == nil else {
            return VerifyMonthReport(month: month, items: [], materializationSkipped: true)
        }
        guard let state = output.state.months[month] else {
            return VerifyMonthReport(month: month, items: [])
        }

        let presence = try await materializedPresenceSnapshot(month: month, state: state)
        let linksByFingerprint = Dictionary(grouping: state.assetResources.values, by: \.assetFingerprint)

        var items: [VerifyMonthReportItem] = []
        for fp in state.assets.keys {
            let links = linksByFingerprint[fp] ?? []
            if presence.hasInconclusiveResource(in: links) {
                // Don't let a budget-inconclusive sibling mask damage that is already confirmed by a
                // budget-independent missing/mismatched resource. Classify with inconclusive-as-present:
                // if the asset is still report-only damage, surface it (confirmed regardless of budget,
                // no cleanup risk); otherwise the inconclusive resource is decisive → incomplete. Cleanup
                // kinds stay incomplete here so an assumed-present resource never drives a tombstone.
                let optimistic = RemoteAssetIntegrityClassifier.classify(
                    assetFingerprint: fp,
                    links: links,
                    isResourceAvailable: { presence.isPresentAssumingInconclusivePresent($0) }
                )
                if let item = VerifyMonthReportItem.from(state: optimistic, fingerprint: fp, linkCount: links.count),
                   MonthVerifyOutcome.damageKinds.contains(item.kind) {
                    items.append(item)
                } else if presence.hasConfirmedMismatchedResource(in: links) {
                    // A byte-proven mismatch is budget-independent and decisive even when assuming the
                    // inconclusive sibling present collapses the asset to a non-damage kind (e.g. metadataOnlyLeft).
                    items.append(VerifyMonthReportItem(
                        kind: .fingerprintMismatch,
                        assetFingerprint: fp,
                        detail: "remote content hash mismatch for \(links.count) resource(s)"
                    ))
                } else {
                    items.append(VerifyMonthReportItem(
                        kind: .verificationIncomplete,
                        assetFingerprint: fp,
                        detail: "content trust budget exhausted before all listed same-size resources were verified"
                    ))
                }
                continue
            }
            let classified = RemoteAssetIntegrityClassifier.classify(
                assetFingerprint: fp,
                links: links,
                isResourceAvailable: { presence.isPresent($0) }
            )
            // Present-but-corrupt is recoverable damage, not absence: surface it instead of auto-tombstoning + stamping verified-OK.
            if classified.allowsCleanup, presence.hasConfirmedMismatchedResource(in: links) {
                items.append(VerifyMonthReportItem(
                    kind: .fingerprintMismatch,
                    assetFingerprint: fp,
                    detail: "remote content hash mismatch for \(links.count) resource(s)"
                ))
            } else if let item = VerifyMonthReportItem.from(state: classified, fingerprint: fp, linkCount: links.count) {
                items.append(item)
            }
        }

        // A future backedUpAtMs is peer clock skew, not freshness: an inconclusive-by-absence resource with
        // a future timestamp never ages out of the grace window, so it can't be confirmed or safely cleaned
        // up. Keep it inconclusive (no tombstone) but withhold the verified-OK stamp. .probeFailure covers
        // both an actually-probed recorded path and a budget-skipped recorded-path probe (the probe budget
        // gate reports the not-listed case as .probeFailure); a size-matched-but-budget-unverified resource
        // stays .verifyBudgetExhausted and is excluded here (it is listed-present and stampable).
        let nowMs = Int64(Date().timeIntervalSince1970 * 1000)
        let hasUnverifiableFutureResource = state.resources.values.contains { resource in
            guard resource.backedUpAtMs > nowMs else { return false }
            guard let presenceForHash = presence.presenceByHash[resource.contentHash] else { return false }
            if case .inconclusive(.probeFailure) = presenceForHash { return true }
            return false
        }

        return VerifyMonthReport(
            month: month,
            items: items,
            hasUnverifiableFutureResource: hasUnverifiableFutureResource
        )
    }

    private func materializedPresenceSnapshot(
        month: LibraryMonthKey,
        state: RepoMonthState
    ) async throws -> PresenceSnapshot {
        let (probe, mismatchSink) = try await contentTrustPresenceProbe(month: month, state: state)
        let hashes = Set(state.resources.values.map(\.contentHash))
        var presenceByHash: [Data: RemoteResourcePresence] = [:]
        for hash in hashes {
            try Task.checkCancellation()
            presenceByHash[hash] = try await probe(hash)
        }
        return PresenceSnapshot(presenceByHash: presenceByHash, confirmedMismatchedHashes: mismatchSink.hashes)
    }

    private func contentTrustPresenceProbe(
        month: LibraryMonthKey,
        state: RepoMonthState
    ) async throws -> (probe: (Data) async throws -> RemoteResourcePresence, mismatchSink: ProbeMismatchSink) {
        let mismatchSink = ProbeMismatchSink()
        let monthRelativePath = String(format: "%04d/%02d", month.year, month.month)
        let monthAbsolutePath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: monthAbsolutePath)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            if isStorageNotFoundError(error) {
                if state.resources.isEmpty {
                    return ({ _ in .missing }, mismatchSink)
                }
                let graceSeconds = client.readAfterWriteGraceSeconds
                // Zero-grace: a whole-month 404 is treated conservatively as a transient probe failure
                // rather than tombstoning every recorded resource off one listing.
                guard graceSeconds > 0 else {
                    return ({ _ in .inconclusive(.probeFailure) }, mismatchSink)
                }
                // Grace backend: gate each resource on freshness, mirroring the file-level recorded-path
                // probe. An old committed resource whose entire month dir is gone has had ample time to
                // become consistent → genuinely missing (cleanup-eligible); a within-grace resource could
                // be a just-written month not yet listed → inconclusive.
                let now = Date()
                var withinGraceByHash: [Data: Bool] = [:]
                for resource in state.resources.values {
                    let within = Self.isWithinGraceWindow(
                        backedUpAtMs: resource.backedUpAtMs, now: now, graceSeconds: graceSeconds
                    )
                    withinGraceByHash[resource.contentHash] = (withinGraceByHash[resource.contentHash] ?? false) || within
                }
                return ({ hash in
                    (withinGraceByHash[hash] ?? false) ? .inconclusive(.probeFailure) : .missing
                }, mismatchSink)
            }
            throw error
        }
        let nameCase = client.backendNameCaseSensitivity
        var entriesByKey: [String: [(size: Int64, name: String)]] = [:]
        var entriesByCanonicalKey: [String: [(size: Int64, name: String)]] = [:]
        for entry in entries where !entry.isDirectory {
            entriesByKey[nameCase.presenceKey(for: entry.name), default: []].append((entry.size, entry.name))
            entriesByCanonicalKey[nameCase.canonicalEquivalenceKey(for: entry.name), default: []].append((entry.size, entry.name))
        }
        struct Expected: Sendable {
            let key: String
            let canonicalKey: String
            let size: Int64
            let hash: Data
            let relativePath: String
            let backedUpAtMs: Int64
        }
        var expectationsByHash: [Data: [Expected]] = [:]
        for resource in state.resources.values {
            let leaf = (resource.physicalRemotePath as NSString).lastPathComponent
            expectationsByHash[resource.contentHash, default: []].append(Expected(
                key: nameCase.presenceKey(for: leaf),
                canonicalKey: nameCase.canonicalEquivalenceKey(for: leaf),
                size: resource.fileSize,
                hash: resource.contentHash,
                relativePath: resource.physicalRemotePath,
                backedUpAtMs: resource.backedUpAtMs
            ))
        }
        let clientRef = client
        let basePathRef = basePath
        let graceSeconds = client.readAfterWriteGraceSeconds
        let graceBackend = graceSeconds > 0
        let now = Date()
        let nowMs = Int64(now.timeIntervalSince1970 * 1000)
        var verifiedFileCount = 0
        var verifiedByteCount: Int64 = 0
        let probe: (Data) async throws -> RemoteResourcePresence = { hash in
            var inconclusiveReason: RemoteResourcePresence.InconclusiveReason?
            guard let candidates = expectationsByHash[hash], !candidates.isEmpty else { return .missing }
            for candidate in candidates {
                let listed = entriesByKey[candidate.key] ?? []
                let sizeMatches = listed.filter { $0.size == candidate.size }
                let probePaths: [String]
                // A grace recorded-path probe found nothing but the resource is older than the
                // read-after-write window: the 404 is genuine absence, so let it fall through to
                // .missing for cleanup instead of looping on .inconclusive forever.
                var staleRecordedPathProbe = false
                if sizeMatches.isEmpty {
                    // Byte-exact key missed. A listed canonically-equivalent same-size leaf (a normalizing
                    // HFS+/SFTP server that stored our NFC leaf as NFD), or — on a grace backend — a stale
                    // LIST that hid a just-written file, is only a reason to probe. Probe the *recorded*
                    // path that restore uses, never a listed sibling: an exact-name backend can list an
                    // unrelated same-hash orphan under an equivalent spelling while the committed object is
                    // gone, and proving presence from that orphan marks a non-restorable asset healthy.
                    let hasCanonicalMatch = !(entriesByCanonicalKey[candidate.canonicalKey] ?? [])
                        .filter { $0.size == candidate.size }.isEmpty
                    let outsideGraceWindow = !Self.isWithinGraceWindow(
                        backedUpAtMs: candidate.backedUpAtMs, now: now, graceSeconds: graceSeconds
                    )
                    // The exact recorded restore leaf is listed at the wrong size: the restore target is
                    // durable present-but-wrong-size = confirmed damage, not absence. Only a recent real write
                    // on a grace backend, within its read-after-write window, can still be a write in flight; a
                    // future backedUpAtMs is peer clock skew (you can't write in the future and it never ages
                    // out of the window), not lag, so it is durable corruption like zero-grace / out-of-window.
                    // A same-size canonical sibling is a different object restore can't use, so it can't excuse
                    // the wrong-size leaf — record the mismatch as report-only damage, not a tombstone or stamp.
                    let writeInFlightPossible = graceBackend && !outsideGraceWindow && candidate.backedUpAtMs <= nowMs
                    if !listed.isEmpty, !writeInFlightPossible {
                        mismatchSink.hashes.insert(candidate.hash)
                        continue
                    }
                    guard hasCanonicalMatch || graceBackend else { continue }
                    staleRecordedPathProbe = outsideGraceWindow
                    probePaths = [RemotePathBuilder.absolutePath(
                        basePath: basePathRef,
                        remoteRelativePath: candidate.relativePath
                    )]
                } else {
                    probePaths = sizeMatches.map { match in
                        RemotePathBuilder.absolutePath(
                            basePath: basePathRef,
                            remoteRelativePath: monthRelativePath + "/" + match.name
                        )
                    }
                }
                for path in probePaths {
                    let candidateSize = max(candidate.size, 0)
                    let fileCap = verifiedFileCount >= Self.contentTrustMaxVerifiedFilesPerMonth
                    let byteCap = verifiedByteCount + candidateSize > Self.contentTrustMaxVerifiedBytesPerMonth
                    if fileCap || byteCap {
                        // A size-matched resource skipped here is listed-present, just not hash-verified —
                        // routine budget incompleteness (stampable). A recorded-path probe skipped here
                        // (resource not listed at its recorded size) leaves the restore target unconfirmed,
                        // so report it as a probe failure — otherwise a future-timestamp absent resource is
                        // masked as routine budget exhaustion and the month is falsely stamped verified OK.
                        return .inconclusive(sizeMatches.isEmpty ? .probeFailure : .verifyBudgetExhausted)
                    }
                    // Transport errors must abort verify rather than be silently classified absent — a false absent here drives tombstone issuance against healthy bytes.
                    do {
                        switch try await RemoteContentTrust.verifyHashResult(
                            client: clientRef,
                            remotePath: path,
                            expectedSize: candidate.size,
                            expectedHash: candidate.hash
                        ) {
                        case .matched:
                            verifiedFileCount += 1
                            verifiedByteCount += candidateSize
                            return .hashVerified
                        case .mismatched:
                            verifiedFileCount += 1
                            verifiedByteCount += candidateSize
                            mismatchSink.hashes.insert(candidate.hash)
                        case .noContent:
                            continue
                        case .inconclusive:
                            // A not-found recorded-path probe past the grace window is genuine absence;
                            // don't latch inconclusive or cleanup could never tombstone a gone resource.
                            if staleRecordedPathProbe { continue }
                            inconclusiveReason = .probeFailure
                        }
                    } catch {
                        if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                        if isStorageNotFoundError(error) { continue }
                        throw error
                    }
                }
            }
            if let reason = inconclusiveReason { return .inconclusive(reason) }
            return .missing
        }
        return (probe, mismatchSink)
    }

    @discardableResult
    func applyTombstones(
        month: LibraryMonthKey,
        cleanupItems: [VerifyMonthReportItem],
        services: BackupV2RuntimeServices
    ) async throws -> Set<AssetFingerprint> {
        let eligible = cleanupItems.filter { $0.allowsCleanup }
        guard !eligible.isEmpty else { return [] }

        let materializer = RepoMaterializer(client: client, basePath: basePath)

        func buildTombstonePlan() async throws -> TombstonePlan {
            // Re-classify against a fresh materialize so a peer's heal between verify and apply lands as "no longer eligible".
            let fresh = try await materializer.materializeMonth(month, expectedRepoID: expectedRepoID)
            let freshOutcome = fresh.outcomeByMonth[month]
            guard freshOutcome == .clean || freshOutcome == nil else {
                return TombstonePlan(tombstones: [], perWriterMaxSeq: [:], lamportWatermark: 0)
            }
            let monthState = fresh.state.months[month] ?? .empty
            // tickRange must produce clocks above any peer op we just observed; advance lamport before allocating.
            try await services.lamport.observe(fresh.state.observedClock)
            let lamportWatermark = await services.lamport.value()
            var freshLinksByFP: [AssetFingerprint: [SnapshotAssetResourceRow]] = [:]
            for ar in monthState.assetResources.values {
                freshLinksByFP[ar.assetFingerprint, default: []].append(ar)
            }

            let presence = try await materializedPresenceSnapshot(month: month, state: monthState)

            let coveredAtObservation = fresh.coveredByMonth[month] ?? .empty
            var perWriterMaxSeq: [String: UInt64] = [:]
            for (writer, ranges) in coveredAtObservation.rangesByWriter {
                perWriterMaxSeq[writer] = ranges.map(\.high).max() ?? 0
            }

            var stillEligible: [VerifyMonthReportItem] = []
            for item in eligible {
                let links = freshLinksByFP[item.assetFingerprint] ?? []
                if presence.hasInconclusiveResource(in: links) {
                    continue
                }
                // Present-but-corrupt is recoverable damage, never auto-tombstone it.
                if presence.hasConfirmedMismatchedResource(in: links) {
                    continue
                }
                let state = RemoteAssetIntegrityClassifier.classify(
                    assetFingerprint: item.assetFingerprint,
                    links: links,
                    isResourceAvailable: { presence.isPresent($0) }
                )
                if state.allowsCleanup {
                    stillEligible.append(item)
                }
            }

            let tombstones: [(item: VerifyMonthReportItem, reason: CommitTombstoneBody.Reason)] = stillEligible.compactMap { item in
                guard let reason = item.kind.tombstoneReason else {
                    assertionFailure("allowsCleanup must filter \(item.kind)")
                    return nil
                }
                return (item, reason)
            }
            return TombstonePlan(
                tombstones: tombstones,
                perWriterMaxSeq: perWriterMaxSeq,
                lamportWatermark: lamportWatermark
            )
        }

        var plan = try await buildTombstonePlan()
        guard !plan.tombstones.isEmpty else { return [] }
        try Task.checkCancellation()

        // Mirror flushV2's alreadyExists retry; concurrent verify or seq drift would otherwise abort cleanup permanently.
        let maxRetries = 4
        var attempt = 0
        while true {
            let basis = TombstoneObservationBasis(
                perWriterMaxSeq: plan.perWriterMaxSeq,
                lamportWatermark: plan.lamportWatermark
            )
            let clockRange = try await services.lamport.tickRange(count: plan.tombstones.count)
            var clockCursor = clockRange.low
            var ops: [CommitOp] = []
            ops.reserveCapacity(plan.tombstones.count)
            for (index, tombstone) in plan.tombstones.enumerated() {
                ops.append(CommitOp(
                    opSeq: index,
                    clock: clockCursor,
                    body: .tombstoneAsset(CommitTombstoneBody(
                        assetFingerprint: tombstone.item.assetFingerprint,
                        reason: tombstone.reason,
                        observedBasis: basis
                    ))
                ))
                if index + 1 < plan.tombstones.count { clockCursor += 1 }
            }
            let seq = try await services.seqAllocator.allocate()
            let header = CommitHeader(
                version: CommitHeader.currentVersion,
                repoID: services.repoID,
                writerID: services.writerID,
                seq: seq,
                runID: services.runID,
                scope: CommitHeader.monthScope(month),
                clockMin: clockRange.low,
                clockMax: clockRange.high,
                bodyKind: CommitHeader.bodyKindPlain
            )
            do {
                _ = try await services.commitWriter.write(header: header, ops: ops, month: month, respectTaskCancellation: false)
                return Set(plan.tombstones.map { $0.item.assetFingerprint })
            } catch CommitLogWriter.WriteError.alreadyExists {
                attempt += 1
                if attempt >= maxRetries { throw CommitLogWriter.WriteError.alreadyExists }
                plan = try await buildTombstonePlan()
                if plan.tombstones.isEmpty { return [] }
                try Task.checkCancellation()
                continue
            }
        }
    }
}
