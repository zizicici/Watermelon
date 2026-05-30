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
        guard let state = output.state.months[month] else {
            return VerifyMonthReport(month: month, items: [])
        }

        let presence = try await materializedPresenceSnapshot(month: month, state: state)
        let linksByFingerprint = Dictionary(grouping: state.assetResources.values, by: \.assetFingerprint)

        var items: [VerifyMonthReportItem] = []
        for fp in state.assets.keys {
            let links = linksByFingerprint[fp] ?? []
            if presence.hasInconclusiveResource(in: links) {
                items.append(VerifyMonthReportItem(
                    kind: .verificationIncomplete,
                    assetFingerprint: fp,
                    detail: "content trust budget exhausted before all listed same-size resources were verified"
                ))
                continue
            }
            let state = RemoteAssetIntegrityClassifier.classify(
                assetFingerprint: fp,
                links: links,
                isResourceAvailable: { presence.isPresent($0) }
            )
            if let item = VerifyMonthReportItem.from(state: state, fingerprint: fp, linkCount: links.count) {
                items.append(item)
            }
        }

        return VerifyMonthReport(month: month, items: items)
    }

    private func materializedPresenceSnapshot(
        month: LibraryMonthKey,
        state: RepoMonthState
    ) async throws -> PresenceSnapshot {
        let probe = try await contentTrustPresenceProbe(month: month, state: state)
        let hashes = Set(state.resources.values.map(\.contentHash))
        var presenceByHash: [Data: RemoteResourcePresence] = [:]
        for hash in hashes {
            try Task.checkCancellation()
            presenceByHash[hash] = try await probe(hash)
        }
        return PresenceSnapshot(presenceByHash: presenceByHash)
    }

    private func contentTrustPresenceProbe(
        month: LibraryMonthKey,
        state: RepoMonthState
    ) async throws -> (Data) async throws -> RemoteResourcePresence {
        let monthRelativePath = String(format: "%04d/%02d", month.year, month.month)
        let monthAbsolutePath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: monthAbsolutePath)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            if isStorageNotFoundError(error) {
                if state.resources.isEmpty {
                    return { _ in .missing }
                }
                let graceSeconds = client.readAfterWriteGraceSeconds
                // Zero-grace: a whole-month 404 is treated conservatively as a transient probe failure
                // rather than tombstoning every recorded resource off one listing.
                guard graceSeconds > 0 else {
                    return { _ in .inconclusive(.probeFailure) }
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
                return { hash in
                    (withinGraceByHash[hash] ?? false) ? .inconclusive(.probeFailure) : .missing
                }
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
        var verifiedFileCount = 0
        var verifiedByteCount: Int64 = 0
        return { hash in
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
                    guard hasCanonicalMatch || graceBackend else { continue }
                    staleRecordedPathProbe = !Self.isWithinGraceWindow(
                        backedUpAtMs: candidate.backedUpAtMs, now: now, graceSeconds: graceSeconds
                    )
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
                        return .inconclusive(.verifyBudgetExhausted)
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
