import Foundation

actor RepoVerifyMonthService {
    private static let contentTrustMaxVerifiedFilesPerMonth = 64
    private static let contentTrustMaxVerifiedBytesPerMonth: Int64 = 32 * 1024 * 1024

    private let client: any RemoteStorageClientProtocol
    private let basePath: String
    private let expectedRepoID: String?

    private struct TombstonePlan {
        let tombstones: [(item: VerifyMonthReportItem, reason: CommitTombstoneBody.Reason)]
        let perWriterMaxSeq: [String: UInt64]
    }

    private enum PresenceProbeResult: Sendable {
        case present
        case absent
        case inconclusive
    }

    private struct PresenceSnapshot: Sendable {
        let presentHashes: Set<Data>
        let inconclusiveHashes: Set<Data>

        func isPresent(_ hash: Data) -> Bool {
            presentHashes.contains(hash)
        }

        func hasInconclusiveResource(in links: [SnapshotAssetResourceRow]) -> Bool {
            links.contains { inconclusiveHashes.contains($0.resourceHash) }
        }
    }

    init(client: any RemoteStorageClientProtocol, basePath: String, expectedRepoID: String? = nil) {
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
            switch state {
            case .healthy: continue
            case .phantom:
                items.append(VerifyMonthReportItem(
                    kind: .phantomAsset,
                    assetFingerprint: fp,
                    detail: "no asset_resources rows; fingerprint=\(fp.hexString)"
                ))
            case .fullyMissing:
                items.append(VerifyMonthReportItem(
                    kind: .allResourcesGone,
                    assetFingerprint: fp,
                    detail: "all \(links.count) resources missing on remote"
                ))
            case .metadataOnlyLeft:
                items.append(VerifyMonthReportItem(
                    kind: .metadataOnlyLeft,
                    assetFingerprint: fp,
                    detail: "only adjustment-data roles remain"
                ))
            case .fingerprintMismatch:
                items.append(VerifyMonthReportItem(
                    kind: .fingerprintMismatch,
                    assetFingerprint: fp,
                    detail: "stored fp does not match recomputed from \(links.count) link(s)"
                ))
            case .partiallyMissing(let missing):
                items.append(VerifyMonthReportItem(
                    kind: .partiallyMissing,
                    assetFingerprint: fp,
                    detail: "\(missing.count)/\(links.count) resources missing"
                ))
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
        var present: Set<Data> = []
        var inconclusive: Set<Data> = []
        for hash in hashes {
            try Task.checkCancellation()
            switch try await probe(hash) {
            case .present:
                present.insert(hash)
            case .absent:
                break
            case .inconclusive:
                inconclusive.insert(hash)
            }
        }
        return PresenceSnapshot(presentHashes: present, inconclusiveHashes: inconclusive)
    }

    private func contentTrustPresenceProbe(
        month: LibraryMonthKey,
        state: RepoMonthState
    ) async throws -> (Data) async throws -> PresenceProbeResult {
        let monthRelativePath = String(format: "%04d/%02d", month.year, month.month)
        let monthAbsolutePath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: monthAbsolutePath)
        } catch {
            if isStorageNotFoundError(error) {
                return { _ in .absent }
            }
            throw error
        }
        let caseSensitive = client.backendNameCaseSensitivity.usesExactNameMatchingForPresence
        func presenceKey(_ name: String) -> String {
            caseSensitive ? name : RemoteFileNaming.collisionKey(for: name)
        }
        var entriesByKey: [String: [(size: Int64, name: String)]] = [:]
        for entry in entries where !entry.isDirectory {
            entriesByKey[presenceKey(entry.name), default: []].append((entry.size, entry.name))
        }
        struct Expected: Sendable {
            let key: String
            let size: Int64
            let hash: Data
        }
        var expectationsByHash: [Data: [Expected]] = [:]
        for resource in state.resources.values {
            let leaf = (resource.physicalRemotePath as NSString).lastPathComponent
            expectationsByHash[resource.contentHash, default: []].append(Expected(
                key: presenceKey(leaf),
                size: resource.fileSize,
                hash: resource.contentHash
            ))
        }
        let clientRef = client
        let basePathRef = basePath
        var verifiedFileCount = 0
        var verifiedByteCount: Int64 = 0
        return { hash in
            guard let candidates = expectationsByHash[hash], !candidates.isEmpty else { return .absent }
            for candidate in candidates {
                let listed = entriesByKey[candidate.key] ?? []
                let sizeMatches = listed.filter { $0.size == candidate.size }
                if sizeMatches.isEmpty { continue }
                for match in sizeMatches {
                    let candidateSize = max(candidate.size, 0)
                    let wouldExceedByteBudget = verifiedByteCount >
                        Self.contentTrustMaxVerifiedBytesPerMonth - candidateSize
                    let budgetExceeded = verifiedFileCount >= Self.contentTrustMaxVerifiedFilesPerMonth ||
                        wouldExceedByteBudget
                    if budgetExceeded {
                        return .inconclusive
                    }
                    let path = RemotePathBuilder.absolutePath(
                        basePath: basePathRef,
                        remoteRelativePath: monthRelativePath + "/" + match.name
                    )
                    // Transport errors must abort verify rather than be silently classified absent — a false absent here drives tombstone issuance against healthy bytes.
                    do {
                        let verified = try await RemoteContentTrust.verifyHash(
                            client: clientRef,
                            remotePath: path,
                            expectedSize: candidate.size,
                            expectedHash: candidate.hash
                        )
                        verifiedFileCount += 1
                        verifiedByteCount += candidateSize
                        if verified {
                            return .present
                        }
                    } catch is CancellationError {
                        throw CancellationError()
                    } catch {
                        if isStorageNotFoundError(error) { continue }
                        throw error
                    }
                }
            }
            return .absent
        }
    }

    @discardableResult
    func applyTombstones(
        month: LibraryMonthKey,
        cleanupItems: [VerifyMonthReportItem],
        services: BackupV2RuntimeServices
    ) async throws -> Set<Data> {
        let eligible = cleanupItems.filter { $0.allowsCleanup }
        guard !eligible.isEmpty else { return [] }

        let materializer = RepoMaterializer(client: client, basePath: basePath)

        func buildTombstonePlan() async throws -> TombstonePlan {
            // Re-classify against a fresh materialize so a peer's heal between verify and apply lands as "no longer eligible".
            let fresh = try await materializer.materializeMonth(month, expectedRepoID: expectedRepoID)
            let monthState = fresh.state.months[month] ?? .empty
            // tickRange must produce clocks above any peer op we just observed; advance lamport before allocating.
            try await services.lamport.observe(fresh.state.observedClock)
            var freshLinksByFP: [Data: [SnapshotAssetResourceRow]] = [:]
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
                switch item.kind {
                case .phantomAsset, .metadataOnlyLeft:
                    return (item, .manifestOrphan)
                case .allResourcesGone:
                    return (item, .verifyFailed)
                case .partiallyMissing, .fingerprintMismatch, .verificationIncomplete:
                    assertionFailure("allowsCleanup must filter \(item.kind)")
                    return nil
                }
            }
            return TombstonePlan(tombstones: tombstones, perWriterMaxSeq: perWriterMaxSeq)
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
                lamportWatermark: await services.lamport.value()
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
