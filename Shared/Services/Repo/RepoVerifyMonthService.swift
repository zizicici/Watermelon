import Foundation

actor RepoVerifyMonthService {
    private let client: any RemoteStorageClientProtocol
    private let basePath: String
    private let expectedRepoID: String?

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

        // Size-aware: surface truncated / wrong-size resources as partiallyMissing. allowsCleanup excludes partiallyMissing, so a peer's in-flight upload still can't be auto-tombstoned.
        let isResourceAvailable = try await sizeAwarePresencePredicate(month: month, state: state)
        let linksByFingerprint = Dictionary(grouping: state.assetResources.values, by: \.assetFingerprint)

        var items: [VerifyMonthReportItem] = []
        for fp in state.assets.keys {
            let links = linksByFingerprint[fp] ?? []
            let state = RemoteAssetIntegrityClassifier.classify(
                assetFingerprint: fp,
                links: links,
                isResourceAvailable: isResourceAvailable
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

    private func sizeAwarePresencePredicate(
        month: LibraryMonthKey,
        state: RepoMonthState
    ) async throws -> (Data) -> Bool {
        let monthRelativePath = String(format: "%04d/%02d", month.year, month.month)
        let monthAbsolutePath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)
        let entries = try await client.list(path: monthAbsolutePath)
        // Same collision key can map to multiple real names (case/Unicode variants); single-size dict was last-write-wins.
        var sizesByKey: [String: Set<Int64>] = [:]
        for entry in entries where !entry.isDirectory {
            sizesByKey[RemoteFileNaming.collisionKey(for: entry.name), default: []].insert(entry.size)
        }
        var expectationsByHash: [Data: [(key: String, size: Int64)]] = [:]
        for resource in state.resources.values {
            let leaf = (resource.physicalRemotePath as NSString).lastPathComponent
            expectationsByHash[resource.contentHash, default: []].append((RemoteFileNaming.collisionKey(for: leaf), resource.fileSize))
        }
        return { hash in
            guard let candidates = expectationsByHash[hash], !candidates.isEmpty else { return false }
            return candidates.contains { candidate in
                guard let listedSizes = sizesByKey[candidate.key] else { return false }
                return listedSizes.contains(candidate.size)
            }
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

        // Re-classify against a fresh materialize; a peer may have healed an asset between verify and apply.
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let fresh = try await materializer.materializeMonth(month, expectedRepoID: expectedRepoID)
        let monthState = fresh.state.months[month] ?? .empty
        // Observe-before-send: tickRange below must produce clocks above any peer write we just read.
        try await services.lamport.observe(fresh.state.observedClock)
        var freshLinksByFP: [Data: [SnapshotAssetResourceRow]] = [:]
        for ar in monthState.assetResources.values {
            freshLinksByFP[ar.assetFingerprint, default: []].append(ar)
        }

        // Size-aware again at apply time, for the same reason as verify(): truncated files surface as partiallyMissing (cleanup-blocked) rather than healthy.
        let isResourceAvailable = try await sizeAwarePresencePredicate(month: month, state: monthState)

        // Basis = covered ranges + lamport observed now; materializer compares future heal ops against this.
        let coveredAtObservation = fresh.coveredByMonth[month] ?? .empty
        var perWriterMaxSeq: [String: UInt64] = [:]
        for (writer, ranges) in coveredAtObservation.rangesByWriter {
            perWriterMaxSeq[writer] = ranges.map(\.high).max() ?? 0
        }
        let basis = TombstoneObservationBasis(
            perWriterMaxSeq: perWriterMaxSeq,
            lamportWatermark: fresh.state.observedClock
        )

        var stillEligible: [VerifyMonthReportItem] = []
        for item in eligible {
            let links = freshLinksByFP[item.assetFingerprint] ?? []
            let state = RemoteAssetIntegrityClassifier.classify(
                assetFingerprint: item.assetFingerprint,
                links: links,
                isResourceAvailable: isResourceAvailable
            )
            // Only tombstone if STILL cleanup-eligible; healed assets drop out here.
            if state.allowsCleanup {
                stillEligible.append(item)
            }
        }
        guard !stillEligible.isEmpty else { return [] }

        let clockRange = try await services.lamport.tickRange(count: stillEligible.count)
        var clockCursor = clockRange.low
        var ops: [CommitOp] = []
        ops.reserveCapacity(stillEligible.count)
        for (index, item) in stillEligible.enumerated() {
            let reason: CommitTombstoneBody.Reason
            switch item.kind {
            case .phantomAsset, .metadataOnlyLeft: reason = .manifestOrphan
            case .allResourcesGone: reason = .verifyFailed
            case .partiallyMissing, .fingerprintMismatch:
                assertionFailure("allowsCleanup must filter \(item.kind)")
                continue
            }
            ops.append(CommitOp(
                opSeq: index,
                clock: clockCursor,
                body: .tombstoneAsset(CommitTombstoneBody(
                    assetFingerprint: item.assetFingerprint,
                    reason: reason,
                    observedBasis: basis
                ))
            ))
            clockCursor &+= 1
        }
        // Mirror flushV2's retry on alreadyExists — concurrent verify operations or local
        // seq drift can otherwise abort cleanup permanently.
        let maxRetries = 4
        var attempt = 0
        while true {
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
                return Set(stillEligible.map(\.assetFingerprint))
            } catch CommitLogWriter.WriteError.alreadyExists {
                attempt += 1
                if attempt >= maxRetries { throw CommitLogWriter.WriteError.alreadyExists }
                continue
            }
        }
    }
}
