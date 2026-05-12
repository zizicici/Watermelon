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

        let isResourceAvailable = try await fileTruthPredicate(month: month, state: state)
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

    /// OR across paths (multi-writer collision-rename) and collisionKey match (V2MonthSession /
    /// probeMonthForMissing already fold case + unicode; exact-path would false-tombstone on
    /// case-folding servers). List errors surface — a network blip mustn't tombstone the month.
    private func fileTruthPredicate(
        month: LibraryMonthKey,
        state: RepoMonthState
    ) async throws -> (Data) -> Bool {
        let monthRelativePath = String(format: "%04d/%02d", month.year, month.month)
        let monthAbsolutePath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)
        let entries = try await client.list(path: monthAbsolutePath)
        let presentKeys: Set<String> = Set(entries
            .filter { !$0.isDirectory }
            .map { RemoteFileNaming.collisionKey(for: $0.name) })
        var leafKeysByHash: [Data: [String]] = [:]
        for resource in state.resources.values {
            let leaf = (resource.physicalRemotePath as NSString).lastPathComponent
            leafKeysByHash[resource.contentHash, default: []].append(RemoteFileNaming.collisionKey(for: leaf))
        }
        return { hash in
            guard let keys = leafKeysByHash[hash], !keys.isEmpty else { return false }
            return keys.contains(where: { presentKeys.contains($0) })
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

        // Re-materialize at apply time and re-classify each candidate. A concurrent
        // backup may have healed an asset between verify and apply; without this
        // check we'd write a tombstone the basis would later reject (correct), but
        // the cheaper path is to not write it at all.
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let fresh = try await materializer.materializeMonth(month, expectedRepoID: expectedRepoID)
        let monthState = fresh.state.months[month] ?? .empty
        var freshLinksByFP: [Data: [SnapshotAssetResourceRow]] = [:]
        for ar in monthState.assetResources.values {
            freshLinksByFP[ar.assetFingerprint, default: []].append(ar)
        }

        // File-truth (listing), not commit-log truth: an `.allResourcesGone`
        // candidate's commit-log resource rows are still there, so a hash-only
        // check would always re-derive `.healthy` and skip the tombstone.
        let isResourceAvailable = try await fileTruthPredicate(month: month, state: monthState)

        // Observation basis: covered ranges + lamport at re-verify time. This is
        // the snapshot of "what truth I saw" that the materializer compares
        // future heal ops against.
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
