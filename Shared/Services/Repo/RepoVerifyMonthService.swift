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

        let isResourceAvailable = try await materializedPresencePredicate(month: month, state: state)
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

    private func materializedPresencePredicate(
        month: LibraryMonthKey,
        state: RepoMonthState
    ) async throws -> (Data) -> Bool {
        let probe = try await contentTrustPresencePredicate(month: month, state: state)
        let hashes = Set(state.resources.values.map(\.contentHash))
        var present: Set<Data> = []
        for hash in hashes {
            try Task.checkCancellation()
            if try await probe(hash) {
                present.insert(hash)
            }
        }
        return { present.contains($0) }
    }

    private func contentTrustPresencePredicate(
        month: LibraryMonthKey,
        state: RepoMonthState
    ) async throws -> (Data) async throws -> Bool {
        let monthRelativePath = String(format: "%04d/%02d", month.year, month.month)
        let monthAbsolutePath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)
        let entries = try await client.list(path: monthAbsolutePath)
        let caseSensitive = client.backendNameCaseSensitivity == .caseSensitive
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
        return { hash in
            guard let candidates = expectationsByHash[hash], !candidates.isEmpty else { return false }
            for candidate in candidates {
                let listed = entriesByKey[candidate.key] ?? []
                let sizeMatches = listed.filter { $0.size == candidate.size }
                if sizeMatches.isEmpty { continue }
                for match in sizeMatches {
                    let path = RemotePathBuilder.absolutePath(
                        basePath: basePathRef,
                        remoteRelativePath: monthRelativePath + "/" + match.name
                    )
                    // Transport errors must abort verify rather than be silently classified absent — a false absent here drives tombstone issuance against healthy bytes.
                    do {
                        if try await RemoteContentTrust.verifyHash(
                            client: clientRef,
                            remotePath: path,
                            expectedSize: candidate.size,
                            expectedHash: candidate.hash
                        ) {
                            return true
                        }
                    } catch is CancellationError {
                        throw CancellationError()
                    } catch {
                        if isStorageNotFoundError(error) { continue }
                        throw error
                    }
                }
            }
            return false
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

        // Re-classify against a fresh materialize so a peer's heal between verify and apply lands as "no longer eligible".
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let fresh = try await materializer.materializeMonth(month, expectedRepoID: expectedRepoID)
        let monthState = fresh.state.months[month] ?? .empty
        // tickRange must produce clocks above any peer op we just observed; advance lamport before allocating.
        try await services.lamport.observe(fresh.state.observedClock)
        var freshLinksByFP: [Data: [SnapshotAssetResourceRow]] = [:]
        for ar in monthState.assetResources.values {
            freshLinksByFP[ar.assetFingerprint, default: []].append(ar)
        }

        let isResourceAvailable = try await materializedPresencePredicate(month: month, state: monthState)

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
            if state.allowsCleanup {
                stillEligible.append(item)
            }
        }
        guard !stillEligible.isEmpty else { return [] }

        let tombstones: [(item: VerifyMonthReportItem, reason: CommitTombstoneBody.Reason)] = stillEligible.compactMap { item in
            switch item.kind {
            case .phantomAsset, .metadataOnlyLeft:
                return (item, .manifestOrphan)
            case .allResourcesGone:
                return (item, .verifyFailed)
            case .partiallyMissing, .fingerprintMismatch:
                assertionFailure("allowsCleanup must filter \(item.kind)")
                return nil
            }
        }
        guard !tombstones.isEmpty else { return [] }

        // Mirror flushV2's alreadyExists retry; concurrent verify or seq drift would otherwise abort cleanup permanently.
        let maxRetries = 4
        var attempt = 0
        while true {
            let clockRange = try await services.lamport.tickRange(count: tombstones.count)
            var clockCursor = clockRange.low
            var ops: [CommitOp] = []
            ops.reserveCapacity(tombstones.count)
            for (index, tombstone) in tombstones.enumerated() {
                ops.append(CommitOp(
                    opSeq: index,
                    clock: clockCursor,
                    body: .tombstoneAsset(CommitTombstoneBody(
                        assetFingerprint: tombstone.item.assetFingerprint,
                        reason: tombstone.reason,
                        observedBasis: basis
                    ))
                ))
                clockCursor &+= 1
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
                return Set(tombstones.map { $0.item.assetFingerprint })
            } catch CommitLogWriter.WriteError.alreadyExists {
                attempt += 1
                if attempt >= maxRetries { throw CommitLogWriter.WriteError.alreadyExists }
                continue
            }
        }
    }
}
