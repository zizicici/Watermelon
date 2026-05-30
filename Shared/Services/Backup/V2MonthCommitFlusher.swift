import Foundation

struct V2MonthCommitFlusher {
    let services: BackupV2RuntimeServices
    let monthKey: LibraryMonthKey
    let materializedCovered: CoveredRanges
    let observedClockAtLoad: UInt64
    let indexes: V2MonthIndexes

    struct Result {
        let lastSeq: UInt64
        let committedAssets: Set<AssetFingerprint>
        let committedTombstones: Set<AssetFingerprint>
    }

    struct Basis: Sendable, Equatable {
        let clockFloor: UInt64
        let tombstoneObservationBasis: TombstoneObservationBasis
    }

    func flushPending(
        sessionWrittenCovered: CoveredRanges,
        barrierAwareBasis: Basis? = nil,
        limit: Int? = nil,
        ignoreCancellation: Bool
    ) async throws -> Result? {
        let pending = indexes.snapshotPending(limit: limit)
        let opCount = pending.assets.count + pending.tombstones.count
        if opCount == 0 { return nil }

        // Per-flush basis (not session-constant): tombstones must reflect our own intra-session adds, else replay would suppress them.
        let priorCovered = materializedCovered.merging(sessionWrittenCovered)
        var perWriterMaxSeq: [String: UInt64] = [:]
        for (writer, ranges) in priorCovered.rangesByWriter {
            perWriterMaxSeq[writer] = ranges.map(\.high).max() ?? 0
        }

        // Retry must re-tick Lamport clocks so path-level LWW reflects the successful attempt.
        let maxRetries = 4
        var lastSeq: UInt64 = 0
        var attempt = 0
        var committedAddAssetClocks: [AssetFingerprint: UInt64] = [:]
        var committedTombstoneClocks: [AssetFingerprint: UInt64] = [:]
        // Build rows with replay's projection so snapshot baselines equal materialized commits.
        var committedResources: [RemotePhysicalPathKey: RemoteManifestResource] = [:]
        // Defer row stamps until seq allocation completes.
        var committedResourceClocks: [RemotePhysicalPathKey: UInt64] = [:]
        while true {
            let observedBasis: TombstoneObservationBasis
            if let barrierAwareBasis {
                try await services.lamport.observe(barrierAwareBasis.clockFloor)
                observedBasis = barrierAwareBasis.tombstoneObservationBasis
            } else {
                let lamportWatermark = max(observedClockAtLoad, await services.lamport.value())
                observedBasis = TombstoneObservationBasis(
                    perWriterMaxSeq: perWriterMaxSeq,
                    lamportWatermark: lamportWatermark
                )
            }
            let clockRange = try await services.lamport.tickRange(count: opCount)
            var clockCursor = clockRange.low
            var ops: [CommitOp] = []
            ops.reserveCapacity(opCount)
            var opSeq = 0
            committedAddAssetClocks = [:]
            committedTombstoneClocks = [:]
            committedResources = [:]
            committedResourceClocks = [:]

            for fp in pending.assets {
                guard let asset = indexes.asset(forFingerprint: fp),
                      let links = indexes.links(forFingerprint: fp) else { continue }
                var resources: [CommitResourceEntry] = []
                resources.reserveCapacity(links.count)
                for link in links {
                    let resource = try indexes.resourceForCommitOp(hash: link.resourceHash)
                    resources.append(CommitResourceEntry(
                        physicalRemotePath: resource.physicalRemotePath,
                        logicalName: link.logicalName.isEmpty ? resource.logicalName : link.logicalName,
                        contentHash: link.resourceHash,
                        fileSize: resource.fileSize,
                        resourceType: resource.resourceType,
                        role: link.role,
                        slot: link.slot,
                        crypto: resource.crypto
                    ))
                    let resourcePathKey = RemotePhysicalPathKey(resource.physicalRemotePath)
                    committedResources[resourcePathKey] = RemoteManifestResource(
                        year: monthKey.year,
                        month: monthKey.month,
                        physicalRemotePath: resource.physicalRemotePath,
                        contentHash: link.resourceHash,
                        fileSize: resource.fileSize,
                        resourceType: resource.resourceType,
                        creationDateMs: asset.creationDateMs,
                        backedUpAtMs: asset.backedUpAtMs,
                        crypto: resource.crypto
                    )
                    committedResourceClocks[resourcePathKey] = clockCursor
                }
                ops.append(CommitOp(opSeq: opSeq, clock: clockCursor, body: .addAsset(CommitAddAssetBody(
                    assetFingerprint: fp,
                    creationDateMs: asset.creationDateMs,
                    backedUpAtMs: asset.backedUpAtMs,
                    resources: resources
                ))))
                committedAddAssetClocks[fp] = clockCursor
                opSeq += 1
                if opSeq < opCount { clockCursor += 1 }
            }
            for fp in pending.tombstones {
                ops.append(CommitOp(opSeq: opSeq, clock: clockCursor, body: .tombstoneAsset(CommitTombstoneBody(
                    assetFingerprint: fp,
                    reason: .manifestOrphan,
                    observedBasis: observedBasis
                ))))
                committedTombstoneClocks[fp] = clockCursor
                opSeq += 1
                if opSeq < opCount { clockCursor += 1 }
            }

            let seq = try await services.seqAllocator.allocate()
            lastSeq = seq
            let header = CommitHeader(
                version: CommitHeader.currentVersion,
                repoID: services.repoID,
                writerID: services.writerID,
                seq: seq,
                runID: services.runID,
                scope: CommitHeader.monthScope(monthKey),
                clockMin: clockRange.low,
                clockMax: clockRange.high,
                bodyKind: CommitHeader.bodyKindPlain
            )
            do {
                _ = try await services.commitWriter.write(
                    header: header,
                    ops: ops,
                    month: monthKey,
                    respectTaskCancellation: !ignoreCancellation
                )
                break
            } catch CommitLogWriter.WriteError.alreadyExists {
                attempt += 1
                if attempt >= maxRetries { throw CommitLogWriter.WriteError.alreadyExists }
                if !ignoreCancellation { try Task.checkCancellation() }
                continue
            }
        }

        let committedAssets = Set(committedAddAssetClocks.keys)
        let committedTombstones = Set(committedTombstoneClocks.keys)
        indexes.recordCommit(
            assetClocks: committedAddAssetClocks,
            tombstoneClocks: committedTombstoneClocks,
            committedResources: committedResources,
            committedResourceClocks: committedResourceClocks,
            writerID: services.writerID,
            seq: lastSeq
        )

        return Result(
            lastSeq: lastSeq,
            committedAssets: committedAssets,
            committedTombstones: committedTombstones
        )
    }
}
