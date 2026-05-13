import Foundation
import os.log

private let v2SessionLog = Logger(subsystem: "com.zizicici.watermelon", category: "V2MonthSession")

/// V2-native in-memory month state, keyed by `physicalRemotePath` so multi-writer
/// multi-path doesn't need the V1 sqlite UNIQUE(contentHash) workaround. Lives
/// per-month per-worker; `flushToRemote` writes the commit + snapshot files for
/// pending changes.
final class V2MonthSession: BackupMonthStore {
    /// Raised when commit write succeeded but the subsequent snapshot write failed.
    /// Carries the fingerprints that ARE durable on remote — caller MUST update
    /// inflight-tracker state with them before re-throwing, otherwise the executor
    /// treats them as un-flushed and resume planner double-counts.
    enum FlushError: Error {
        case snapshotWriteFailed(committedAssets: Set<Data>, committedTombstones: Set<Data>, underlying: Error)

        /// Peels SnapshotWriter.finalizationFailed which wraps CancellationError.
        var cancellationCause: CancellationError? {
            switch self {
            case .snapshotWriteFailed(_, _, let underlying):
                if let cancel = underlying as? CancellationError { return cancel }
                if let write = underlying as? SnapshotWriter.WriteError,
                   case .finalizationFailed(let inner) = write,
                   let cancel = inner as? CancellationError {
                    return cancel
                }
                return nil
            }
        }
    }

    let year: Int
    let month: Int
    let v2Services: BackupV2RuntimeServices?
    private let basePath: String
    private let client: any RemoteStorageClientProtocol
    private let stepLogger: MonthManifestStepLogger?

    var monthRelativePath: String {
        String(format: "%04d/%02d", year, month)
    }

    var monthAbsolutePath: String {
        RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)
    }

    // Materialized state — keyed by physicalRemotePath so multi-path same-hash is natural.
    // **Faithful to commit log** (no listing-based filter). The session-view layer
    // uses `physicallyMissingHashes` to gate actionability without dropping
    // entries from the snapshot-emit path.
    private var resourcesByPath: [String: RemoteManifestResource]
    private var assetsByFingerprint: [Data: RemoteManifestAsset]
    private var linksByFingerprint: [Data: [RemoteAssetResourceLink]]
    /// `findResourceByHash` returns lex-min over present paths only; missing-path lookup would bind metadata to undownloadable bytes.
    private var pathsByHash: [Data: Set<String>]
    /// Reverse leaf-name index: linear scan was N² per month under prepareUpload.
    private var resourcesByLeafName: [String: RemoteManifestResource] = [:]
    private var collisionKeysCache: Set<String>?
    /// Session-scoped physical-missing set used for upsert/flush validation.
    /// Workers push it into `RepoCommittedView.physicallyMissingByMonth` for
    /// cache-wide consumers (Home/download/health/resume).
    private var physicallyMissingHashes: Set<Data>
    /// Per-path granularity needed when multi-path hashes have only some paths missing.
    private var physicallyMissingPaths: Set<String>

    // Existing remote files from start-of-month directory listing (collision rename input).
    private var remoteFilesByName: [String: MonthManifestStore.RemoteFileMetadata]
    private var existingFileNameSet: Set<String>

    // Pending V2 ops since last flush.
    private var pendingV2AssetFingerprints: Set<Data> = []
    private var pendingV2TombstoneFingerprints: Set<Data> = []

    /// Mirror of RepoMonthState.deletedAssetStamps; survives flushes so snapshot
    /// emits deletedKey rows. Legacy unstamped tombstones live in the Set side.
    private var deletedAssetStamps: [Data: OpStamp]
    private var legacyDeletedAssetFingerprints: Set<Data>

    // Coverage ledger.
    private let materializedCovered: CoveredRanges
    private var sessionWrittenCovered: CoveredRanges = .empty

    /// Lamport clock observed at session load. Per-flush basis adds whatever
    /// we've ticked since, so tombstones written later in the session don't
    /// suppress concurrent peer adds whose clock < our latest local watermark.
    private let observedClockAtLoad: UInt64

    private(set) var dirty: Bool = false

    /// Pinned when commit landed but snapshot write failed; drives standalone retry on next flush.
    private var pendingSnapshotRetrySeq: UInt64?
    private var pendingRebaselineOnly: Bool = false

    var hasAnyAsset: Bool { !assetsByFingerprint.isEmpty }

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        v2Services: BackupV2RuntimeServices,
        materializedState: RepoMonthState,
        materializedCovered: CoveredRanges,
        observedClockAtLoad: UInt64,
        remoteFilesByName: [String: MonthManifestStore.RemoteFileMetadata],
        stepLogger: MonthManifestStepLogger? = nil
    ) {
        self.client = client
        self.basePath = basePath
        self.year = year
        self.month = month
        self.v2Services = v2Services
        self.materializedCovered = materializedCovered
        self.remoteFilesByName = remoteFilesByName
        self.existingFileNameSet = Set(remoteFilesByName.keys)
        self.stepLogger = stepLogger
        self.observedClockAtLoad = observedClockAtLoad

        // Faithful projection; filtering here would leak into snapshot writes and break the covered-range invariant.
        var resourcesByPath: [String: RemoteManifestResource] = [:]
        var pathsByHash: [Data: Set<String>] = [:]
        var physicallyMissingPaths: Set<String> = []
        // Same collision key may map to multiple real names (case/Unicode variants); single-size dict was last-write-wins.
        var sizesByCollisionKey: [String: Set<Int64>] = [:]
        for (name, meta) in remoteFilesByName {
            sizesByCollisionKey[RemoteFileNaming.collisionKey(for: name), default: []].insert(meta.size)
        }
        for row in materializedState.resources.values {
            let logicalName = (row.physicalRemotePath as NSString).lastPathComponent
            let key = RemoteFileNaming.collisionKey(for: logicalName)
            // Size mismatch = stale/truncated; treat as missing so the worker re-uploads.
            let isPresent: Bool
            if let listedSizes = sizesByCollisionKey[key] {
                isPresent = listedSizes.contains(row.fileSize)
            } else {
                isPresent = false
            }
            let resource = RemoteManifestResource(
                year: year,
                month: month,
                physicalRemotePath: row.physicalRemotePath,
                contentHash: row.contentHash,
                fileSize: row.fileSize,
                resourceType: row.resourceType,
                creationDateMs: row.creationDateMs,
                backedUpAtMs: row.backedUpAtMs,
                crypto: row.crypto
            )
            resourcesByPath[row.physicalRemotePath] = resource
            pathsByHash[row.contentHash, default: []].insert(row.physicalRemotePath)
            // Last-write-wins matches upsertResource policy; collision-rename keeps leafs unique.
            let leaf = (row.physicalRemotePath as NSString).lastPathComponent
            resourcesByLeafName[leaf] = resource
            if !isPresent {
                physicallyMissingPaths.insert(row.physicalRemotePath)
            }
        }
        self.resourcesByPath = resourcesByPath
        self.pathsByHash = pathsByHash
        self.physicallyMissingPaths = physicallyMissingPaths
        // Hash missing iff every path missing; overlay consumers use the hash set, in-session lookup uses paths.
        var refinedMissing: Set<Data> = []
        for (hash, paths) in pathsByHash {
            if paths.isSubset(of: physicallyMissingPaths) {
                refinedMissing.insert(hash)
            }
        }
        self.physicallyMissingHashes = refinedMissing

        var assetsByFingerprint: [Data: RemoteManifestAsset] = [:]
        for row in materializedState.assets.values {
            assetsByFingerprint[row.assetFingerprint] = RemoteManifestAsset(
                year: year,
                month: month,
                assetFingerprint: row.assetFingerprint,
                creationDateMs: row.creationDateMs,
                backedUpAtMs: row.backedUpAtMs,
                resourceCount: row.resourceCount,
                totalFileSizeBytes: row.totalFileSizeBytes,
                stamp: row.stamp
            )
        }
        self.assetsByFingerprint = assetsByFingerprint

        var linksByFingerprint: [Data: [RemoteAssetResourceLink]] = [:]
        for row in materializedState.assetResources.values {
            linksByFingerprint[row.assetFingerprint, default: []].append(RemoteAssetResourceLink(
                year: year,
                month: month,
                assetFingerprint: row.assetFingerprint,
                resourceHash: row.resourceHash,
                role: row.role,
                slot: row.slot,
                logicalName: row.logicalName
            ))
        }
        self.linksByFingerprint = linksByFingerprint

        self.deletedAssetStamps = materializedState.deletedAssetStamps
        self.legacyDeletedAssetFingerprints = materializedState.deletedAssetFingerprints
            .subtracting(materializedState.deletedAssetStamps.keys)
    }

    /// Materializes from V2 commit/snapshot + lists month dir for collision-rename input.
    static func loadOrCreate(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        v2Services: BackupV2RuntimeServices,
        stepLogger: MonthManifestStepLogger? = nil
    ) async throws -> V2MonthSession {
        let monthKey = LibraryMonthKey(year: year, month: month)
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materializeMonth(monthKey, expectedRepoID: v2Services.repoID)
        let monthState = output.state.months[monthKey] ?? .empty
        let materializedCovered = output.coveredByMonth[monthKey] ?? .empty
        // Observe-before-send: subsequent tickRange must happen above any peer clock we just read.
        try await v2Services.lamport.observe(output.state.observedClock)

        // WebDAV/SMB/SFTP don't auto-create parents; ensure dir exists for the list below.
        let monthRelativePath = String(format: "%04d/%02d", year, month)
        let monthAbsolutePath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)
        do {
            try await client.createDirectory(path: monthAbsolutePath)
        } catch {
            stepLogger?(String.localizedStringWithFormat(
                String(localized: "backup.manifest.diagnostic.createMonthDirFailed"),
                monthRelativePath,
                error.localizedDescription
            ))
            throw error
        }
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: monthAbsolutePath)
        } catch {
            stepLogger?(String.localizedStringWithFormat(
                String(localized: "backup.manifest.diagnostic.listMonthDirFailed"),
                monthRelativePath,
                error.localizedDescription
            ))
            throw error
        }
        let remoteFilesByName = MonthManifestStore.dedupedRemoteFilesByName(
            entries: entries, year: year, month: month
        )

        let session = V2MonthSession(
            client: client,
            basePath: basePath,
            year: year,
            month: month,
            v2Services: v2Services,
            materializedState: monthState,
            materializedCovered: materializedCovered,
            observedClockAtLoad: output.state.observedClock,
            remoteFilesByName: remoteFilesByName,
            stepLogger: stepLogger
        )
        if output.corruptedSnapshotMonths.contains(monthKey),
           materializedCovered.rangesByWriter.values.contains(where: { !$0.isEmpty }) {
            session.requestSnapshotRebaseline()
        }
        return session
    }

    func requestSnapshotRebaseline() {
        pendingRebaselineOnly = true
        dirty = true
    }

    // MARK: - Read

    func containsAssetFingerprint(_ fingerprint: Data) -> Bool {
        assetsByFingerprint[fingerprint] != nil
    }

    func isAssetIncomplete(_ fingerprint: Data) -> Bool {
        guard let asset = assetsByFingerprint[fingerprint] else { return false }
        let links = linksByFingerprint[fingerprint] ?? []
        // Filter gates actionability only; materialized state stays faithful to the commit log.
        return MonthManifestStore.isAssetIncomplete(
            links: links,
            isResourceAvailable: { hash in
                self.anyPresentPath(forHash: hash) != nil
            },
            assetFingerprint: asset.assetFingerprint
        )
    }

    func findResourceByHash(_ contentHash: Data) -> RemoteManifestResource? {
        // Lex-min over all paths would let a missing path shadow a present one and bind metadata to undownloadable bytes.
        guard let chosen = anyPresentPath(forHash: contentHash) else { return nil }
        return resourcesByPath[chosen]
    }

    func findByFileName(_ logicalName: String) -> RemoteManifestResource? {
        assert(!logicalName.contains("/"), "findByFileName takes a leaf, got: \(logicalName)")
        guard let resource = resourcesByLeafName[logicalName] else { return nil }
        if physicallyMissingPaths.contains(resource.physicalRemotePath) { return nil }
        return resource
    }

    /// Lex-min of present paths for `hash`; nil if no path's file is on remote.
    private func anyPresentPath(forHash hash: Data) -> String? {
        guard let paths = pathsByHash[hash], !paths.isEmpty else { return nil }
        let present = paths.subtracting(physicallyMissingPaths)
        return present.min()
    }

    func existingFileNames() -> Set<String> {
        existingFileNameSet
    }

    func existingCollisionKeys() -> Set<String> {
        if let cache = collisionKeysCache { return cache }
        let built = RemoteFileNaming.collisionKeySet(from: existingFileNameSet)
        collisionKeysCache = built
        return built
    }

    func remoteFileSize(named logicalName: String) -> Int64? {
        remoteFilesByName[logicalName]?.size
    }

    func unsortedSnapshot() -> (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink]) {
        let resources = Array(resourcesByPath.values)
        let assets = Array(assetsByFingerprint.values)
        let links = linksByFingerprint.values.flatMap { $0 }
        return (resources, assets, links)
    }

    func physicallyMissingHashesSnapshot() -> Set<Data> {
        physicallyMissingHashes
    }

    // MARK: - Write

    @discardableResult
    func upsertResource(_ resource: RemoteManifestResource) throws -> RemoteManifestResource {
        // If the same path is being repurposed to a different content hash, drop the
        // stale (oldHash → path) entry first; otherwise findResourceByHash(oldHash)
        // would still return this slot and serve up the new content under the wrong key.
        if let existing = resourcesByPath[resource.physicalRemotePath],
           existing.contentHash != resource.contentHash {
            pathsByHash[existing.contentHash]?.remove(resource.physicalRemotePath)
            if pathsByHash[existing.contentHash]?.isEmpty == true {
                pathsByHash.removeValue(forKey: existing.contentHash)
            }
        }
        resourcesByPath[resource.physicalRemotePath] = resource
        pathsByHash[resource.contentHash, default: []].insert(resource.physicalRemotePath)
        let leaf = (resource.physicalRemotePath as NSString).lastPathComponent
        resourcesByLeafName[leaf] = resource
        if !existingFileNameSet.contains(resource.logicalName) {
            existingFileNameSet.insert(resource.logicalName)
            collisionKeysCache?.insert(RemoteFileNaming.collisionKey(for: resource.logicalName))
        }
        // Just wrote bytes; clear missing markers so findResourceByHash can re-resolve.
        physicallyMissingPaths.remove(resource.physicalRemotePath)
        physicallyMissingHashes.remove(resource.contentHash)
        dirty = true
        return resource
    }

    func upsertAsset(
        _ asset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink],
        replacingSubsetFingerprints: Set<Data>
    ) throws {
        // Validate every link's resourceHash has a matching resource on file
        // AND its physical file is present — otherwise flush would emit a commit
        // body with empty resources[] and the snapshot covering that seq would
        // break `state == fold(covered)`.
        for link in links {
            guard pathsByHash[link.resourceHash]?.isEmpty == false else {
                throw NSError(
                    domain: "V2MonthSession",
                    code: -11,
                    userInfo: [NSLocalizedDescriptionKey: String(localized: "backup.manifest.error.missingResourceHash")]
                )
            }
            if physicallyMissingHashes.contains(link.resourceHash) {
                throw NSError(
                    domain: "V2MonthSession",
                    code: -12,
                    userInfo: [NSLocalizedDescriptionKey: String(localized: "backup.manifest.error.missingResourceHash")]
                )
            }
        }

        // Subset replacement — older partial assets that are strict subsets of this one
        // get tombstoned. Mirrors MonthManifestStore behavior for legacy import.
        for sub in replacingSubsetFingerprints {
            assetsByFingerprint.removeValue(forKey: sub)
            linksByFingerprint.removeValue(forKey: sub)
            pendingV2AssetFingerprints.remove(sub)
            pendingV2TombstoneFingerprints.insert(sub)
        }

        assetsByFingerprint[asset.assetFingerprint] = asset
        linksByFingerprint[asset.assetFingerprint] = links
        pendingV2AssetFingerprints.insert(asset.assetFingerprint)
        pendingV2TombstoneFingerprints.remove(asset.assetFingerprint)
        // Resurrect: mirrors RepoMaterializer's apply-addAsset gate so the snapshot
        // baseline doesn't carry both an asset row and its historical tombstone.
        deletedAssetStamps.removeValue(forKey: asset.assetFingerprint)
        legacyDeletedAssetFingerprints.remove(asset.assetFingerprint)
        dirty = true
    }

    func markRemoteFile(name: String, size: Int64) {
        remoteFilesByName[name] = MonthManifestStore.RemoteFileMetadata(size: size)
        if !existingFileNameSet.contains(name) {
            existingFileNameSet.insert(name)
            collisionKeysCache?.insert(RemoteFileNaming.collisionKey(for: name))
        }
    }

    // MARK: - Flush

    @discardableResult
    func flushToRemote(ignoreCancellation: Bool = false) async throws -> MonthManifestStore.FlushDelta {
        guard dirty, let services = v2Services else {
            return .none
        }
        if !ignoreCancellation { try Task.checkCancellation() }

        let monthKey = LibraryMonthKey(year: year, month: month)
        let opCount = pendingV2AssetFingerprints.count + pendingV2TombstoneFingerprints.count
        if opCount == 0 {
            // dirty + no ops = stranded snapshot retry from a prior failure or a rebaseline request.
            if pendingSnapshotRetrySeq != nil || pendingRebaselineOnly, let services = v2Services {
                do {
                    try await writeSnapshot(
                        services: services,
                        month: monthKey,
                        ownCommitSeq: pendingSnapshotRetrySeq,
                        ignoreCancellation: ignoreCancellation
                    )
                    pendingSnapshotRetrySeq = nil
                    pendingRebaselineOnly = false
                    dirty = false
                } catch {
                    throw FlushError.snapshotWriteFailed(
                        committedAssets: [],
                        committedTombstones: [],
                        underlying: error
                    )
                }
            } else {
                dirty = false
            }
            return .none
        }

        // Per-flush basis (not session-constant): tombstones must reflect our own intra-session adds, else replay would suppress them.
        let priorCovered = materializedCovered.merging(sessionWrittenCovered)
        var perWriterMaxSeq: [String: UInt64] = [:]
        for (writer, ranges) in priorCovered.rangesByWriter {
            perWriterMaxSeq[writer] = ranges.map(\.high).max() ?? 0
        }
        let lamportWatermark = max(observedClockAtLoad, await services.lamport.value())
        let observedBasis = TombstoneObservationBasis(
            perWriterMaxSeq: perWriterMaxSeq,
            lamportWatermark: lamportWatermark
        )

        let clockRange = try await services.lamport.tickRange(count: opCount)
        var clockCursor = clockRange.low
        var ops: [CommitOp] = []
        ops.reserveCapacity(opCount)
        var opSeq = 0
        var addAssetClocks: [Data: UInt64] = [:]
        var tombstoneClocks: [Data: UInt64] = [:]

        for fp in pendingV2AssetFingerprints.sorted(by: { $0.lexicographicallyPrecedes($1) }) {
            guard let asset = assetsByFingerprint[fp],
                  let links = linksByFingerprint[fp] else { continue }
            var resources: [CommitResourceEntry] = []
            resources.reserveCapacity(links.count)
            for link in links {
                // Fail-fast: dropping a link here would emit a commit body with
                // fewer resources than in-memory and break the snapshot
                // covered-range invariant once the next snapshot ships the seq.
                guard let resource = findResourceByHash(link.resourceHash) else {
                    throw NSError(
                        domain: "V2MonthSession",
                        code: -13,
                        userInfo: [NSLocalizedDescriptionKey:
                            "flush aborted: link hash \(link.resourceHash.hexString) lost its resource between upsert and flush"]
                    )
                }
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
            }
            ops.append(CommitOp(opSeq: opSeq, clock: clockCursor, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp,
                creationDateMs: asset.creationDateMs,
                backedUpAtMs: asset.backedUpAtMs,
                resources: resources
            ))))
            addAssetClocks[fp] = clockCursor
            opSeq += 1
            clockCursor &+= 1
        }
        for fp in pendingV2TombstoneFingerprints.sorted(by: { $0.lexicographicallyPrecedes($1) }) {
            ops.append(CommitOp(opSeq: opSeq, clock: clockCursor, body: .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: fp,
                reason: .manifestOrphan,
                observedBasis: observedBasis
            ))))
            tombstoneClocks[fp] = clockCursor
            opSeq += 1
            clockCursor &+= 1
        }

        // Retry on alreadyExists — local seq drift can produce a colliding filename.
        let maxRetries = 4
        var lastSeq: UInt64 = 0
        var attempt = 0
        while true {
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
                continue
            }
        }

        // Coverage must reflect the durable commit even if snapshot write later fails; otherwise the next materialize would replay this seq atop a future baseline.
        sessionWrittenCovered.add(writerID: services.writerID, seq: lastSeq)

        // Stamp committed rows so the snapshot baseline matches what a future replay would derive (LWW gate).
        for (fp, clock) in addAssetClocks {
            guard let asset = assetsByFingerprint[fp] else { continue }
            assetsByFingerprint[fp] = RemoteManifestAsset(
                year: asset.year,
                month: asset.month,
                assetFingerprint: asset.assetFingerprint,
                creationDateMs: asset.creationDateMs,
                backedUpAtMs: asset.backedUpAtMs,
                resourceCount: asset.resourceCount,
                totalFileSizeBytes: asset.totalFileSizeBytes,
                stamp: OpStamp(writerID: services.writerID, seq: lastSeq, clock: clock)
            )
        }
        // Tombstones need stamps too; without them replay loses LWW evidence against stale adds once the snapshot covers the tombstone seq.
        for (fp, clock) in tombstoneClocks {
            deletedAssetStamps[fp] = OpStamp(writerID: services.writerID, seq: lastSeq, clock: clock)
            legacyDeletedAssetFingerprints.remove(fp)
        }

        // Clear pending before writeSnapshot: commit is durable, so a snapshot failure must not re-emit the same ops in the next flush.
        let committedAssets = pendingV2AssetFingerprints
        let committedTombstones = pendingV2TombstoneFingerprints
        pendingV2AssetFingerprints.removeAll()
        pendingV2TombstoneFingerprints.removeAll()
        dirty = false

        do {
            try await writeSnapshot(
                services: services,
                month: monthKey,
                ownCommitSeq: lastSeq,
                ignoreCancellation: ignoreCancellation
            )
            pendingSnapshotRetrySeq = nil
            pendingRebaselineOnly = false
        } catch {
            dirty = true
            pendingSnapshotRetrySeq = lastSeq
            throw FlushError.snapshotWriteFailed(
                committedAssets: committedAssets,
                committedTombstones: committedTombstones,
                underlying: error
            )
        }

        return MonthManifestStore.FlushDelta(
            didFlush: true,
            committedV2AssetFingerprints: committedAssets,
            committedV2TombstoneFingerprints: committedTombstones
        )
    }

    private func writeSnapshot(
        services: BackupV2RuntimeServices,
        month: LibraryMonthKey,
        ownCommitSeq: UInt64?,
        ignoreCancellation: Bool
    ) async throws {
        // Emit un-filtered state; snapshot must satisfy `state == fold(commits in covered)`.
        let snapshotState = currentMaterializedState()
        var covered = materializedCovered.merging(sessionWrittenCovered)
        // Only own-writer commit seqs may be added; peer-derived rebaseline values come through `materializedCovered`.
        if let ownSeq = ownCommitSeq {
            covered.add(writerID: services.writerID, seq: ownSeq)
        }
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: services.writerID,
            repoID: services.repoID,
            covered: covered
        )
        let parts = RepoSnapshotBuilder.build(header: header, state: snapshotState)
        // Tick: retry needs a fresh filename, else alreadyExists loops forever.
        let lamportRange = try await services.lamport.tickRange(count: 1)
        _ = try await services.snapshotWriter.write(
            header: header,
            assets: parts.assets,
            resources: parts.resources,
            assetResources: parts.assetResources,
            deletedKeys: parts.deletedKeys,
            month: month,
            lamport: lamportRange.high,
            runID: services.runID,
            respectTaskCancellation: !ignoreCancellation
        )
    }

    /// Project our in-memory bookkeeping back into a `RepoMonthState` shape — the
    /// fold-of-covered-commits truth that `RepoSnapshotBuilder` requires. No
    /// listing-based filtering (that lives in the session-view layer).
    private func currentMaterializedState() -> RepoMonthState {
        var state = RepoMonthState.empty
        for (fp, asset) in assetsByFingerprint {
            state.assets[fp] = SnapshotAssetRow(
                assetFingerprint: asset.assetFingerprint,
                creationDateMs: asset.creationDateMs,
                backedUpAtMs: asset.backedUpAtMs,
                resourceCount: asset.resourceCount,
                totalFileSizeBytes: asset.totalFileSizeBytes,
                stamp: asset.stamp
            )
        }
        for (path, resource) in resourcesByPath {
            state.resources[path] = SnapshotResourceRow(
                physicalRemotePath: resource.physicalRemotePath,
                contentHash: resource.contentHash,
                fileSize: resource.fileSize,
                resourceType: resource.resourceType,
                creationDateMs: resource.creationDateMs,
                backedUpAtMs: resource.backedUpAtMs,
                crypto: resource.crypto
            )
        }
        for (fp, links) in linksByFingerprint {
            for link in links {
                let key = AssetResourceKey(assetFingerprint: fp, role: link.role, slot: link.slot)
                state.assetResources[key] = SnapshotAssetResourceRow(
                    assetFingerprint: fp,
                    role: link.role,
                    slot: link.slot,
                    resourceHash: link.resourceHash,
                    logicalName: link.logicalName
                )
            }
        }
        state.deletedAssetStamps = deletedAssetStamps
        state.deletedAssetFingerprints = legacyDeletedAssetFingerprints.union(deletedAssetStamps.keys)
        return state
    }
}
