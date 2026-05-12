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
    /// All known physical paths per content hash. `findResourceByHash` returns lex-min.
    private var pathsByHash: [Data: Set<String>]
    /// Reverse leaf-name index: linear scan was N² per month under prepareUpload.
    private var resourcesByLeafName: [String: RemoteManifestResource] = [:]
    private var collisionKeysCache: Set<String>?
    /// Session-scoped physical-missing set used for upsert/flush validation.
    /// Workers push it into `RepoCommittedView.physicallyMissingByMonth` for
    /// cache-wide consumers (Home/download/health/resume).
    private var physicallyMissingHashes: Set<Data>

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

        // Project materialized state into in-memory shape **faithfully**. Earlier
        // designs filtered out resources whose physical file was missing from the
        // listing here — that turned into a multi-round bug class because the
        // filtered state then leaked into snapshot writes, which violated the
        // covered-range invariant. Now we keep the commit-log truth and stash the
        // missing-hash set separately for the session-view layer.
        var resourcesByPath: [String: RemoteManifestResource] = [:]
        var pathsByHash: [Data: Set<String>] = [:]
        var physicallyMissingHashes: Set<Data> = []
        // Use Unicode-folded collision keys for matching: case-insensitive backends
        // (most cloud / SMB) report listing names that may differ in case from what
        // the manifest stored.
        let existingCollisionKeys = RemoteFileNaming.collisionKeySet(from: existingFileNameSet)
        for row in materializedState.resources.values {
            let logicalName = (row.physicalRemotePath as NSString).lastPathComponent
            let isPresent = existingCollisionKeys.contains(RemoteFileNaming.collisionKey(for: logicalName))
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
                physicallyMissingHashes.insert(row.contentHash)
            }
        }
        self.resourcesByPath = resourcesByPath
        self.pathsByHash = pathsByHash
        // A hash is missing iff ALL of its known paths are missing — promote to
        // `physicallyMissingHashes` only when no path is present.
        var refinedMissing: Set<Data> = []
        for hash in physicallyMissingHashes {
            let paths = pathsByHash[hash] ?? []
            let anyPresent = paths.contains { path in
                let leaf = (path as NSString).lastPathComponent
                return existingCollisionKeys.contains(RemoteFileNaming.collisionKey(for: leaf))
            }
            if !anyPresent { refinedMissing.insert(hash) }
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
        // All snapshots corrupt → next flush writes fresh baseline.
        if output.corruptedSnapshotMonths.contains(monthKey) {
            let ourMaxSeq = materializedCovered.rangesByWriter[v2Services.writerID]?
                .map(\.high).max() ?? 0
            if ourMaxSeq > 0 {
                session.requestSnapshotRebaseline(at: ourMaxSeq)
            }
        }
        return session
    }

    /// Force next flush to emit a fresh snapshot baseline even without new ops.
    func requestSnapshotRebaseline(at seq: UInt64) {
        pendingSnapshotRetrySeq = seq
        dirty = true
    }

    // MARK: - Read

    func containsAssetFingerprint(_ fingerprint: Data) -> Bool {
        assetsByFingerprint[fingerprint] != nil
    }

    func isAssetIncomplete(_ fingerprint: Data) -> Bool {
        guard let asset = assetsByFingerprint[fingerprint] else { return false }
        let links = linksByFingerprint[fingerprint] ?? []
        // Session-view predicate: resource is available iff path exists in our
        // bookkeeping AND the file is physically present (not in
        // `physicallyMissingHashes`). The materialized state itself remains
        // faithful to the commit log; this filter only gates actionability.
        let missing = physicallyMissingHashes
        return MonthManifestStore.isAssetIncomplete(
            links: links,
            isResourceAvailable: { hash in
                guard pathsByHash[hash]?.isEmpty == false else { return false }
                return !missing.contains(hash)
            },
            assetFingerprint: asset.assetFingerprint
        )
    }

    func findResourceByHash(_ contentHash: Data) -> RemoteManifestResource? {
        guard !physicallyMissingHashes.contains(contentHash) else { return nil }
        guard let paths = pathsByHash[contentHash], !paths.isEmpty else { return nil }
        // Lex-min for determinism — restore / HomeAlbumMatching get all paths via
        // their own multi-path APIs.
        guard let chosen = paths.min() else { return nil }
        return resourcesByPath[chosen]
    }

    func findByFileName(_ logicalName: String) -> RemoteManifestResource? {
        assert(!logicalName.contains("/"), "findByFileName takes a leaf, got: \(logicalName)")
        return resourcesByLeafName[logicalName]
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
        // We just wrote this hash to remote → it's no longer missing. Removing
        // from the set re-enables findResourceByHash for the new content.
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
            // dirty + no ops = stranded snapshot retry from a prior failure.
            if let retrySeq = pendingSnapshotRetrySeq, let services = v2Services {
                do {
                    try await writeSnapshot(services: services, month: monthKey, currentSeq: retrySeq, ignoreCancellation: ignoreCancellation)
                    pendingSnapshotRetrySeq = nil
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

        // Basis = what we'd observed BEFORE allocating this flush's clocks.
        // Using the session-constant load-time basis would let our own later
        // flushes' addAsset ops appear as "after observation" to a tombstone
        // emitted in the same session, suppressing it on replay.
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

        // Record coverage BEFORE writing the snapshot — the commit succeeded above, so
        // the seq IS on remote. If writeSnapshot fails, the next snapshot must still
        // include this seq in its covered range; otherwise the materializer would replay
        // this commit on top of a future snapshot baseline (bad if a later upsert for
        // the same fp landed in between, since the older commit's data would overwrite
        // the newer one stored in the snapshot).
        sessionWrittenCovered.add(writerID: services.writerID, seq: lastSeq)

        // Stamp just-committed assets so the snapshot baseline matches what a
        // future replay would derive — feeds the materializer's LWW gate.
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
        // Tombstones too — without stamps, replay loses LWW evidence against
        // stale adds once the snapshot covers the tombstone seq.
        for (fp, clock) in tombstoneClocks {
            deletedAssetStamps[fp] = OpStamp(writerID: services.writerID, seq: lastSeq, clock: clock)
            legacyDeletedAssetFingerprints.remove(fp)
        }

        // Clear pending BEFORE writeSnapshot. The commit is already durable on remote
        // (we passed the write above), so its fingerprints are "committed" regardless
        // of whether the snapshot baseline writes successfully. Leaving them in pending
        // would let the next flush re-emit the same addAsset/tombstone ops as a NEW
        // commit. A snapshot-write failure still surfaces, but as `FlushError.snapshotWriteFailed`
        // carrying the committed sets so caller can update inflight tracker before rethrow —
        // otherwise the executor's `markCommittedV2` never runs and resume planner double-counts.
        let committedAssets = pendingV2AssetFingerprints
        let committedTombstones = pendingV2TombstoneFingerprints
        pendingV2AssetFingerprints.removeAll()
        pendingV2TombstoneFingerprints.removeAll()
        dirty = false

        do {
            try await writeSnapshot(services: services, month: monthKey, currentSeq: lastSeq, ignoreCancellation: ignoreCancellation)
            pendingSnapshotRetrySeq = nil
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
        currentSeq: UInt64,
        ignoreCancellation: Bool
    ) async throws {
        // Build the materialized state we just folded — faithful to commit log,
        // no listing-based filter. RepoSnapshotBuilder enforces:
        //   `state == fold(commit ops in covered)`.
        // Whether resources are physically present on remote is NOT the snapshot
        // writer's concern; that's session-view (findResourceByHash) territory.
        // Passing the un-filtered state ensures the next materialize sees the
        // same truth the commit log encoded — no silent history loss.
        let snapshotState = currentMaterializedState()
        var covered = materializedCovered.merging(sessionWrittenCovered)
        covered.add(writerID: services.writerID, seq: currentSeq)
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
