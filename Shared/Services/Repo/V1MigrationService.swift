import Foundation
import os.log

private let v1MigrationLog = Logger(subsystem: "com.zizicici.watermelon", category: "V1Migration")

actor V1MigrationService {
    enum MigrationError: Error {
        case ioFailure(Error)
        case noProfileID
    }

    struct ScannedV1Month: Sendable {
        let year: Int
        let month: Int
        let manifestAbsolutePath: String
    }

    static let residueManifestFileName = ".watermelon_manifest.legacy-residue.sqlite"

    enum MigrationPhase: Int, Sendable {
        case phase1 = 1
        case phase2 = 2
        case phase3 = 3
    }

    private let client: any RemoteStorageClientProtocol
    private let basePath: String
    private let database: DatabaseManager
    private let identity: RepoIdentity
    private let bootstrap: RepoBootstrap

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        database: DatabaseManager,
        identity: RepoIdentity,
        bootstrap: RepoBootstrap
    ) {
        self.client = client
        self.basePath = basePath
        self.database = database
        self.identity = identity
        self.bootstrap = bootstrap
    }

    func scanV1Months() async throws -> [ScannedV1Month] {
        let entries = try await client.list(path: basePath)
        var results: [ScannedV1Month] = []
        let yearEntries = entries.filter { $0.isDirectory && $0.name.range(of: "^[0-9]{4}$", options: .regularExpression) != nil }
        for yearEntry in yearEntries {
            guard let year = Int(yearEntry.name) else { continue }
            let yearPath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: yearEntry.name)
            // Silent skip + phase3 retry-scan would DELETE V1 manifests phase1 never processed; surface list failures.
            let monthEntries = try await client.list(path: yearPath)
            for monthEntry in monthEntries where monthEntry.isDirectory && monthEntry.name.range(of: "^[0-9]{2}$", options: .regularExpression) != nil {
                guard let month = Int(monthEntry.name), (1...12).contains(month) else { continue }
                let monthPath = RemotePathBuilder.absolutePath(basePath: yearPath, remoteRelativePath: monthEntry.name)
                let monthFiles = try await client.list(path: monthPath)
                if monthFiles.contains(where: { !$0.isDirectory && $0.name == MonthManifestStore.manifestFileName }) {
                    let absPath = RemotePathBuilder.absolutePath(basePath: monthPath, remoteRelativePath: MonthManifestStore.manifestFileName)
                    results.append(ScannedV1Month(year: year, month: month, manifestAbsolutePath: absPath))
                }
            }
        }
        return results
    }

    @discardableResult
    func runPhase1(profileID: Int64, repoID: String, writerID: String, runID: String) async throws -> Int {
        // WebDAV/SMB/SFTP don't auto-create parents on PUT.
        try await bootstrap.ensureSubdirectories()
        // Marker before any commit so a phase1 crash routes inspect back to `.v1`.
        try await writeMigrationMarker(writerID: writerID, phase: .phase1, runID: runID)

        let allocator = SeqAllocator(database: database, profileID: profileID, repoID: repoID, initial: try await initialSeq(repoID: repoID, profileID: profileID))
        let clock = PersistedLamportClock(database: database, profileID: profileID, repoID: repoID, initial: try await initialClock(repoID: repoID, profileID: profileID))
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        let months = try await scanV1Months()
        var processed = 0

        for scanned in months {
            try Task.checkCancellation()
            let storeOrNil = try await MonthManifestStore.loadManifestDirect(
                client: client,
                basePath: basePath,
                year: scanned.year,
                month: scanned.month,
                manifestAbsolutePath: scanned.manifestAbsolutePath,
                pushSchemaUpgrade: false
            )
            guard let store = storeOrNil else {
                // Quarantine corrupt residue so detectV1Manifests stops looping it as .v1.
                v1MigrationLog.warning("V1 manifest at \(scanned.manifestAbsolutePath, privacy: .public) unreadable — quarantining as legacy residue")
                try await quarantineV1ResidueManifest(year: scanned.year, month: scanned.month, sourcePath: scanned.manifestAbsolutePath)
                continue
            }

            let snapshot = store.unsortedSnapshot()
            if snapshot.assets.isEmpty {
                // No assets to migrate; remove or quarantine so the V1 scan stops finding it.
                if snapshot.resources.isEmpty && snapshot.links.isEmpty {
                    try await deleteIfPresent(path: scanned.manifestAbsolutePath)
                } else {
                    v1MigrationLog.warning("V1 manifest for \(scanned.year, privacy: .public)-\(scanned.month, privacy: .public) has \(snapshot.resources.count, privacy: .public) resources / \(snapshot.links.count, privacy: .public) links but no assets — quarantining as legacy residue")
                    try await quarantineV1ResidueManifest(year: scanned.year, month: scanned.month, sourcePath: scanned.manifestAbsolutePath)
                }
                continue
            }

            let monthKey = LibraryMonthKey(year: scanned.year, month: scanned.month)
            let resourcesByHash: [Data: RemoteManifestResource] = Dictionary(uniqueKeysWithValues: snapshot.resources.map { ($0.contentHash, $0) })
            let linksByAssetFP: [Data: [RemoteAssetResourceLink]] = Dictionary(grouping: snapshot.links, by: { $0.assetFingerprint })

            // Empty resources[] commits as a phantom V2 asset that restore can't reconstruct.
            let migrableAssets = snapshot.assets.filter { asset in
                let links = linksByAssetFP[asset.assetFingerprint] ?? []
                if links.isEmpty {
                    v1MigrationLog.warning("V1 asset \(asset.assetFingerprint.hexString, privacy: .public) in \(scanned.year)-\(scanned.month) has no resource links — skipping migration")
                    return false
                }
                return true
            }
            if migrableAssets.isEmpty {
                v1MigrationLog.warning("V1 manifest for \(scanned.year, privacy: .public)-\(scanned.month, privacy: .public) had only resource-less assets — quarantining as residue")
                try await quarantineV1ResidueManifest(year: scanned.year, month: scanned.month, sourcePath: scanned.manifestAbsolutePath)
                continue
            }

            let migrationMaxRetries = 4
            var migrationAttempt = 0
            var migrationHeader: CommitHeader
            var ops: [CommitOp]
            // Retry must re-tick clocks; reusing them with a new seq corrupts LWW ordering.
            while true {
                let clockRange = try await clock.tickRange(count: migrableAssets.count)
                var clockCursor = clockRange.low
                var pendingOps: [CommitOp] = []
                pendingOps.reserveCapacity(migrableAssets.count)
                for (index, asset) in migrableAssets.enumerated() {
                    let links = linksByAssetFP[asset.assetFingerprint] ?? []
                    var resourcesForOp: [CommitResourceEntry] = []
                    resourcesForOp.reserveCapacity(links.count)
                    for link in links {
                        guard let res = resourcesByHash[link.resourceHash] else {
                            throw NSError(
                                domain: "V1MigrationService",
                                code: -10,
                                userInfo: [NSLocalizedDescriptionKey:
                                    "V1 manifest link references missing resource hash \(link.resourceHash.hexString) — migration aborted"]
                            )
                        }
                        resourcesForOp.append(CommitResourceEntry(
                            physicalRemotePath: res.physicalRemotePath,
                            logicalName: link.logicalName.isEmpty ? res.logicalName : link.logicalName,
                            contentHash: res.contentHash,
                            fileSize: res.fileSize,
                            resourceType: res.resourceType,
                            role: link.role,
                            slot: link.slot,
                            crypto: res.crypto
                        ))
                    }
                    let body = CommitAddAssetBody(
                        assetFingerprint: asset.assetFingerprint,
                        creationDateMs: asset.creationDateMs,
                        backedUpAtMs: asset.backedUpAtMs,
                        resources: resourcesForOp
                    )
                    pendingOps.append(CommitOp(opSeq: index, clock: clockCursor, body: .addAsset(body)))
                    clockCursor &+= 1
                }
                let seq = try await allocator.allocate()
                let header = CommitHeader(
                    version: CommitHeader.currentVersion,
                    repoID: repoID,
                    writerID: writerID,
                    seq: seq,
                    runID: runID,
                    scope: CommitHeader.monthScope(monthKey),
                    clockMin: clockRange.low,
                    clockMax: clockRange.high,
                    bodyKind: CommitHeader.bodyKindPlain
                )
                do {
                    _ = try await commitWriter.write(header: header, ops: pendingOps, month: monthKey, respectTaskCancellation: false)
                    migrationHeader = header
                    ops = pendingOps
                    break
                } catch CommitLogWriter.WriteError.alreadyExists {
                    migrationAttempt += 1
                    if migrationAttempt >= migrationMaxRetries { throw CommitLogWriter.WriteError.alreadyExists }
                    continue
                }
            }

            // Quarantine before snapshot — a snapshot-write failure with the V1 manifest still in place would commit duplicates on retry.
            try await quarantineV1ResidueManifest(
                year: scanned.year,
                month: scanned.month,
                sourcePath: scanned.manifestAbsolutePath
            )

            let finalSeq = migrationHeader.seq
            var state = RepoMonthState.empty
            for op in ops {
                guard case .addAsset(let body) = op.body else { continue }
                let stamp = OpStamp(writerID: writerID, seq: finalSeq, clock: op.clock)
                let totalSize = body.resources.reduce(Int64(0)) { $0 + $1.fileSize }
                state.assets[body.assetFingerprint] = SnapshotAssetRow(
                    assetFingerprint: body.assetFingerprint,
                    creationDateMs: body.creationDateMs,
                    backedUpAtMs: body.backedUpAtMs,
                    resourceCount: body.resources.count,
                    totalFileSizeBytes: totalSize,
                    stamp: stamp
                )
                for resource in body.resources {
                    state.resources[resource.physicalRemotePath] = SnapshotResourceRow(
                        physicalRemotePath: resource.physicalRemotePath,
                        contentHash: resource.contentHash,
                        fileSize: resource.fileSize,
                        resourceType: resource.resourceType,
                        creationDateMs: body.creationDateMs,
                        backedUpAtMs: body.backedUpAtMs,
                        crypto: resource.crypto
                    )
                    state.assetResources[
                        AssetResourceKey(
                            assetFingerprint: body.assetFingerprint,
                            role: resource.role,
                            slot: resource.slot
                        )
                    ] = SnapshotAssetResourceRow(
                        assetFingerprint: body.assetFingerprint,
                        role: resource.role,
                        slot: resource.slot,
                        resourceHash: resource.contentHash,
                        logicalName: resource.logicalName
                    )
                }
            }
            var covered = CoveredRanges()
            covered.add(writerID: writerID, range: ClosedSeqRange(low: finalSeq, high: finalSeq))
            let snapshotHeader = SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(monthKey),
                writerID: writerID,
                repoID: repoID,
                covered: covered
            )
            let parts = RepoSnapshotBuilder.build(header: snapshotHeader, state: state)
            let lamport = await clock.value()
            _ = try await snapshotWriter.write(
                header: snapshotHeader,
                assets: parts.assets,
                resources: parts.resources,
                assetResources: parts.assetResources,
                deletedKeys: parts.deletedKeys,
                month: monthKey,
                lamport: lamport,
                runID: runID,
                respectTaskCancellation: false
            )
            processed += 1
        }
        return processed
    }

    func runPhase2(profileID: Int64, repoID: String, writerID: String, runID: String) async throws {
        try await bootstrap.ensureVersionJSON(writerID: writerID)
        try await bootstrap.ensureSubdirectories()
        try await identity.setMigrationCompleted(profileID: profileID, repoID: repoID)
        try await writeMigrationMarker(writerID: writerID, phase: .phase2, runID: runID)
    }

    func runPhase3(writerID: String, runID: String) async throws {
        try await writeMigrationMarker(writerID: writerID, phase: .phase3, runID: runID)
        try await sweepResidueManifests()
        try await deleteIfPresent(path: RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID))
    }

    func runPhase3Cleanup(writerID: String, runID: String) async throws {
        try await runPhase3(writerID: writerID, runID: runID)
    }

    /// Pre-phase markers (no `phase` field) report `.phase1`; phase1 is idempotent.
    func currentPhase(writerID: String) async throws -> MigrationPhase? {
        let path = RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID)
        guard let meta = try await client.metadata(path: path), !meta.isDirectory else { return nil }
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("migration-marker-read-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        do {
            try await client.download(remotePath: path, localURL: temp)
        } catch {
            return .phase1
        }
        let data = (try? Data(contentsOf: temp)) ?? Data()
        guard let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            return .phase1
        }
        if let phaseRaw = dict["phase"] as? Int, let phase = MigrationPhase(rawValue: phaseRaw) {
            return phase
        }
        return .phase1
    }

    private func sweepResidueManifests() async throws {
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: basePath)
        } catch {
            if isStorageNotFoundError(error) { return }
            throw error
        }
        let yearEntries = entries.filter { $0.isDirectory && $0.name.range(of: "^[0-9]{4}$", options: .regularExpression) != nil }
        for yearEntry in yearEntries {
            let yearPath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: yearEntry.name)
            let monthEntries: [RemoteStorageEntry]
            do {
                monthEntries = try await client.list(path: yearPath)
            } catch {
                if isStorageNotFoundError(error) { continue }
                throw error
            }
            for monthEntry in monthEntries where monthEntry.isDirectory && monthEntry.name.range(of: "^[0-9]{2}$", options: .regularExpression) != nil {
                let monthPath = RemotePathBuilder.absolutePath(basePath: yearPath, remoteRelativePath: monthEntry.name)
                let residuePath = RemotePathBuilder.absolutePath(basePath: monthPath, remoteRelativePath: Self.residueManifestFileName)
                try await deleteIfPresent(path: residuePath)
            }
        }
    }

    /// Distinguishes our incomplete phase3 cleanup (marker survived) from a real V1 regression.
    func ownsMigrationMarker(writerID: String) async throws -> Bool {
        let path = RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID)
        return try await client.metadata(path: path) != nil
    }

    /// Used by builder to tell "peer mid-migration" apart from "real V1 regression".
    func anyMigrationMarkerExists() async throws -> Bool {
        let dir = RepoLayout.migrationsDirectoryPath(base: basePath)
        do {
            let entries = try await client.list(path: dir)
            return entries.contains { !$0.isDirectory && $0.name.hasSuffix(".json") }
        } catch {
            if isStorageNotFoundError(error) { return false }
            throw error
        }
    }

    private func deleteIfPresent(path: String) async throws {
        guard try await client.metadata(path: path) != nil else { return }
        try await client.delete(path: path)
    }

    /// Idempotent rename so detectV1Manifests stops routing the month as `.v1`.
    private func quarantineV1ResidueManifest(year: Int, month: Int, sourcePath: String) async throws {
        let monthRel = String(format: "%04d/%02d", year, month)
        let residuePath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: monthRel + "/" + Self.residueManifestFileName
        )
        if let meta = try await client.metadata(path: residuePath), !meta.isDirectory {
            try await deleteIfPresent(path: sourcePath)
            return
        }
        // Peer-deletion between scan and quarantine must not abort the whole phase.
        guard try await client.metadata(path: sourcePath) != nil else { return }
        try await client.move(from: sourcePath, to: residuePath)
    }

    private func writeMigrationMarker(writerID: String, phase: MigrationPhase, runID: String) async throws {
        try await client.createDirectory(path: RepoLayout.migrationsDirectoryPath(base: basePath))
        let path = RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID)
        let nowMs = Int64(Date().timeIntervalSince1970 * 1000)
        var startedAtMs = nowMs
        let existingMeta = try? await client.metadata(path: path)
        if let meta = existingMeta, !meta.isDirectory {
            let temp = FileManager.default.temporaryDirectory
                .appendingPathComponent("migration-marker-existing-\(UUID().uuidString).json")
            defer { try? FileManager.default.removeItem(at: temp) }
            if (try? await client.download(remotePath: path, localURL: temp)) != nil,
               let data = try? Data(contentsOf: temp),
               let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
               let existing = (dict["started_at_ms"] as? Int64) ?? (dict["started_at_ms"] as? Int).map(Int64.init) {
                startedAtMs = existing
            }
        }
        let dict: [String: Any] = [
            "v": 2,
            "writer_id": writerID,
            "run_id": runID,
            "phase": phase.rawValue,
            "started_at_ms": startedAtMs,
            "last_step_at_ms": nowMs
        ]
        let data = try JSONSerialization.data(withJSONObject: dict, options: [.sortedKeys])
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("migration-marker-\(UUID().uuidString).json")
        try data.write(to: temp, options: .atomic)
        defer { try? FileManager.default.removeItem(at: temp) }
        if existingMeta == nil {
            _ = try await client.atomicCreate(localURL: temp, remotePath: path, respectTaskCancellation: false)
        } else {
            // Overwrite in place — delete+create left a window where inspect could see no marker on a half-migrated repo.
            try await client.upload(localURL: temp, remotePath: path, respectTaskCancellation: false, onProgress: nil)
        }
    }

    private func initialSeq(repoID: String, profileID: Int64) async throws -> UInt64 {
        guard let row = try await identity.loadRepoState(profileID: profileID, repoID: repoID) else {
            return 0
        }
        return UInt64(bitPattern: row.lastSeq)
    }

    private func initialClock(repoID: String, profileID: Int64) async throws -> UInt64 {
        guard let row = try await identity.loadRepoState(profileID: profileID, repoID: repoID) else {
            return 0
        }
        return UInt64(bitPattern: row.lastClock)
    }
}
