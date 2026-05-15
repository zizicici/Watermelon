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
    private static let partialMigrationMarkerFileName = ".watermelon_manifest.legacy-partial-migration.json"

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
        // Marker before any commit keeps interrupted migration visible to inspect.
        try await writeMigrationMarker(writerID: writerID, phase: .phase1, runID: runID)

        let allocator = SeqAllocator(database: database, profileID: profileID, repoID: repoID, initial: try await initialSeq(repoID: repoID, profileID: profileID))
        let clock = PersistedLamportClock(database: database, profileID: profileID, repoID: repoID, initial: try await initialClock(repoID: repoID, profileID: profileID))
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        let months = try await scanV1Months()
        var processed = 0
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        var existingV2Output: RepoMaterializer.MaterializeOutput?

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
            var resourcesByHash: [Data: RemoteManifestResource] = [:]
            resourcesByHash.reserveCapacity(snapshot.resources.count)
            for resource in snapshot.resources where Self.isValidV2Hash(resource.contentHash) {
                resourcesByHash[resource.contentHash] = resource
            }
            let linksByAssetFP: [Data: [RemoteAssetResourceLink]] = Dictionary(grouping: snapshot.links, by: { $0.assetFingerprint })

            var migrableAssets: [(asset: RemoteManifestAsset, resources: [CommitResourceEntry])] = []
            migrableAssets.reserveCapacity(snapshot.assets.count)
            var skippedAssetFailures: [String] = []
            for asset in snapshot.assets {
                guard Self.isValidV2Hash(asset.assetFingerprint) else {
                    let reason = "asset has invalid fingerprint length \(asset.assetFingerprint.count)"
                    skippedAssetFailures.append(reason)
                    v1MigrationLog.warning("V1 asset in \(scanned.year)-\(scanned.month) has invalid fingerprint length \(asset.assetFingerprint.count, privacy: .public)")
                    continue
                }
                let links = linksByAssetFP[asset.assetFingerprint] ?? []
                if links.isEmpty {
                    let reason = "asset \(asset.assetFingerprint.hexString) has no resource links"
                    skippedAssetFailures.append(reason)
                    v1MigrationLog.warning("V1 asset \(asset.assetFingerprint.hexString, privacy: .public) in \(scanned.year)-\(scanned.month) has no resource links")
                    continue
                }
                var resourcesForOp: [CommitResourceEntry] = []
                resourcesForOp.reserveCapacity(links.count)
                var missingResourceHash: Data?
                var invalidResourceHash: Data?
                for link in links {
                    guard Self.isValidV2Hash(link.resourceHash) else {
                        invalidResourceHash = link.resourceHash
                        break
                    }
                    guard let res = resourcesByHash[link.resourceHash] else {
                        missingResourceHash = link.resourceHash
                        break
                    }
                    guard Self.isValidV2Hash(res.contentHash) else {
                        invalidResourceHash = res.contentHash
                        break
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
                if let invalidResourceHash {
                    let reason = "asset \(asset.assetFingerprint.hexString) references invalid resource hash length \(invalidResourceHash.count)"
                    skippedAssetFailures.append(reason)
                    v1MigrationLog.warning("V1 asset \(asset.assetFingerprint.hexString, privacy: .public) in \(scanned.year)-\(scanned.month) references invalid resource hash length \(invalidResourceHash.count, privacy: .public)")
                    continue
                }
                if let missingResourceHash {
                    let reason = "asset \(asset.assetFingerprint.hexString) references missing resource \(missingResourceHash.hexString)"
                    skippedAssetFailures.append(reason)
                    v1MigrationLog.warning("V1 asset \(asset.assetFingerprint.hexString, privacy: .public) in \(scanned.year)-\(scanned.month) references missing resource \(missingResourceHash.hexString, privacy: .public)")
                    continue
                }
                migrableAssets.append((asset, resourcesForOp))
            }
            if !migrableAssets.isEmpty {
                if existingV2Output == nil {
                    existingV2Output = try await materializer.materialize(expectedRepoID: repoID)
                }
                let existingFingerprints: Set<Data>
                if let existingAssets = existingV2Output?.state.months[monthKey]?.assets {
                    existingFingerprints = Set(existingAssets.keys)
                } else {
                    existingFingerprints = []
                }
                if !existingFingerprints.isEmpty {
                    migrableAssets.removeAll { existingFingerprints.contains($0.asset.assetFingerprint) }
                }
            }
            if migrableAssets.isEmpty {
                if let firstSkipped = skippedAssetFailures.first {
                    v1MigrationLog.warning("V1 manifest for \(scanned.year, privacy: .public)-\(scanned.month, privacy: .public) has \(skippedAssetFailures.count, privacy: .public) asset integrity issue(s) and no migrable assets; quarantining as legacy residue: \(firstSkipped, privacy: .public)")
                    try await writePartialMigrationMarker(
                        year: scanned.year,
                        month: scanned.month,
                        runID: runID,
                        migratedAssetCount: 0,
                        totalAssetCount: snapshot.assets.count,
                        failures: skippedAssetFailures
                    )
                } else {
                    v1MigrationLog.warning("V1 manifest for \(scanned.year, privacy: .public)-\(scanned.month, privacy: .public) has no migrable assets; quarantining as legacy residue")
                }
                try await quarantineV1ResidueManifest(year: scanned.year, month: scanned.month, sourcePath: scanned.manifestAbsolutePath)
                continue
            }
            if let firstSkipped = skippedAssetFailures.first {
                v1MigrationLog.warning("V1 manifest for \(scanned.year, privacy: .public)-\(scanned.month, privacy: .public) migrating \(migrableAssets.count, privacy: .public) asset(s) and leaving \(skippedAssetFailures.count, privacy: .public) non-migrable asset(s) in residue: \(firstSkipped, privacy: .public)")
                try await writePartialMigrationMarker(
                    year: scanned.year,
                    month: scanned.month,
                    runID: runID,
                    migratedAssetCount: migrableAssets.count,
                    totalAssetCount: snapshot.assets.count,
                    failures: skippedAssetFailures
                )
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
                for (index, migrable) in migrableAssets.enumerated() {
                    let asset = migrable.asset
                    let body = CommitAddAssetBody(
                        assetFingerprint: asset.assetFingerprint,
                        creationDateMs: asset.creationDateMs,
                        backedUpAtMs: asset.backedUpAtMs,
                        resources: migrable.resources
                    )
                    pendingOps.append(CommitOp(opSeq: index, clock: clockCursor, body: .addAsset(body)))
                    if index + 1 < migrableAssets.count {
                        clockCursor += 1
                    }
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
                } catch {
                    migrationAttempt += 1
                    if migrationAttempt >= migrationMaxRetries || !Self.shouldRetryMigrationCommitWrite(error) {
                        throw error
                    }
                    v1MigrationLog.warning("V1 migration commit write failed for \(monthKey.text, privacy: .public), retrying with fresh clock/seq: \(error.localizedDescription, privacy: .public)")
                    continue
                }
            }

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
            try await quarantineV1ResidueManifest(
                year: scanned.year,
                month: scanned.month,
                sourcePath: scanned.manifestAbsolutePath
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
        try await bootstrap.ensureVersionJSON(writerID: writerID)
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
            if isStorageNotFoundError(error) { return .phase1 }
            throw error
        }
        let data: Data
        do {
            data = try Data(contentsOf: temp)
        } catch {
            v1MigrationLog.warning("migration marker at \(path, privacy: .public) could not be read locally; treating as phase1: \(error.localizedDescription, privacy: .public)")
            return .phase1
        }
        guard let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            v1MigrationLog.warning("migration marker at \(path, privacy: .public) is unparseable; treating as phase1")
            return .phase1
        }
        if let phaseRaw = dict["phase"] as? Int, let phase = MigrationPhase(rawValue: phaseRaw) {
            return phase
        }
        if dict["phase"] != nil {
            v1MigrationLog.warning("migration marker at \(path, privacy: .public) has unknown phase; treating as phase1")
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
            try Task.checkCancellation()
            let yearPath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: yearEntry.name)
            let monthEntries: [RemoteStorageEntry]
            do {
                monthEntries = try await client.list(path: yearPath)
            } catch {
                if isStorageNotFoundError(error) { continue }
                throw error
            }
            for monthEntry in monthEntries where monthEntry.isDirectory && monthEntry.name.range(of: "^[0-9]{2}$", options: .regularExpression) != nil {
                try Task.checkCancellation()
                let monthPath = RemotePathBuilder.absolutePath(basePath: yearPath, remoteRelativePath: monthEntry.name)
                let files: [RemoteStorageEntry]
                do {
                    files = try await client.list(path: monthPath)
                } catch {
                    if isStorageNotFoundError(error) { continue }
                    throw error
                }
                let hasPartialMigrationMarker = files.contains { !$0.isDirectory && $0.name == Self.partialMigrationMarkerFileName }
                let residueFiles = files.filter { !$0.isDirectory && Self.isResidueManifestName($0.name) }
                if hasPartialMigrationMarker && !residueFiles.isEmpty {
                    v1MigrationLog.info(
                        "preserving \(residueFiles.count, privacy: .public) V1 residue manifest(s) under partial migration marker at \(monthPath, privacy: .public)"
                    )
                    continue
                }
                for file in residueFiles {
                    try Task.checkCancellation()
                    try await deleteIfPresent(path: file.path)
                }
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

    private func metadataIfPresent(path: String) async throws -> RemoteStorageEntry? {
        do {
            return try await client.metadata(path: path)
        } catch {
            if isStorageNotFoundError(error) { return nil }
            throw error
        }
    }

    private func deleteIfPresent(path: String) async throws {
        guard try await metadataIfPresent(path: path) != nil else { return }
        try await client.delete(path: path)
    }

    /// Idempotent rename so detectV1Manifests stops routing the month as `.v1`.
    private func quarantineV1ResidueManifest(year: Int, month: Int, sourcePath: String) async throws {
        let monthRel = String(format: "%04d/%02d", year, month)
        let residuePath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: monthRel + "/" + Self.residueManifestFileName
        )
        if let meta = try await metadataIfPresent(path: residuePath), !meta.isDirectory {
            guard try await metadataIfPresent(path: sourcePath) != nil else { return }
            if try await remoteFilesEqual(sourcePath, residuePath) {
                try await deleteIfPresent(path: sourcePath)
                return
            }
            try await moveSourceToUniqueResidue(monthRel: monthRel, sourcePath: sourcePath)
            return
        }
        // Peer-deletion between scan and quarantine must not abort the whole phase.
        guard try await metadataIfPresent(path: sourcePath) != nil else { return }
        do {
            switch try await client.moveIfAbsent(from: sourcePath, to: residuePath) {
            case .created:
                return
            case .bestEffortRetry:
                try await finishBestEffortResidueMove(sourcePath: sourcePath, destinationPath: residuePath)
                return
            case .alreadyExists:
                if try await remoteFilesEqual(sourcePath, residuePath) {
                    try await deleteIfPresent(path: sourcePath)
                } else {
                    try await moveSourceToUniqueResidue(monthRel: monthRel, sourcePath: sourcePath)
                }
                return
            }
        } catch {
            if isStorageNotFoundError(error) { return }
            if S3Client.isMoveIfAbsentUnsupported(error) {
                try await copySourceToUniqueResidue(monthRel: monthRel, sourcePath: sourcePath)
                return
            }
            if let meta = try await metadataIfPresent(path: residuePath), !meta.isDirectory {
                guard try await metadataIfPresent(path: sourcePath) != nil else { return }
                if try await remoteFilesEqual(sourcePath, residuePath) {
                    try await deleteIfPresent(path: sourcePath)
                } else {
                    try await moveSourceToUniqueResidue(monthRel: monthRel, sourcePath: sourcePath)
                }
                return
            }
            throw error
        }
    }

    private func moveSourceToUniqueResidue(monthRel: String, sourcePath: String) async throws {
        let uniqueResiduePath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: monthRel + "/" + Self.residueManifestFileName + ".\(UUID().uuidString)"
        )
        do {
            switch try await client.moveIfAbsent(from: sourcePath, to: uniqueResiduePath) {
            case .created:
                return
            case .bestEffortRetry:
                try await finishBestEffortResidueMove(sourcePath: sourcePath, destinationPath: uniqueResiduePath)
                return
            case .alreadyExists:
                throw NSError(domain: "V1MigrationService", code: -32, userInfo: [
                    NSLocalizedDescriptionKey: "unique residue path already exists for \(sourcePath)"
                ])
            }
        } catch {
            if isStorageNotFoundError(error) { return }
            if S3Client.isMoveIfAbsentUnsupported(error) {
                try await copySourceToVerifiedResidue(sourcePath: sourcePath, destinationPath: uniqueResiduePath)
                return
            }
            throw error
        }
    }

    private func copySourceToUniqueResidue(monthRel: String, sourcePath: String) async throws {
        for _ in 0..<4 {
            let uniqueResiduePath = RemotePathBuilder.absolutePath(
                basePath: basePath,
                remoteRelativePath: monthRel + "/" + Self.residueManifestFileName + ".\(UUID().uuidString)"
            )
            guard try await metadataIfPresent(path: uniqueResiduePath) == nil else { continue }
            try await copySourceToVerifiedResidue(sourcePath: sourcePath, destinationPath: uniqueResiduePath)
            return
        }
        throw NSError(domain: "V1MigrationService", code: -34, userInfo: [
            NSLocalizedDescriptionKey: "could not allocate unique residue path for \(sourcePath)"
        ])
    }

    private func copySourceToVerifiedResidue(sourcePath: String, destinationPath: String) async throws {
        guard try await metadataIfPresent(path: sourcePath) != nil else { return }
        do {
            try await client.copy(from: sourcePath, to: destinationPath)
        } catch {
            if isStorageNotFoundError(error) { return }
            throw error
        }
        guard try await remoteFilesEqual(sourcePath, destinationPath) else {
            try? await deleteIfPresent(path: destinationPath)
            if try await metadataIfPresent(path: sourcePath) == nil { return }
            throw residueMoveIncompleteError(sourcePath: sourcePath, destinationPath: destinationPath)
        }
        do {
            try await client.delete(path: sourcePath)
        } catch {
            if isStorageNotFoundError(error) { return }
            throw error
        }
        guard try await metadataIfPresent(path: sourcePath) == nil else {
            throw residueMoveIncompleteError(sourcePath: sourcePath, destinationPath: destinationPath)
        }
    }

    private func finishBestEffortResidueMove(sourcePath: String, destinationPath: String) async throws {
        guard try await metadataIfPresent(path: sourcePath) != nil else { return }
        guard try await remoteFilesEqual(sourcePath, destinationPath) else {
            if try await metadataIfPresent(path: sourcePath) == nil { return }
            throw residueMoveIncompleteError(sourcePath: sourcePath, destinationPath: destinationPath)
        }
        do {
            try await client.delete(path: sourcePath)
        } catch {
            if isStorageNotFoundError(error) { return }
            throw error
        }
        guard try await metadataIfPresent(path: sourcePath) == nil else {
            throw residueMoveIncompleteError(sourcePath: sourcePath, destinationPath: destinationPath)
        }
    }

    private func residueMoveIncompleteError(sourcePath: String, destinationPath: String) -> NSError {
        NSError(domain: "V1MigrationService", code: -33, userInfo: [
            NSLocalizedDescriptionKey: "V1 manifest quarantine incomplete: source still visible at \(sourcePath) after moving to \(destinationPath)"
        ])
    }

    private static func isResidueManifestName(_ name: String) -> Bool {
        name == residueManifestFileName || name.hasPrefix(residueManifestFileName + ".")
    }

    private func writePartialMigrationMarker(
        year: Int,
        month: Int,
        runID: String,
        migratedAssetCount: Int,
        totalAssetCount: Int,
        failures: [String]
    ) async throws {
        let monthRel = String(format: "%04d/%02d", year, month)
        let markerPath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: monthRel + "/" + Self.partialMigrationMarkerFileName
        )
        let dict: [String: Any] = [
            "v": 1,
            "run_id": runID,
            "year": year,
            "month": month,
            "total_asset_count": totalAssetCount,
            "migrated_asset_count": migratedAssetCount,
            "skipped_asset_count": failures.count,
            "failures": failures
        ]
        let data = try JSONSerialization.data(withJSONObject: dict, options: [.sortedKeys])
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("v1-partial-migration-\(UUID().uuidString).json")
        try data.write(to: temp, options: .atomic)
        defer { try? FileManager.default.removeItem(at: temp) }
        _ = try await MetadataCreateGate.createWithStagingFallback(
            client: client,
            localURL: temp,
            remotePath: markerPath,
            respectTaskCancellation: false
        )
    }

    private func remoteFilesEqual(_ lhsPath: String, _ rhsPath: String) async throws -> Bool {
        guard let lhsMeta = try await metadataIfPresent(path: lhsPath), !lhsMeta.isDirectory,
              let rhsMeta = try await metadataIfPresent(path: rhsPath), !rhsMeta.isDirectory,
              lhsMeta.size == rhsMeta.size else {
            return false
        }
        let lhsURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("v1-residue-compare-\(UUID().uuidString)-lhs.sqlite")
        let rhsURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("v1-residue-compare-\(UUID().uuidString)-rhs.sqlite")
        defer {
            try? FileManager.default.removeItem(at: lhsURL)
            try? FileManager.default.removeItem(at: rhsURL)
        }
        do {
            try await client.download(remotePath: lhsPath, localURL: lhsURL)
            try await client.download(remotePath: rhsPath, localURL: rhsURL)
        } catch {
            if isStorageNotFoundError(error) { return false }
            throw error
        }
        do {
            return try localFilesEqual(lhsURL, rhsURL, expectedSize: lhsMeta.size)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            return false
        }
    }

    private func localFilesEqual(_ lhsURL: URL, _ rhsURL: URL, expectedSize: Int64) throws -> Bool {
        let lhsSize = try FileManager.default.attributesOfItem(atPath: lhsURL.path)[.size] as? Int64
        let rhsSize = try FileManager.default.attributesOfItem(atPath: rhsURL.path)[.size] as? Int64
        guard lhsSize == expectedSize, rhsSize == expectedSize else { return false }

        let lhs = try FileHandle(forReadingFrom: lhsURL)
        defer { try? lhs.close() }
        let rhs = try FileHandle(forReadingFrom: rhsURL)
        defer { try? rhs.close() }

        let chunkSize = 64 * 1024
        while true {
            try Task.checkCancellation()
            let lhsChunk = try lhs.read(upToCount: chunkSize) ?? Data()
            let rhsChunk = try rhs.read(upToCount: chunkSize) ?? Data()
            if lhsChunk != rhsChunk { return false }
            if lhsChunk.isEmpty { return true }
        }
    }

    private func writeMigrationMarker(writerID: String, phase: MigrationPhase, runID: String) async throws {
        try await client.createDirectory(path: RepoLayout.migrationsDirectoryPath(base: basePath))
        let path = RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID)
        let nowMs = Int64(Date().timeIntervalSince1970 * 1000)
        var startedAtMs = nowMs
        let existingMeta = try await metadataIfPresent(path: path)
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
            let result = try await MetadataCreateGate.createWithStagingFallback(
                client: client,
                localURL: temp,
                remotePath: path,
                respectTaskCancellation: false
            )
            if case .alreadyExists = result {
                throw NSError(domain: "V1MigrationService", code: -40, userInfo: [
                    NSLocalizedDescriptionKey: "migration marker already exists at \(path)"
                ])
            }
        } else {
            // Overwrite in place — delete+create left a window where inspect could see no marker on a half-migrated repo.
            try await client.upload(localURL: temp, remotePath: path, respectTaskCancellation: false, onProgress: nil)
        }
        try await verifyMigrationMarkerWrite(remotePath: path, localURL: temp)
    }

    private func verifyMigrationMarkerWrite(remotePath: String, localURL: URL) async throws {
        var lastError: Error?
        for attempt in 0..<3 {
            do {
                if try await remoteFileMatchesLocal(remotePath: remotePath, localURL: localURL) {
                    return
                }
                lastError = nil
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                lastError = error
            }
            if attempt + 1 < 3 {
                try await Task.sleep(for: .milliseconds(200 * (1 << attempt)))
            }
        }

        var userInfo: [String: Any] = [
            NSLocalizedDescriptionKey: "migration marker bytes did not verify at \(remotePath)"
        ]
        if let lastError {
            userInfo[NSUnderlyingErrorKey] = lastError
        }
        throw NSError(domain: "V1MigrationService", code: -41, userInfo: userInfo)
    }

    private func remoteFileMatchesLocal(remotePath: String, localURL: URL) async throws -> Bool {
        let remoteURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("migration-marker-verify-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: remoteURL) }
        try await client.download(remotePath: remotePath, localURL: remoteURL)
        let localSize = try FileManager.default.attributesOfItem(atPath: localURL.path)[.size] as? Int64
        guard let localSize else { return false }
        return try localFilesEqual(localURL, remoteURL, expectedSize: localSize)
    }

    private func initialSeq(repoID: String, profileID: Int64) async throws -> UInt64 {
        guard let row = try await identity.loadRepoState(profileID: profileID, repoID: repoID) else {
            return 0
        }
        return UInt64(bitPattern: row.lastSeq)
    }

    private static func shouldRetryMigrationCommitWrite(_ error: Error) -> Bool {
        if error is CancellationError { return false }
        if case CommitLogWriter.WriteError.alreadyExists = error { return true }
        if case CommitLogWriter.WriteError.encodingFailed(_) = error { return false }
        if case CommitLogWriter.WriteError.ioFailure(let underlying) = error {
            return isTransientMigrationCommitWriteError(underlying)
        }
        return isTransientMigrationCommitWriteError(error)
    }

    private static func isTransientMigrationCommitWriteError(_ error: Error) -> Bool {
        if error is CancellationError { return false }
        if isStorageNotFoundError(error) { return false }
        if let storage = error as? RemoteStorageClientError {
            switch storage {
            case .notConnected, .unavailable:
                return true
            case .externalStorageUnavailable, .invalidConfiguration, .unsupportedStorageType(_):
                return false
            case .underlying(let underlying):
                return isTransientMigrationCommitWriteError(underlying)
            }
        }
        if SMBErrorClassifier.isConnectionUnavailable(error) ||
            WebDAVErrorClassifier.isConnectionUnavailable(error) ||
            S3ErrorClassifier.isConnectionUnavailable(error) ||
            SFTPErrorClassifier.isConnectionUnavailable(error) {
            return true
        }
        for nsError in nsErrorChain(error) {
            if nsError.domain == WebDAVClient.errorDomain,
               (500 ... 599).contains(nsError.code) || nsError.code == 408 || nsError.code == 429 {
                return true
            }
            if nsError.domain == S3ErrorClassifier.errorDomain {
                if let status = nsError.userInfo[S3ErrorClassifier.userInfoStatusCodeKey] as? Int,
                   (500 ... 599).contains(status) || status == 408 || status == 429 {
                    return true
                }
                if let serverCode = nsError.userInfo[S3ErrorClassifier.userInfoServerCodeKey] as? String,
                   serverCode == "InternalError" || serverCode == "SlowDown" || serverCode == "ServiceUnavailable" {
                    return true
                }
            }
        }
        return false
    }

    private static func nsErrorChain(_ error: Error) -> [NSError] {
        var collected: [NSError] = []
        var pending: [NSError] = [error as NSError]
        var visited: Set<ObjectIdentifier> = []
        while let current = pending.popLast() {
            guard visited.insert(ObjectIdentifier(current)).inserted else { continue }
            collected.append(current)
            if let underlying = current.userInfo[NSUnderlyingErrorKey] as? Error {
                pending.append(underlying as NSError)
            }
        }
        return collected
    }

    private static func isValidV2Hash(_ hash: Data) -> Bool {
        hash.count == 32
    }

    private func initialClock(repoID: String, profileID: Int64) async throws -> UInt64 {
        guard let row = try await identity.loadRepoState(profileID: profileID, repoID: repoID) else {
            return 0
        }
        return UInt64(bitPattern: row.lastClock)
    }
}
