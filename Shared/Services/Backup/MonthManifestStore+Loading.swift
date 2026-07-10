import Foundation
import GRDB
import os.log

private let manifestLoadLog = Logger(subsystem: "com.zizicici.watermelon", category: "MonthManifestStore")

extension MonthManifestStore {
    static func loadOrCreate(
        client: RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        seed: Seed? = nil,
        layout: ManifestLayout,
        stepLogger: MonthManifestStepLogger? = nil,
        assertOwnership: MonthManifestOwnershipAssertion? = nil,
        liteMonthsListing: LiteMonthsListingSnapshot? = nil
    ) async throws -> MonthManifestStore {
        if let seed {
            return try await loadSeeded(
                client: client,
                basePath: basePath,
                year: year,
                month: month,
                seed: seed,
                layout: layout,
                stepLogger: stepLogger,
                assertOwnership: assertOwnership,
                liteMonthsListing: liteMonthsListing
            )
        }

        let monthRelativePath = String(format: "%04d/%02d", year, month)
        let monthAbsolutePath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)

        var needsDataDirectoryCreation = false
        let entries: [RemoteStorageEntry]
        if layout == .lite {
            do {
                let listing = try await LiteDataDirectoryProbe.probe(client: client, monthAbsolutePath: monthAbsolutePath)
                entries = listing.entries
                needsDataDirectoryCreation = listing.directoryMissing
            } catch {
                stepLogger?(String.localizedStringWithFormat(
                    String(localized: "backup.manifest.diagnostic.listMonthDirFailed"),
                    monthRelativePath,
                    error.localizedDescription
                ))
                throw error
            }
        } else {
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
        }
        let remoteFilesByName = Self.dedupedRemoteFilesByName(entries: entries, year: year, month: month)

        let localURL = Self.makeLocalManifestURL(year: year, month: month)

        let manifestAbsolutePath = layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)

        // V1 keeps the manifest inside the listed data dir, so the listing is authoritative. Lite
        // stores it under .watermelon/months, so existence must be probed at the manifest path.
        let manifestExists: Bool
        switch layout {
        case .v1:
            manifestExists = entries.contains { $0.name == Self.manifestFileName && !$0.isDirectory }
        case .lite:
            let manifestMeta = try await client.metadata(path: manifestAbsolutePath)
            if manifestMeta?.isDirectory == true {
                // A directory at the canonical month-manifest path is damaged/foreign control state: fail
                // closed instead of minting a fresh manifest and flushing over it (mirrors the V1→Lite
                // migration directory-conflict guard).
                throw LiteRepoError.existingLiteManifestConflict(
                    month: LibraryMonthKey(year: year, month: month).text
                )
            }
            manifestExists = manifestMeta != nil
        }

        // A best-effort cleanup pass may have failed to restore a recoverable `.bak`/`.tmp` for this month,
        // so an absent canonical must not be read as "genuinely fresh" — fail closed before minting over it.
        if layout == .lite, !manifestExists {
            try await Self.refuseFreshOverRecoverableMonthScratch(
                client: client,
                basePath: basePath,
                year: year,
                month: month,
                liteMonthsListing: liteMonthsListing
            )
        }

        if manifestExists {
            do {
                try await client.download(remotePath: manifestAbsolutePath, localURL: localURL)
            } catch {
                try? FileManager.default.removeItem(at: localURL)
                throw NSError(
                    domain: "MonthManifestStore",
                    code: -31,
                    userInfo: [
                        NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                            String(localized: "backup.manifest.error.downloadExistingManifest"),
                            monthRelativePath
                        ),
                        NSUnderlyingErrorKey: error
                    ]
                )
            }
        }

        let prepared: PreparedManifestQueue
        do {
            prepared = try Self.prepareLocalManifest(
                localURL: localURL,
                origin: manifestExists ? .downloadedFromRemote : .freshlyCreated
            )
        } catch {
            try? FileManager.default.removeItem(at: localURL)
            if manifestExists {
                throw NSError(
                    domain: "MonthManifestStore",
                    code: -32,
                    userInfo: [
                        NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                            String(localized: "backup.manifest.error.existingManifestInvalid"),
                            monthRelativePath
                        ),
                        NSUnderlyingErrorKey: error
                    ]
                )
            }
            prepared = try Self.prepareLocalManifest(
                localURL: localURL,
                origin: .freshlyCreated
            )
        }

        let store = MonthManifestStore(
            client: client,
            basePath: basePath,
            year: year,
            month: month,
            localManifestURL: localURL,
            dbQueue: prepared.queue,
            remoteFilesByName: remoteFilesByName,
            dirty: prepared.requiresRemoteSync,
            layout: layout,
            liteWriteOwnership: assertOwnership,
            liteMonthsListing: liteMonthsListing,
            stepLogger: stepLogger
        )

        // reloadCache doubles as the integrity check: corruption surfaces
        // here before flush can overwrite remote with a bad manifest.
        try store.reloadCache()

        let reconcileNames: Set<String>
        if layout == .lite {
            let gated = try await Self.liteReconcileListing(
                client: client,
                monthAbsolutePath: monthAbsolutePath,
                entries: entries,
                directoryMissing: needsDataDirectoryCreation,
                manifestFileNames: store.manifestFileNames()
            )
            reconcileNames = gated.fileNames
            needsDataDirectoryCreation = gated.directoryMissing
        } else {
            reconcileNames = Set(remoteFilesByName.keys)
        }
        _ = try await store.reconcileWithRemoteListing(reconcileNames)

        if layout == .lite, let assertOwnership {
            try await assertOwnership()
        }

        if needsDataDirectoryCreation {
            try await client.createDirectory(path: monthAbsolutePath)
        }

        return store
    }

    static func loadSeeded(
        client: RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        seed: Seed,
        layout: ManifestLayout,
        stepLogger: MonthManifestStepLogger? = nil,
        assertOwnership: MonthManifestOwnershipAssertion? = nil,
        liteMonthsListing: LiteMonthsListingSnapshot? = nil
    ) async throws -> MonthManifestStore {
        let localURL = Self.makeLocalManifestURL(year: year, month: month)

        let dbQueue = try Self.makeManifestQueue(path: localURL.path)
        try Self.migrate(dbQueue)

        // List actual remote directory to detect orphaned files (uploaded but
        // not recorded in manifest due to crash / force-kill). Without this,
        // prepareUpload misses disk-level collisions and upload fails with
        // STATUS_OBJECT_NAME_COLLISION.
        let monthRelativePath = String(format: "%04d/%02d", year, month)
        let monthAbsolutePath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)
        var needsDataDirectoryCreation = false
        var seededCanonicalMissing = false
        let entries: [RemoteStorageEntry]
        if layout == .lite {
            // A directory introduced at the canonical month-manifest path behind the seed is damaged/foreign
            // control state: fail closed (mirrors loadOrCreate's load-time guard) so a clean/pre-covered
            // seeded month cannot certify completion while its manifest slot is a directory. An unresolved
            // probe surfaces; a genuine absence/file slot proceeds.
            let manifestAbsolutePath = layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
            let manifestMeta = try await client.metadata(path: manifestAbsolutePath)
            if manifestMeta?.isDirectory == true {
                throw LiteRepoError.existingLiteManifestConflict(
                    month: LibraryMonthKey(year: year, month: month).text
                )
            }
            // The seed is cache, not authority: an absent canonical behind it must not let a clean pre-covered
            // month certify completion over missing month truth (the symmetric case to the directory guard
            // above). Refuse fresh over recoverable scratch so cleanup restores a newer copy first; otherwise
            // mark dirty so the seed is republished as the canonical month sqlite via the flush.
            if manifestMeta == nil {
                try await Self.refuseFreshOverRecoverableMonthScratch(
                    client: client,
                    basePath: basePath,
                    year: year,
                    month: month,
                    liteMonthsListing: liteMonthsListing
                )
                seededCanonicalMissing = true
            }
            // Lite stores month truth in .watermelon/months; the data directory is separate. A confirmed
            // missing data directory collapses to an empty listing; any other fault surfaces (fail closed).
            do {
                let listing = try await LiteDataDirectoryProbe.probe(client: client, monthAbsolutePath: monthAbsolutePath)
                entries = listing.entries
                needsDataDirectoryCreation = listing.directoryMissing
            } catch {
                stepLogger?(String.localizedStringWithFormat(
                    String(localized: "backup.manifest.diagnostic.listMonthDirFailed"),
                    monthRelativePath,
                    error.localizedDescription
                ))
                throw error
            }
        } else {
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
        }
        let remoteFilesByName = Self.dedupedRemoteFilesByName(entries: entries, year: year, month: month)

        let store = MonthManifestStore(
            client: client,
            basePath: basePath,
            year: year,
            month: month,
            localManifestURL: localURL,
            dbQueue: dbQueue,
            remoteFilesByName: remoteFilesByName,
            dirty: seededCanonicalMissing,
            layout: layout,
            liteWriteOwnership: assertOwnership,
            liteMonthsListing: liteMonthsListing,
            stepLogger: stepLogger
        )
        try store.seedDatabase(seed)
        try store.reloadCache()

        let reconcileNames: Set<String>
        if layout == .lite {
            let gated = try await Self.liteReconcileListing(
                client: client,
                monthAbsolutePath: monthAbsolutePath,
                entries: entries,
                directoryMissing: needsDataDirectoryCreation,
                manifestFileNames: store.manifestFileNames()
            )
            reconcileNames = gated.fileNames
            needsDataDirectoryCreation = gated.directoryMissing
        } else {
            reconcileNames = Set(remoteFilesByName.keys)
        }
        _ = try await store.reconcileWithRemoteListing(reconcileNames)

        // A Lite seeded load uses in-memory cache as month truth; assert ownership even when
        // reconcile was clean (dirty == false) so a lost lease cannot seed stale month state.
        if layout == .lite, let assertOwnership {
            try await assertOwnership()
        }

        if needsDataDirectoryCreation {
            try await client.createDirectory(path: monthAbsolutePath)
        }

        return store
    }

    /// Confirms which data filenames a Lite load may reconcile against. A destructive prune (whole-month
    /// clear or large-ratio) of a non-empty manifest must be confirmed by a second listing before it is
    /// allowed; an unconfirmed destructive prune falls back to the manifest's own names so nothing is
    /// pruned and `needsDataDirectoryCreation` is dropped (no recreate from an unconfirmed absence).
    static func liteReconcileListing(
        client: RemoteStorageClientProtocol,
        monthAbsolutePath: String,
        entries: [RemoteStorageEntry],
        directoryMissing: Bool,
        manifestFileNames: Set<String>
    ) async throws -> (fileNames: Set<String>, directoryMissing: Bool) {
        switch try await LiteDataDirectoryProbe.confirmPrune(
            client: client,
            monthAbsolutePath: monthAbsolutePath,
            initial: LiteDataDirectoryProbe.Listing(entries: entries, directoryMissing: directoryMissing),
            manifestFileNames: manifestFileNames
        ) {
        case .reconcile(let names, let missing):
            return (names, missing)
        case .skip:
            return (manifestFileNames, false)
        }
    }

    static func loadManifestOnlyIfExists(
        client: RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        layout: ManifestLayout
    ) async throws -> MonthManifestStore? {
        let manifestAbsolutePath = layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)

        guard let manifestEntry = try await client.metadata(path: manifestAbsolutePath),
              manifestEntry.isDirectory == false else {
            return nil
        }

        return try await loadManifestDirect(
            client: client,
            basePath: basePath,
            year: year,
            month: month,
            layout: layout,
            manifestAbsolutePath: manifestAbsolutePath
        )
    }

    /// Downloads and loads a manifest directly, skipping the existence check.
    /// Use when the caller has already confirmed the manifest exists (e.g., via scanManifestDigests).
    /// Pass `pushSchemaUpgrade: false` when reading a manifest you don't own (e.g. legacy-import
    /// scanning a backup folder) so a schema migration doesn't trigger flushToRemote on the source.
    /// `assertOwnership`, when provided, marks this an owned Lite write load: it is carried into the store
    /// so the schema-upgrade flush gates through the same primitive. A `.lite` load with no assertion is
    /// read-only and never schema-pushes; V1 keeps its default schema-push behavior either way.
    static func loadManifestDirect(
        client: RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        layout: ManifestLayout,
        manifestAbsolutePath: String? = nil,
        pushSchemaUpgrade: Bool = true,
        assertOwnership: MonthManifestOwnershipAssertion? = nil,
        liteMonthsListing: LiteMonthsListingSnapshot? = nil,
        surfaceDownloadNotFound: Bool = false
    ) async throws -> MonthManifestStore? {
        let monthRelativePath = String(format: "%04d/%02d", year, month)
        let absPath = manifestAbsolutePath
            ?? layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)

        let localURL = Self.makeLocalManifestURL(year: year, month: month)

        do {
            try await client.download(remotePath: absPath, localURL: localURL)
        } catch {
            try? FileManager.default.removeItem(at: localURL)
            if error is CancellationError || Task.isCancelled {
                throw CancellationError()
            }
            // A clear backend not-found means the canonical was deleted between the caller's metadata probe and
            // this download. Surface it as a dedicated marker (callers opt in via `surfaceDownloadNotFound`) so a
            // later schema-flush/read-back error that merely wraps a not-found can never be mistaken for this
            // initial-download absence. The default keeps the nil "couldn't load" contract for other callers.
            if surfaceDownloadNotFound, RemoteFaultLite.classify(error) == .notFound {
                throw Self.makeManifestDownloadNotFoundError(manifestPath: monthRelativePath, underlying: error)
            }
            return nil
        }

        let prepared: PreparedManifestQueue
        do {
            prepared = try Self.prepareLocalManifest(
                localURL: localURL,
                origin: .downloadedFromRemote
            )
        } catch {
            try? FileManager.default.removeItem(at: localURL)
            throw NSError(
                domain: "MonthManifestStore",
                code: -34,
                userInfo: [
                    NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                        String(localized: "backup.manifest.error.downloadedManifestInvalid"),
                        monthRelativePath
                    ),
                    NSUnderlyingErrorKey: error
                ]
            )
        }

        let store = MonthManifestStore(
            client: client,
            basePath: basePath,
            year: year,
            month: month,
            localManifestURL: localURL,
            dbQueue: prepared.queue,
            remoteFilesByName: [:],
            dirty: prepared.requiresRemoteSync,
            layout: layout,
            liteWriteOwnership: assertOwnership,
            liteMonthsListing: liteMonthsListing
        )

        do {
            try store.reloadCache()
        } catch {
            throw NSError(
                domain: "MonthManifestStore",
                code: -35,
                userInfo: [
                    NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                        String(localized: "backup.manifest.error.downloadedManifestInvalid"),
                        monthRelativePath
                    ),
                    NSUnderlyingErrorKey: error
                ]
            )
        }

        // A read-only Lite load (no write lease) must never push a schema upgrade. Only an owned Lite
        // write path flushes, and that flush re-asserts ownership through the store gate. V1 is unchanged.
        let shouldPushSchemaUpgrade: Bool
        switch layout {
        case .v1:
            shouldPushSchemaUpgrade = pushSchemaUpgrade
        case .lite:
            shouldPushSchemaUpgrade = pushSchemaUpgrade && assertOwnership != nil
        }
        if prepared.requiresRemoteSync && shouldPushSchemaUpgrade {
            try await store.flushToRemote()
        }

        return store
    }

    // Canonical absence must agree with the months listing before minting a fresh Lite manifest.
    private static func refuseFreshOverRecoverableMonthScratch(
        client: RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        liteMonthsListing: LiteMonthsListingSnapshot? = nil
    ) async throws {
        let entries: [RemoteStorageEntry]
        if let liteMonthsListing {
            entries = try await liteMonthsListing.entries(client: client, basePath: basePath)
        } else {
            let monthsDirectory = RepoLayoutLite.monthsDirectoryPath(basePath: basePath)
            do {
                entries = try await client.list(path: monthsDirectory)
            } catch {
                if RemoteFaultLite.classify(error) == .notFound { return }
                throw error
            }
        }
        let target = LibraryMonthKey(year: year, month: month)
        let canonicalName = RepoLayoutLite.monthFilename(month: target)
        if entries.contains(where: { !$0.isDirectory && $0.name == canonicalName }) {
            throw freshManifestRefusalError(year: year, month: month)
        }
        if entries.contains(where: { entry in
            !entry.isDirectory && RepoLayoutLite.month(fromScratchFilename: entry.name) == target
        }) {
            throw freshManifestRefusalError(year: year, month: month)
        }
    }

    static func freshManifestRefusalError(year: Int, month: Int) -> NSError {
        NSError(
            domain: "MonthManifestStore",
            code: -38,
            userInfo: [
                NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                    String(localized: "backup.manifest.error.freshManifestRefused"),
                    String(format: "%04d-%02d", year, month)
                )
            ]
        )
    }

    func seedDatabase(_ seed: Seed) throws {
        try dbQueue.write { db in
            try db.execute(sql: "DELETE FROM asset_resources")
            try db.execute(sql: "DELETE FROM assets")
            try db.execute(sql: "DELETE FROM resources")

            for resource in seed.resources {
                try db.execute(
                    sql: """
                    INSERT INTO resources (
                        fileName,
                        contentHash,
                        fileSize,
                        resourceType,
                        creationDateMs,
                        backedUpAtMs,
                        storageCodec,
                        storedFileSize,
                        encryptionKeyID
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    arguments: [
                        resource.fileName,
                        resource.contentHash,
                        resource.fileSize,
                        resource.resourceType,
                        resource.creationDateMs,
                        resource.backedUpAtMs,
                        resource.storageCodec,
                        resource.storedFileSize,
                        resource.encryptionKeyID
                    ]
                )
            }

            for asset in seed.assets {
                try db.execute(
                    sql: """
                    INSERT INTO assets (
                        assetFingerprint,
                        creationDateMs,
                        backedUpAtMs,
                        resourceCount,
                        totalFileSizeBytes
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    arguments: [
                        asset.assetFingerprint,
                        asset.creationDateMs,
                        asset.backedUpAtMs,
                        asset.resourceCount,
                        asset.totalFileSizeBytes
                    ]
                )
            }

            for link in seed.assetResourceLinks.sorted(by: { lhs, rhs in
                if lhs.assetFingerprint != rhs.assetFingerprint {
                    return lhs.assetFingerprint.lexicographicallyPrecedes(rhs.assetFingerprint)
                }
                if lhs.role != rhs.role { return lhs.role < rhs.role }
                return lhs.slot < rhs.slot
            }) {
                try db.execute(
                    sql: """
                    INSERT INTO asset_resources (
                        assetFingerprint,
                        resourceHash,
                        resourceFileName,
                        role,
                        slot
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    arguments: [
                        link.assetFingerprint,
                        link.resourceHash,
                        link.resourceFileName,
                        link.role,
                        link.slot
                    ]
                )
            }
        }
    }

    func reloadCache() throws {
        // One read transaction, three streaming cursors with positional decoding — avoids the intermediate
        // [Row] arrays (per-row detach allocations) and per-column name lookups of fetchAll + row["name"].
        // Positions follow each SELECT's column order; NOT NULL columns decode non-optional (schema-enforced).
        let loaded = try dbQueue.read { db -> (
            resByName: [String: RemoteManifestResource],
            resByHash: [Data: Set<String>],
            assets: [Data: RemoteManifestAsset],
            links: [Data: [RemoteAssetResourceLink]]
        ) in
            var resByName: [String: RemoteManifestResource] = [:]
            var resByHash: [Data: Set<String>] = [:]
            let resCursor = try Row.fetchCursor(
                db,
                sql: "SELECT fileName, contentHash, fileSize, resourceType, creationDateMs, backedUpAtMs, storageCodec, storedFileSize, encryptionKeyID FROM resources"
            )
            while let row = try resCursor.next() {
                let item = RemoteManifestResource(
                    year: year,
                    month: month,
                    fileName: row[0],
                    contentHash: row[1],
                    fileSize: row[2],
                    resourceType: row[3],
                    creationDateMs: row[4],
                    backedUpAtMs: row[5],
                    storageCodec: row[6],
                    storedFileSize: row[7],
                    encryptionKeyID: row[8]
                )
                resByName[item.fileName] = item
                resByHash[item.contentHash, default: []].insert(item.fileName)
            }

            var assets: [Data: RemoteManifestAsset] = [:]
            let assetCursor = try Row.fetchCursor(
                db,
                sql: "SELECT assetFingerprint, creationDateMs, backedUpAtMs, resourceCount, totalFileSizeBytes FROM assets"
            )
            while let row = try assetCursor.next() {
                let fingerprint: Data = row[0]
                assets[fingerprint] = RemoteManifestAsset(
                    year: year,
                    month: month,
                    assetFingerprint: fingerprint,
                    creationDateMs: row[1],
                    backedUpAtMs: row[2],
                    resourceCount: row[3],
                    totalFileSizeBytes: row[4]
                )
            }

            var links: [Data: [RemoteAssetResourceLink]] = [:]
            links.reserveCapacity(assets.count)
            let linkCursor = try Row.fetchCursor(
                db,
                sql: "SELECT assetFingerprint, resourceHash, resourceFileName, role, slot FROM asset_resources ORDER BY assetFingerprint, role, slot"
            )
            while let row = try linkCursor.next() {
                let link = RemoteAssetResourceLink(
                    year: year,
                    month: month,
                    assetFingerprint: row[0],
                    resourceHash: row[1],
                    resourceFileName: row[2],
                    role: row[3],
                    slot: row[4]
                )
                links[link.assetFingerprint, default: []].append(link)
            }

            return (resByName, resByHash, assets, links)
        }

        itemsByFileName = loaded.resByName
        itemsByHash = loaded.resByHash
        assetsByFingerprint = loaded.assets
        assetLinksByFingerprint = loaded.links
        existingFileNameSet = Set(loaded.resByName.keys).union(remoteFilesByName.keys)
        rebuildLinkIndexes()
        invalidateCollisionKeyCache()
    }

    // Defensive: case-insensitive volumes / buggy listings can surface the same name twice.
    static func dedupedRemoteFilesByName(
        entries: [RemoteStorageEntry],
        year: Int,
        month: Int
    ) -> [String: RemoteFileMetadata] {
        var result: [String: RemoteFileMetadata] = [:]
        result.reserveCapacity(entries.count)
        for entry in entries where !entry.isDirectory && entry.name != Self.manifestFileName {
            if let existing = result[entry.name] {
                manifestLoadLog.error("[MonthManifestStore] duplicate remote entry month=\(year)-\(month) name=\(entry.name, privacy: .public) existingSize=\(existing.size) duplicateSize=\(entry.size)")
                assertionFailure("Duplicate remote file name in client.list result")
                continue
            }
            result[entry.name] = RemoteFileMetadata(size: entry.size)
        }
        return result
    }
}
