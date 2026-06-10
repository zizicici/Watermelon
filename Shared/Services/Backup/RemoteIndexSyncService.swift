import Foundation
import os.log

private let syncLog = Logger(subsystem: "com.zizicici.watermelon", category: "SyncTiming")

final class RemoteIndexSyncService: Sendable {
    private actor SyncGate {
        private var isLocked = false
        private var waiters: [CheckedContinuation<Void, Never>] = []

        private func acquire() async {
            if !isLocked {
                isLocked = true
                return
            }
            await withCheckedContinuation { continuation in
                waiters.append(continuation)
            }
        }

        private func release() {
            if !waiters.isEmpty {
                let next = waiters.removeFirst()
                next.resume()
            } else {
                isLocked = false
            }
        }

        func withLock<T>(_ operation: () async throws -> T) async throws -> T {
            await acquire()
            defer { release() }
            try Task.checkCancellation()
            return try await operation()
        }
    }

    private actor MutableState {
        private var activeRemoteProfileKey: String?
        private var remoteManifestDigests: [LibraryMonthKey: RemoteMonthManifestDigest] = [:]

        func ensureRemoteContext(profileKey: String) -> Bool {
            guard activeRemoteProfileKey != profileKey else { return false }
            activeRemoteProfileKey = profileKey
            remoteManifestDigests.removeAll()
            return true
        }

        func currentRemoteManifestDigests() -> [LibraryMonthKey: RemoteMonthManifestDigest] {
            remoteManifestDigests
        }

        func updateRemoteManifestDigests(_ digests: [LibraryMonthKey: RemoteMonthManifestDigest]) {
            remoteManifestDigests = digests
        }

        func reset() {
            activeRemoteProfileKey = nil
            remoteManifestDigests.removeAll()
        }
    }

    private let snapshotCache: RemoteLibrarySnapshotCache
    private let syncGate = SyncGate()
    private let state = MutableState()

    init(
        snapshotCache: RemoteLibrarySnapshotCache = RemoteLibrarySnapshotCache()
    ) {
        self.snapshotCache = snapshotCache
    }

    func syncIndex(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream? = nil,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)? = nil,
        layout: MonthManifestStore.ManifestLayout = .v1
    ) async throws -> RemoteIndexSyncDigest {
        try await syncGate.withLock {
            try await syncIndexUnlocked(
                client: client,
                profile: profile,
                eventStream: eventStream,
                onSyncProgress: onSyncProgress,
                layout: layout
            )
        }
    }

    /// Materialize a flat-array `RemoteLibrarySnapshot` from the current cache. O(N) in
    /// cached entries; call only when a caller actually needs the flat arrays (the
    /// `MonthSeedLookup` in `BackupRunPreparation` is the only current use).
    func fullSnapshot() -> RemoteLibrarySnapshot {
        snapshotCache.current()
    }

    private func syncIndexUnlocked(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream?,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)?,
        layout: MonthManifestStore.ManifestLayout
    ) async throws -> RemoteIndexSyncDigest {
        let syncStart = CFAbsoluteTimeGetCurrent()

        let shouldResetSnapshot = await state.ensureRemoteContext(profileKey: Self.remoteProfileKey(profile))
        if shouldResetSnapshot {
            snapshotCache.reset()
        }

        let scanStart = CFAbsoluteTimeGetCurrent()
        let remoteDigests = try await scanManifestDigests(
            client: client,
            basePath: profile.basePath,
            layout: layout
        )
        let scanElapsed = CFAbsoluteTimeGetCurrent() - scanStart
        syncLog.info("[SyncTiming] scanManifestDigests: \(Self.ms(scanElapsed))s (\(remoteDigests.count) months)")

        let previousDigests = await state.currentRemoteManifestDigests()

        let previousMonths = Set(previousDigests.keys)
        let remoteMonths = Set(remoteDigests.keys)

        var changedMonths = Set<LibraryMonthKey>()
        if previousDigests.isEmpty {
            changedMonths = remoteMonths
        } else {
            for (month, digest) in remoteDigests {
                if previousDigests[month] != digest || digest.manifestModifiedAtMs == nil {
                    changedMonths.insert(month)
                }
            }
        }

        let removedMonths = previousMonths.subtracting(remoteMonths)
        let totalMonthsToProcess = changedMonths.count + removedMonths.count
        onSyncProgress?(RemoteSyncProgress(current: 0, total: totalMonthsToProcess))

        // Evict unanchored cache entries: months in snapshotCache but absent from both current
        // and previous remote digests. These can arise from optimistic uploads whose flush never
        // committed a month sqlite.
        let cachedMonths = snapshotCache.allKnownMonths()
        let anchoredMonths = remoteMonths.union(previousMonths)
        let unanchored = cachedMonths.subtracting(anchoredMonths)
        for month in unanchored {
            _ = snapshotCache.removeMonth(month)
        }

        if changedMonths.isEmpty, removedMonths.isEmpty {
            snapshotCache.markSynced(Date())
            let digest = snapshotCache.counts()
            let totalElapsed = CFAbsoluteTimeGetCurrent() - syncStart
            syncLog.info("[SyncTiming] No changes. Total: \(Self.ms(totalElapsed))s")
            eventStream?.emitLog(
                String.localizedStringWithFormat(String(localized: "backup.remoteIndex.unchanged"), remoteMonths.count),
                level: .debug
            )
            return digest
        }

        syncLog.info("[SyncTiming] changedMonths: \(changedMonths.count), removedMonths: \(removedMonths.count)")

        var appliedChangedMonths = 0
        var appliedRemovedMonths = 0
        var processedMonthCount = 0

        for month in changedMonths.sorted() {
            let monthStart = CFAbsoluteTimeGetCurrent()
            // Sync is a pure read of remote manifests: never let a schema-version difference trigger
            // a remote write here for Lite. V1 keeps its existing schema-push behavior.
            guard let store = try await MonthManifestStore.loadManifestDirect(
                client: client,
                basePath: profile.basePath,
                year: month.year,
                month: month.month,
                layout: layout,
                pushSchemaUpgrade: layout == .v1
            ) else {
                throw NSError(
                    domain: "RemoteIndexSyncService",
                    code: -21,
                    userInfo: [
                        NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                            String(localized: "backup.remoteIndex.error.missingMonthManifest"),
                            month.text
                        )
                    ]
                )
            }
            let downloadElapsed = CFAbsoluteTimeGetCurrent() - monthStart

            let processStart = CFAbsoluteTimeGetCurrent()
            let snapshot = store.unsortedSnapshot()
            if snapshotCache.replaceMonth(
                month,
                resources: snapshot.resources,
                assets: snapshot.assets,
                assetResourceLinks: snapshot.links
            ) {
                appliedChangedMonths += 1
            }
            processedMonthCount += 1
            onSyncProgress?(RemoteSyncProgress(current: processedMonthCount, total: totalMonthsToProcess))
            let processElapsed = CFAbsoluteTimeGetCurrent() - processStart
            syncLog.info(
                "[SyncTiming] Month \(month.text): download=\(Self.ms(downloadElapsed))s, process=\(Self.ms(processElapsed))s, assets=\(snapshot.assets.count), resources=\(snapshot.resources.count), links=\(snapshot.links.count)"
            )
        }

        for month in removedMonths.sorted() {
            if snapshotCache.removeMonth(month) {
                appliedRemovedMonths += 1
            }
            processedMonthCount += 1
            onSyncProgress?(RemoteSyncProgress(current: processedMonthCount, total: totalMonthsToProcess))
        }

        // Keep the scan-time digests even for months rewritten by
        // loadManifestDirect: the stale cache costs one extra download per
        // upgraded month next sync, while refreshing post-flush would race
        // with concurrent remote writers and silently drop their updates.
        await state.updateRemoteManifestDigests(remoteDigests)

        snapshotCache.markSynced(Date())
        let digest = snapshotCache.counts()
        let totalElapsed = CFAbsoluteTimeGetCurrent() - syncStart
        syncLog.info("[SyncTiming] Sync complete. Total: \(Self.ms(totalElapsed))s, changed: \(appliedChangedMonths), removed: \(appliedRemovedMonths)")

        return digest
    }

    /// Backup workers reconcile inline via `MonthManifestStore.loadOrCreate`.
    /// `assertOwnership`, when provided (Lite write lease), must confirm ownership before the reconcile
    /// flush; a `false` result fails closed so we never push a manifest we no longer own.
    func verifyMonth(
        client: RemoteStorageClientProtocol,
        basePath: String,
        month: LibraryMonthKey,
        layout: MonthManifestStore.ManifestLayout = .v1,
        assertOwnership: (@Sendable () async -> Bool)? = nil
    ) async throws {
        let monthRelativePath = String(format: "%04d/%02d", month.year, month.month)
        let manifestPath = layout.manifestAbsolutePath(basePath: basePath, year: month.year, month: month.month)
        // Pre-check distinguishes "manifest gone" (drop stale cache entry) from "download failed" (error); `loadManifestDirect` collapses both into nil.
        guard let metadata = try await client.metadata(path: manifestPath),
              !metadata.isDirectory else {
            _ = snapshotCache.removeMonth(month)
            return
        }

        guard let store = try await MonthManifestStore.loadManifestDirect(
            client: client,
            basePath: basePath,
            year: month.year,
            month: month.month,
            layout: layout,
            manifestAbsolutePath: manifestPath,
            pushSchemaUpgrade: layout == .v1,
            assertOwnership: assertOwnership
        ) else {
            throw NSError(
                domain: "RemoteIndexSyncService",
                code: -1,
                userInfo: [
                    NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                        String(localized: "backup.manifest.error.downloadExistingManifest"),
                        monthRelativePath
                    )
                ]
            )
        }

        let internalResult = try store.reconcileMonth()

        let monthAbsolutePath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: monthRelativePath
        )
        let remoteFileNames: Set<String>
        if layout == .lite {
            // Lite stores month truth in .watermelon/months; a confirmed missing data directory collapses
            // to an empty listing, but any other fault surfaces. A destructive prune (whole-month clear or
            // large-ratio) of the manifest must be confirmed by a second listing before it is applied.
            let listing = try await LiteDataDirectoryProbe.probe(client: client, monthAbsolutePath: monthAbsolutePath)
            switch await LiteDataDirectoryProbe.confirmPrune(
                client: client,
                monthAbsolutePath: monthAbsolutePath,
                initial: listing,
                manifestFileNames: store.existingFileNames()
            ) {
            case .reconcile(let names, _):
                remoteFileNames = names
            case .skip:
                remoteFileNames = store.existingFileNames()   // no listing-based prune this run
            }
        } else {
            let entries = try await client.list(path: monthAbsolutePath)
            remoteFileNames = Set(entries
                .filter { !$0.isDirectory && $0.name != MonthManifestStore.manifestFileName }
                .map(\.name))
        }
        let listingMissing = store.existingFileNames().subtracting(remoteFileNames)
        let listingResult = try store.reconcileMonth(missingFileNames: listingMissing)

        let touched = internalResult.removedResourceCount + internalResult.removedAssetCount
            + internalResult.removedOrphanLinkCount
            + listingResult.removedResourceCount + listingResult.removedAssetCount
            + listingResult.removedOrphanLinkCount
        guard touched > 0 else { return }

        if store.dirty {
            // Reconcile/flush is a Lite write: the store-owned ownership gate inside flushToRemote
            // re-asserts the lease and fails closed if it is lost.
            try await store.flushToRemote()
        }
        let snapshot = store.unsortedSnapshot()
        _ = snapshotCache.replaceMonth(
            month,
            resources: snapshot.resources,
            assets: snapshot.assets,
            assetResourceLinks: snapshot.links
        )
        syncLog.info("[verify] \(month.text): internal=\(internalResult.removedAssetCount)+\(internalResult.removedOrphanLinkCount)L, listing=\(listingResult.removedAssetCount)+\(listingResult.removedOrphanLinkCount)L")
    }

    func remoteMonthSummaries() -> [(month: LibraryMonthKey, assetCount: Int, photoCount: Int, videoCount: Int, totalSizeBytes: Int64)] {
        snapshotCache.monthSummaries()
    }

    func healthDigest() -> RemoteHealthDigest {
        snapshotCache.healthDigest()
    }

    func allKnownMonths() -> Set<LibraryMonthKey> {
        snapshotCache.allKnownMonths()
    }

    func remoteMonthRawData(for month: LibraryMonthKey) -> RemoteLibraryMonthDelta? {
        snapshotCache.monthRawData(for: month)
    }

    func currentState(since revision: UInt64?) -> RemoteLibrarySnapshotState {
        snapshotCache.state(since: revision)
    }

    func upsertCachedResource(_ item: RemoteManifestResource) {
        snapshotCache.upsertResource(item)
    }

    func upsertCachedAsset(_ asset: RemoteManifestAsset, links: [RemoteAssetResourceLink]? = nil) {
        snapshotCache.upsertAsset(asset, links: links)
    }

    func replaceCachedMonth(
        _ month: LibraryMonthKey,
        resources: [RemoteManifestResource],
        assets: [RemoteManifestAsset],
        links: [RemoteAssetResourceLink]
    ) {
        _ = snapshotCache.replaceMonth(month, resources: resources, assets: assets, assetResourceLinks: links)
    }

    private static func remoteProfileKey(_ profile: ServerProfileRecord) -> String {
        [
            String(profile.id ?? 0),
            profile.storageType,
            profile.host,
            String(profile.port),
            profile.shareName,
            profile.basePath,
            profile.username,
            profile.domain ?? ""
        ].joined(separator: "|")
    }

    func scanManifestDigests(
        client: RemoteStorageClientProtocol,
        basePath: String,
        layout: MonthManifestStore.ManifestLayout = .v1,
        cancellationController: BackupCancellationController? = nil
    ) async throws -> [LibraryMonthKey: RemoteMonthManifestDigest] {
        switch layout {
        case .v1:
            return try await scanV1ManifestDigests(
                client: client,
                basePath: basePath,
                cancellationController: cancellationController
            )
        case .lite:
            return try await scanLiteManifestDigests(
                client: client,
                basePath: basePath,
                cancellationController: cancellationController
            )
        }
    }

    private func scanV1ManifestDigests(
        client: RemoteStorageClientProtocol,
        basePath: String,
        cancellationController: BackupCancellationController?
    ) async throws -> [LibraryMonthKey: RemoteMonthManifestDigest] {
        let manifests = try await V1ManifestScanner(client: client, basePath: basePath).scan(
            checkCancellation: {
                try cancellationController?.throwIfCancelled()
                try Task.checkCancellation()
            }
        )

        var digests: [LibraryMonthKey: RemoteMonthManifestDigest] = [:]
        digests.reserveCapacity(manifests.count)
        for manifest in manifests {
            digests[manifest.month] = RemoteMonthManifestDigest(
                month: manifest.month,
                manifestSize: manifest.size,
                manifestModifiedAtMs: manifest.modificationDate?.millisecondsSinceEpoch
            )
        }
        return digests
    }

    private func scanLiteManifestDigests(
        client: RemoteStorageClientProtocol,
        basePath: String,
        cancellationController: BackupCancellationController?
    ) async throws -> [LibraryMonthKey: RemoteMonthManifestDigest] {
        try cancellationController?.throwIfCancelled()
        try Task.checkCancellation()

        let monthsDirectory = RepoLayoutLite.monthsDirectoryPath(basePath: basePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: monthsDirectory)
        } catch {
            // Absent months directory means a Lite repo with no months yet — not a fault. Any other
            // failure (offline / permissions) must surface so we never read it as "zero months".
            if RemoteFaultLite.classify(error) == .notFound { return [:] }
            throw error
        }

        var digests: [LibraryMonthKey: RemoteMonthManifestDigest] = [:]
        digests.reserveCapacity(entries.count)
        for entry in entries {
            try cancellationController?.throwIfCancelled()
            try Task.checkCancellation()
            guard !entry.isDirectory, let month = RepoLayoutLite.month(fromFilename: entry.name) else { continue }
            digests[month] = RemoteMonthManifestDigest(
                month: month,
                manifestSize: entry.size,
                manifestModifiedAtMs: entry.modificationDate?.millisecondsSinceEpoch
            )
        }
        return digests
    }

    private static func ms(_ seconds: CFAbsoluteTime) -> String {
        String(format: "%.3f", seconds)
    }
}
