import Foundation
import os.log

private let syncLog = Logger(subsystem: "com.zizicici.watermelon", category: "SyncTiming")

final class RemoteIndexSyncService: Sendable {
    private static let manifestDownloadRetryLimit = 3

    private actor ScanGate {
        private struct Waiter { let id: UUID; let continuation: CheckedContinuation<Void, Error> }
        private var isLocked = false
        private var waiters: [Waiter] = []

        // Only scans queue here; cancellation never waits for an earlier scan's network request.
        private func acquire() async throws {
            if !isLocked {
                isLocked = true
                return
            }
            let id = UUID()
            try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                    if Task.isCancelled {
                        continuation.resume(throwing: CancellationError())
                    } else {
                        waiters.append(Waiter(id: id, continuation: continuation))
                    }
                }
            } onCancel: {
                Task { await self.cancelWaiter(id) }
            }
        }

        private func cancelWaiter(_ id: UUID) {
            guard let index = waiters.firstIndex(where: { $0.id == id }) else { return }
            let waiter = waiters.remove(at: index)
            waiter.continuation.resume(throwing: CancellationError())
        }

        private func release() {
            if !waiters.isEmpty {
                let next = waiters.removeFirst()
                next.continuation.resume()
            } else {
                isLocked = false
            }
        }

        // Scan serialization does not include business reads, writes or verification.
        func withLock<T>(_ operation: () async throws -> T) async throws -> T {
            try await acquire()
            defer { release() }
            try Task.checkCancellation()
            return try await operation()
        }
    }

    // Hands changed months to concurrent download workers in deterministic order.
    private actor MonthKeyQueue {
        private let months: [LibraryMonthKey]
        private var index = 0
        init(months: [LibraryMonthKey]) { self.months = months }
        func next() -> LibraryMonthKey? {
            guard index < months.count else { return nil }
            defer { index += 1 }
            return months[index]
        }
    }

    // Aggregates worker results and emits progress. record+emit run together under actor isolation, so
    // even with out-of-order worker completion the callback is delivered in monotonic 1, 2, 3, … order.
    private actor SyncProgressAggregator {
        private let total: Int
        private let onProgress: (@Sendable (RemoteSyncProgress) -> Void)?
        private var applied = 0
        private var processed = 0

        init(total: Int, onProgress: (@Sendable (RemoteSyncProgress) -> Void)?) {
            self.total = total
            self.onProgress = onProgress
        }

        func recordAndReport(appliedDelta: Int) {
            applied += appliedDelta
            processed += 1
            onProgress?(RemoteSyncProgress(current: processed, total: total))
        }
        func appliedCount() -> Int { applied }
        func processedCount() -> Int { processed }
    }

    let snapshotCache: RemoteLibrarySnapshotCache
    private let scanGate = ScanGate()
    private let diskCache: RemoteManifestSnapshotDiskCache
    private let persistenceObserverID: UUID

    init(
        snapshotCache: RemoteLibrarySnapshotCache = RemoteLibrarySnapshotCache(),
        diskCache: RemoteManifestSnapshotDiskCache = RemoteManifestSnapshotDiskCache()
    ) {
        self.snapshotCache = snapshotCache
        self.diskCache = diskCache
        persistenceObserverID = snapshotCache.persistedSnapshotInvalidations.addObserver { profileKey in
            diskCache.invalidate(profileKey: profileKey)
        }
    }

    deinit { snapshotCache.persistedSnapshotInvalidations.removeObserver(persistenceObserverID) }

    // Whether a sync may take the shared cache over from another profile. `.claimAlways` is the connect /
    // reload behaviour (a switch is exactly a takeover). `.claimIfUnowned` is for a backup run's preflight:
    // a run whose captured profile lost the cache to a cross-profile connect mid-flight must not steal it
    // back — it throws `foreignSnapshotContextError` instead, and the run degrades to a nil seed lookup.
    enum ContextClaimPolicy: Sendable {
        case claimAlways
        case claimIfUnowned
    }

    static func foreignSnapshotContextError() -> NSError {
        NSError(domain: "RemoteIndexSyncService", code: -40)
    }

    func syncIndex(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream? = nil,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)? = nil,
        layout: MonthManifestStore.ManifestLayout,
        liteMonthsListing: LiteMonthsListingSnapshot? = nil,
        makeClient: (@Sendable () throws -> any RemoteStorageClientProtocol)? = nil,
        downloadConcurrency: Int = 1,
        monthFilter: Set<LibraryMonthKey>? = nil,
        newestMonthFirst: Bool = false,
        contextPolicy: ContextClaimPolicy = .claimAlways,
        shouldStop: @escaping @Sendable () -> Bool = { false }
    ) async throws -> RemoteIndexSyncDigest {
        try await scanGate.withLock {
            try await syncIndexUnlocked(
                client: client,
                profile: profile,
                eventStream: eventStream,
                onSyncProgress: onSyncProgress,
                layout: layout,
                liteMonthsListing: liteMonthsListing,
                makeClient: makeClient,
                downloadConcurrency: downloadConcurrency,
                monthFilter: monthFilter,
                newestMonthFirst: newestMonthFirst,
                contextPolicy: contextPolicy,
                shouldStop: shouldStop
            )
        }
    }

    private func syncIndexUnlocked(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream?,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)?,
        layout: MonthManifestStore.ManifestLayout,
        liteMonthsListing: LiteMonthsListingSnapshot?,
        makeClient: (@Sendable () throws -> any RemoteStorageClientProtocol)?,
        downloadConcurrency: Int,
        monthFilter: Set<LibraryMonthKey>? = nil,
        newestMonthFirst: Bool = false,
        contextPolicy: ContextClaimPolicy = .claimAlways,
        shouldStop: @escaping @Sendable () -> Bool = { false }
    ) async throws -> RemoteIndexSyncDigest {
        let syncStart = CFAbsoluteTimeGetCurrent()
        if shouldStop() { throw CancellationError() }

        let activeProfileKey = Self.remoteProfileKey(profile)
        let scan = try snapshotCache.beginScan(profileKey: activeProfileKey,
            allowContextSwitch: contextPolicy == .claimAlways)
        var allPreviousDigests = scan.digests

        let scanStart = CFAbsoluteTimeGetCurrent()
        var remoteDigests = try await scanManifestDigests(
            client: client,
            basePath: profile.basePath,
            layout: layout,
            liteMonthsListing: liteMonthsListing,
            shouldStop: shouldStop
        )
        if shouldStop() { throw CancellationError() }
        // Stop cancelled scans before they can hydrate or publish data.
        try Task.checkCancellation()
        // A scoped run only reconciles the filtered months. Restrict every set the diff/eviction below derives
        // from — including previous digests — so out-of-scope cached months are left untouched, never evicted.
        if let monthFilter {
            remoteDigests = remoteDigests.filter { monthFilter.contains($0.key) }
        }
        if scan.isNewContext {
            allPreviousDigests = seedOneDriveSnapshotFromDiskIfAvailable(
                scan: scan,
                profile: profile,
                profileKey: activeProfileKey,
                layout: layout,
                remoteDigests: remoteDigests,
                monthFilter: monthFilter
            )
        }
        let scanElapsed = CFAbsoluteTimeGetCurrent() - scanStart
        syncLog.info("[SyncTiming] scanManifestDigests: \(Self.ms(scanElapsed))s (\(remoteDigests.count) months)")

        let previousDigests = monthFilter.map { filter in
            allPreviousDigests.filter { filter.contains($0.key) }
        } ?? allPreviousDigests

        let previousMonths = Set(previousDigests.keys)
        let remoteMonths = Set(remoteDigests.keys)

        var changedMonths = Set<LibraryMonthKey>()
        if previousDigests.isEmpty || scan.requiresReconciliation {
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

        if changedMonths.isEmpty, removedMonths.isEmpty {
            if shouldStop() { throw CancellationError() }
            try Task.checkCancellation()
            try snapshotCache.finishScan(scan, digests: remoteDigests, monthFilter: monthFilter)
            await saveOneDriveSnapshotToDiskIfNeeded(
                profile: profile,
                profileKey: activeProfileKey,
                layout: layout,
                monthFilter: monthFilter,
                scan: scan
            )
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

        // Order changed-month processing to match the run's upload order. This only affects download/log order
        // within the sync phase (sync fully precedes upload, and background discards its per-run cache); the
        // durable newest-first guarantee comes from the upload scheduler + incremental flush, not from here.
        let orderedChangedMonths = changedMonths.sorted(by: newestMonthFirst ? (>) : (<))
        let (appliedChangedMonths, processedMonths) = try await processChangedMonths(
            scan: scan,
            months: orderedChangedMonths,
            client: client,
            basePath: profile.basePath,
            layout: layout,
            totalMonthsToProcess: totalMonthsToProcess,
            makeClient: makeClient,
            downloadConcurrency: downloadConcurrency,
            enableManifestDownloadRetry: profile.resolvedStorageType == .onedrive,
            onSyncProgress: onSyncProgress,
            shouldStop: shouldStop
        )
        if shouldStop() { throw CancellationError() }
        try Task.checkCancellation()

        try snapshotCache.finishScan(scan, digests: remoteDigests, monthFilter: monthFilter)
        if processedMonths < totalMonthsToProcess {
            onSyncProgress?(RemoteSyncProgress(current: totalMonthsToProcess, total: totalMonthsToProcess))
        }
        await saveOneDriveSnapshotToDiskIfNeeded(profile: profile, profileKey: activeProfileKey,
            layout: layout, monthFilter: monthFilter, scan: scan)
        let digest = snapshotCache.counts()
        let totalElapsed = CFAbsoluteTimeGetCurrent() - syncStart
        syncLog.info("[SyncTiming] Sync complete. Total: \(Self.ms(totalElapsed))s, changed: \(appliedChangedMonths), removed: \(removedMonths.count)")

        return digest
    }

    private func seedOneDriveSnapshotFromDiskIfAvailable(
        scan: RemoteLibrarySnapshotCache.Scan,
        profile: ServerProfileRecord,
        profileKey: String,
        layout: MonthManifestStore.ManifestLayout,
        remoteDigests: [LibraryMonthKey: RemoteMonthManifestDigest],
        monthFilter: Set<LibraryMonthKey>?
    ) -> [LibraryMonthKey: RemoteMonthManifestDigest] {
        guard profile.resolvedStorageType == .onedrive, layout == .lite, monthFilter == nil,
              let cached = diskCache.load(profileKey: profileKey, layout: layout) else { return [:] }
        var hydratedDigests: [LibraryMonthKey: RemoteMonthManifestDigest] = [:]
        var hydratedMonthCount = 0
        for monthSnapshot in cached.months {
            guard let cachedDigest = cached.digests[monthSnapshot.month],
                  remoteDigests[monthSnapshot.month] == cachedDigest else { continue }
            _ = snapshotCache.applyScannedMonth(monthSnapshot, scan: scan)
            hydratedDigests[monthSnapshot.month] = cachedDigest
            hydratedMonthCount += 1
        }
        syncLog.info("[SyncTiming] seeded OneDrive remote index cache: \(hydratedMonthCount)/\(cached.months.count) months")
        return hydratedDigests
    }

    private func saveOneDriveSnapshotToDiskIfNeeded(
        profile: ServerProfileRecord, profileKey: String, layout: MonthManifestStore.ManifestLayout,
        monthFilter: Set<LibraryMonthKey>?, scan: RemoteLibrarySnapshotCache.Scan
    ) async {
        guard profile.resolvedStorageType == .onedrive, layout == .lite, monthFilter == nil,
              let snapshot = snapshotCache.scannedSnapshot(for: scan) else { return }
        diskCache.save(profileKey: profileKey, layout: layout, digests: snapshot.digests, months: snapshot.months)
        if !snapshotCache.isCurrent(scan) { diskCache.invalidate(profileKey: profileKey) }
    }

    // Each concurrent download worker owns a separate client.
    private func processChangedMonths(
        scan: RemoteLibrarySnapshotCache.Scan,
        months: [LibraryMonthKey],
        client: RemoteStorageClientProtocol,
        basePath: String,
        layout: MonthManifestStore.ManifestLayout,
        totalMonthsToProcess: Int,
        makeClient: (@Sendable () throws -> any RemoteStorageClientProtocol)?,
        downloadConcurrency: Int,
        enableManifestDownloadRetry: Bool,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)?,
        shouldStop: @escaping @Sendable () -> Bool
    ) async throws -> (applied: Int, processed: Int) {
        let totalWorkers = min(downloadConcurrency, months.count)
        // Legacy scans retain their existing single-client behavior.
        guard let makeClient, totalWorkers >= 2, layout != .v1 else {
            return try await processChangedMonthsSerially(
                scan: scan,
                months: months,
                client: client,
                basePath: basePath,
                layout: layout,
                totalMonthsToProcess: totalMonthsToProcess,
                enableManifestDownloadRetry: enableManifestDownloadRetry,
                onSyncProgress: onSyncProgress,
                shouldStop: shouldStop
            )
        }

        // Fresh pool holds totalWorkers-1 connections; worker 0 reuses the caller-owned primary client, so
        // the data-plane peak is totalWorkers connections — ≤ the backup execution pool, never one more.
        // (The write-lock client, when a run holds one, is carried identically in both phases.)
        let pool = StorageClientPool(maxConnections: totalWorkers - 1, makeClient: makeClient)

        let queue = MonthKeyQueue(months: months)
        let aggregator = SyncProgressAggregator(total: totalMonthsToProcess, onProgress: onSyncProgress)
        do {
            try Task.checkCancellation()
            try await withThrowingTaskGroup(of: Void.self) { group in
                for workerIndex in 0 ..< totalWorkers {
                    group.addTask { [self] in
                        // Worker 0 uses the already-connected primary and starts downloading immediately (no
                        // handshake on the critical path). Other workers open their pooled connection lazily,
                        // concurrently with worker 0's downloads; if a connection can't be established this
                        // worker bows out and the primary (plus any worker that did connect) drains the rest.
                        let isPrimary = workerIndex == 0
                        let workerClient: RemoteStorageClientProtocol
                        if isPrimary {
                            workerClient = client
                        } else {
                            do {
                                workerClient = try await pool.acquire()
                            } catch {
                                if error is CancellationError || Task.isCancelled { throw error }
                                return
                            }
                        }
                        do {
                            try await self.runDownloadWorker(
                                scan: scan,
                                client: workerClient,
                                basePath: basePath,
                                layout: layout,
                                enableManifestDownloadRetry: enableManifestDownloadRetry,
                                queue: queue,
                                aggregator: aggregator,
                                shouldStop: shouldStop
                            )
                        } catch {
                            if !isPrimary { await pool.release(workerClient, reusable: true) }
                            throw error
                        }
                        if !isPrimary { await pool.release(workerClient, reusable: true) }
                    }
                }
                try await group.waitForAll()
            }
        } catch {
            await pool.shutdown()
            throw error
        }
        await pool.shutdown()
        return (await aggregator.appliedCount(), await aggregator.processedCount())
    }

    private func processChangedMonthsSerially(
        scan: RemoteLibrarySnapshotCache.Scan,
        months: [LibraryMonthKey],
        client: RemoteStorageClientProtocol,
        basePath: String,
        layout: MonthManifestStore.ManifestLayout,
        totalMonthsToProcess: Int,
        enableManifestDownloadRetry: Bool,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)?,
        shouldStop: @escaping @Sendable () -> Bool
    ) async throws -> (applied: Int, processed: Int) {
        var applied = 0
        var processed = 0
        for month in months {
            if shouldStop() { throw CancellationError() }
            try Task.checkCancellation()
            let didApply = try await downloadAndApplyMonth(
                scan: scan,
                month: month,
                client: client,
                basePath: basePath,
                layout: layout,
                enableManifestDownloadRetry: enableManifestDownloadRetry
            )
            if shouldStop() { throw CancellationError() }
            if didApply { applied += 1 }
            processed += 1
            onSyncProgress?(RemoteSyncProgress(current: processed, total: totalMonthsToProcess))
        }
        return (applied, processed)
    }

    private func runDownloadWorker(
        scan: RemoteLibrarySnapshotCache.Scan,
        client: RemoteStorageClientProtocol,
        basePath: String,
        layout: MonthManifestStore.ManifestLayout,
        enableManifestDownloadRetry: Bool,
        queue: MonthKeyQueue,
        aggregator: SyncProgressAggregator,
        shouldStop: @escaping @Sendable () -> Bool
    ) async throws {
        while let month = await queue.next() {
            if shouldStop() { throw CancellationError() }
            try Task.checkCancellation()
            let didApply = try await downloadAndApplyMonth(
                scan: scan,
                month: month,
                client: client,
                basePath: basePath,
                layout: layout,
                enableManifestDownloadRetry: enableManifestDownloadRetry
            )
            if shouldStop() { throw CancellationError() }
            await aggregator.recordAndReport(appliedDelta: didApply ? 1 : 0)
        }
    }

    // Downloads one month's manifest and applies it to the snapshot cache; returns whether the cache changed.
    // Scans never publish schema changes; writers own all remote mutations.
    private func downloadAndApplyMonth(
        scan: RemoteLibrarySnapshotCache.Scan,
        month: LibraryMonthKey,
        client: RemoteStorageClientProtocol,
        basePath: String,
        layout: MonthManifestStore.ManifestLayout,
        enableManifestDownloadRetry: Bool
    ) async throws -> Bool {
        let monthStart = CFAbsoluteTimeGetCurrent()
        func missingMonthManifestError() -> NSError {
            NSError(
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
        var store: MonthManifestStore?
        if !enableManifestDownloadRetry {
            store = try await MonthManifestStore.loadManifestDirect(
                client: client,
                basePath: basePath,
                year: month.year,
                month: month.month,
                layout: layout,
                pushSchemaUpgrade: false
            )
            guard let store else {
                throw missingMonthManifestError()
            }
            return try applyDownloadedMonth(store, month: month, startedAt: monthStart, scan: scan)
        }
        for attempt in 0..<Self.manifestDownloadRetryLimit {
            do {
                guard let loaded = try await MonthManifestStore.loadManifestDirect(
                    client: client,
                    basePath: basePath,
                    year: month.year,
                    month: month.month,
                    layout: layout,
                    pushSchemaUpgrade: false,
                    surfaceDownloadNotFound: true,
                    surfaceDownloadFailure: true
                ) else {
                    throw missingMonthManifestError()
                }
                store = loaded
                break
            } catch {
                if MonthManifestStore.isManifestDownloadNotFoundError(error) {
                    throw missingMonthManifestError()
                }
                guard RemoteFaultLite.classify(error) == .retryable,
                      attempt + 1 < Self.manifestDownloadRetryLimit else { throw error }
                syncLog.info("[SyncTiming] Month \(month.text): manifest download retry \(attempt + 1)/\(Self.manifestDownloadRetryLimit)")
                try await Task.sleep(for: .milliseconds(250 * (attempt + 1)))
            }
        }
        guard let store else {
            throw missingMonthManifestError()
        }
        return try applyDownloadedMonth(store, month: month, startedAt: monthStart, scan: scan)
    }

    private func applyDownloadedMonth(
        _ store: MonthManifestStore,
        month: LibraryMonthKey,
        startedAt monthStart: CFAbsoluteTime,
        scan: RemoteLibrarySnapshotCache.Scan
    ) throws -> Bool {
        try Task.checkCancellation()
        let downloadElapsed = CFAbsoluteTimeGetCurrent() - monthStart

        let processStart = CFAbsoluteTimeGetCurrent()
        let snapshot = store.unsortedSnapshot()
        let applied = snapshotCache.applyScannedMonth(RemoteLibraryMonthDelta(month: month,
            resources: snapshot.resources, assets: snapshot.assets, assetResourceLinks: snapshot.links), scan: scan)
        let processElapsed = CFAbsoluteTimeGetCurrent() - processStart
        syncLog.info(
            "[SyncTiming] Month \(month.text): download=\(Self.ms(downloadElapsed))s, process=\(Self.ms(processElapsed))s, assets=\(snapshot.assets.count), resources=\(snapshot.resources.count), links=\(snapshot.links.count)"
        )
        return applied
    }

    // Directory entries occupying a `<YYYY-MM>.sqlite` Lite month slot. The digest scan skips these (a
    // directory is not a readable manifest), so an owned full verify must enumerate them separately to fail
    // closed on damaged control state instead of silently certifying the repo healthy.
    static func directoryValuedLiteMonthSlots(in entries: [RemoteStorageEntry]) -> Set<LibraryMonthKey> {
        Set(entries.compactMap { $0.isDirectory ? RepoLayoutLite.month(fromFilename: $0.name) : nil })
    }

    static func remoteProfileKey(_ profile: ServerProfileRecord) -> String {
        "v2|\(profile.id ?? 0)|\(profile.remoteDestinationIdentity.cacheKeyComponent)"
    }

    func scanManifestDigests(
        client: RemoteStorageClientProtocol,
        basePath: String,
        layout: MonthManifestStore.ManifestLayout,
        cancellationController: BackupCancellationController? = nil,
        liteMonthsListing: LiteMonthsListingSnapshot? = nil,
        shouldStop: @escaping @Sendable () -> Bool = { false }
    ) async throws -> [LibraryMonthKey: RemoteMonthManifestDigest] {
        switch layout {
        case .v1:
            return try await scanV1ManifestDigests(
                client: client,
                basePath: basePath,
                cancellationController: cancellationController,
                shouldStop: shouldStop
            )
        case .lite:
            return try await scanLiteManifestDigests(
                client: client,
                basePath: basePath,
                cancellationController: cancellationController,
                liteMonthsListing: liteMonthsListing,
                shouldStop: shouldStop
            )
        }
    }

    private func scanV1ManifestDigests(
        client: RemoteStorageClientProtocol,
        basePath: String,
        cancellationController: BackupCancellationController?,
        shouldStop: @escaping @Sendable () -> Bool
    ) async throws -> [LibraryMonthKey: RemoteMonthManifestDigest] {
        let manifests = try await V1ManifestScanner(client: client, basePath: basePath).scan(
            checkCancellation: {
                if shouldStop() { throw CancellationError() }
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
        cancellationController: BackupCancellationController?,
        liteMonthsListing: LiteMonthsListingSnapshot?,
        shouldStop: @escaping @Sendable () -> Bool
    ) async throws -> [LibraryMonthKey: RemoteMonthManifestDigest] {
        if shouldStop() { throw CancellationError() }
        try cancellationController?.throwIfCancelled()
        try Task.checkCancellation()

        let entries: [RemoteStorageEntry]
        if let liteMonthsListing {
            entries = try await liteMonthsListing.entries(client: client, basePath: basePath)
        } else {
            let monthsDirectory = RepoLayoutLite.monthsDirectoryPath(basePath: basePath)
            do {
                entries = try await client.list(path: monthsDirectory)
            } catch {
                // Absent months directory means a Lite repo with no months yet — not a fault. Any other
                // failure (offline / permissions) must surface so we never read it as "zero months".
                if RemoteFaultLite.classify(error) == .notFound { return [:] }
                throw error
            }
        }

        var digests: [LibraryMonthKey: RemoteMonthManifestDigest] = [:]
        digests.reserveCapacity(entries.count)
        for entry in entries {
            if shouldStop() { throw CancellationError() }
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
