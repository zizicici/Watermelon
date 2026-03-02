import Foundation

final class RemoteIndexSyncService: @unchecked Sendable {
    private actor SyncGate {
        private var busy = false
        private var waiters: [CheckedContinuation<Void, Never>] = []

        func withLock<T>(_ operation: @Sendable () async throws -> T) async rethrows -> T {
            if busy {
                await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                    waiters.append(continuation)
                }
            } else {
                busy = true
            }

            defer {
                if waiters.isEmpty {
                    busy = false
                } else {
                    let next = waiters.removeFirst()
                    next.resume()
                }
            }

            return try await operation()
        }
    }

    private let scanner: RemoteManifestIndexScannerProtocol
    private let snapshotCache: RemoteLibrarySnapshotCache
    private let syncGate = SyncGate()
    private let stateLock = NSLock()
    private var activeRemoteProfileKey: String?
    private var remoteManifestDigests: [LibraryMonthKey: RemoteMonthManifestDigest] = [:]

    init(
        scanner: RemoteManifestIndexScannerProtocol = RemoteManifestIndexScanner(),
        snapshotCache: RemoteLibrarySnapshotCache = RemoteLibrarySnapshotCache()
    ) {
        self.scanner = scanner
        self.snapshotCache = snapshotCache
    }

    func syncIndex(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream? = nil
    ) async throws -> RemoteLibrarySnapshot {
        // `syncIndex` must run sequentially; single-flight gate avoids duplicate scans and cache churn.
        try await syncGate.withLock { [self] in
            try await syncIndexUnlocked(client: client, profile: profile, eventStream: eventStream)
        }
    }

    private func syncIndexUnlocked(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream? = nil
    ) async throws -> RemoteLibrarySnapshot {
        ensureRemoteContext(for: profile)

        let remoteDigests = try await scanner.scanManifestDigests(
            client: client,
            basePath: profile.basePath,
            cancellationController: nil
        )
        let previousDigests = currentRemoteManifestDigests()

        let previousMonths = Set(previousDigests.keys)
        let remoteMonths = Set(remoteDigests.keys)

        var changedMonths = Set<LibraryMonthKey>()
        if previousDigests.isEmpty {
            changedMonths = remoteMonths
        } else {
            for (month, digest) in remoteDigests where previousDigests[month] != digest {
                changedMonths.insert(month)
            }
        }

        let removedMonths = previousMonths.subtracting(remoteMonths)

        if changedMonths.isEmpty, removedMonths.isEmpty {
            let snapshot = snapshotCache.current()
            eventStream?.emit(.log("Remote index unchanged. Month digests matched (\(remoteMonths.count) month(s))."))
            return snapshot
        }

        var monthDeltas: [LibraryMonthKey: RemoteLibraryMonthDelta] = [:]
        monthDeltas.reserveCapacity(changedMonths.count)

        for month in changedMonths.sorted() {
            guard let store = try await MonthManifestStore.loadManifestOnlyIfExists(
                client: client,
                basePath: profile.basePath,
                year: month.year,
                month: month.month
            ) else {
                throw NSError(
                    domain: "RemoteIndexSyncService",
                    code: -21,
                    userInfo: [NSLocalizedDescriptionKey: "Month manifest is missing for \(month.text)."]
                )
            }

            let monthAssets = store.allAssets()
            let monthLinks = monthAssets.flatMap { store.links(forAssetFingerprint: $0.assetFingerprint) }
            monthDeltas[month] = RemoteLibraryMonthDelta(
                month: month,
                resources: store.allItems(),
                assets: monthAssets,
                assetResourceLinks: monthLinks
            )
        }

        var appliedChangedMonths = 0
        var appliedRemovedMonths = 0

        for month in changedMonths.sorted() {
            guard let monthDelta = monthDeltas[month] else { continue }
            if snapshotCache.replaceMonth(
                month,
                resources: monthDelta.resources,
                assets: monthDelta.assets,
                assetResourceLinks: monthDelta.assetResourceLinks
            ) {
                appliedChangedMonths += 1
            }
        }

        for month in removedMonths.sorted() {
            if snapshotCache.removeMonth(month) {
                appliedRemovedMonths += 1
            }
        }

        updateRemoteManifestDigests(remoteDigests)

        let snapshot = snapshotCache.current()
        eventStream?.emit(.remoteIndexSynced(RemoteIndexSyncEvent(
            resourceCount: snapshot.totalResourceCount,
            assetCount: snapshot.totalCount,
            changedMonths: appliedChangedMonths,
            removedMonths: appliedRemovedMonths
        )))

        return snapshot
    }

    func currentSnapshot() -> RemoteLibrarySnapshot {
        snapshotCache.current()
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

    func reset() {
        stateLock.lock()
        activeRemoteProfileKey = nil
        remoteManifestDigests.removeAll()
        stateLock.unlock()
        snapshotCache.reset()
    }

    private func ensureRemoteContext(for profile: ServerProfileRecord) {
        let profileKey = Self.remoteProfileKey(profile)
        var shouldReset = false
        stateLock.lock()
        if activeRemoteProfileKey != profileKey {
            activeRemoteProfileKey = profileKey
            remoteManifestDigests.removeAll()
            shouldReset = true
        }
        stateLock.unlock()
        if shouldReset {
            snapshotCache.reset()
        }
    }

    private static func remoteProfileKey(_ profile: ServerProfileRecord) -> String {
        "\(profile.id ?? 0):\(profile.storageType):\(profile.host):\(profile.basePath)"
    }

    private func currentRemoteManifestDigests() -> [LibraryMonthKey: RemoteMonthManifestDigest] {
        stateLock.lock()
        let digests = remoteManifestDigests
        stateLock.unlock()
        return digests
    }

    private func updateRemoteManifestDigests(_ digests: [LibraryMonthKey: RemoteMonthManifestDigest]) {
        stateLock.lock()
        remoteManifestDigests = digests
        stateLock.unlock()
    }
}
