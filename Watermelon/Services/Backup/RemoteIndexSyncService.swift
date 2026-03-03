import Foundation

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

    private let scanner: RemoteManifestIndexScannerProtocol
    private let snapshotCache: RemoteLibrarySnapshotCache
    private let syncGate = SyncGate()
    private let state = MutableState()

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
        try await syncGate.withLock {
            try await syncIndexUnlocked(
                client: client,
                profile: profile,
                eventStream: eventStream
            )
        }
    }

    private func syncIndexUnlocked(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream?
    ) async throws -> RemoteLibrarySnapshot {
        let shouldResetSnapshot = await state.ensureRemoteContext(profileKey: Self.remoteProfileKey(profile))
        if shouldResetSnapshot {
            snapshotCache.reset()
        }

        let remoteDigests = try await scanner.scanManifestDigests(
            client: client,
            basePath: profile.basePath
        )
        let previousDigests = await state.currentRemoteManifestDigests()

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

        var appliedChangedMonths = 0
        var appliedRemovedMonths = 0

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
            if snapshotCache.replaceMonth(
                month,
                resources: store.allItems(),
                assets: monthAssets,
                assetResourceLinks: monthLinks
            ) {
                appliedChangedMonths += 1
            }
        }

        for month in removedMonths.sorted() {
            if snapshotCache.removeMonth(month) {
                appliedRemovedMonths += 1
            }
        }

        await state.updateRemoteManifestDigests(remoteDigests)

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

    func reset() async {
        await state.reset()
        snapshotCache.reset()
    }

    private static func remoteProfileKey(_ profile: ServerProfileRecord) -> String {
        "\(profile.id ?? 0):\(profile.storageType):\(profile.host):\(profile.basePath)"
    }
}
