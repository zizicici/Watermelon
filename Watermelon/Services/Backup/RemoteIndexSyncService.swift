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

    private let scanner: RemoteManifestIndexScanner
    private let snapshotCache: RemoteLibrarySnapshotCache
    private let syncGate = SyncGate()
    private let state = MutableState()

    init(
        scanner: RemoteManifestIndexScanner = RemoteManifestIndexScanner(),
        snapshotCache: RemoteLibrarySnapshotCache = RemoteLibrarySnapshotCache()
    ) {
        self.scanner = scanner
        self.snapshotCache = snapshotCache
    }

    func syncIndex(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream? = nil,
        onMonthSynced: (@Sendable () -> Void)? = nil
    ) async throws -> RemoteLibrarySnapshot {
        try await syncGate.withLock {
            try await syncIndexUnlocked(
                client: client,
                profile: profile,
                eventStream: eventStream,
                onMonthSynced: onMonthSynced
            )
        }
    }

    private func syncIndexUnlocked(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream?,
        onMonthSynced: (@Sendable () -> Void)?
    ) async throws -> RemoteLibrarySnapshot {
        let syncStart = CFAbsoluteTimeGetCurrent()

        let shouldResetSnapshot = await state.ensureRemoteContext(profileKey: Self.remoteProfileKey(profile))
        if shouldResetSnapshot {
            snapshotCache.reset()
        }

        let scanStart = CFAbsoluteTimeGetCurrent()
        let remoteDigests = try await scanner.scanManifestDigests(
            client: client,
            basePath: profile.basePath
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
            for (month, digest) in remoteDigests where previousDigests[month] != digest {
                changedMonths.insert(month)
            }
        }

        let removedMonths = previousMonths.subtracting(remoteMonths)

        if changedMonths.isEmpty, removedMonths.isEmpty {
            let snapshot = snapshotCache.current()
            let totalElapsed = CFAbsoluteTimeGetCurrent() - syncStart
            syncLog.info("[SyncTiming] No changes. Total: \(Self.ms(totalElapsed))s")
            eventStream?.emit(.log("Remote index unchanged. Month digests matched (\(remoteMonths.count) month(s))."))
            return snapshot
        }

        syncLog.info("[SyncTiming] changedMonths: \(changedMonths.count), removedMonths: \(removedMonths.count)")

        var appliedChangedMonths = 0
        var appliedRemovedMonths = 0

        for month in changedMonths.sorted() {
            let monthStart = CFAbsoluteTimeGetCurrent()
            guard let store = try await MonthManifestStore.loadManifestDirect(
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
                onMonthSynced?()
            }
            let processElapsed = CFAbsoluteTimeGetCurrent() - processStart
            syncLog.info("[SyncTiming] Month \(month.text): download=\(Self.ms(downloadElapsed))s, process=\(Self.ms(processElapsed))s, assets=\(snapshot.assets.count)")
        }

        for month in removedMonths.sorted() {
            if snapshotCache.removeMonth(month) {
                appliedRemovedMonths += 1
                onMonthSynced?()
            }
        }

        await state.updateRemoteManifestDigests(remoteDigests)

        let snapshot = snapshotCache.current()
        let totalElapsed = CFAbsoluteTimeGetCurrent() - syncStart
        syncLog.info("[SyncTiming] Sync complete. Total: \(Self.ms(totalElapsed))s, changed: \(appliedChangedMonths), removed: \(appliedRemovedMonths)")

        return snapshot
    }

    func remoteMonthSummaries() -> [(month: LibraryMonthKey, assetCount: Int, photoCount: Int, videoCount: Int, totalSizeBytes: Int64)] {
        snapshotCache.monthSummaries()
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

    private static func remoteProfileKey(_ profile: ServerProfileRecord) -> String {
        "\(profile.id ?? 0):\(profile.storageType):\(profile.host):\(profile.basePath)"
    }

    private static func ms(_ seconds: CFAbsoluteTime) -> String {
        String(format: "%.3f", seconds)
    }
}
