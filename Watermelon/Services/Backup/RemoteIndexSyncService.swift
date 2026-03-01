import Foundation

final class RemoteIndexSyncService: @unchecked Sendable {
    private struct SyncContext {
        let profileKey: String
        let previousDigests: [LibraryMonthKey: RemoteMonthManifestDigest]
        let didResetSnapshot: Bool
    }

    private let scanner: RemoteManifestIndexScannerProtocol
    private let snapshotCache: RemoteLibrarySnapshotCache
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
        eventStream: BackupEventStream? = nil,
        cancellationController: BackupCancellationController? = nil
    ) async throws -> RemoteLibrarySnapshot {
        let context = beginSyncContext(for: profile)
        if context.didResetSnapshot {
            snapshotCache.reset()
        }

        let remoteDigests = try await scanner.scanManifestDigests(
            client: client,
            basePath: profile.basePath,
            cancellationController: cancellationController
        )
        let previousDigests = context.previousDigests

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

        guard isCurrentContext(profileKey: context.profileKey) else {
            eventStream?.emit(.log("Remote index context switched. Ignored stale sync result."))
            return snapshotCache.current()
        }

        for month in changedMonths.sorted() {
            guard isCurrentContext(profileKey: context.profileKey) else {
                eventStream?.emit(.log("Remote index context switched. Ignored stale sync result."))
                return snapshotCache.current()
            }
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
            guard isCurrentContext(profileKey: context.profileKey) else {
                eventStream?.emit(.log("Remote index context switched. Ignored stale sync result."))
                return snapshotCache.current()
            }
            if snapshotCache.removeMonth(month) {
                appliedRemovedMonths += 1
            }
        }

        guard commitRemoteDigests(remoteDigests, profileKey: context.profileKey) else {
            eventStream?.emit(.log("Remote index context switched. Ignored stale sync result."))
            return snapshotCache.current()
        }

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

    private func beginSyncContext(for profile: ServerProfileRecord) -> SyncContext {
        let profileKey = Self.remoteProfileKey(profile)
        stateLock.lock()
        let didSwitch = activeRemoteProfileKey != profileKey
        if didSwitch {
            activeRemoteProfileKey = profileKey
            remoteManifestDigests.removeAll()
        }
        let previousDigests = remoteManifestDigests
        stateLock.unlock()
        return SyncContext(
            profileKey: profileKey,
            previousDigests: previousDigests,
            didResetSnapshot: didSwitch
        )
    }

    private func commitRemoteDigests(
        _ digests: [LibraryMonthKey: RemoteMonthManifestDigest],
        profileKey: String
    ) -> Bool {
        stateLock.lock()
        defer { stateLock.unlock() }
        guard activeRemoteProfileKey == profileKey else {
            return false
        }
        remoteManifestDigests = digests
        return true
    }

    private func isCurrentContext(profileKey: String) -> Bool {
        stateLock.lock()
        defer { stateLock.unlock() }
        return activeRemoteProfileKey == profileKey
    }

    private static func remoteProfileKey(_ profile: ServerProfileRecord) -> String {
        "\(profile.id ?? 0):\(profile.storageType):\(profile.host):\(profile.basePath)"
    }
}
