import Foundation
import os.log

private let v1SyncLog = Logger(subsystem: "com.zizicici.watermelon", category: "SyncTiming")

struct RemoteIndexV1MonthSnapshot: Sendable {
    let resources: [RemoteManifestResource]
    let assets: [RemoteManifestAsset]
    let links: [RemoteAssetResourceLink]
}

struct RemoteIndexV1SyncResult: Sendable {
    let effectiveRemoteDigests: [LibraryMonthKey: RemoteMonthManifestDigest]
    let changedMonths: [LibraryMonthKey: RemoteIndexV1MonthSnapshot]
    let missingMonths: Set<LibraryMonthKey>
    let removedMonths: Set<LibraryMonthKey>
    let remoteMonthCount: Int
    let totalMonthsToProcess: Int
}

struct RemoteIndexV1SyncEngine: Sendable {
    func sync(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        previousDigests: [LibraryMonthKey: RemoteMonthManifestDigest],
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)?
    ) async throws -> RemoteIndexV1SyncResult {
        let scanStart = CFAbsoluteTimeGetCurrent()
        let remoteDigests = try await scanManifestDigests(client: client, basePath: basePath)
        var effectiveRemoteDigests = remoteDigests
        let scanElapsed = CFAbsoluteTimeGetCurrent() - scanStart
        v1SyncLog.info("[SyncTiming] scanManifestDigests: \(Self.ms(scanElapsed))s (\(remoteDigests.count) months)")

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
        v1SyncLog.info("[SyncTiming] changedMonths: \(changedMonths.count), removedMonths: \(removedMonths.count)")

        if changedMonths.isEmpty {
            return RemoteIndexV1SyncResult(
                effectiveRemoteDigests: effectiveRemoteDigests,
                changedMonths: [:],
                missingMonths: [],
                removedMonths: removedMonths,
                remoteMonthCount: remoteMonths.count,
                totalMonthsToProcess: totalMonthsToProcess
            )
        }

        var stagedChangedMonths: [LibraryMonthKey: RemoteIndexV1MonthSnapshot] = [:]
        var stagedMissingMonths = Set<LibraryMonthKey>()
        var processedMonthCount = 0

        for month in changedMonths.sorted() {
            try Task.checkCancellation()
            let monthStart = CFAbsoluteTimeGetCurrent()
            guard let store = try await MonthManifestStore.loadManifestDirect(
                client: client,
                basePath: basePath,
                year: month.year,
                month: month.month
            ) else {
                stagedMissingMonths.insert(month)
                effectiveRemoteDigests.removeValue(forKey: month)
                processedMonthCount += 1
                onSyncProgress?(RemoteSyncProgress(current: processedMonthCount, total: totalMonthsToProcess))
                continue
            }
            let downloadElapsed = CFAbsoluteTimeGetCurrent() - monthStart

            let processStart = CFAbsoluteTimeGetCurrent()
            let snapshot = store.unsortedSnapshot()
            stagedChangedMonths[month] = RemoteIndexV1MonthSnapshot(
                resources: snapshot.resources,
                assets: snapshot.assets,
                links: snapshot.links
            )
            processedMonthCount += 1
            onSyncProgress?(RemoteSyncProgress(current: processedMonthCount, total: totalMonthsToProcess))
            let processElapsed = CFAbsoluteTimeGetCurrent() - processStart
            v1SyncLog.info(
                "[SyncTiming] Month \(month.text): download=\(Self.ms(downloadElapsed))s, process=\(Self.ms(processElapsed))s, assets=\(snapshot.assets.count), resources=\(snapshot.resources.count), links=\(snapshot.links.count)"
            )
        }

        return RemoteIndexV1SyncResult(
            effectiveRemoteDigests: effectiveRemoteDigests,
            changedMonths: stagedChangedMonths,
            missingMonths: stagedMissingMonths,
            removedMonths: removedMonths,
            remoteMonthCount: remoteMonths.count,
            totalMonthsToProcess: totalMonthsToProcess
        )
    }

    private func scanManifestDigests(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> [LibraryMonthKey: RemoteMonthManifestDigest] {
        let normalizedBasePath = RemotePathBuilder.normalizePath(basePath)

        var digests: [LibraryMonthKey: RemoteMonthManifestDigest] = [:]
        // Strict (`.propagate`) so a transient year/month list failure can't be misread as "month gone".
        // `minYear: 1900` and the ascending sort preserve this engine's stricter year domain and order.
        try await V1MonthIterator.forEachMonth(
            client: client,
            basePath: normalizedBasePath,
            options: .init(
                listFailurePolicy: .propagate,
                yearOrder: .ascending,
                monthOrder: .ascending,
                minYear: 1900
            )
        ) { year, month, monthPath in
            try Task.checkCancellation()
            let manifestPath = RemotePathBuilder.absolutePath(
                basePath: monthPath,
                remoteRelativePath: MonthManifestStore.manifestFileName
            )
            let manifestEntry: RemoteStorageEntry?
            do {
                manifestEntry = try await client.metadata(path: manifestPath)
            } catch {
                if isStorageNotFoundError(error) {
                    return .continue
                }
                throw error
            }
            guard let manifestEntry, manifestEntry.isDirectory == false else {
                return .continue
            }

            let monthKey = LibraryMonthKey(year: year, month: month)
            let modifiedMs = manifestEntry.modificationDate?.millisecondsSinceEpoch
            digests[monthKey] = RemoteMonthManifestDigest(
                month: monthKey,
                manifestSize: manifestEntry.size,
                manifestModifiedAtMs: modifiedMs
            )
            return .continue
        }

        return digests
    }

    private static func ms(_ seconds: CFAbsoluteTime) -> String {
        String(format: "%.3f", seconds)
    }
}
