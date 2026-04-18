import Foundation
import Photos
import os.log

private let localHashIndexLog = Logger(subsystem: "com.zizicici.watermelon", category: "LocalHashIndex")

typealias LocalHashIndexProgressHandler = @Sendable (String, ExecutionLogLevel) async -> Void

struct LocalHashIndexBuildResult: Sendable {
    let requestedAssetIDs: Set<String>
    let readyAssetIDs: Set<String>
    let unavailableAssetIDs: Set<String>
    let failedAssetIDs: Set<String>
    let missingAssetIDs: Set<String>

    var incompleteAssetIDs: Set<String> {
        unavailableAssetIDs.union(failedAssetIDs)
    }
}

private struct LocalHashIndexAssetRef: @unchecked Sendable {
    let asset: PHAsset
}

private enum LocalHashIndexAssetOutcome: Sendable {
    case ready(String)
    case unavailable(String)
    case failed(String)
}

private struct LocalHashIndexWorkerResult: Sendable {
    var readyAssetIDs = Set<String>()
    var unavailableAssetIDs = Set<String>()
    var failedAssetIDs = Set<String>()

    mutating func record(_ outcome: LocalHashIndexAssetOutcome) {
        switch outcome {
        case .ready(let assetID):
            readyAssetIDs.insert(assetID)
        case .unavailable(let assetID):
            unavailableAssetIDs.insert(assetID)
        case .failed(let assetID):
            failedAssetIDs.insert(assetID)
        }
    }
}

private struct LocalHashIndexProcessedAssetResult: Sendable {
    let outcome: LocalHashIndexAssetOutcome
    let reusedCache: Bool
}

private actor LocalHashIndexWorklist {
    private let assets: [LocalHashIndexAssetRef]
    private var nextIndex = 0

    init(assets: [LocalHashIndexAssetRef]) {
        self.assets = assets
    }

    func nextAsset() -> LocalHashIndexAssetRef? {
        guard nextIndex < assets.count else { return nil }
        let asset = assets[nextIndex]
        nextIndex += 1
        return asset
    }
}

private actor LocalHashIndexBuildProgressReporter {
    private static let logCountStep = 500

    private let total: Int
    private let phaseLabel: String
    private let logHandler: LocalHashIndexProgressHandler?

    private var processed = 0
    private var cacheHitCount = 0
    private var rebuiltCount = 0
    private var unavailableCount = 0
    private var failedCount = 0
    private var lastLoggedProcessed = 0

    init(
        total: Int,
        phaseLabel: String,
        logHandler: LocalHashIndexProgressHandler?
    ) {
        self.total = total
        self.phaseLabel = phaseLabel
        self.logHandler = logHandler
    }

    func record(_ result: LocalHashIndexProcessedAssetResult) async {
        processed += 1
        switch result.outcome {
        case .ready:
            if result.reusedCache {
                cacheHitCount += 1
            } else {
                rebuiltCount += 1
            }
        case .unavailable:
            unavailableCount += 1
        case .failed:
            failedCount += 1
        }
        await maybeLog(force: false)
    }

    func finish() async {
        await maybeLog(force: true)
    }

    private func maybeLog(force: Bool) async {
        guard let logHandler, total > 0 else { return }

        let shouldLog = force
            || processed == total
            || processed - lastLoggedProcessed >= Self.logCountStep
        guard shouldLog else { return }
        guard processed > lastLoggedProcessed || force else { return }

        lastLoggedProcessed = processed
        await logHandler(
            String.localizedStringWithFormat(
                String(localized: "backup.preflight.progressBuild"),
                phaseLabel,
                processed,
                total,
                cacheHitCount,
                rebuiltCount,
                unavailableCount,
                failedCount
            ),
            .debug
        )
    }
}

private struct LocalHashIndexBuildPreparedInput: Sendable {
    let cachedHashesByAssetID: [String: LocalAssetHashCache]
    let assets: [LocalHashIndexAssetRef]
    let missingAssetIDs: Set<String>
}

private struct LocalHashIndexAssetFetchInput: Sendable {
    let assets: [LocalHashIndexAssetRef]
    let missingAssetIDs: Set<String>
}

final class LocalHashIndexBuildService: @unchecked Sendable {
    private let photoLibraryService: PhotoLibraryService
    private let repository: ContentHashIndexRepository

    init(
        photoLibraryService: PhotoLibraryService,
        repository: ContentHashIndexRepository
    ) {
        self.photoLibraryService = photoLibraryService
        self.repository = repository
    }

    func buildIndex(
        for assetIDs: Set<String>,
        workerCount: Int = 2,
        allowNetworkAccess: Bool = false,
        progressHandler: LocalHashIndexProgressHandler? = nil
    ) async throws -> LocalHashIndexBuildResult {
        guard !assetIDs.isEmpty else {
            return LocalHashIndexBuildResult(
                requestedAssetIDs: [],
                readyAssetIDs: [],
                unavailableAssetIDs: [],
                failedAssetIDs: [],
                missingAssetIDs: []
            )
        }

        let startedAt = Date()
        localHashIndexLog.info(
            "[LocalHashIndex] build started at \(startedAt.ISO8601Format(), privacy: .public), assets=\(assetIDs.count), workers=\(workerCount), network=\(allowNetworkAccess)"
        )

        do {
            try await ensurePhotoAuthorization()

            let phaseLabel = Self.phaseLabel(forNetworkAccess: allowNetworkAccess)
            let prepareStart = CFAbsoluteTimeGetCurrent()
            await progressHandler?(
                String.localizedStringWithFormat(
                    String(localized: "backup.preflight.prepareStart"),
                    phaseLabel,
                    assetIDs.count
                ),
                .info
            )
            let preparedInput = try await prepareInput(for: assetIDs)
            let prepareElapsed = CFAbsoluteTimeGetCurrent() - prepareStart
            let worklist = LocalHashIndexWorklist(assets: preparedInput.assets)
            await progressHandler?(
                String.localizedStringWithFormat(
                    String(localized: "backup.preflight.prepareDone"),
                    phaseLabel,
                    preparedInput.assets.count,
                    preparedInput.cachedHashesByAssetID.count,
                    preparedInput.missingAssetIDs.count,
                    Self.formatElapsed(prepareElapsed)
                ),
                .debug
            )

            let effectiveWorkerCount = min(max(workerCount, 1), max(preparedInput.assets.count, 1))
            await progressHandler?(
                String.localizedStringWithFormat(
                    String(localized: "backup.preflight.scanStart"),
                    phaseLabel,
                    effectiveWorkerCount
                ),
                .info
            )
            let progressReporter = LocalHashIndexBuildProgressReporter(
                total: preparedInput.assets.count,
                phaseLabel: phaseLabel,
                logHandler: progressHandler
            )
            let scanStart = CFAbsoluteTimeGetCurrent()
            let aggregate = try await withThrowingTaskGroup(of: LocalHashIndexWorkerResult.self) { group in
                for _ in 0 ..< effectiveWorkerCount {
                    group.addTask { [self, progressReporter] in
                        var result = LocalHashIndexWorkerResult()

                        while let assetRef = await worklist.nextAsset() {
                            try Task.checkCancellation()
                            let cachedLocalHash = preparedInput.cachedHashesByAssetID[assetRef.asset.localIdentifier]
                            let processedAsset = try await processAsset(
                                assetRef.asset,
                                cachedLocalHash: cachedLocalHash,
                                allowNetworkAccess: allowNetworkAccess
                            )
                            result.record(processedAsset.outcome)
                            await progressReporter.record(processedAsset)
                        }

                        return result
                    }
                }

                var aggregate = LocalHashIndexWorkerResult()
                for try await result in group {
                    aggregate.readyAssetIDs.formUnion(result.readyAssetIDs)
                    aggregate.unavailableAssetIDs.formUnion(result.unavailableAssetIDs)
                    aggregate.failedAssetIDs.formUnion(result.failedAssetIDs)
                }
                return aggregate
            }
            await progressReporter.finish()
            let scanElapsed = CFAbsoluteTimeGetCurrent() - scanStart
            await progressHandler?(
                String.localizedStringWithFormat(
                    String(localized: "backup.preflight.scanDone"),
                    phaseLabel,
                    Self.formatElapsed(scanElapsed)
                ),
                .debug
            )

            let result = LocalHashIndexBuildResult(
                requestedAssetIDs: assetIDs,
                readyAssetIDs: aggregate.readyAssetIDs,
                unavailableAssetIDs: aggregate.unavailableAssetIDs,
                failedAssetIDs: aggregate.failedAssetIDs,
                missingAssetIDs: preparedInput.missingAssetIDs
            )
            let endedAt = Date()
            let elapsed = endedAt.timeIntervalSince(startedAt)
            localHashIndexLog.info(
                "[LocalHashIndex] build finished at \(endedAt.ISO8601Format(), privacy: .public), elapsed=\(String(format: "%.3f", elapsed), privacy: .public)s, ready=\(result.readyAssetIDs.count), unavailable=\(result.unavailableAssetIDs.count), failed=\(result.failedAssetIDs.count), missing=\(result.missingAssetIDs.count)"
            )
            return result
        } catch is CancellationError {
            let endedAt = Date()
            let elapsed = endedAt.timeIntervalSince(startedAt)
            localHashIndexLog.info(
                "[LocalHashIndex] build cancelled at \(endedAt.ISO8601Format(), privacy: .public), elapsed=\(String(format: "%.3f", elapsed), privacy: .public)s"
            )
            throw CancellationError()
        } catch {
            let endedAt = Date()
            let elapsed = endedAt.timeIntervalSince(startedAt)
            localHashIndexLog.error(
                "[LocalHashIndex] build failed at \(endedAt.ISO8601Format(), privacy: .public), elapsed=\(String(format: "%.3f", elapsed), privacy: .public)s, error=\(error.localizedDescription, privacy: .public)"
            )
            throw error
        }
    }

    private func ensurePhotoAuthorization() async throws {
        let status = photoLibraryService.authorizationStatus()
        if status != .authorized && status != .limited {
            let requested = await photoLibraryService.requestAuthorization()
            guard requested == .authorized || requested == .limited else {
                throw BackupError.photoPermissionDenied
            }
        }
    }

    private func prepareInput(
        for assetIDs: Set<String>
    ) async throws -> LocalHashIndexBuildPreparedInput {
        async let cachedHashesByAssetID = loadCachedHashes(for: assetIDs)
        async let assetFetchInput = prepareAssetFetchInput(for: assetIDs)

        let preparedAssets = try await assetFetchInput
        let cachedHashes = try await cachedHashesByAssetID
        return LocalHashIndexBuildPreparedInput(
            cachedHashesByAssetID: cachedHashes,
            assets: preparedAssets.assets,
            missingAssetIDs: preparedAssets.missingAssetIDs
        )
    }

    private func prepareAssetFetchInput(
        for assetIDs: Set<String>
    ) async throws -> LocalHashIndexAssetFetchInput {
        let photoLibraryService = self.photoLibraryService
        let task = Task.detached(priority: .userInitiated) {
            try Task.checkCancellation()
            let fetchedAssets = photoLibraryService.fetchAssets(localIdentifiers: assetIDs)
            let sortedAssets = fetchedAssets.sorted { lhs, rhs in
                let lhsDate = lhs.creationDate ?? .distantPast
                let rhsDate = rhs.creationDate ?? .distantPast
                if lhsDate != rhsDate {
                    return lhsDate < rhsDate
                }
                return lhs.localIdentifier < rhs.localIdentifier
            }
            let resolvedAssetIDs = Set(sortedAssets.map(\.localIdentifier))
            return LocalHashIndexAssetFetchInput(
                assets: sortedAssets.map(LocalHashIndexAssetRef.init(asset:)),
                missingAssetIDs: assetIDs.subtracting(resolvedAssetIDs)
            )
        }

        return try await withTaskCancellationHandler {
            try await task.value
        } onCancel: {
            task.cancel()
        }
    }

    private func loadCachedHashes(
        for assetIDs: Set<String>
    ) async throws -> [String: LocalAssetHashCache] {
        let repository = self.repository
        let task = Task.detached(priority: .userInitiated) {
            try Task.checkCancellation()
            return try repository.fetchAssetHashCaches(assetIDs: assetIDs)
        }

        return try await withTaskCancellationHandler {
            try await task.value
        } onCancel: {
            task.cancel()
        }
    }

    private func processAsset(
        _ asset: PHAsset,
        cachedLocalHash: LocalAssetHashCache?,
        allowNetworkAccess: Bool
    ) async throws -> LocalHashIndexProcessedAssetResult {
        let selectedResources = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(
            from: PHAssetResource.assetResources(for: asset)
        )
        guard !selectedResources.isEmpty else {
            return LocalHashIndexProcessedAssetResult(
                outcome: .failed(asset.localIdentifier),
                reusedCache: false
            )
        }

        if canReuseCache(
            asset: asset,
            selectedResources: selectedResources,
            cachedLocalHash: cachedLocalHash
        ) {
            // Cache fingerprint stays valid across iCloud eviction, but the resource
            // bytes may have been offloaded since — probe to keep `unavailable` signal
            // accurate for the worker-count downgrade decision.
            if !allowNetworkAccess {
                do {
                    for selected in selectedResources {
                        try Task.checkCancellation()
                        let isLocal = try await photoLibraryService.isResourceLocallyAvailable(
                            selected.resource
                        )
                        if !isLocal {
                            return LocalHashIndexProcessedAssetResult(
                                outcome: .unavailable(asset.localIdentifier),
                                reusedCache: true
                            )
                        }
                    }
                } catch is CancellationError {
                    throw CancellationError()
                } catch {
                    return LocalHashIndexProcessedAssetResult(
                        outcome: .failed(asset.localIdentifier),
                        reusedCache: true
                    )
                }
            }
            return LocalHashIndexProcessedAssetResult(
                outcome: .ready(asset.localIdentifier),
                reusedCache: true
            )
        }

        do {
            var roleSlotHashes: [(role: Int, slot: Int, contentHash: Data, fileSize: Int64)] = []
            roleSlotHashes.reserveCapacity(selectedResources.count)
            var totalFileSizeBytes: Int64 = 0

            for selected in selectedResources {
                try Task.checkCancellation()
                let exported = try await photoLibraryService.exportResourceToTempFileAndDigest(
                    selected.resource,
                    allowNetworkAccess: allowNetworkAccess
                )
                defer { try? FileManager.default.removeItem(at: exported.fileURL) }

                let localFileSize = max(
                    PhotoLibraryService.resourceFileSize(selected.resource),
                    exported.fileSize
                )
                totalFileSizeBytes += max(localFileSize, 0)
                roleSlotHashes.append((
                    role: selected.role,
                    slot: selected.slot,
                    contentHash: exported.contentHash,
                    fileSize: localFileSize
                ))
            }

            let fingerprint = BackupAssetResourcePlanner.assetFingerprint(
                resourceRoleSlotHashes: roleSlotHashes.map { item in
                    (role: item.role, slot: item.slot, contentHash: item.contentHash)
                }
            )

            try repository.upsertAssetHashSnapshot(
                assetLocalIdentifier: asset.localIdentifier,
                assetFingerprint: fingerprint,
                resources: roleSlotHashes.map { item in
                    LocalAssetResourceHashRecord(
                        role: item.role,
                        slot: item.slot,
                        contentHash: item.contentHash,
                        fileSize: item.fileSize
                    )
                },
                totalFileSizeBytes: totalFileSizeBytes,
                modificationDateNs: asset.modificationDate?.nanosecondsSinceEpoch
            )
            return LocalHashIndexProcessedAssetResult(
                outcome: .ready(asset.localIdentifier),
                reusedCache: false
            )
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            if !allowNetworkAccess && PhotoLibraryService.isNetworkAccessRequiredError(error) {
                return LocalHashIndexProcessedAssetResult(
                    outcome: .unavailable(asset.localIdentifier),
                    reusedCache: false
                )
            }
            return LocalHashIndexProcessedAssetResult(
                outcome: .failed(asset.localIdentifier),
                reusedCache: false
            )
        }
    }

    private func canReuseCache(
        asset: PHAsset,
        selectedResources: [BackupSelectedResource],
        cachedLocalHash: LocalAssetHashCache?
    ) -> Bool {
        guard let cachedLocalHash else { return false }
        guard cachedLocalHash.resourceCount == selectedResources.count else { return false }

        if let modificationDate = asset.modificationDate,
           modificationDate > cachedLocalHash.updatedAt {
            return false
        }

        for selected in selectedResources {
            let key = AssetResourceRoleSlot(role: selected.role, slot: selected.slot)
            guard cachedLocalHash.hashesByRoleSlot[key] != nil else {
                return false
            }
        }

        return true
    }

    private static func phaseLabel(forNetworkAccess allowNetworkAccess: Bool) -> String {
        allowNetworkAccess
            ? String(localized: "backup.preflight.phaseLabel.icloudIndex")
            : String(localized: "backup.preflight.phaseLabel.localIndex")
    }

    private static func formatElapsed(_ elapsed: TimeInterval) -> String {
        String(format: "%.1f", elapsed)
    }
}
