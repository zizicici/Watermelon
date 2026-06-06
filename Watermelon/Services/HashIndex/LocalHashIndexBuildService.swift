import Foundation
import Photos
import os.log

private let localHashIndexLog = Logger(subsystem: "com.zizicici.watermelon", category: "LocalHashIndex")

typealias LocalHashIndexProgressHandler = @Sendable (String, ExecutionLogLevel) async -> Void
typealias LocalHashIndexProgressTickHandler = @Sendable (_ processed: Int, _ total: Int) async -> Void

struct LocalHashIndexBuildResult: Sendable {
    let requestedAssetIDs: Set<PhotoKitLocalIdentifier>
    let readyAssetIDs: Set<PhotoKitLocalIdentifier>
    // Cache-valid assets (fingerprint already durable) whose bytes are offloaded or
    // whose offline probe errored. Index-complete, so never gating; bytes still flagged
    // for the upload worker-count downgrade.
    let cachedBytesUnavailableAssetIDs: Set<PhotoKitLocalIdentifier>
    let unavailableAssetIDs: Set<PhotoKitLocalIdentifier>
    let failedAssetIDs: Set<PhotoKitLocalIdentifier>
    let missingAssetIDs: Set<PhotoKitLocalIdentifier>

    init(
        requestedAssetIDs: Set<PhotoKitLocalIdentifier>,
        readyAssetIDs: Set<PhotoKitLocalIdentifier>,
        cachedBytesUnavailableAssetIDs: Set<PhotoKitLocalIdentifier> = [],
        unavailableAssetIDs: Set<PhotoKitLocalIdentifier>,
        failedAssetIDs: Set<PhotoKitLocalIdentifier>,
        missingAssetIDs: Set<PhotoKitLocalIdentifier>
    ) {
        self.requestedAssetIDs = requestedAssetIDs
        self.readyAssetIDs = readyAssetIDs
        self.cachedBytesUnavailableAssetIDs = cachedBytesUnavailableAssetIDs
        self.unavailableAssetIDs = unavailableAssetIDs
        self.failedAssetIDs = failedAssetIDs
        self.missingAssetIDs = missingAssetIDs
    }

    var incompleteAssetIDs: Set<PhotoKitLocalIdentifier> {
        unavailableAssetIDs.union(failedAssetIDs)
    }

    var bytesUnavailableForUploadAssetIDs: Set<PhotoKitLocalIdentifier> {
        unavailableAssetIDs.union(cachedBytesUnavailableAssetIDs)
    }
}

private enum LocalHashIndexAssetOutcome: Sendable {
    case ready(PhotoKitLocalIdentifier)
    case cachedBytesUnavailable(PhotoKitLocalIdentifier)
    case unavailable(PhotoKitLocalIdentifier)
    case failed(PhotoKitLocalIdentifier)
}

private struct LocalHashIndexWorkerResult: Sendable {
    var readyAssetIDs = Set<PhotoKitLocalIdentifier>()
    var cachedBytesUnavailableAssetIDs = Set<PhotoKitLocalIdentifier>()
    var unavailableAssetIDs = Set<PhotoKitLocalIdentifier>()
    var failedAssetIDs = Set<PhotoKitLocalIdentifier>()

    mutating func record(_ outcome: LocalHashIndexAssetOutcome) {
        switch outcome {
        case .ready(let assetID):
            readyAssetIDs.insert(assetID)
        case .cachedBytesUnavailable(let assetID):
            cachedBytesUnavailableAssetIDs.insert(assetID)
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
    private let assetIDs: [PhotoKitLocalIdentifier]
    private var nextIndex = 0

    init(assetIDs: [PhotoKitLocalIdentifier]) {
        self.assetIDs = assetIDs
    }

    func nextBatch(maxSize: Int) -> ArraySlice<PhotoKitLocalIdentifier> {
        let start = nextIndex
        let end = min(start + maxSize, assetIDs.count)
        nextIndex = end
        return assetIDs[start ..< end]
    }
}

private actor LocalHashIndexBuildProgressReporter {
    private static let logCountStep = 500

    private let total: Int
    private let phaseLabel: String
    private let logHandler: LocalHashIndexProgressHandler?
    private let tickHandler: LocalHashIndexProgressTickHandler?

    private var processed = 0
    private var cacheHitCount = 0
    private var rebuiltCount = 0
    private var unavailableCount = 0
    private var failedCount = 0
    private var lastReportedProcessed = 0

    init(
        total: Int,
        phaseLabel: String,
        logHandler: LocalHashIndexProgressHandler?,
        tickHandler: LocalHashIndexProgressTickHandler?
    ) {
        self.total = total
        self.phaseLabel = phaseLabel
        self.logHandler = logHandler
        self.tickHandler = tickHandler
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
        case .cachedBytesUnavailable:
            cacheHitCount += 1
        case .unavailable:
            unavailableCount += 1
        case .failed:
            failedCount += 1
        }
        await maybeReport(force: false)
    }

    func finish() async {
        await maybeReport(force: true)
    }

    private func maybeReport(force: Bool) async {
        guard total > 0 else { return }
        let shouldReport = force
            || processed == total
            || processed - lastReportedProcessed >= Self.logCountStep
        guard shouldReport else { return }
        guard processed > lastReportedProcessed || force else { return }

        lastReportedProcessed = processed

        if let logHandler {
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
        await tickHandler?(processed, total)
    }
}

private struct LocalHashIndexBuildPreparedInput: Sendable {
    let cachedHashesByAssetID: [PhotoKitLocalIdentifier: LocalAssetHashCache]
    let assetIDs: [PhotoKitLocalIdentifier]
    let missingAssetIDs: Set<PhotoKitLocalIdentifier>
}

private struct LocalHashIndexAssetFetchInput: Sendable {
    let assetIDs: [PhotoKitLocalIdentifier]
    let missingAssetIDs: Set<PhotoKitLocalIdentifier>
}

final class LocalHashIndexBuildService: @unchecked Sendable {
    private static let workerFetchBatchSize = 200

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
        for assetIDs: Set<PhotoKitLocalIdentifier>,
        workerCount: Int = 2,
        allowNetworkAccess: Bool = false,
        progressHandler: LocalHashIndexProgressHandler? = nil,
        tickHandler: LocalHashIndexProgressTickHandler? = nil
    ) async throws -> LocalHashIndexBuildResult {
        guard !assetIDs.isEmpty else {
            return LocalHashIndexBuildResult(
                requestedAssetIDs: [],
                readyAssetIDs: [],
                cachedBytesUnavailableAssetIDs: [],
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
            let worklist = LocalHashIndexWorklist(assetIDs: preparedInput.assetIDs)
            await progressHandler?(
                String.localizedStringWithFormat(
                    String(localized: "backup.preflight.prepareDone"),
                    phaseLabel,
                    preparedInput.assetIDs.count,
                    preparedInput.cachedHashesByAssetID.count,
                    preparedInput.missingAssetIDs.count,
                    Self.formatElapsed(prepareElapsed)
                ),
                .debug
            )

            let effectiveWorkerCount = min(max(workerCount, 1), max(preparedInput.assetIDs.count, 1))
            await progressHandler?(
                String.localizedStringWithFormat(
                    String(localized: "backup.preflight.scanStart"),
                    phaseLabel,
                    effectiveWorkerCount
                ),
                .info
            )
            let progressReporter = LocalHashIndexBuildProgressReporter(
                total: preparedInput.assetIDs.count,
                phaseLabel: phaseLabel,
                logHandler: progressHandler,
                tickHandler: tickHandler
            )
            let scanStart = CFAbsoluteTimeGetCurrent()
            let aggregate = try await withThrowingTaskGroup(of: LocalHashIndexWorkerResult.self) { group in
                for _ in 0 ..< effectiveWorkerCount {
                    group.addTask { [self, progressReporter] in
                        var result = LocalHashIndexWorkerResult()

                        while true {
                            let batch = await worklist.nextBatch(maxSize: Self.workerFetchBatchSize)
                            if batch.isEmpty { break }

                            // Anchor every row written from this batch to the snapshot-acquisition time:
                            // these PHAssets are not re-fetched per asset, so an edit landing after the
                            // batch fetch must not be masked by a later per-asset write timestamp.
                            let batchFetchedAt = Date()
                            let phAssets = self.photoLibraryService
                                .fetchAssets(localIdentifiers: Set(batch))
                            var assetByID: [PhotoKitLocalIdentifier: PHAsset] = [:]
                            assetByID.reserveCapacity(phAssets.count)
                            for asset in phAssets {
                                assetByID[PhotoKitLocalIdentifier(asset)] = asset
                            }

                            for assetID in batch {
                                try Task.checkCancellation()
                                guard let asset = assetByID[assetID] else {
                                    let processedAsset = LocalHashIndexProcessedAssetResult(
                                        outcome: .failed(assetID),
                                        reusedCache: false
                                    )
                                    result.record(processedAsset.outcome)
                                    await progressReporter.record(processedAsset)
                                    continue
                                }
                                let cachedLocalHash = preparedInput.cachedHashesByAssetID[assetID]
                                let processedAsset = try await processAsset(
                                    asset,
                                    cachedLocalHash: cachedLocalHash,
                                    capturedAt: batchFetchedAt,
                                    allowNetworkAccess: allowNetworkAccess
                                )
                                result.record(processedAsset.outcome)
                                await progressReporter.record(processedAsset)
                            }
                        }

                        return result
                    }
                }

                var aggregate = LocalHashIndexWorkerResult()
                for try await result in group {
                    aggregate.readyAssetIDs.formUnion(result.readyAssetIDs)
                    aggregate.cachedBytesUnavailableAssetIDs.formUnion(result.cachedBytesUnavailableAssetIDs)
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
                cachedBytesUnavailableAssetIDs: aggregate.cachedBytesUnavailableAssetIDs,
                unavailableAssetIDs: aggregate.unavailableAssetIDs,
                failedAssetIDs: aggregate.failedAssetIDs,
                missingAssetIDs: preparedInput.missingAssetIDs
            )
            let endedAt = Date()
            let elapsed = endedAt.timeIntervalSince(startedAt)
            localHashIndexLog.info(
                "[LocalHashIndex] build finished at \(endedAt.ISO8601Format(), privacy: .public), elapsed=\(String(format: "%.3f", elapsed), privacy: .public)s, ready=\(result.readyAssetIDs.count), cachedBytesUnavailable=\(result.cachedBytesUnavailableAssetIDs.count), unavailable=\(result.unavailableAssetIDs.count), failed=\(result.failedAssetIDs.count), missing=\(result.missingAssetIDs.count)"
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
        for assetIDs: Set<PhotoKitLocalIdentifier>
    ) async throws -> LocalHashIndexBuildPreparedInput {
        async let cachedHashesByAssetID = loadCachedHashes(for: assetIDs)
        async let assetFetchInput = prepareAssetFetchInput(for: assetIDs)

        let preparedAssets = try await assetFetchInput
        let cachedHashes = try await cachedHashesByAssetID
        return LocalHashIndexBuildPreparedInput(
            cachedHashesByAssetID: cachedHashes,
            assetIDs: preparedAssets.assetIDs,
            missingAssetIDs: preparedAssets.missingAssetIDs
        )
    }

    private func prepareAssetFetchInput(
        for assetIDs: Set<PhotoKitLocalIdentifier>
    ) async throws -> LocalHashIndexAssetFetchInput {
        let photoLibraryService = self.photoLibraryService
        let task = Task.detached(priority: .userInitiated) {
            try Task.checkCancellation()
            let fetchedAssets = photoLibraryService.fetchAssets(localIdentifiers: assetIDs)
            let sortedIDs: [PhotoKitLocalIdentifier] = fetchedAssets
                .sorted { lhs, rhs in
                    let lhsDate = lhs.creationDate ?? .distantPast
                    let rhsDate = rhs.creationDate ?? .distantPast
                    if lhsDate != rhsDate {
                        return lhsDate < rhsDate
                    }
                    return lhs.localIdentifier < rhs.localIdentifier
                }
                .map { PhotoKitLocalIdentifier($0) }
            return LocalHashIndexAssetFetchInput(
                assetIDs: sortedIDs,
                missingAssetIDs: assetIDs.subtracting(sortedIDs)
            )
        }

        return try await withTaskCancellationHandler {
            try await task.value
        } onCancel: {
            task.cancel()
        }
    }

    private func loadCachedHashes(
        for assetIDs: Set<PhotoKitLocalIdentifier>
    ) async throws -> [PhotoKitLocalIdentifier: LocalAssetHashCache] {
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
        capturedAt: Date,
        allowNetworkAccess: Bool
    ) async throws -> LocalHashIndexProcessedAssetResult {
        let selectedResources = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(
            from: PHAssetResource.assetResources(for: asset)
        )
        guard !selectedResources.isEmpty else {
            return LocalHashIndexProcessedAssetResult(
                outcome: .failed(PhotoKitLocalIdentifier(asset)),
                reusedCache: false
            )
        }

        if canReuseCache(
            asset: asset,
            selectedResources: selectedResources,
            cachedLocalHash: cachedLocalHash
        ) {
            // Cached fingerprint is already durable, so the index is complete regardless of
            // byte availability; the offline probe only flags offloaded bytes for the
            // worker-count downgrade and must not demote the asset into an incomplete bucket.
            if !allowNetworkAccess {
                do {
                    for selected in selectedResources {
                        try Task.checkCancellation()
                        let isLocal = try await photoLibraryService.isResourceLocallyAvailable(
                            selected.resource
                        )
                        if !isLocal {
                            return LocalHashIndexProcessedAssetResult(
                                outcome: .cachedBytesUnavailable(PhotoKitLocalIdentifier(asset)),
                                reusedCache: true
                            )
                        }
                    }
                } catch is CancellationError {
                    throw CancellationError()
                } catch {
                    return LocalHashIndexProcessedAssetResult(
                        outcome: .cachedBytesUnavailable(PhotoKitLocalIdentifier(asset)),
                        reusedCache: true
                    )
                }
            }
            return LocalHashIndexProcessedAssetResult(
                outcome: .ready(PhotoKitLocalIdentifier(asset)),
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

            let signature = BackupAssetResourcePlanner.resourceSignature(orderedResources: selectedResources)
            try repository.upsertAssetHashSnapshot(
                assetLocalIdentifier: PhotoKitLocalIdentifier(asset),
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
                modificationDateMs: asset.modificationDate?.millisecondsSinceEpoch,
                selectionVersion: BackupAssetResourcePlanner.currentSelectionVersion,
                resourceSignature: signature,
                updatedAt: capturedAt
            )
            return LocalHashIndexProcessedAssetResult(
                outcome: .ready(PhotoKitLocalIdentifier(asset)),
                reusedCache: false
            )
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            if !allowNetworkAccess && PhotoLibraryService.isNetworkAccessRequiredError(error) {
                return LocalHashIndexProcessedAssetResult(
                    outcome: .unavailable(PhotoKitLocalIdentifier(asset)),
                    reusedCache: false
                )
            }
            return LocalHashIndexProcessedAssetResult(
                outcome: .failed(PhotoKitLocalIdentifier(asset)),
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
        guard LocalHashIndexTrust.cacheFieldsPassCheapChecks(
            cachedLocalHash.trustFields,
            modificationDate: asset.modificationDate
        ) else { return false }

        let currentSignature = BackupAssetResourcePlanner.resourceSignature(orderedResources: selectedResources)
        guard LocalHashIndexTrust.signatureMatches(cachedLocalHash.trustFields, currentSignature: currentSignature) else { return false }

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
