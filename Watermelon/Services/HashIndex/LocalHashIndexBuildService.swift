import Foundation
import Photos
import os.log

private let localHashIndexLog = Logger(subsystem: "com.zizicici.watermelon", category: "LocalHashIndex")

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

struct LocalAssetAvailabilityProbeResult: Sendable {
    let requestedAssetIDs: Set<String>
    let unavailableAssetIDs: Set<String>
    let failedAssetIDs: Set<String>
    let missingAssetIDs: Set<String>

    var requiresSingleWorker: Bool {
        !unavailableAssetIDs.isEmpty
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
        allowNetworkAccess: Bool = false
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

            let preparedInput = try await prepareInput(for: assetIDs)
            let worklist = LocalHashIndexWorklist(assets: preparedInput.assets)

            let effectiveWorkerCount = min(max(workerCount, 1), max(preparedInput.assets.count, 1))
            let aggregate = try await withThrowingTaskGroup(of: LocalHashIndexWorkerResult.self) { group in
                for _ in 0 ..< effectiveWorkerCount {
                    group.addTask { [self] in
                        var result = LocalHashIndexWorkerResult()

                        while let assetRef = await worklist.nextAsset() {
                            try Task.checkCancellation()
                            let cachedLocalHash = preparedInput.cachedHashesByAssetID[assetRef.asset.localIdentifier]
                            let outcome = try await processAsset(
                                assetRef.asset,
                                cachedLocalHash: cachedLocalHash,
                                allowNetworkAccess: allowNetworkAccess
                            )
                            result.record(outcome)
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

    func probeAvailability(
        for assetIDs: Set<String>,
        workerCount: Int = 2
    ) async throws -> LocalAssetAvailabilityProbeResult {
        guard !assetIDs.isEmpty else {
            return LocalAssetAvailabilityProbeResult(
                requestedAssetIDs: [],
                unavailableAssetIDs: [],
                failedAssetIDs: [],
                missingAssetIDs: []
            )
        }

        try await ensurePhotoAuthorization()
        let preparedInput = try await prepareAssetFetchInput(for: assetIDs)
        let worklist = LocalHashIndexWorklist(assets: preparedInput.assets)
        let effectiveWorkerCount = min(max(workerCount, 1), max(preparedInput.assets.count, 1))

        let aggregate = try await withThrowingTaskGroup(of: LocalHashIndexWorkerResult.self) { group in
            for _ in 0 ..< effectiveWorkerCount {
                group.addTask { [self] in
                    var result = LocalHashIndexWorkerResult()

                    while let assetRef = await worklist.nextAsset() {
                        try Task.checkCancellation()
                        let outcome = try await probeAssetAvailability(assetRef.asset)
                        result.record(outcome)
                    }

                    return result
                }
            }

            var aggregate = LocalHashIndexWorkerResult()
            for try await result in group {
                aggregate.unavailableAssetIDs.formUnion(result.unavailableAssetIDs)
                aggregate.failedAssetIDs.formUnion(result.failedAssetIDs)
            }
            return aggregate
        }

        return LocalAssetAvailabilityProbeResult(
            requestedAssetIDs: assetIDs,
            unavailableAssetIDs: aggregate.unavailableAssetIDs,
            failedAssetIDs: aggregate.failedAssetIDs,
            missingAssetIDs: preparedInput.missingAssetIDs
        )
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
    ) async throws -> LocalHashIndexAssetOutcome {
        let selectedResources = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(
            from: PHAssetResource.assetResources(for: asset)
        )
        guard !selectedResources.isEmpty else {
            return .failed(asset.localIdentifier)
        }

        if canReuseCache(
            asset: asset,
            selectedResources: selectedResources,
            cachedLocalHash: cachedLocalHash
        ) {
            return .ready(asset.localIdentifier)
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
                totalFileSizeBytes: totalFileSizeBytes
            )
            return .ready(asset.localIdentifier)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            if !allowNetworkAccess && PhotoLibraryService.isNetworkAccessRequiredError(error) {
                return .unavailable(asset.localIdentifier)
            }
            return .failed(asset.localIdentifier)
        }
    }

    private func probeAssetAvailability(
        _ asset: PHAsset
    ) async throws -> LocalHashIndexAssetOutcome {
        let selectedResources = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(
            from: PHAssetResource.assetResources(for: asset)
        )
        guard !selectedResources.isEmpty else {
            return .failed(asset.localIdentifier)
        }

        do {
            for selected in selectedResources {
                try Task.checkCancellation()
                let isLocallyAvailable = try await photoLibraryService.isResourceLocallyAvailable(
                    selected.resource
                )
                if !isLocallyAvailable {
                    return .unavailable(asset.localIdentifier)
                }
            }
            return .ready(asset.localIdentifier)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            return .failed(asset.localIdentifier)
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
}
