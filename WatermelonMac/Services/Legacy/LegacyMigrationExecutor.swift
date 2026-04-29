import Foundation

/// Drives a real legacy-data import for a single profile + scan report.
/// Reports progress via an AsyncStream of LegacyImportEvent.
final class LegacyMigrationExecutor {
    private let storageClientFactory: StorageClientFactory
    private let session: LegacyImportSession

    init(storageClientFactory: StorageClientFactory, session: LegacyImportSession) {
        self.storageClientFactory = storageClientFactory
        self.session = session
    }

    func run(
        report: LegacyScanReport,
        profile: ServerProfileRecord,
        password: String
    ) -> AsyncStream<LegacyImportEvent> {
        AsyncStream { continuation in
            let task = Task { [self] in
                do {
                    try await self.execute(
                        report: report,
                        profile: profile,
                        password: password,
                        emit: { continuation.yield($0) }
                    )
                } catch is CancellationError {
                    continuation.finish()
                    return
                } catch {
                    continuation.yield(.failed(error: error, totals: LegacyImportTotals()))
                }
                continuation.finish()
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    private func execute(
        report: LegacyScanReport,
        profile: ServerProfileRecord,
        password: String,
        emit: @Sendable (LegacyImportEvent) -> Void
    ) async throws {
        session.begin()
        defer { session.end() }

        var totals = LegacyImportTotals()
        totals.monthsTotal = report.plans.count
        totals.bundlesPlanned = report.plans.reduce(0) { $0 + $1.totalAssetCount }
        emit(.started(totals: totals))

        let client = try storageClientFactory.makeClient(profile: profile, password: password)
        try await client.connect()
        defer {
            Task { await client.disconnect() }
        }
        try await ensureBasePathExists(profile: profile, client: client)

        for plan in report.plans {
            try Task.checkCancellation()
            emit(.monthStarted(month: plan.month, bundleCount: plan.bundles.count))

            let store: MonthManifestStore
            do {
                store = try await MonthManifestStore.loadOrCreate(
                    client: client,
                    basePath: profile.basePath,
                    year: plan.month.year,
                    month: plan.month.month
                )
            } catch {
                totals.bundlesFailed += plan.bundles.count
                totals.bundlesProcessed += plan.bundles.count
                totals.monthsDone += 1
                emit(.logMessage("Failed to open manifest \(plan.month.text): \(error.localizedDescription)"))
                emit(.progress(totals: totals))
                continue
            }

            for bundle in plan.bundles {
                try Task.checkCancellation()
                let outcome = await processBundleWithRetry(
                    bundle: bundle,
                    monthStore: store,
                    totals: &totals,
                    emit: emit
                )
                emit(.bundleResult(month: plan.month, bundle: bundle, outcome: outcome))
                emit(.progress(totals: totals))
            }

            do {
                _ = try await store.flushToRemote()
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                emit(.logMessage("Failed to flush manifest \(plan.month.text): \(error.localizedDescription)"))
            }

            totals.monthsDone += 1
            emit(.monthCompleted(month: plan.month))
            emit(.progress(totals: totals))
        }

        emit(.finished(totals: totals))
    }

    private static let maxRetryAttempts = 3
    private static let retryBackoffSchedule: [UInt64] = [
        500_000_000,
        1_500_000_000,
        3_500_000_000
    ]

    private func processBundleWithRetry(
        bundle: LegacyAssetBundle,
        monthStore: MonthManifestStore,
        totals: inout LegacyImportTotals,
        emit: @Sendable (LegacyImportEvent) -> Void
    ) async -> LegacyImportBundleOutcome {
        for attempt in 0..<Self.maxRetryAttempts {
            if Task.isCancelled {
                totals.bundlesProcessed += 1
                totals.bundlesFailed += 1
                return .failed(reason: "cancelled")
            }
            let attemptResult = await processBundle(bundle: bundle, monthStore: monthStore)
            switch attemptResult.outcome {
            case .imported(let bytes, let uploaded, let skipped):
                totals.bundlesProcessed += 1
                totals.bundlesImported += 1
                totals.bytesUploaded += bytes
                totals.resourcesUploaded += uploaded
                totals.resourcesSkippedHashExists += skipped
                return attemptResult.outcome
            case .skippedFingerprintExists:
                totals.bundlesProcessed += 1
                totals.bundlesSkippedFingerprintExists += 1
                return attemptResult.outcome
            case .failed:
                let isLastAttempt = attempt >= Self.maxRetryAttempts - 1
                let shouldRetry = !isLastAttempt
                    && attemptResult.error.map(LegacyTransientErrorClassifier.isTransient) == true
                if shouldRetry {
                    let delay = Self.retryBackoffSchedule[attempt]
                    emit(.logMessage("transient error: retrying in \(delay / 1_000_000_000)s"))
                    try? await Task.sleep(nanoseconds: delay)
                    continue
                }
                totals.bundlesProcessed += 1
                totals.bundlesFailed += 1
                return attemptResult.outcome
            }
        }
        totals.bundlesProcessed += 1
        totals.bundlesFailed += 1
        return .failed(reason: "exceeded retry attempts")
    }

    private struct BundleAttemptResult {
        let outcome: LegacyImportBundleOutcome
        let error: Error?
    }

    private func processBundle(
        bundle: LegacyAssetBundle,
        monthStore: MonthManifestStore
    ) async -> BundleAttemptResult {
        if monthStore.containsAssetFingerprint(bundle.assetFingerprint) {
            return BundleAttemptResult(outcome: .skippedFingerprintExists, error: nil)
        }

        let identities = bundle.resources.map {
            RemoteFileNaming.ResourceIdentity(
                role: $0.role,
                slot: $0.slot,
                originalFilename: $0.originalFilename
            )
        }
        let assetStem = RemoteFileNaming.preferredAssetNameStem(
            orderedResources: identities,
            fallbackTimestampMs: bundle.creationDate?.millisecondsSinceEpoch
        )
        var collisionKeys = RemoteFileNaming.collisionKeySet(from: monthStore.existingFileNames())

        var perBundleBytesUploaded: Int64 = 0
        var resourcesUploaded = 0
        var resourcesSkipped = 0
        var resourceLinks: [RemoteAssetResourceLink] = []
        let backedUpAtMs = Date().millisecondsSinceEpoch

        for component in bundle.resources {
            do {
                let outcome = try await processResource(
                    bundle: bundle,
                    component: component,
                    assetStem: assetStem,
                    monthStore: monthStore,
                    backedUpAtMs: backedUpAtMs,
                    collisionKeys: &collisionKeys
                )
                switch outcome {
                case .uploaded(let bytes):
                    perBundleBytesUploaded += bytes
                    resourcesUploaded += 1
                case .skipped:
                    resourcesSkipped += 1
                }
                resourceLinks.append(
                    RemoteAssetResourceLink(
                        year: monthStore.year,
                        month: monthStore.month,
                        assetFingerprint: bundle.assetFingerprint,
                        resourceHash: component.contentHash,
                        role: component.role,
                        slot: component.slot
                    )
                )
            } catch is CancellationError {
                return BundleAttemptResult(outcome: .failed(reason: "cancelled"), error: nil)
            } catch {
                return BundleAttemptResult(outcome: .failed(reason: error.localizedDescription), error: error)
            }
        }

        let asset = RemoteManifestAsset(
            year: monthStore.year,
            month: monthStore.month,
            assetFingerprint: bundle.assetFingerprint,
            creationDateMs: bundle.creationDate?.millisecondsSinceEpoch,
            backedUpAtMs: backedUpAtMs,
            resourceCount: bundle.resources.count,
            totalFileSizeBytes: bundle.totalFileSize
        )
        do {
            try monthStore.upsertAsset(asset, links: resourceLinks)
        } catch {
            return BundleAttemptResult(
                outcome: .failed(reason: "upsertAsset: \(error.localizedDescription)"),
                error: error
            )
        }

        return BundleAttemptResult(
            outcome: .imported(
                bytesUploaded: perBundleBytesUploaded,
                resourcesUploaded: resourcesUploaded,
                resourcesSkippedHashExists: resourcesSkipped
            ),
            error: nil
        )
    }

    private enum ResourceOutcome {
        case uploaded(bytes: Int64)
        case skipped
    }

    private func processResource(
        bundle: LegacyAssetBundle,
        component: LegacyResourceComponent,
        assetStem: String,
        monthStore: MonthManifestStore,
        backedUpAtMs: Int64,
        collisionKeys: inout Set<String>
    ) async throws -> ResourceOutcome {
        if monthStore.findResourceByHash(component.contentHash) != nil {
            return .skipped
        }

        let baseFileName = RemoteFileNaming.preferredRemoteFileName(
            preferredAssetNameStem: assetStem,
            resource: RemoteFileNaming.ResourceIdentity(
                role: component.role,
                slot: component.slot,
                originalFilename: component.originalFilename
            )
        )
        let targetFileName = RemoteFileNaming.resolveNextAvailableName(
            baseName: baseFileName,
            collisionKeys: collisionKeys
        )
        collisionKeys.insert(RemoteFileNaming.collisionKey(for: targetFileName))

        let remoteRelativePath = monthStore.monthRelativePath + "/" + targetFileName
        let remoteAbsolutePath = RemotePathBuilder.absolutePath(
            basePath: monthStore.basePath,
            remoteRelativePath: remoteRelativePath
        )

        try await monthStore.client.upload(
            localURL: component.url,
            remotePath: remoteAbsolutePath,
            respectTaskCancellation: true,
            onProgress: nil
        )

        let resource = RemoteManifestResource(
            year: monthStore.year,
            month: monthStore.month,
            fileName: targetFileName,
            contentHash: component.contentHash,
            fileSize: component.fileSize,
            resourceType: component.role,
            creationDateMs: bundle.creationDate?.millisecondsSinceEpoch,
            backedUpAtMs: backedUpAtMs
        )
        _ = try monthStore.upsertResource(resource)
        monthStore.markRemoteFile(name: targetFileName, size: component.fileSize)

        return .uploaded(bytes: component.fileSize)
    }

    private func ensureBasePathExists(
        profile: ServerProfileRecord,
        client: any RemoteStorageClientProtocol
    ) async throws {
        let normalized = RemotePathBuilder.normalizePath(profile.basePath)
        try await client.createDirectory(path: normalized)
    }
}
