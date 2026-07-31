import Foundation
@preconcurrency import Photos

enum MacRemoteThumbnailPurgeOutcome: Equatable, Sendable {
    case completed
    case cancelled
    case failed

    static func from(
        error: Error,
        taskIsCancelled: Bool
    ) -> Self {
        if taskIsCancelled
            || RemoteFaultLite.classify(error) == .cancelled {
            return .cancelled
        }
        return .failed
    }
}

final class MacRemoteThumbnailMaintenanceService:
    @unchecked Sendable
{
    struct BackfillResult: Sendable {
        var generated = 0
        var skipped = 0
        var failed = 0
    }

    private let storageClientFactory: StorageClientFactory
    private let hashIndexRepository: ContentHashIndexRepository
    private let photoLibraryService: PhotoLibraryService
    private let renderer = MacThumbnailRenderer()

    init(
        storageClientFactory: StorageClientFactory,
        hashIndexRepository: ContentHashIndexRepository,
        photoLibraryService: PhotoLibraryService
    ) {
        self.storageClientFactory = storageClientFactory
        self.hashIndexRepository = hashIndexRepository
        self.photoLibraryService = photoLibraryService
    }

    func backfill(
        profile: ServerProfileRecord,
        credential: String,
        fingerprints: [Data],
        progress:
            @MainActor @Sendable @escaping (
                _ completed: Int,
                _ total: Int
            ) -> Void
    ) async -> BackfillResult {
        let uniqueFingerprints = Array(Set(fingerprints))
            .sorted {
                $0.lexicographicallyPrecedes($1)
            }
        guard !uniqueFingerprints.isEmpty else {
            return BackfillResult()
        }
        let localAssets = await currentLocalAssets(
            fingerprints: Set(uniqueFingerprints)
        )
        guard !Task.isCancelled else {
            return BackfillResult()
        }

        let client: any RemoteStorageClientProtocol
        do {
            client = try storageClientFactory.makeClient(
                profile: profile,
                credentialPayload: credential
            )
            try await client.connect()
        } catch {
            return BackfillResult(
                generated: 0,
                skipped: 0,
                failed: uniqueFingerprints.count
            )
        }
        var result = BackfillResult()
        for (index, fingerprint) in uniqueFingerprints.enumerated() {
            if Task.isCancelled { break }
            await progress(index + 1, uniqueFingerprints.count)
            guard let asset = localAssets[fingerprint] else {
                result.skipped += 1
                continue
            }
            let fingerprintHex = fingerprint.hexString
            let remotePath = RemoteThumbnailPaths.absolutePath(
                basePath: profile.basePath,
                fingerprintHex: fingerprintHex
            )
            do {
                if try await client.exists(path: remotePath) {
                    result.skipped += 1
                    continue
                }
            } catch {
                if profile.isConnectionUnavailableError(error) {
                    result.failed += uniqueFingerprints.count
                        - index
                    break
                }
            }

            guard let data = await renderer.renderThumbnailJPEG(
                for: asset,
                allowNetworkAccess: false
            ), await fingerprintIsCurrent(
                fingerprint,
                asset: asset
            ) else {
                result.skipped += 1
                continue
            }
            let temporaryURL = FileManager.default
                .temporaryDirectory
                .appendingPathComponent(
                    "wm-thumb-backfill-\(UUID().uuidString).jpg"
                )
            do {
                try data.write(
                    to: temporaryURL,
                    options: .atomic
                )
                defer {
                    try? FileManager.default.removeItem(
                        at: temporaryURL
                    )
                }
                let shard = RemoteThumbnailPaths
                    .shardDirectoryAbsolutePath(
                        basePath: profile.basePath,
                        fingerprintHex: fingerprintHex
                    )
                try? await client.createDirectory(path: shard)
                let transfer = Task.detached {
                    try await client.upload(
                        localURL: temporaryURL,
                        remotePath: remotePath,
                        mode: .createIfAbsent,
                        respectTaskCancellation: false,
                        onProgress: nil
                    )
                }
                do {
                    try await transfer.value
                    result.generated += 1
                } catch {
                    if SMBErrorClassifier.isNameCollision(error) {
                        result.skipped += 1
                    } else {
                        result.failed += 1
                    }
                }
            } catch {
                try? FileManager.default.removeItem(
                    at: temporaryURL
                )
                result.failed += 1
            }
        }
        await client.disconnectSafely()
        return result
    }

    func purge(
        profile: ServerProfileRecord,
        credential: String
    ) async -> MacRemoteThumbnailPurgeOutcome {
        let client: any RemoteStorageClientProtocol
        do {
            client = try storageClientFactory.makeClient(
                profile: profile,
                credentialPayload: credential
            )
            try await client.connect()
        } catch {
            return .from(
                error: error,
                taskIsCancelled: Task.isCancelled
            )
        }
        let outcome: MacRemoteThumbnailPurgeOutcome
        do {
            let root = RemoteThumbnailPaths.rootAbsolutePath(
                basePath: profile.basePath
            )
            let failures = try await Self.recursiveDelete(
                path: root,
                client: client
            )
            outcome = failures == 0 ? .completed : .failed
        } catch {
            outcome = .from(
                error: error,
                taskIsCancelled: Task.isCancelled
            )
        }
        await client.disconnectSafely()
        return outcome
    }

    private func currentLocalAssets(
        fingerprints: Set<Data>
    ) async -> [Data: PHAsset] {
        await withCancellableDetachedValue(
            priority: .userInitiated
        ) {
            let idsByFingerprint = (
                try? self.hashIndexRepository
                    .fetchAssetIDsByFingerprints(fingerprints)
            ) ?? [:]
            let allIDs = Set(idsByFingerprint.values.flatMap { $0 })
            let rows = (
                try? self.hashIndexRepository
                    .fetchValidIndexedRows(assetIDs: allIDs)
            ) ?? [:]
            let assets = self.photoLibraryService.fetchAssets(
                localIdentifiers: allIDs
            )
            var result: [Data: PHAsset] = [:]
            for asset in assets {
                guard let row = rows[asset.localIdentifier],
                      fingerprints.contains(
                        row.assetFingerprint
                      ) else {
                    continue
                }
                if let modified = asset.modificationDate,
                   modified > row.updatedAt {
                    continue
                }
                if result[row.assetFingerprint] == nil {
                    result[row.assetFingerprint] = asset
                }
            }
            return result
        }
    }

    private func fingerprintIsCurrent(
        _ fingerprint: Data,
        asset: PHAsset
    ) async -> Bool {
        await withCancellableDetachedValue(
            priority: .userInitiated
        ) {
            guard let row = try? self.hashIndexRepository
                .fetchValidIndexedRows(
                    assetIDs: [asset.localIdentifier]
                )[asset.localIdentifier],
                  row.assetFingerprint == fingerprint else {
                return false
            }
            if let modified = asset.modificationDate,
               modified > row.updatedAt {
                return false
            }
            return true
        }
    }

    private nonisolated static func recursiveDelete(
        path: String,
        client: any RemoteStorageClientProtocol
    ) async throws -> Int {
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: path)
        } catch {
            switch RemoteFaultLite.classify(error) {
            case .cancelled:
                throw error
            case .notFound:
                return 0
            case .retryable, .terminal:
                return 1
            }
        }
        var failures = 0
        for entry in entries {
            try Task.checkCancellation()
            if entry.isDirectory {
                failures += try await recursiveDelete(
                    path: entry.path,
                    client: client
                )
            } else {
                do {
                    try await client.delete(path: entry.path)
                } catch {
                    if RemoteFaultLite.classify(error)
                        == .cancelled {
                        throw error
                    }
                    failures += 1
                }
            }
        }
        do {
            try await client.delete(path: path)
        } catch {
            switch RemoteFaultLite.classify(error) {
            case .cancelled:
                throw error
            case .notFound:
                break
            case .retryable, .terminal:
                failures += 1
            }
        }
        return failures
    }
}
