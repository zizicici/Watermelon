import CryptoKit
import Foundation
import Photos

final class RestoreService {
    private let storageClientFactory: StorageClientFactory

    init(
        databaseManager _: DatabaseManager,
        storageClientFactory: StorageClientFactory = StorageClientFactory()
    ) {
        self.storageClientFactory = storageClientFactory
    }

    struct RestoreItemDescriptor: Sendable {
        let instances: [RemoteAssetResourceInstance]
        let identity: Data
    }

    struct RestoredAsset {
        let localIdentifier: String
        let importedInstances: [RemoteAssetResourceInstance]
    }

    struct RestoredItem: Sendable {
        let identity: Data
        let asset: RestoredAsset
    }

    func restoreItems(
        items: [RestoreItemDescriptor],
        profile: ServerProfileRecord,
        password: String,
        onItemCompleted: @Sendable (Int, Int, RestoredItem?) async throws -> Void
    ) async throws -> [RestoredItem] {
        guard !items.isEmpty else { return [] }

        let storageClient = try storageClientFactory.makeClient(profile: profile, password: password)

        try await storageClient.connect()

        // disconnect must finish before return — credential session quota.
        do {
            var results: [RestoredItem] = []
            var failureCount = 0
            var firstFailure: Error?
            for (index, item) in items.enumerated() {
                try Task.checkCancellation()
                let creationDate = item.instances
                    .compactMap(\.creationDateMs)
                    .min()
                    .map { Date(millisecondsSinceEpoch: $0) }
                let group = RestoreGroup(creationDate: creationDate, instances: item.instances)
                var restoredItem: RestoredItem?
                // Per-item try/catch: one bad asset (hash mismatch, schema corruption) must not
                // abort restoring the remaining N-1. Cancellation propagates. Connection-unavailable
                // aborts the batch — running the remaining N-1 against a dead connection just turns
                // every item into a "per-item failure" instead of surfacing the real cause.
                do {
                    if let asset = try await restoreGroup(group, profile: profile, storageClient: storageClient) {
                        let restored = RestoredItem(identity: item.identity, asset: asset)
                        results.append(restored)
                        restoredItem = restored
                    }
                } catch is CancellationError {
                    throw CancellationError()
                } catch {
                    if profile.isConnectionUnavailableError(error) {
                        throw error
                    }
                    failureCount += 1
                    if firstFailure == nil { firstFailure = error }
                    print("[RestoreService] item \(index + 1)/\(items.count) failed: \(error.localizedDescription)")
                }
                // Callback throw (e.g. hash-index DB write) shouldn't strand the imported
                // asset or mask other per-item failures; treat as per-item, propagate cancel.
                do {
                    try await onItemCompleted(index + 1, items.count, restoredItem)
                } catch is CancellationError {
                    throw CancellationError()
                } catch {
                    failureCount += 1
                    if firstFailure == nil { firstFailure = error }
                    print("[RestoreService] onItemCompleted \(index + 1)/\(items.count) failed: \(error.localizedDescription)")
                }
            }
            if failureCount > 0 {
                throw NSError(
                    domain: "RestoreService",
                    code: -3,
                    userInfo: [
                        NSLocalizedDescriptionKey: "\(failureCount) of \(items.count) item(s) failed to restore",
                        NSUnderlyingErrorKey: firstFailure as Any
                    ]
                )
            }
            await storageClient.disconnectSafely()
            return results
        } catch {
            await storageClient.disconnectSafely()
            throw error
        }
    }

    private func restoreGroup(
        _ group: RestoreGroup,
        profile: ServerProfileRecord,
        storageClient: RemoteStorageClientProtocol
    ) async throws -> RestoredAsset? {
        let resourceDesc = group.instances.map { instance in
            let mapped = instance.resourceType
            let typeStr = mapped.map { String($0.rawValue) } ?? "skip"
            return "\(instance.fileName) (role=\(instance.role), slot=\(instance.slot), type=\(typeStr), size=\(instance.fileSize), hash=\(instance.contentHashHex.prefix(8)))"
        }.joined(separator: ", ")
        print("[RestoreService] restoreGroup: \(group.instances.count) instance(s), creationDate=\(group.creationDate?.description ?? "nil") — [\(resourceDesc)]")

        var downloadedURLsByHash: [Data: URL] = [:]
        downloadedURLsByHash.reserveCapacity(group.instances.count)
        var downloaded: [(RemoteAssetResourceInstance, URL)] = []
        downloaded.reserveCapacity(group.instances.count)

        for instance in group.instances {
            try Task.checkCancellation()
            // Empty hash = legacy entry; can't safely dedup by hash key (all empty hashes
            // collide), so each instance gets its own download.
            if !instance.resourceHash.isEmpty, let cachedURL = downloadedURLsByHash[instance.resourceHash] {
                downloaded.append((instance, cachedURL))
                continue
            }

            // V2 logicalName comes from a peer's commit; sanitize so "../etc/foo" can't
            // escape the tmp dir via appendPathComponent.
            let safeFileName = RemotePathBuilder.sanitizeFilename(instance.fileName)
            let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent(
                "restore_\(UUID().uuidString)_\(safeFileName)"
            )
            try? FileManager.default.removeItem(at: tempURL)

            do {
                try await Self.downloadWithFallback(
                    instance: instance,
                    profile: profile,
                    storageClient: storageClient,
                    localURL: tempURL
                )
            } catch {
                // The per-instance cleanup loop below only iterates `downloaded`, so an
                // aborted entry leaks until iOS purges tmp unless we remove it here.
                try? FileManager.default.removeItem(at: tempURL)
                throw error
            }

            let fileSize = (try? FileManager.default.attributesOfItem(atPath: tempURL.path)[.size] as? Int64) ?? -1
            let fileExists = FileManager.default.fileExists(atPath: tempURL.path)
            print("[RestoreService]   downloaded: \(instance.fileName) → \(tempURL.lastPathComponent), exists=\(fileExists), localSize=\(fileSize), expectedSize=\(instance.fileSize)")
            if !instance.resourceHash.isEmpty {
                downloadedURLsByHash[instance.resourceHash] = tempURL
            }
            downloaded.append((instance, tempURL))
        }

        // `cleanupURLs` is initialized BEFORE any throwing call below and the
        // single `defer` guarantees cleanup on every exit path (success, throw,
        // cancellation). Avoids the historic class of bug where success/catch had
        // separate cleanup loops that drifted apart.
        var cleanupURLs = Set(downloaded.map(\.1))
        defer {
            for url in cleanupURLs {
                try? FileManager.default.removeItem(at: url)
            }
        }

        let (entries, extras) = try acceptedDownloadedResources(from: downloaded)
        cleanupURLs.formUnion(extras)
        let acceptedDownloaded = entries

        try Task.checkCancellation()
        let localID: String?
        do {
            localID = try await saveToPhotoLibrary(downloaded: acceptedDownloaded, creationDate: group.creationDate)
            print("[RestoreService]   saveToPhotoLibrary succeeded, localID=\(localID ?? "nil")")
        } catch {
            print("[RestoreService]   saveToPhotoLibrary FAILED: \(error)")
            throw error
        }
        guard let localID else { return nil }
        return RestoredAsset(
            localIdentifier: localID,
            importedInstances: acceptedDownloaded.map(\.0)
        )
    }

    private struct RestoreGroup {
        let creationDate: Date?
        let instances: [RemoteAssetResourceInstance]
    }

    /// Try primary then each alternate; throw last seen error if all fail. Multi-writer
    /// V2 can publish the same content under different paths.
    /// After download, validates size AND content hash against the manifest. A primary
    /// that returns wrong-content bytes (manual overwrite, corruption, peer race)
    /// triggers fallback to the next path — accepting wrong content silently corrupts
    /// the user's library.
    static func downloadWithFallback(
        instance: RemoteAssetResourceInstance,
        profile: ServerProfileRecord,
        storageClient: RemoteStorageClientProtocol,
        localURL: URL
    ) async throws {
        let candidatePaths = [instance.remoteRelativePath] + instance.alternateRemoteRelativePaths
        var lastError: Error?
        for path in candidatePaths {
            let remotePath = RemotePathBuilder.absolutePath(
                basePath: profile.basePath,
                remoteRelativePath: path
            )
            do {
                try? FileManager.default.removeItem(at: localURL)
                try await storageClient.download(remotePath: remotePath, localURL: localURL)
                if let mismatch = try Self.contentMismatchReason(
                    localURL: localURL,
                    expectedSize: instance.fileSize,
                    expectedHash: instance.resourceHash
                ) {
                    print("[RestoreService]   download integrity mismatch: \(instance.fileName), remotePath=\(remotePath), \(mismatch)")
                    try? FileManager.default.removeItem(at: localURL)
                    lastError = NSError(
                        domain: "RestoreService",
                        code: -10,
                        userInfo: [NSLocalizedDescriptionKey: "downloaded bytes don't match manifest (\(mismatch))"]
                    )
                    continue
                }
                return
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                lastError = error
                print("[RestoreService]   download FAILED: \(instance.fileName), remotePath=\(remotePath), reason=\(error.localizedDescription)")
            }
        }
        throw lastError ?? CancellationError()
    }

    /// Returns nil when the file matches both expected size and (when known) hash.
    /// Manifest entries with `expectedSize <= 0` skip the size check (legacy / unknown).
    /// Empty `expectedHash` skips the hash check (legacy entries pre-V2 may not carry one).
    private static func contentMismatchReason(localURL: URL, expectedSize: Int64, expectedHash: Data) throws -> String? {
        let attrs = try? FileManager.default.attributesOfItem(atPath: localURL.path)
        guard let actualSize = attrs?[.size] as? Int64 else {
            return "actual size unreadable"
        }
        if expectedSize > 0 && actualSize != expectedSize {
            return "size: expected \(expectedSize) got \(actualSize)"
        }
        // Legacy entry with no size/hash: reject 0-byte download — otherwise it'd accept anything.
        if expectedSize <= 0 && expectedHash.isEmpty && actualSize == 0 {
            return "legacy entry without size/hash; downloaded 0 bytes"
        }
        guard !expectedHash.isEmpty else { return nil }
        do {
            let handle = try FileHandle(forReadingFrom: localURL)
            defer { try? handle.close() }
            var hasher = SHA256()
            while true {
                try Task.checkCancellation()
                let chunk = try handle.read(upToCount: 1 << 20) ?? Data()
                if chunk.isEmpty { break }
                hasher.update(data: chunk)
            }
            let actualHash = Data(hasher.finalize())
            if actualHash != expectedHash {
                return "hash mismatch (size matches: \(actualSize))"
            }
            return nil
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            return "hash read failed: \(error.localizedDescription)"
        }
    }

    private func acceptedDownloadedResources(
        from downloaded: [(RemoteAssetResourceInstance, URL)]
    ) throws -> (entries: [(RemoteAssetResourceInstance, URL)], extraTmpURLs: [URL]) {
        var accepted: [(RemoteAssetResourceInstance, URL)] = []
        var extraTmpURLs: [URL] = []
        accepted.reserveCapacity(downloaded.count)
        // Backup fingerprint key is (role, slot, hash); deduping by `resourceType` alone
        // (which is a coarser projection of role) silently drops same-type-different-slot
        // resources — happens with edited Live Photos where slot=N>0 carries an
        // additional paired video. Use (role, slot) to match the fingerprint contract.
        struct RoleSlotKey: Hashable { let role: Int; let slot: Int }
        var addedRoleSlots = Set<RoleSlotKey>()
        // PHAssetCreationRequest.addResource consumes/owns the file URL it's handed.
        // Two roles can share a contentHash (Live Photo where photo and fullSizePhoto
        // happen to be byte-identical, or duplicate audio tracks); the per-hash download
        // cache then hands the same URL to addResource twice, leaving the second add
        // to race with whatever Photos did to the first one. Copy to a fresh tmp so each
        // addResource call gets its own file.
        var seenURLs = Set<URL>()

        for entry in downloaded {
            let instance = entry.0
            guard instance.resourceType != nil else { continue }
            let key = RoleSlotKey(role: instance.role, slot: instance.slot)
            if !addedRoleSlots.insert(key).inserted {
                print("[RestoreService]   duplicate (role,slot) skipped: role=\(instance.role), slot=\(instance.slot), file=\(instance.fileName)")
                continue
            }
            let url = entry.1
            if seenURLs.insert(url).inserted {
                accepted.append(entry)
            } else {
                let safeName = RemotePathBuilder.sanitizeFilename(instance.fileName)
                let copyURL = FileManager.default.temporaryDirectory.appendingPathComponent(
                    "restore_\(UUID().uuidString)_\(safeName)"
                )
                try? FileManager.default.removeItem(at: copyURL)
                try FileManager.default.copyItem(at: url, to: copyURL)
                extraTmpURLs.append(copyURL)
                accepted.append((instance, copyURL))
            }
        }

        return (accepted, extraTmpURLs)
    }

    private func saveToPhotoLibrary(downloaded: [(RemoteAssetResourceInstance, URL)], creationDate: Date?) async throws -> String? {
        // Guard: if all instances had unknown resourceType, addResource never runs and
        // we'd ship an empty creation request → Photos fails the whole asset.
        let hasUsable = downloaded.contains { $0.0.resourceType != nil }
        guard hasUsable else {
            throw NSError(
                domain: "RestoreService",
                code: -2,
                userInfo: [NSLocalizedDescriptionKey: "all resources unrecognized — refusing empty PHAssetCreationRequest"]
            )
        }
        return try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<String?, Error>) in
            var placeholderID: String?
            PHPhotoLibrary.shared().performChanges {
                let request = PHAssetCreationRequest.forAsset()
                request.creationDate = creationDate
                placeholderID = request.placeholderForCreatedAsset?.localIdentifier

                for (instance, url) in downloaded {
                    guard let type = instance.resourceType else { continue }
                    let options = PHAssetResourceCreationOptions()
                    // V1 manifests don't go through the V2 wire validator — sanitize at the Photos boundary.
                    options.originalFilename = RemotePathBuilder.sanitizeFilename(instance.fileName)
                    request.addResource(with: type, fileURL: url, options: options)
                }
            } completionHandler: { success, error in
                if let error {
                    continuation.resume(throwing: error)
                } else if success {
                    continuation.resume(returning: placeholderID)
                } else {
                    continuation.resume(throwing: NSError(
                        domain: "RestoreService",
                        code: -1,
                        userInfo: [NSLocalizedDescriptionKey: String(localized: "restore.error.unknownFailure")]
                    ))
                }
            }
        }
    }
}
