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
        let assetFingerprint: AssetFingerprint
        /// Asset-row creation date. Authoritative per-asset truth; not derived from
        /// resource instances, whose dates can belong to a duplicate-content peer.
        let creationDateMs: Int64?
    }

    /// Restore creation date is the asset row's own value. Resource-instance dates are
    /// stamped from whichever asset committed a shared content path, so deriving from them
    /// imports duplicate-content assets with a peer's creation date.
    static func restoreCreationDate(for descriptor: RestoreItemDescriptor) -> Date? {
        descriptor.creationDateMs.map { Date(millisecondsSinceEpoch: $0) }
    }

    struct RestoredAsset {
        // Newly-created PhotoKit asset's id, valid only on this device after the import.
        let localIdentifier: String
        let importedInstances: [RemoteAssetResourceInstance]
    }

    struct RestoredItem: Sendable {
        let assetFingerprint: AssetFingerprint
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
                let creationDate = Self.restoreCreationDate(for: item)
                let group = RestoreGroup(creationDate: creationDate, instances: item.instances)
                var restoredItem: RestoredItem?
                // Per-item try/catch: one bad asset (hash mismatch, schema corruption) must not
                // abort restoring the remaining N-1. Cancellation propagates. Connection-unavailable
                // aborts the batch — running the remaining N-1 against a dead connection just turns
                // every item into a "per-item failure" instead of surfacing the real cause.
                do {
                    if let asset = try await restoreGroup(group, profile: profile, storageClient: storageClient) {
                        let restored = RestoredItem(assetFingerprint: item.assetFingerprint, asset: asset)
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
                do {
                    try await notifyItemCompletedWithRetry(
                        index: index + 1,
                        total: items.count,
                        restoredItem: restoredItem,
                        onItemCompleted: onItemCompleted
                    )
                } catch is CancellationError {
                    throw CancellationError()
                } catch {
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

    private func notifyItemCompletedWithRetry(
        index: Int,
        total: Int,
        restoredItem: RestoredItem?,
        onItemCompleted: @Sendable (Int, Int, RestoredItem?) async throws -> Void
    ) async throws {
        var lastError: Error?
        for attempt in 0..<3 {
            do {
                try await onItemCompleted(index, total, restoredItem)
                return
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                lastError = error
                if attempt < 2 {
                    try await Task.sleep(for: .milliseconds(250 * (1 << attempt)))
                }
            }
        }
        if let lastError { throw lastError }
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

        do {
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
        } catch {
            for (_, url) in downloaded {
                try? FileManager.default.removeItem(at: url)
            }
            throw error
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
        // A just-committed object (e.g. a single-part create) can be durable yet briefly unreadable
        // inside the backend's advertised read-after-write window. Retry the candidate set within
        // grace when every failure was a data-path not-found; proven wrong bytes (size/hash mismatch)
        // never grace-retry since waiting cannot heal them.
        let graceDeadline = storageClient.readAfterWriteGraceSeconds > 0
            ? storageClient.metadataReadAfterWriteDeadline(floorSeconds: 1)
            : nil
        var lastError: Error?
        // Proven wrong bytes cannot heal by waiting; keep the strongest mismatch to surface over any 404.
        var integrityMismatch: Error?
        var attempt = 0
        while true {
            var sawNotFoundLag = false
            var allFailuresWereNotFound = true
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
                        let mismatchError = NSError(
                            domain: "RestoreService",
                            code: -10,
                            userInfo: [NSLocalizedDescriptionKey: "downloaded bytes don't match manifest (\(mismatch))"]
                        )
                        lastError = mismatchError
                        if integrityMismatch == nil { integrityMismatch = mismatchError }
                        allFailuresWereNotFound = false
                        continue
                    }
                    return
                } catch is CancellationError {
                    throw CancellationError()
                } catch {
                    lastError = error
                    if isStorageNotFoundError(error) {
                        sawNotFoundLag = true
                    } else {
                        allFailuresWereNotFound = false
                    }
                    print("[RestoreService]   download FAILED: \(instance.fileName), remotePath=\(remotePath), reason=\(error.localizedDescription)")
                }
            }
            // Grace-retry only a pure read-after-write lag: every failure was a data-path 404 and no
            // candidate proved wrong bytes. A deterministic mismatch is surfaced over any later 404.
            guard let graceDeadline, sawNotFoundLag, allFailuresWereNotFound, Date() < graceDeadline else {
                throw integrityMismatch ?? lastError ?? CancellationError()
            }
            let millis = 200 * (1 << min(attempt, 3))
            attempt += 1
            do {
                try await Task.sleep(nanoseconds: UInt64(millis) * 1_000_000)
            } catch {
                throw CancellationError()
            }
        }
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
        // Copy shared temp files per role because PHAssetCreationRequest consumes each URL it receives.
        var seenURLs = Set<URL>()

        var pendingCopyURL: URL?
        do {
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
                    pendingCopyURL = copyURL
                    try FileManager.default.copyItem(at: url, to: copyURL)
                    pendingCopyURL = nil
                    extraTmpURLs.append(copyURL)
                    accepted.append((instance, copyURL))
                }
            }
        } catch {
            for url in extraTmpURLs {
                try? FileManager.default.removeItem(at: url)
            }
            if let pending = pendingCopyURL {
                try? FileManager.default.removeItem(at: pending)
            }
            throw error
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
                    // A success with no placeholder id leaves no PhotoKit local id to bind/verify the
                    // hash index against, so the download would "complete" with no durable local
                    // fingerprint. Fail closed so the item counts as a restore failure, not a silent skip.
                    if let placeholderID {
                        continuation.resume(returning: placeholderID)
                    } else {
                        continuation.resume(throwing: NSError(
                            domain: "RestoreService",
                            code: -11,
                            userInfo: [NSLocalizedDescriptionKey: "Photos import succeeded without a placeholder identifier"]
                        ))
                    }
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
