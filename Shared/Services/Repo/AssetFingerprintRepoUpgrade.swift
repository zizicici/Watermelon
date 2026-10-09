import Foundation
import ImageIO

struct AssetFingerprintRepoUpgrade: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String
    let assertOwnership: RepoOwnershipGates
    let monthsListing: LiteMonthsListingSnapshot
    let onProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)?

    func run(createdAt: String, createdBy: String) async throws {
        try Task.checkCancellation()
        let writer = VersionManifestWriter(client: client, basePath: basePath, assertOwnership: assertOwnership)
        let pending = try await writer.commit(createdAt: createdAt, createdBy: createdBy, upgradePending: true)
        let recoveryPath = RepoLayoutLite.versionTempPath(basePath: basePath)
        let recoveryURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: recoveryURL) }
        let pendingBytes = try VersionManifestLite.encode(pending)
        try pendingBytes.write(to: recoveryURL)
        try await assertOwnership.assertWrite()
        try await client.upload(localURL: recoveryURL, remotePath: recoveryPath, respectTaskCancellation: false, onProgress: nil)
        try await client.download(remotePath: recoveryPath, localURL: recoveryURL)
        guard try Data(contentsOf: recoveryURL) == pendingBytes else { throw VersionManifestWriter.WriteError.readBackMismatch }
        try await retireLegacyVersionScratch()

        let months = try await discoverMonths()
        var thumbnailEntriesByDirectory: [String: Set<String>] = [:]
        await onProgress?(.init(phase: .copying, current: 0, total: months.count))
        for (index, month) in months.enumerated() {
            try Task.checkCancellation()
            try await assertOwnership.assertWrite()
            guard let store = try await MonthManifestStore.loadManifestDirect(
                client: client, basePath: basePath, year: month.year, month: month.month,
                layout: .lite, pushSchemaUpgrade: false, assertOwnership: assertOwnership,
                liteMonthsListing: monthsListing, surfaceDownloadFailure: true
            ) else { throw LiteRepoError.repoDamaged }
            try await migrateThumbnails(for: store, entriesByDirectory: &thumbnailEntriesByDirectory)
            try await store.flushToRemote()
            await onProgress?(.init(phase: .copying, current: index + 1, total: months.count))
        }
        guard try await discoverMonths() == months else { throw LiteRepoError.repoDamaged }
        try Task.checkCancellation()
        try await assertOwnership.assertWrite()
        await onProgress?(.init(phase: .finalizing, current: 0, total: 0))
        try await writer.commit(createdAt: createdAt, createdBy: createdBy)
        await monthsListing.invalidate(basePath: basePath)
        do {
            try await assertOwnership.assertDestructive()
            try await client.delete(path: recoveryPath)
        } catch { }
    }

    private func migrateThumbnails(for store: MonthManifestStore, entriesByDirectory: inout [String: Set<String>]) async throws {
        let changes = store.rekeyedAssetFingerprints
        guard !changes.isEmpty else { return }
        let root = RemoteThumbnailPaths.rootAbsolutePath(basePath: basePath)
        let shards = try await thumbnailEntries(at: root, cache: &entriesByDirectory)
        guard !shards.isEmpty else { return }
        for change in changes {
            try Task.checkCancellation()
            let oldHex = change.previous.hexString
            guard shards.contains(RemoteThumbnailPaths.shard(forFingerprintHex: oldHex)) else { continue }
            let oldShard = RemoteThumbnailPaths.shardDirectoryAbsolutePath(basePath: basePath, fingerprintHex: oldHex)
            let files = try await thumbnailEntries(at: oldShard, cache: &entriesByDirectory)
            guard files.contains(oldHex + ".jpg") else { continue }
            let destination = RemoteThumbnailPaths.absolutePath(basePath: basePath, fingerprintHex: change.current.hexString)
            if try await thumbnailData(at: destination) != nil { continue }
            let source = RemoteThumbnailPaths.absolutePath(basePath: basePath, fingerprintHex: change.previous.hexString)
            guard let data = try await thumbnailData(at: source) else { continue }
            let shard = RemoteThumbnailPaths.shardDirectoryAbsolutePath(basePath: basePath, fingerprintHex: change.current.hexString)
            let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            defer { try? FileManager.default.removeItem(at: url) }
            try data.write(to: url)
            try await assertOwnership.assertWrite()
            try await client.createDirectory(path: shard)
            do {
                try await uploadThumbnail(at: url, to: destination, mode: .createIfAbsent)
            } catch {
                guard SMBErrorClassifier.isNameCollision(error) else { throw error }
                if try await thumbnailData(at: destination) != nil { continue }
                try await assertOwnership.assertDestructive()
                try await uploadThumbnail(at: url, to: destination, mode: .replace)
            }
            guard try await thumbnailData(at: destination) == data else {
                throw CocoaError(.fileReadCorruptFile)
            }
        }
    }

    private func thumbnailEntries(at path: String, cache: inout [String: Set<String>]) async throws -> Set<String> {
        if let entries = cache[path] { return entries }
        let entries: Set<String>
        do { entries = Set(try await client.list(path: path).map(\.name)) }
        catch {
            if RemoteFaultLite.classify(error) != .notFound { throw error }
            entries = []
        }
        cache[path] = entries
        return entries
    }

    private func thumbnailData(at path: String) async throws -> Data? {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: url) }
        do { try await client.download(remotePath: path, localURL: url) }
        catch {
            if RemoteFaultLite.classify(error) == .notFound { return nil }
            throw error
        }
        let data = try Data(contentsOf: url)
        guard data.starts(with: [0xFF, 0xD8]), data.suffix(2) == Data([0xFF, 0xD9]),
              let source = CGImageSourceCreateWithData(data as CFData, nil),
              CGImageSourceCreateImageAtIndex(source, 0, nil) != nil else { return nil }
        return data
    }

    private func uploadThumbnail(at url: URL, to destination: String, mode: RemoteUploadMode) async throws {
        try Task.checkCancellation()
        try await assertOwnership.assertWrite()
        let client = client
        // Independent, cancellation-shielded writes preserve the old thumbnail on every backend.
        try await Task.detached {
            try await client.upload(localURL: url, remotePath: destination, mode: mode,
                respectTaskCancellation: false, onProgress: nil)
        }.value
    }

    private func discoverMonths() async throws -> [LibraryMonthKey] {
        let entries: [RemoteStorageEntry]
        do { entries = try await client.list(path: RepoLayoutLite.monthsDirectoryPath(basePath: basePath)) }
        catch {
            if RemoteFaultLite.classify(error) == .notFound { return [] }
            throw error
        }
        var canonical = Set<LibraryMonthKey>()
        var recoverable = Set<LibraryMonthKey>()
        for entry in entries {
            if let month = RepoLayoutLite.month(fromFilename: entry.name) {
                guard !entry.isDirectory else { throw LiteRepoError.repoDamaged }
                canonical.insert(month)
            } else if let month = RepoLayoutLite.month(fromScratchFilename: entry.name) {
                recoverable.insert(month)
            }
        }
        guard recoverable.isSubset(of: canonical) else { throw LiteRepoError.repoDamaged }
        return canonical.sorted()
    }

    private func retireLegacyVersionScratch() async throws {
        let entries = try await client.list(path: RepoLayoutLite.repoDirectoryPath(basePath: basePath))
        for entry in entries where !entry.isDirectory && VersionManifestLite.isVersionScratchFileName(entry.name) {
            let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            defer { try? FileManager.default.removeItem(at: url) }
            try await client.download(remotePath: entry.path, localURL: url)
            guard let manifest = try? VersionManifestLite.decode(Data(contentsOf: url)), manifest.formatVersion == 2 else { continue }
            // A legacy recovery marker could let an older client reopen partially converted months.
            try await assertOwnership.assertDestructive()
            try await client.delete(path: entry.path)
        }
    }
}
