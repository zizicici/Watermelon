import Foundation
import ImageIO

struct AssetFingerprintRepoUpgrade: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String
    let assertOwnership: RepoOwnershipGates
    let monthsListing: LiteMonthsListingSnapshot
    let onProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)?
    var execution = RepoUpgradeExecution()

    private actor ThumbnailMigrationState {
        private var entriesByDirectory: [String: Set<String>] = [:]
        private var isMigrating = false
        private var waiters: [CheckedContinuation<Void, Never>] = []

        func migrate(changes: [(previous: Data, current: Data)], using upgrade: AssetFingerprintRepoUpgrade) async throws {
            guard !changes.isEmpty else { return }
            // Different months can refer to the same thumbnail destination.
            if isMigrating {
                await withCheckedContinuation { waiters.append($0) }
            }
            isMigrating = true
            defer {
                // Keep the lock held during handoff so another month cannot overtake the resumed waiter.
                if waiters.isEmpty { isMigrating = false }
                else { waiters.removeFirst().resume() }
            }
            try Task.checkCancellation()
            var entries = entriesByDirectory
            try await upgrade.migrateThumbnails(changes: changes, entriesByDirectory: &entries)
            entriesByDirectory = entries
        }
    }

    func run(createdAt: String, createdBy: String) async throws {
        try Task.checkCancellation()
        // A lease lost during cleanup leaves scratch unrepaired; report the lost lease, not a missing month.
        try await assertOwnership.assertWrite()
        // Before the pending marker, so a blocked upgrade leaves a format-2 repo usable by older clients.
        let months = try await discoverMonths()
        let writer = VersionManifestWriter(client: client, basePath: basePath, assertOwnership: assertOwnership)
        let pending = try await RepoFailureDiagnostics.capture(stage: "upgrade.write_pending",
            path: RepoLayoutLite.versionPath(basePath: basePath)) {
            try await writer.commit(createdAt: createdAt, createdBy: createdBy, upgradePending: true)
        }
        let recoveryPath = RepoLayoutLite.versionTempPath(basePath: basePath)
        let recoveryURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: recoveryURL) }
        let pendingBytes = try VersionManifestLite.encode(pending)
        try pendingBytes.write(to: recoveryURL)
        try await assertOwnership.assertWrite()
        try await RepoFailureDiagnostics.capture(stage: "upgrade.write_recovery_marker", path: recoveryPath) {
            try await client.upload(localURL: recoveryURL, remotePath: recoveryPath, respectTaskCancellation: false, onProgress: nil)
        }
        try await RepoFailureDiagnostics.capture(stage: "upgrade.verify_recovery_marker", path: recoveryPath) {
            try await client.download(remotePath: recoveryPath, localURL: recoveryURL)
            guard try Data(contentsOf: recoveryURL) == pendingBytes else { throw VersionManifestWriter.WriteError.readBackMismatch }
        }
        try await RepoFailureDiagnostics.capture(stage: "upgrade.retire_stale_markers") {
            try await retireStaleVersionScratch(keeping: (recoveryPath as NSString).lastPathComponent)
        }

        let thumbnails = ThumbnailMigrationState()
        await onProgress?(.init(phase: .copying, current: 0, total: months.count))
        try await execution.run(months: months, client: client, operation: { month, workerClient in
            try Task.checkCancellation()
            try await assertOwnership.assertWrite()
            let path = RepoLayoutLite.monthPath(basePath: basePath, month: month)
            let loaded = try await RepoFailureDiagnostics.capture(stage: "upgrade.load_month", path: path) {
                try await MonthManifestStore.loadManifestDirect(
                    client: workerClient, basePath: basePath, year: month.year, month: month.month,
                    layout: .lite, pushSchemaUpgrade: false, assertOwnership: assertOwnership,
                    liteMonthsListing: monthsListing, surfaceDownloadFailure: true
                )
            }
            guard let store = loaded else { throw LiteRepoError.upgradeMonthUnavailable(month: month.text) }
            let worker = AssetFingerprintRepoUpgrade(client: workerClient, basePath: basePath,
                assertOwnership: assertOwnership, monthsListing: monthsListing, onProgress: nil)
            try await RepoFailureDiagnostics.capture(stage: "upgrade.thumbnails", path: path) {
                try await thumbnails.migrate(changes: store.rekeyedAssetFingerprints, using: worker)
            }
            try await RepoFailureDiagnostics.capture(stage: "upgrade.publish_month", path: path) {
                try await store.flushToRemote()
            }
        }, onMonthCompleted: { completed in
            await onProgress?(.init(phase: .copying, current: completed, total: months.count))
        })
        let finalMonths = try await discoverMonths()
        guard finalMonths == months else {
            let missing = Set(months).subtracting(finalMonths).sorted().prefix(12).map(\.text).joined(separator: ",")
            let added = Set(finalMonths).subtracting(months).sorted().prefix(12).map(\.text).joined(separator: ",")
            RepoFailureDiagnostics.record(.monthListChanged, stage: "upgrade.verify_months",
                path: RepoLayoutLite.monthsDirectoryPath(basePath: basePath),
                detail: "before=\(months.count); after=\(finalMonths.count); missing=\(missing); added=\(added)")
            throw LiteRepoError.upgradeMonthsChanged
        }
        try Task.checkCancellation()
        try await assertOwnership.assertWrite()
        await onProgress?(.init(phase: .finalizing, current: 0, total: 0))
        try await RepoFailureDiagnostics.capture(stage: "upgrade.commit_version",
            path: RepoLayoutLite.versionPath(basePath: basePath)) {
            _ = try await writer.commit(createdAt: createdAt, createdBy: createdBy)
        }
        await monthsListing.invalidate(basePath: basePath)
        do {
            try await assertOwnership.assertDestructive()
            try await client.delete(path: recoveryPath)
        } catch { }
    }

    private func migrateThumbnails(changes: [(previous: Data, current: Data)], entriesByDirectory: inout [String: Set<String>]) async throws {
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
            RepoFailureDiagnostics.record(.operationFailed, stage: "upgrade.list_months",
                path: RepoLayoutLite.monthsDirectoryPath(basePath: basePath), error: error)
            throw error
        }
        var canonical = Set<LibraryMonthKey>()
        var recoverable = Set<LibraryMonthKey>()
        for entry in entries {
            if let month = RepoLayoutLite.month(fromFilename: entry.name) {
                guard !entry.isDirectory else { throw LiteRepoError.upgradeMonthPathConflict(month: month.text) }
                canonical.insert(month)
            } else if let month = RepoLayoutLite.month(fromScratchFilename: entry.name) {
                recoverable.insert(month)
            }
        }
        if let missing = recoverable.subtracting(canonical).sorted().first {
            throw LiteRepoError.upgradeMonthUnavailable(month: missing.text)
        }
        return canonical.sorted()
    }

    private func retireStaleVersionScratch(keeping currentRecoveryName: String) async throws {
        let entries = try await client.list(path: RepoLayoutLite.repoDirectoryPath(basePath: basePath))
        for entry in entries where !entry.isDirectory && entry.name != currentRecoveryName
            && VersionManifestLite.isVersionScratchFileName(entry.name) {
            let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            defer { try? FileManager.default.removeItem(at: url) }
            do {
                try await client.download(remotePath: entry.path, localURL: url)
            } catch {
                // Some gateways keep listing a file after it is gone; an absent marker needs no retiring.
                if RemoteFaultLite.classify(error) == .notFound { continue }
                throw error
            }
            guard let manifest = try? VersionManifestLite.decode(Data(contentsOf: url)) else { continue }
            if manifest.formatVersion == 2 {
                // A legacy recovery marker could let an older client reopen partially converted months.
                try await assertOwnership.assertDestructive()
                do {
                    try await client.delete(path: entry.path)
                } catch {
                    guard RemoteFaultLite.classify(error) == .notFound else { throw error }
                }
            } else if manifest.upgradePending == true {
                // Redundant with the marker just verified, so a failed delete only leaves a duplicate.
                do {
                    try await assertOwnership.assertDestructive()
                    try await client.delete(path: entry.path)
                } catch { }
            }
        }
    }
}
