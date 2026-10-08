import Foundation

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
        await onProgress?(.init(phase: .copying, current: 0, total: months.count))
        for (index, month) in months.enumerated() {
            try Task.checkCancellation()
            try await assertOwnership.assertWrite()
            guard let store = try await MonthManifestStore.loadManifestDirect(
                client: client, basePath: basePath, year: month.year, month: month.month,
                layout: .lite, pushSchemaUpgrade: false, assertOwnership: assertOwnership,
                liteMonthsListing: monthsListing, surfaceDownloadFailure: true
            ) else { throw LiteRepoError.repoDamaged }
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
