import Foundation

// A remote data file present under <basePath>/YYYY/MM that the month's manifest does not record.
struct LeftoverFile: Sendable, Hashable {
    let month: LibraryMonthKey
    let fileName: String
    let path: String
    let size: Int64
}

struct LeftoverMonthGroup: Sendable {
    let month: LibraryMonthKey
    let files: [LeftoverFile]

    var totalBytes: Int64 { files.reduce(0) { $0 + $1.size } }
}

struct LeftoverScanResult: Sendable {
    let groups: [LeftoverMonthGroup]
    let orphanThumbnailCount: Int
    let orphanThumbnailBytes: Int64

    init(groups: [LeftoverMonthGroup], orphanThumbnailCount: Int = 0, orphanThumbnailBytes: Int64 = 0) {
        self.groups = groups
        self.orphanThumbnailCount = orphanThumbnailCount
        self.orphanThumbnailBytes = orphanThumbnailBytes
    }

    static let empty = LeftoverScanResult(groups: [])

    var allFiles: [LeftoverFile] { groups.flatMap(\.files) }
    var totalCount: Int { groups.reduce(0) { $0 + $1.files.count } }
    var totalBytes: Int64 { groups.reduce(0) { $0 + $1.totalBytes } }
    var hasAnythingToClean: Bool { totalCount > 0 || orphanThumbnailCount > 0 }
}

struct LeftoverDeleteResult: Sendable {
    let deletedCount: Int
    let deletedBytes: Int64
    let failedCount: Int   // re-checked as no-longer-leftover, month skipped on fault, or transport delete failure
    let deletedThumbnailCount: Int
    let deletedThumbnailBytes: Int64

    init(
        deletedCount: Int,
        deletedBytes: Int64,
        failedCount: Int,
        deletedThumbnailCount: Int = 0,
        deletedThumbnailBytes: Int64 = 0
    ) {
        self.deletedCount = deletedCount
        self.deletedBytes = deletedBytes
        self.failedCount = failedCount
        self.deletedThumbnailCount = deletedThumbnailCount
        self.deletedThumbnailBytes = deletedThumbnailBytes
    }

    static let empty = LeftoverDeleteResult(deletedCount: 0, deletedBytes: 0, failedCount: 0)
}

// Forward leftover-file scan/cleanup: complements `RemoteIndexSyncService.verifyMonth` (which prunes manifest
// entries whose remote file is missing). Here we find remote data files the manifest never recorded —
// left by a backup interrupted after byte upload but before the manifest flush. Pure logic: no connection
// or lease management. Only months with an authoritative manifest are considered (the caller proves a
// month is ours by enumerating `.watermelon/months`); a month whose manifest can't be established is
// skipped, never treated as "everything is leftover".
struct LeftoverFileScanner: Sendable {
    // nil = the month has no authoritative manifest (genuinely absent) → skip it. A transport/load fault
    // must throw (fail closed) so a transient blip never collapses the expected set to empty.
    typealias ManifestNamesProvider = @Sendable (LibraryMonthKey) async throws -> Set<String>?

    let client: any RemoteStorageClientProtocol
    let basePath: String
    let months: [LibraryMonthKey]
    let manifestNames: ManifestNamesProvider

    func scan(onProgress: (@Sendable (Int, Int) -> Void)? = nil) async throws -> LeftoverScanResult {
        var groups: [LeftoverMonthGroup] = []
        let total = months.count
        for (index, month) in months.enumerated() {
            try Task.checkCancellation()
            // A transport/load fault aborts the scan (the user is offline / the share is down); only a
            // genuinely absent manifest (nil) silently skips the month.
            if let expected = try await manifestNames(month),
               let group = try await leftoverGroup(for: month, expected: expected) {
                groups.append(group)
            }
            onProgress?(index + 1, total)
        }
        return LeftoverScanResult(groups: groups)
    }

    func delete(
        _ targets: [LeftoverFile],
        assertOwnership: MonthManifestOwnershipAssertion?,
        onProgress: (@Sendable (Int, Int) -> Void)? = nil
    ) async throws -> LeftoverDeleteResult {
        let total = targets.count
        guard total > 0 else { return .empty }

        var byMonth: [LibraryMonthKey: [LeftoverFile]] = [:]
        for target in targets { byMonth[target.month, default: []].append(target) }

        var deletedCount = 0
        var deletedBytes: Int64 = 0
        var failedCount = 0
        var processed = 0

        for (month, monthTargets) in byMonth.sorted(by: { $0.key < $1.key }) {
            try Task.checkCancellation()

            // Re-establish the current leftover set under the fresh lease. A fault here (offline mid-delete)
            // degrades gracefully: skip the month and count its targets as failed rather than aborting and
            // losing the partial result. nil = manifest now absent → also skip (never delete the whole dir).
            let freshLeftover: [String: RemoteStorageEntry]
            do {
                guard let expected = try await manifestNames(month) else {
                    failedCount += monthTargets.count
                    processed += monthTargets.count
                    onProgress?(processed, total)
                    continue
                }
                let listing = try await LiteDataDirectoryProbe.probe(
                    client: client,
                    monthAbsolutePath: Self.monthDataPath(basePath: basePath, month: month)
                )
                freshLeftover = Dictionary(
                    Self.leftoverEntries(in: listing.entries, expected: expected).map { ($0.name, $0) },
                    uniquingKeysWith: { first, _ in first }
                )
            } catch {
                if RemoteFaultLite.classify(error) == .cancelled { throw error }
                failedCount += monthTargets.count
                processed += monthTargets.count
                onProgress?(processed, total)
                continue
            }

            for target in monthTargets {
                try Task.checkCancellation()
                processed += 1
                // Delete the freshly-listed entry, not the scan-time path — a file recorded by the manifest
                // (or vanished) since the scan is no longer a deletable leftover and is kept. The size must
                // also still equal what the user reviewed: ANY change (including to/from an unreported 0)
                // means a same-named file was swapped in during the scan→confirm window — keep it and let the
                // user re-scan rather than delete bytes they never saw. A backend that never reports a size
                // lists 0 on both sides, so 0 == 0 still deletes there.
                guard let entry = freshLeftover[target.fileName], entry.size == target.size else {
                    failedCount += 1
                    onProgress?(processed, total)
                    continue
                }
                // Prove we still own the write lock immediately before each irreversible photo-byte delete;
                // a lost lease (refresh failure / foreign takeover) stops the loop closed rather than
                // deleting data we no longer own.
                try await assertOwnership?()
                do {
                    try await client.delete(path: entry.path)
                    deletedCount += 1
                    deletedBytes += entry.size
                } catch {
                    if RemoteFaultLite.classify(error) == .cancelled { throw error }
                    failedCount += 1
                }
                onProgress?(processed, total)
            }
        }

        return LeftoverDeleteResult(deletedCount: deletedCount, deletedBytes: deletedBytes, failedCount: failedCount)
    }

    private func leftoverGroup(for month: LibraryMonthKey, expected: Set<String>) async throws -> LeftoverMonthGroup? {
        let listing = try await LiteDataDirectoryProbe.probe(
            client: client,
            monthAbsolutePath: Self.monthDataPath(basePath: basePath, month: month)
        )
        let leftovers = Self.leftoverEntries(in: listing.entries, expected: expected)
            .sorted { $0.name < $1.name }
        guard !leftovers.isEmpty else { return nil }
        let files = leftovers.map {
            LeftoverFile(month: month, fileName: $0.name, path: $0.path, size: $0.size)
        }
        return LeftoverMonthGroup(month: month, files: files)
    }

    // Data files (excluding directories and the manifest sibling) whose name is not recorded by the
    // manifest. Names are folded to collision keys before comparison so a case-/Unicode-variant of a
    // recorded file on a case-insensitive backend is never deleted — mirrors the upload-side naming and
    // errs toward keeping a file.
    private static func leftoverEntries(in entries: [RemoteStorageEntry], expected: Set<String>) -> [RemoteStorageEntry] {
        let expectedKeys = RemoteFileNaming.collisionKeySet(from: expected)
        return entries.filter {
            !$0.isDirectory
                && $0.name != MonthManifestStore.manifestFileName
                && !expectedKeys.contains(RemoteFileNaming.collisionKey(for: $0.name))
        }
    }

    static func monthDataPath(basePath: String, month: LibraryMonthKey) -> String {
        RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: String(format: "%04d/%02d", month.year, month.month)
        )
    }
}
