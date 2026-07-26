import Foundation
import GRDB

// A remote data file present under <basePath>/YYYY/MM that the month's manifest does not record.
struct LeftoverFile: Sendable, Hashable {
    let month: LibraryMonthKey
    let fileName: String
    let path: String
    let size: Int64
    let modificationDateMs: Int64?

    init(
        month: LibraryMonthKey,
        fileName: String,
        path: String,
        size: Int64,
        modificationDateMs: Int64? = nil
    ) {
        self.month = month
        self.fileName = fileName
        self.path = path
        self.size = size
        self.modificationDateMs = modificationDateMs
    }
}

struct LeftoverMonthGroup: Sendable {
    let month: LibraryMonthKey
    let files: [LeftoverFile]

    var totalBytes: Int64 { files.reduce(0) { $0 + $1.size } }
}

struct LeftoverKnownResource: Sendable, Hashable {
    let month: LibraryMonthKey
    let fileName: String
    let fileSize: Int64
    let contentHash: Data
    let creationDateMs: Int64?

    init(
        month: LibraryMonthKey,
        fileName: String,
        fileSize: Int64,
        contentHash: Data,
        creationDateMs: Int64? = nil
    ) {
        self.month = month
        self.fileName = fileName
        self.fileSize = fileSize
        self.contentHash = contentHash
        self.creationDateMs = creationDateMs
    }

    var remoteRelativePath: String {
        String(format: "%04d/%02d/%@", month.year, month.month, fileName)
    }
}

struct LeftoverProbableMatch: Sendable, Hashable {
    let resource: LeftoverKnownResource
    let hasSimilarName: Bool
    let hasMatchingTime: Bool
}

struct LeftoverProbableMatchSummary: Sendable {
    let matches: [LeftoverProbableMatch]
    let totalCount: Int
}

final class LeftoverKnownResourceCatalog: @unchecked Sendable {
    private let databaseURL: URL
    private let databaseQueue: DatabaseQueue

    init() throws {
        databaseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("leftover-resource-catalog-\(UUID().uuidString).sqlite")
        databaseQueue = try DatabaseQueue(path: databaseURL.path)
        try databaseQueue.write { db in
            try db.execute(sql: """
                CREATE TABLE resources (
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    fileName TEXT NOT NULL,
                    fileSize INTEGER NOT NULL,
                    contentHash BLOB NOT NULL,
                    creationDateMs INTEGER,
                    foldedName TEXT NOT NULL,
                    collisionBaseName TEXT,
                    PRIMARY KEY (year, month, fileName)
                )
                """)
        }
    }

    deinit {
        try? databaseQueue.close()
        let fileManager = FileManager.default
        for suffix in ["", "-shm", "-wal"] {
            try? fileManager.removeItem(atPath: databaseURL.path + suffix)
        }
    }

    func insert(_ resources: [LeftoverKnownResource]) throws {
        guard !resources.isEmpty else { return }
        try databaseQueue.write { db in
            let statement = try db.makeStatement(sql: """
                INSERT OR REPLACE INTO resources (
                    year,
                    month,
                    fileName,
                    fileSize,
                    contentHash,
                    creationDateMs,
                    foldedName,
                    collisionBaseName
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """)
            for resource in resources {
                try statement.setArguments([
                    resource.month.year,
                    resource.month.month,
                    resource.fileName,
                    resource.fileSize,
                    resource.contentHash,
                    resource.creationDateMs,
                    LeftoverProbableMatchFinder.foldedName(resource.fileName),
                    LeftoverProbableMatchFinder.collisionBaseName(resource.fileName)
                ])
                try statement.execute()
            }
        }
    }

    func finalize() throws {
        try databaseQueue.write { db in
            try db.execute(sql: "CREATE INDEX IF NOT EXISTS resources_hash ON resources(contentHash)")
            try db.execute(sql: "CREATE INDEX IF NOT EXISTS resources_size_name ON resources(fileSize, foldedName)")
            try db.execute(sql: "CREATE INDEX IF NOT EXISTS resources_size_collision ON resources(fileSize, collisionBaseName)")
            try db.execute(sql: "CREATE INDEX IF NOT EXISTS resources_size_time ON resources(fileSize, creationDateMs)")
        }
    }

    func probableMatches(
        for files: [LeftoverFile]
    ) throws -> [String: LeftoverProbableMatchSummary] {
        try databaseQueue.read { db in
            var result: [String: LeftoverProbableMatchSummary] = [:]
            for file in files where file.size > 0 {
                let exactName = LeftoverProbableMatchFinder.foldedName(file.fileName)
                let baseName = LeftoverProbableMatchFinder.collisionBaseName(file.fileName)
                let timeBounds = file.modificationDateMs.map(Self.timeBounds)
                let rows = try Row.fetchAll(
                    db,
                    sql: """
                        SELECT year, month, fileName, fileSize, contentHash, creationDateMs
                        FROM resources
                        WHERE fileSize = :size
                          AND (
                            foldedName = :exactName
                            OR collisionBaseName = :exactName
                            OR (
                              :baseName IS NOT NULL
                              AND (foldedName = :baseName OR collisionBaseName = :baseName)
                            )
                            OR (
                              :hasTime = 1
                              AND creationDateMs BETWEEN :timeLowerBound AND :timeUpperBound
                            )
                          )
                        """,
                    arguments: [
                        "size": file.size,
                        "exactName": exactName,
                        "baseName": baseName,
                        "hasTime": timeBounds == nil ? 0 : 1,
                        "timeLowerBound": timeBounds?.lower ?? 0,
                        "timeUpperBound": timeBounds?.upper ?? 0
                    ]
                )
                let resources = rows.map(Self.resource)
                if let summary = LeftoverProbableMatchFinder.find(
                    files: [file],
                    resources: resources
                )[file.path] {
                    result[file.path] = summary
                }
            }
            return result
        }
    }

    func resources(matchingHash hash: Data) throws -> [LeftoverKnownResource] {
        try databaseQueue.read { db in
            try Row.fetchAll(
                db,
                sql: """
                    SELECT year, month, fileName, fileSize, contentHash, creationDateMs
                    FROM resources
                    WHERE contentHash = ?
                    """,
                arguments: [hash]
            ).map(Self.resource)
        }
    }

    private static func resource(_ row: Row) -> LeftoverKnownResource {
        LeftoverKnownResource(
            month: LibraryMonthKey(year: row["year"], month: row["month"]),
            fileName: row["fileName"],
            fileSize: row["fileSize"],
            contentHash: row["contentHash"],
            creationDateMs: row["creationDateMs"]
        )
    }

    private static func timeBounds(_ value: Int64) -> (lower: Int64, upper: Int64) {
        let (lower, lowerOverflow) = value.subtractingReportingOverflow(3_000)
        let (upper, upperOverflow) = value.addingReportingOverflow(3_000)
        return (
            lower: lowerOverflow ? .min : lower,
            upper: upperOverflow ? .max : upper
        )
    }
}

enum LeftoverKnownResourceFilter {
    static func relevant(
        to files: [LeftoverFile],
        resources: [LeftoverKnownResource]
    ) -> [LeftoverKnownResource] {
        guard !files.isEmpty else { return [] }
        guard files.allSatisfy({ $0.size > 0 }) else { return resources }
        let targetSizes = Set(files.map(\.size))
        return resources.filter { $0.fileSize <= 0 || targetSizes.contains($0.fileSize) }
    }
}

enum LeftoverProbableMatchFinder {
    private struct SizeNameKey: Hashable {
        let size: Int64
        let name: String
    }

    private struct SizeTimeKey: Hashable {
        let size: Int64
        let second: Int64
    }

    private static let timeToleranceMs: Int64 = 3_000
    private static let maximumDisplayedMatches = 3

    static func find(
        files: [LeftoverFile],
        resources: [LeftoverKnownResource]
    ) -> [String: LeftoverProbableMatchSummary] {
        var byExactName: [SizeNameKey: [LeftoverKnownResource]] = [:]
        var byCollisionBaseName: [SizeNameKey: [LeftoverKnownResource]] = [:]
        var byTime: [SizeTimeKey: [LeftoverKnownResource]] = [:]
        for resource in resources where resource.fileSize > 0 {
            let exactName = foldedName(resource.fileName)
            byExactName[
                SizeNameKey(size: resource.fileSize, name: exactName),
                default: []
            ].append(resource)
            if let collisionBaseName = collisionBaseName(resource.fileName) {
                byCollisionBaseName[
                    SizeNameKey(size: resource.fileSize, name: collisionBaseName),
                    default: []
                ].append(resource)
            }
            if let creationDateMs = resource.creationDateMs {
                byTime[
                    SizeTimeKey(size: resource.fileSize, second: creationDateMs / 1_000),
                    default: []
                ].append(resource)
            }
        }

        var result: [String: LeftoverProbableMatchSummary] = [:]
        for file in files where file.size > 0 {
            let exactName = foldedName(file.fileName)
            var similarNameResources = byExactName[
                SizeNameKey(size: file.size, name: exactName),
                default: []
            ]
            if let collisionBaseName = collisionBaseName(file.fileName) {
                similarNameResources.append(contentsOf: byExactName[
                    SizeNameKey(size: file.size, name: collisionBaseName),
                    default: []
                ])
                similarNameResources.append(contentsOf: byCollisionBaseName[
                    SizeNameKey(size: file.size, name: collisionBaseName),
                    default: []
                ])
            }
            similarNameResources.append(contentsOf: byCollisionBaseName[
                SizeNameKey(size: file.size, name: exactName),
                default: []
            ])
            var candidates: [LeftoverKnownResource: LeftoverProbableMatch] = [:]
            for resource in similarNameResources {
                candidates[resource] = LeftoverProbableMatch(
                    resource: resource,
                    hasSimilarName: true,
                    hasMatchingTime: timestampsMatch(file.modificationDateMs, resource.creationDateMs)
                )
            }
            if let modificationDateMs = file.modificationDateMs {
                let second = modificationDateMs / 1_000
                for offset in -3 ... 3 {
                    let (nearbySecond, overflow) = second.addingReportingOverflow(Int64(offset))
                    guard !overflow else { continue }
                    for resource in byTime[
                        SizeTimeKey(size: file.size, second: nearbySecond),
                        default: []
                    ] {
                        guard timestampsMatch(modificationDateMs, resource.creationDateMs) else { continue }
                        let similarName = namesAreSimilar(file.fileName, resource.fileName)
                        candidates[resource] = LeftoverProbableMatch(
                            resource: resource,
                            hasSimilarName: similarName,
                            hasMatchingTime: true
                        )
                    }
                }
            }
            guard !candidates.isEmpty else { continue }
            let matches = candidates.values.sorted(by: matchSort)
            result[file.path] = LeftoverProbableMatchSummary(
                matches: Array(matches.prefix(maximumDisplayedMatches)),
                totalCount: matches.count
            )
        }
        return result
    }

    static func foldedName(_ fileName: String) -> String {
        RemoteFileNaming.collisionKey(for: fileName)
    }

    static func collisionBaseName(_ fileName: String) -> String? {
        let pathExtension = (fileName as NSString).pathExtension
        let stem = (fileName as NSString).deletingPathExtension
        let baseStem = stem.replacingOccurrences(
            of: "_[1-9][0-9]*$",
            with: "",
            options: .regularExpression
        )
        guard baseStem != stem else { return nil }
        return foldedName(pathExtension.isEmpty ? baseStem : "\(baseStem).\(pathExtension)")
    }

    private static func namesAreSimilar(_ lhs: String, _ rhs: String) -> Bool {
        let lhsExact = foldedName(lhs)
        let rhsExact = foldedName(rhs)
        if lhsExact == rhsExact
            || collisionBaseName(lhs) == rhsExact
            || collisionBaseName(rhs) == lhsExact {
            return true
        }
        guard let lhsBase = collisionBaseName(lhs),
              let rhsBase = collisionBaseName(rhs) else { return false }
        return lhsBase == rhsBase
    }

    private static func timestampsMatch(_ lhs: Int64?, _ rhs: Int64?) -> Bool {
        guard let lhs, let rhs else { return false }
        let (difference, overflow) = lhs.subtractingReportingOverflow(rhs)
        return !overflow && difference >= -timeToleranceMs && difference <= timeToleranceMs
    }

    private static func matchSort(_ lhs: LeftoverProbableMatch, _ rhs: LeftoverProbableMatch) -> Bool {
        let lhsEvidence = (lhs.hasSimilarName ? 1 : 0) + (lhs.hasMatchingTime ? 1 : 0)
        let rhsEvidence = (rhs.hasSimilarName ? 1 : 0) + (rhs.hasMatchingTime ? 1 : 0)
        if lhsEvidence != rhsEvidence { return lhsEvidence > rhsEvidence }
        if lhs.resource.month != rhs.resource.month {
            return lhs.resource.month > rhs.resource.month
        }
        return lhs.resource.fileName < rhs.resource.fileName
    }
}

enum LeftoverHashCheckStatus: Sendable, Equatable {
    case matched(hashHex: String, resources: [LeftoverKnownResource])
    case noMatch(hashHex: String)
    case failed
}

struct LeftoverHashCheckResult: Sendable {
    let statusByPath: [String: LeftoverHashCheckStatus]
}

struct LeftoverScanResult: Sendable {
    let groups: [LeftoverMonthGroup]
    let orphanThumbnailCount: Int
    let orphanThumbnailBytes: Int64
    let knownResources: [LeftoverKnownResource]
    let knownResourceCatalog: LeftoverKnownResourceCatalog?
    let probableMatchesByPath: [String: LeftoverProbableMatchSummary]

    init(
        groups: [LeftoverMonthGroup],
        orphanThumbnailCount: Int = 0,
        orphanThumbnailBytes: Int64 = 0,
        knownResources: [LeftoverKnownResource] = [],
        knownResourceCatalog: LeftoverKnownResourceCatalog? = nil,
        probableMatchesByPath: [String: LeftoverProbableMatchSummary] = [:]
    ) {
        self.groups = groups
        self.orphanThumbnailCount = orphanThumbnailCount
        self.orphanThumbnailBytes = orphanThumbnailBytes
        self.knownResources = knownResources
        self.knownResourceCatalog = knownResourceCatalog
        self.probableMatchesByPath = probableMatchesByPath
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
    let failedCount: Int
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

struct LeftoverManifestSnapshot: Sendable {
    let fileNames: Set<String>
    let assetFingerprintHexes: Set<String>
    let knownResources: [LeftoverKnownResource]

    init(
        fileNames: Set<String>,
        assetFingerprintHexes: Set<String>,
        knownResources: [LeftoverKnownResource] = []
    ) {
        self.fileNames = fileNames
        self.assetFingerprintHexes = assetFingerprintHexes
        self.knownResources = knownResources
    }
}

struct LeftoverMonthScanResult: Sendable {
    let group: LeftoverMonthGroup?
    let liveFingerprintHexes: Set<String>
    let knownResources: [LeftoverKnownResource]
}

struct LeftoverDataScanResult: Sendable {
    let groups: [LeftoverMonthGroup]
    let liveFingerprintHexes: Set<String>
    let knownResources: [LeftoverKnownResource]
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
    typealias ManifestSnapshotProvider = @Sendable (LibraryMonthKey) async throws -> LeftoverManifestSnapshot?

    let client: any RemoteStorageClientProtocol
    let basePath: String
    let months: [LibraryMonthKey]
    let manifestSnapshots: ManifestSnapshotProvider

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        months: [LibraryMonthKey],
        manifestSnapshots: @escaping ManifestSnapshotProvider
    ) {
        self.client = client
        self.basePath = basePath
        self.months = months
        self.manifestSnapshots = manifestSnapshots
    }

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        months: [LibraryMonthKey],
        manifestNames: @escaping ManifestNamesProvider
    ) {
        self.init(client: client, basePath: basePath, months: months) { month in
            try await manifestNames(month).map {
                LeftoverManifestSnapshot(fileNames: $0, assetFingerprintHexes: [])
            }
        }
    }

    func scan(onProgress: (@Sendable (Int, Int) -> Void)? = nil) async throws -> LeftoverScanResult {
        let dataResult = try await scanData(onProgress: onProgress)
        return LeftoverScanResult(groups: dataResult.groups, knownResources: dataResult.knownResources)
    }

    func scanData(onProgress: (@Sendable (Int, Int) -> Void)? = nil) async throws -> LeftoverDataScanResult {
        var groups: [LeftoverMonthGroup] = []
        var liveFingerprintHexes = Set<String>()
        var knownResources: [LeftoverKnownResource] = []
        let total = months.count
        for (index, month) in months.enumerated() {
            try Task.checkCancellation()
            let monthResult = try await scanMonth(month)
            if let group = monthResult.group {
                groups.append(group)
            }
            liveFingerprintHexes.formUnion(monthResult.liveFingerprintHexes)
            knownResources.append(contentsOf: monthResult.knownResources)
            onProgress?(index + 1, total)
        }
        return LeftoverDataScanResult(
            groups: groups,
            liveFingerprintHexes: liveFingerprintHexes,
            knownResources: LeftoverKnownResourceFilter.relevant(
                to: groups.flatMap(\.files),
                resources: knownResources
            )
        )
    }

    func scanMonth(_ month: LibraryMonthKey) async throws -> LeftoverMonthScanResult {
        guard let snapshot = try await manifestSnapshots(month) else {
            return LeftoverMonthScanResult(group: nil, liveFingerprintHexes: [], knownResources: [])
        }
        let listing = try await LiteDataDirectoryProbe.probe(
            client: client,
            monthAbsolutePath: Self.monthDataPath(basePath: basePath, month: month)
        )
        let listedFilesByName = Dictionary(
            listing.entries.lazy
                .filter { !$0.isDirectory }
                .map { ($0.name, $0) },
            uniquingKeysWith: { first, _ in first }
        )
        return LeftoverMonthScanResult(
            group: leftoverGroup(for: month, entries: listing.entries, expected: snapshot.fileNames),
            liveFingerprintHexes: snapshot.assetFingerprintHexes,
            knownResources: snapshot.knownResources.filter {
                guard let entry = listedFilesByName[$0.fileName] else { return false }
                return $0.fileSize <= 0 || entry.size <= 0 || entry.size == $0.fileSize
            }
        )
    }

    func delete(
        _ targets: [LeftoverFile],
        assertOwnership: MonthManifestOwnershipAssertion?,
        onProgress: (@Sendable (Int, Int) async -> Void)? = nil
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
                guard let snapshot = try await manifestSnapshots(month) else {
                    failedCount += monthTargets.count
                    processed += monthTargets.count
                    await onProgress?(processed, total)
                    continue
                }
                let listing = try await LiteDataDirectoryProbe.probe(
                    client: client,
                    monthAbsolutePath: Self.monthDataPath(basePath: basePath, month: month)
                )
                freshLeftover = Dictionary(
                    Self.leftoverEntries(in: listing.entries, expected: snapshot.fileNames).map { ($0.name, $0) },
                    uniquingKeysWith: { first, _ in first }
                )
            } catch {
                if RemoteFaultLite.classify(error) == .cancelled { throw error }
                failedCount += monthTargets.count
                processed += monthTargets.count
                await onProgress?(processed, total)
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
                    await onProgress?(processed, total)
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
                await onProgress?(processed, total)
            }
        }

        return LeftoverDeleteResult(deletedCount: deletedCount, deletedBytes: deletedBytes, failedCount: failedCount)
    }

    private func leftoverGroup(
        for month: LibraryMonthKey,
        entries: [RemoteStorageEntry],
        expected: Set<String>
    ) -> LeftoverMonthGroup? {
        let leftovers = Self.leftoverEntries(in: entries, expected: expected)
            .sorted { $0.name < $1.name }
        guard !leftovers.isEmpty else { return nil }
        let files = leftovers.map {
            LeftoverFile(
                month: month,
                fileName: $0.name,
                path: $0.path,
                size: $0.size,
                modificationDateMs: client.shouldSetModificationDate()
                    ? $0.modificationDate?.millisecondsSinceEpoch
                    : nil
            )
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
