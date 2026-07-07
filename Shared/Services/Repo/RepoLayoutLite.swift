import Foundation

// Repo V2 (Lite) layout vocabulary shared by lock, version, month, and cleanup paths.
nonisolated enum RepoLayoutLite {
    static let repoDirectoryName = WatermelonRemoteFormat.markerDirectoryName
    static let versionFileName = WatermelonRemoteFormat.versionFileName
    static let locksDirectoryName = "locks"
    static let monthsDirectoryName = "months"
    static let lockFileExtension = "lock"
    static let monthFileExtension = "sqlite"
    static let legacyV1PrunePendingFileName = "legacy_v1_prune_pending.json"

    enum ScratchSuffix: String, Sendable {
        case temp = "tmp"
        case backup = "bak"
    }

    // MARK: - Absolute paths under a storage profile base path

    static func repoDirectoryPath(basePath: String) -> String {
        absolute(basePath: basePath, components: [repoDirectoryName])
    }

    static func versionPath(basePath: String) -> String {
        absolute(basePath: basePath, components: [repoDirectoryName, versionFileName])
    }

    static func legacyV1PrunePendingPath(basePath: String) -> String {
        absolute(basePath: basePath, components: [repoDirectoryName, legacyV1PrunePendingFileName])
    }

    static func locksDirectoryPath(basePath: String) -> String {
        absolute(basePath: basePath, components: [repoDirectoryName, locksDirectoryName])
    }

    static func lockPath(basePath: String, writerID: String) -> String? {
        guard let filename = lockFilename(writerID: writerID) else { return nil }
        return absolute(basePath: basePath, components: [repoDirectoryName, locksDirectoryName, filename])
    }

    static func monthsDirectoryPath(basePath: String) -> String {
        absolute(basePath: basePath, components: [repoDirectoryName, monthsDirectoryName])
    }

    static func monthPath(basePath: String, month: LibraryMonthKey) -> String {
        absolute(basePath: basePath, components: [repoDirectoryName, monthsDirectoryName, monthFilename(month: month)])
    }

    // MARK: - Month sqlite filenames (<YYYY-MM>.sqlite)

    static func monthFilename(month: LibraryMonthKey) -> String {
        "\(month.text).\(monthFileExtension)"
    }

    static func month(fromFilename filename: String) -> LibraryMonthKey? {
        guard let base = baseName(of: filename, requiredExtension: monthFileExtension) else { return nil }
        return parseMonthKey(base)
    }

    // Final-derived month scratch ("<YYYY-MM>.sqlite.<token>.tmp" / ".bak"). Returns the canonical month
    // the scratch belongs to so repair-first cleanup can restore it; nil for any other (e.g. legacy
    // opaque "manifest_<uuid>.tmp") shape, which carries no recoverable target.
    static func month(fromScratchFilename filename: String) -> LibraryMonthKey? {
        guard !filename.contains("/"), !filename.contains("\\") else { return nil }
        guard let ext = scratchSuffix(of: filename) else { return nil }
        let withoutScratch = String(filename.dropLast(ext.count + 1))   // drop ".tmp" / ".bak"

        // Split on the first ".sqlite." — the canonical name is "<YYYY-MM>.sqlite" and YYYY-MM never
        // contains it, so the first occurrence delimits canonical-name from the uniqueness token.
        let delimiter = ".\(monthFileExtension)."
        guard let range = withoutScratch.range(of: delimiter) else { return nil }
        let token = String(withoutScratch[range.upperBound...])
        guard !token.isEmpty else { return nil }
        let canonicalName = String(withoutScratch[..<range.lowerBound]) + ".\(monthFileExtension)"
        return month(fromFilename: canonicalName)
    }

    static func liteMonthScratchPath(basePath: String, month: LibraryMonthKey, suffix: ScratchSuffix) -> String {
        monthsDirectoryPath(basePath: basePath)
            + "/\(monthFilename(month: month)).\(UUID().uuidString).\(suffix.rawValue)"
    }

    static func v1OpaqueMonthScratchPath(directory: String, suffix: ScratchSuffix) -> String {
        directory + "/manifest_\(UUID().uuidString).\(suffix.rawValue)"
    }

    // MARK: - V1→Lite migration publish temp (migrate_<uuid>.tmp)

    static let migrationPublishTempPrefix = "migrate_"

    // Transient V1→Lite migration publish temp; reclaimable residue, never a recovery copy.
    static func isMigrationPublishScratch(_ filename: String) -> Bool {
        guard !filename.contains("/"), !filename.contains("\\") else { return false }
        guard filename.hasPrefix(migrationPublishTempPrefix) else { return false }
        return scratchSuffix(of: filename) != nil
    }

    static func isScratchFileName(_ filename: String) -> Bool {
        guard !filename.contains("/"), !filename.contains("\\") else { return false }
        return scratchSuffix(of: filename) != nil
    }

    // A `.bak` scratch holds the prior canonical backed up before an overwrite (vs a `.tmp` in-progress upload).
    static func isBackupScratch(_ filename: String) -> Bool {
        guard !filename.contains("/"), !filename.contains("\\") else { return false }
        return scratchSuffix(of: filename) == ScratchSuffix.backup.rawValue
    }

    static func migrationPublishTempPath(basePath: String) -> String {
        monthsDirectoryPath(basePath: basePath) + "/\(migrationPublishTempPrefix)\(UUID().uuidString).tmp"
    }

    static func repairBackupPath(forCanonicalPath canonicalPath: String) -> String {
        canonicalPath + ".repair-\(UUID().uuidString).bak"
    }

    // MARK: - Version scratch (version_<uuid>.json.tmp / .bak)

    static func versionTempPath(basePath: String) -> String {
        repoDirectoryPath(basePath: basePath) + "/version_\(UUID().uuidString).json.tmp"
    }

    static func versionBackupPath(basePath: String) -> String {
        repoDirectoryPath(basePath: basePath) + "/version_\(UUID().uuidString).json.bak"
    }

    static func isVersionScratchFileName(_ name: String) -> Bool {
        isVersionTempScratchFileName(name) || isVersionBackupScratchFileName(name)
    }

    static func isVersionTempScratchFileName(_ name: String) -> Bool {
        hasValidToken(name, before: ".json.tmp")
    }

    static func isVersionBackupScratchFileName(_ name: String) -> Bool {
        hasValidToken(name, before: ".json.bak")
    }

    // MARK: - MOVE-independence probe scratch (movecheck_<uuid>.<suffix>)

    // Non-dot on purpose: it is the only probe scratch the app writes, and a good WebDAV/NAS that rejects
    // dot-prefixed FILES (while still allowing the `.watermelon` dot-directory) would otherwise fail the probe and
    // be fail-safe'd to non-independent — needlessly losing atomic temp→MOVE for the whole session.
    static let moveProbeScratchPrefix = "movecheck_"

    static func moveProbeScratchPath(basePath: String, token: String, suffix: String) -> String {
        repoDirectoryPath(basePath: basePath) + "/\(moveProbeScratchPrefix)\(token).\(suffix)"
    }

    static func isMoveProbeScratchFileName(_ name: String) -> Bool {
        guard name.hasPrefix(moveProbeScratchPrefix) else { return false }
        let body = name.dropFirst(moveProbeScratchPrefix.count)
        guard body.hasSuffix(".src") || body.hasSuffix(".dst") else { return false }
        return UUID(uuidString: String(body.dropLast(4))) != nil
    }

    // MARK: - Lock filenames (<writerID>.lock)

    static func lockFilename(writerID: String) -> String? {
        guard isCanonicalWriterID(writerID) else { return nil }
        return "\(writerID).\(lockFileExtension)"
    }

    static func writerID(fromLockFilename filename: String) -> String? {
        guard let base = baseName(of: filename, requiredExtension: lockFileExtension) else { return nil }
        return isCanonicalWriterID(base) ? base : nil
    }

    // MARK: - Helpers

    private static func absolute(basePath: String, components: [String]) -> String {
        RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: components.joined(separator: "/")
        )
    }

    // P01 writer IDs are lowercased UUID strings; reject anything else so a foreign or malformed
    // name can't masquerade as a lock owner (this also rules out path separators and empty names).
    private static func isCanonicalWriterID(_ value: String) -> Bool {
        guard value == value.lowercased() else { return false }
        return UUID(uuidString: value) != nil
    }

    private static func baseName(of filename: String, requiredExtension ext: String) -> String? {
        guard !filename.contains("/"), !filename.contains("\\") else { return nil }
        let suffix = ".\(ext)"
        guard filename.hasSuffix(suffix) else { return nil }
        let base = String(filename.dropLast(suffix.count))
        guard !base.isEmpty else { return nil }
        return base
    }

    private static func scratchSuffix(of filename: String) -> String? {
        if filename.hasSuffix(".\(ScratchSuffix.temp.rawValue)") { return ScratchSuffix.temp.rawValue }
        if filename.hasSuffix(".\(ScratchSuffix.backup.rawValue)") { return ScratchSuffix.backup.rawValue }
        return nil
    }

    private static func hasValidToken(_ name: String, before suffix: String) -> Bool {
        guard name.hasPrefix("version_"), name.hasSuffix(suffix) else { return false }
        let tokenStart = name.index(name.startIndex, offsetBy: "version_".count)
        let tokenEnd = name.index(name.endIndex, offsetBy: -suffix.count)
        guard tokenStart < tokenEnd else { return false }
        return UUID(uuidString: String(name[tokenStart..<tokenEnd])) != nil
    }

    private static func parseMonthKey(_ text: String) -> LibraryMonthKey? {
        let parts = text.split(separator: "-", omittingEmptySubsequences: false)
        guard parts.count == 2 else { return nil }
        let yearText = parts[0]
        let monthText = parts[1]
        guard yearText.count == 4, isAllASCIIDigits(yearText),
              monthText.count == 2, isAllASCIIDigits(monthText),
              let year = Int(yearText), let month = Int(monthText),
              (1 ... 12).contains(month) else {
            return nil
        }
        return LibraryMonthKey(year: year, month: month)
    }

    private static func isAllASCIIDigits<S: StringProtocol>(_ value: S) -> Bool {
        !value.isEmpty && value.allSatisfy { $0.isASCII && $0.isNumber }
    }
}

struct LegacyV1PruneMarker: Codable, Equatable, Sendable {
    static let currentSchemaVersion = 1

    struct Source: Codable, Equatable, Sendable {
        let year: Int
        let month: Int
        let manifestPath: String
        let sha256Hex: String

        var monthKey: LibraryMonthKey {
            LibraryMonthKey(year: year, month: month)
        }

        func isCanonicalV1ManifestPath(basePath: String) -> Bool {
            (1 ... 12).contains(month)
                && manifestPath == MonthManifestStore.ManifestLayout.v1.manifestAbsolutePath(
                    basePath: basePath,
                    year: year,
                    month: month
                )
        }
    }

    let schemaVersion: Int
    let sources: [Source]

    init(sources: [Source], schemaVersion: Int = currentSchemaVersion) {
        self.schemaVersion = schemaVersion
        self.sources = sources
    }

    init(migratedSources: [V1ToLiteMigrationSource]) {
        self.init(
            sources: migratedSources.map {
                Source(
                    year: $0.month.year,
                    month: $0.month.month,
                    manifestPath: $0.manifestPath,
                    sha256Hex: $0.sha256Hex
                )
            }
        )
    }

    var isSupported: Bool {
        schemaVersion == Self.currentSchemaVersion
    }
}

actor LiteMonthsListingSnapshot {
    private var cachedBasePath: String?
    private var cachedEntries: [RemoteStorageEntry]?

    func seed(basePath: String, entries: [RemoteStorageEntry]) {
        cachedBasePath = basePath
        cachedEntries = entries
    }

    func entries(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> [RemoteStorageEntry] {
        if cachedBasePath == basePath, let cachedEntries {
            return cachedEntries
        }
        let monthsDirectory = RepoLayoutLite.monthsDirectoryPath(basePath: basePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: monthsDirectory)
        } catch {
            if RemoteFaultLite.classify(error) == .notFound {
                cachedBasePath = basePath
                cachedEntries = []
                return []
            }
            throw error
        }
        cachedBasePath = basePath
        cachedEntries = entries
        return entries
    }

    func invalidate(basePath: String? = nil) {
        guard basePath == nil || basePath == cachedBasePath else { return }
        cachedEntries = nil
    }

    func noteScratchCreated(path: String, basePath: String) {
        guard cachedBasePath == basePath, cachedEntries != nil,
              let name = childName(inMonthsDirectory: path, basePath: basePath),
              name.hasSuffix(".tmp") || name.hasSuffix(".bak") else { return }
        cachedEntries?.removeAll { $0.path == path }
        cachedEntries?.append(RemoteStorageEntry(
            path: path,
            name: name,
            isDirectory: false,
            size: 0,
            creationDate: nil,
            modificationDate: nil
        ))
    }

    func noteDeleted(path: String) {
        cachedEntries?.removeAll { $0.path == path }
    }

    private func childName(inMonthsDirectory path: String, basePath: String) -> String? {
        let directory = RepoLayoutLite.monthsDirectoryPath(basePath: basePath)
        guard path.hasPrefix(directory + "/") else { return nil }
        let name = String(path.dropFirst(directory.count + 1))
        guard !name.isEmpty, !name.contains("/") else { return nil }
        return name
    }
}

nonisolated enum RemoteTimestampComparison {
    // LIST and HEAD/metadata can differ in sub-second precision on S3-compatible backends.
    static func sameSecond(_ a: Date?, _ b: Date?) -> Bool {
        switch (a, b) {
        case let (lhs?, rhs?): return Int(lhs.timeIntervalSince1970) == Int(rhs.timeIntervalSince1970)
        case (nil, nil): return true
        default: return false
        }
    }
}
