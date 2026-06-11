import Foundation

// Repo V2 (Lite) layout vocabulary shared by lock, version, month, and cleanup paths.
nonisolated enum RepoLayoutLite {
    static let repoDirectoryName = WatermelonRemoteFormat.markerDirectoryName
    static let versionFileName = WatermelonRemoteFormat.versionFileName
    static let locksDirectoryName = "locks"
    static let monthsDirectoryName = "months"
    static let lockFileExtension = "lock"
    static let monthFileExtension = "sqlite"

    // MARK: - Absolute paths under a storage profile base path

    static func repoDirectoryPath(basePath: String) -> String {
        absolute(basePath: basePath, components: [repoDirectoryName])
    }

    static func versionPath(basePath: String) -> String {
        absolute(basePath: basePath, components: [repoDirectoryName, versionFileName])
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
        let scratchExtensions = ["tmp", "bak"]
        guard let ext = scratchExtensions.first(where: { filename.hasSuffix(".\($0)") }) else { return nil }
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
