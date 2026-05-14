import Foundation

enum RepoLayout {
    static let watermelonDirectory = ".watermelon"
    static let repoFileName = "repo.json"
    static let versionFileName = "version.json"
    static let identityFinalizationFileName = "repo-identity.json"
    static let snapshotsDirectory = "snapshots"
    static let commitsDirectory = "commits"
    static let livenessDirectory = "liveness"
    static let identityDirectory = "identity"
    static let migrationsDirectory = "migrations"
    /// `formatVersion` is what new writers stamp into `version.json` going forward.
    /// `currentSupportedFormatVersion` is the upper bound we accept on read.
    ///
    /// All V2-development additions are encoded as optional wire fields (asset
    /// stamp triple, deletedKey stamp triple, `observedBasis` on tombstones) so
    /// they're forward-compatible within v2: older readers ignore unknown fields
    /// and degrade to pre-stamp behavior; writers always emit them. The format
    /// version stays at 2 until a non-additive change forces a bump.
    static let formatVersion = 2
    static let currentSupportedFormatVersion = 2
    static let minAppVersionPlaceholder = "2.0.0"

    static func repoFilePath(base: String) -> String {
        normalize(joining: [base, watermelonDirectory, repoFileName])
    }

    static func versionFilePath(base: String) -> String {
        normalize(joining: [base, watermelonDirectory, versionFileName])
    }

    static func identityFinalizationFilePath(base: String) -> String {
        normalize(joining: [base, watermelonDirectory, identityFinalizationFileName])
    }

    static func snapshotsDirectoryPath(base: String) -> String {
        normalize(joining: [base, watermelonDirectory, snapshotsDirectory])
    }

    static func commitsDirectoryPath(base: String) -> String {
        normalize(joining: [base, watermelonDirectory, commitsDirectory])
    }

    static func livenessDirectoryPath(base: String) -> String {
        normalize(joining: [base, watermelonDirectory, livenessDirectory])
    }

    static func livenessFilePath(base: String, writerID: String) -> String {
        normalize(joining: [base, watermelonDirectory, livenessDirectory, "\(writerID).json"])
    }

    static func identityDirectoryPath(base: String) -> String {
        normalize(joining: [base, watermelonDirectory, identityDirectory])
    }

    static func identityClaimPath(base: String, writerID: String) -> String {
        normalize(joining: [base, watermelonDirectory, identityDirectory, "\(writerID).json"])
    }

    static func migrationsDirectoryPath(base: String) -> String {
        normalize(joining: [base, watermelonDirectory, migrationsDirectory])
    }

    static func migrationMarkerPath(base: String, writerID: String) -> String {
        normalize(joining: [base, watermelonDirectory, migrationsDirectory, "\(writerID).json"])
    }

    static func snapshotFileName(month: LibraryMonthKey, lamport: UInt64, writerID: String, runID: String) -> String {
        "\(month.text)--\(format16Hex(lamport))--\(writerID)--\(runIDPrefix(runID)).jsonl"
    }

    static func commitFileName(month: LibraryMonthKey, writerID: String, seq: UInt64) -> String {
        "\(month.text)--\(writerID)--\(format16Hex(seq)).jsonl"
    }

    static func snapshotFilePath(base: String, month: LibraryMonthKey, lamport: UInt64, writerID: String, runID: String) -> String {
        normalize(joining: [
            base,
            watermelonDirectory,
            snapshotsDirectory,
            snapshotFileName(month: month, lamport: lamport, writerID: writerID, runID: runID)
        ])
    }

    static func commitFilePath(base: String, month: LibraryMonthKey, writerID: String, seq: UInt64) -> String {
        normalize(joining: [
            base,
            watermelonDirectory,
            commitsDirectory,
            commitFileName(month: month, writerID: writerID, seq: seq)
        ])
    }

    static func format16Hex(_ value: UInt64) -> String {
        let raw = String(value, radix: 16)
        if raw.count >= 16 { return raw }
        return String(repeating: "0", count: 16 - raw.count) + raw
    }

    static func runIDPrefix(_ runID: String) -> String {
        let stripped = runID.replacingOccurrences(of: "-", with: "")
        if stripped.count >= 6 {
            return String(stripped.prefix(6)).lowercased()
        }
        return stripped.lowercased()
    }

    static func writerIDShort(_ writerID: String) -> String {
        let stripped = writerID.replacingOccurrences(of: "-", with: "")
        if stripped.count >= 6 {
            return String(stripped.prefix(6)).lowercased()
        }
        return stripped.lowercased()
    }

    struct ParsedSnapshotFilename: Equatable, Sendable {
        let month: LibraryMonthKey
        let lamport: UInt64
        let writerID: String
        let runIDPrefix: String
    }

    struct ParsedCommitFilename: Equatable, Sendable {
        let month: LibraryMonthKey
        let writerID: String
        let seq: UInt64
    }

    static func parseSnapshotFilename(_ name: String) -> ParsedSnapshotFilename? {
        let stripped = stripJsonlSuffix(name)
        let parts = stripped.components(separatedBy: "--")
        guard parts.count == 4 else { return nil }
        guard let month = parseMonthKey(parts[0]),
              let lamport = UInt64(parts[1], radix: 16),
              isValidWriterID(parts[2]) else {
            return nil
        }
        return ParsedSnapshotFilename(
            month: month,
            lamport: lamport,
            writerID: parts[2],
            runIDPrefix: parts[3]
        )
    }

    static func parseCommitFilename(_ name: String) -> ParsedCommitFilename? {
        let stripped = stripJsonlSuffix(name)
        let parts = stripped.components(separatedBy: "--")
        guard parts.count == 3 else { return nil }
        guard let month = parseMonthKey(parts[0]),
              let seq = UInt64(parts[2], radix: 16),
              isValidWriterID(parts[1]) else {
            return nil
        }
        return ParsedCommitFilename(month: month, writerID: parts[1], seq: seq)
    }

    static func parseLivenessFilename(_ name: String) -> String? {
        guard name.hasSuffix(".json") else { return nil }
        let candidate = String(name.dropLast(".json".count))
        guard isValidWriterID(candidate) else { return nil }
        return candidate
    }

    /// Writers ID == lowercase UUID. Rejects `.DS_Store`-style files in liveness dir,
    /// arbitrary commit/snapshot filename garbage, and short-suffix prefixes used in
    /// physical file paths (those don't appear in metadata filenames).
    static func isValidWriterID(_ s: String) -> Bool {
        guard s.count == 36 else { return false }
        var index = s.startIndex
        let hexRanges: [Int] = [8, 4, 4, 4, 12]
        for (i, length) in hexRanges.enumerated() {
            if i > 0 {
                guard s[index] == "-" else { return false }
                index = s.index(after: index)
            }
            for _ in 0..<length {
                let c = s[index]
                let isHex = (c >= "0" && c <= "9") || (c >= "a" && c <= "f")
                guard isHex else { return false }
                index = s.index(after: index)
            }
        }
        return index == s.endIndex
    }

    private static func stripJsonlSuffix(_ name: String) -> String {
        if name.hasSuffix(".jsonl") {
            return String(name.dropLast(".jsonl".count))
        }
        return name
    }

    private static func parseMonthKey(_ text: String) -> LibraryMonthKey? {
        let parts = text.split(separator: "-")
        guard parts.count == 2,
              let year = Int(parts[0]),
              let month = Int(parts[1]),
              (1...12).contains(month) else {
            return nil
        }
        return LibraryMonthKey(year: year, month: month)
    }

    static func normalize(joining segments: [String]) -> String {
        let combined = segments
            .map { $0.trimmingCharacters(in: CharacterSet(charactersIn: "/")) }
            .filter { !$0.isEmpty }
            .joined(separator: "/")
        return RemotePathBuilder.normalizePath("/" + combined)
    }
}
