import Foundation

struct RetentionManifestRef: Equatable, Comparable, Sendable {
    var month: LibraryMonthKey
    var lamport: UInt64
    var writerID: String
    var runIDPrefix: String

    static func < (lhs: RetentionManifestRef, rhs: RetentionManifestRef) -> Bool {
        // Lamport is not a global freshness proof; ties fall through deterministic peer keys.
        if lhs.lamport != rhs.lamport { return lhs.lamport < rhs.lamport }
        if lhs.writerID != rhs.writerID { return lhs.writerID < rhs.writerID }
        return lhs.runIDPrefix < rhs.runIDPrefix
    }
}

enum RetentionManifestStore {
    static let retentionDirectory = "retention"

    static func filename(for ref: RetentionManifestRef) -> String {
        "\(ref.month.text)--\(RepoLayout.format16Hex(ref.lamport))--\(ref.writerID)--\(ref.runIDPrefix.lowercased()).json"
    }

    static func parseFilename(_ name: String) -> RetentionManifestRef? {
        guard name.hasSuffix(".json") else { return nil }
        let stripped = String(name.dropLast(".json".count))
        let parts = stripped.components(separatedBy: "--")
        guard parts.count == 4 else { return nil }
        guard let month = CommitHeader.parseMonthScope("month:\(parts[0])"),
              month.text == parts[0] else { return nil }
        guard Self.isCanonicalHex(parts[1], count: 16),
              let lamport = UInt64(parts[1], radix: 16),
              lamport > 0, lamport < LamportClock.maxAdoptableValue else { return nil }
        guard RepoLayout.isValidWriterID(parts[2]) else { return nil }
        guard Self.isCanonicalHex(parts[3], count: 6) else { return nil }
        return RetentionManifestRef(month: month, lamport: lamport, writerID: parts[2], runIDPrefix: parts[3])
    }

    private static func isCanonicalHex(_ value: String, count: Int) -> Bool {
        guard value.count == count else { return false }
        return value.allSatisfy { character in
            (character >= "0" && character <= "9") || (character >= "a" && character <= "f")
        }
    }

    static func encode(_ manifest: RetentionManifest) throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        return try encoder.encode(manifest)
    }

    static func decode(_ data: Data) throws -> RetentionManifest {
        try JSONDecoder().decode(RetentionManifest.self, from: data)
    }
}
