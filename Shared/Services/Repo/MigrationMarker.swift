import Foundation

enum MigrationMarkerPhase: Int, Sendable {
    case phase1 = 1
    case phase2 = 2
    case phase3 = 3

    /// phase1 = upload still in flight; cleanup would delete in-flight data.
    var isCleanupSafe: Bool { self != .phase1 }
}

struct ParsedMigrationMarker: Sendable {
    let writerID: String
    let phase: MigrationMarkerPhase
    let runID: String?
    let startedAtMs: Int64?
    let lastStepMs: Int64?
}

enum MigrationMarkerError: Error {
    case filenameUnparseable(name: String)
    case writerIDMismatch(filename: String, jsonWriter: String)
    case writerIDWrongType
    case phaseWrongType
    case unknownPhase(raw: Int)
    case malformedJSON
}

enum MigrationMarker {
    static let currentFormatVersion = 2

    /// Filename writerID is canonical; JSON `writer_id` is cross-checked, never adopted.
    static func parse(filename: String, bytes: Data) throws -> ParsedMigrationMarker {
        guard let parsedName = RepoLayout.parseMigrationMarkerFilename(filename) else {
            throw MigrationMarkerError.filenameUnparseable(name: filename)
        }
        guard let dict = try? JSONSerialization.jsonObject(with: bytes) as? [String: Any] else {
            throw MigrationMarkerError.malformedJSON
        }
        if let rawWriter = dict["writer_id"] {
            guard let jsonWriter = rawWriter as? String else {
                throw MigrationMarkerError.writerIDWrongType
            }
            if jsonWriter != parsedName.writerID {
                throw MigrationMarkerError.writerIDMismatch(
                    filename: filename,
                    jsonWriter: jsonWriter
                )
            }
        }
        let phase: MigrationMarkerPhase
        if let rawPhase = dict["phase"] {
            // JSON booleans bridge through `as? Int` via NSNumber (true → 1); detect CFBoolean
            // by CF type-id rather than `is Bool`, which also matches numeric NSNumber(0/1).
            if let number = rawPhase as? NSNumber, CFGetTypeID(number) == CFBooleanGetTypeID() {
                throw MigrationMarkerError.phaseWrongType
            }
            guard let raw = rawPhase as? Int else {
                throw MigrationMarkerError.phaseWrongType
            }
            guard let mapped = MigrationMarkerPhase(rawValue: raw) else {
                throw MigrationMarkerError.unknownPhase(raw: raw)
            }
            phase = mapped
        } else {
            // Pre-v:2 markers omit `phase`; treat as phase1.
            phase = .phase1
        }
        let runID = dict["run_id"] as? String
        let startedAtMs = strictInt64(dict["started_at_ms"])
        let lastStepMs = strictInt64(dict["last_step_at_ms"])
        return ParsedMigrationMarker(
            writerID: parsedName.writerID,
            phase: phase,
            runID: runID,
            startedAtMs: startedAtMs,
            lastStepMs: lastStepMs
        )
    }

    /// JSON booleans bridge through `as? Int` (true→1); reject CFBoolean so a
    /// corrupt `started_at_ms: true` can't anchor later phase writes at 1ms.
    private static func strictInt64(_ raw: Any?) -> Int64? {
        guard let raw else { return nil }
        if CFGetTypeID(raw as CFTypeRef) == CFBooleanGetTypeID() { return nil }
        if let v = raw as? Int64 { return v }
        if let v = raw as? Int { return Int64(v) }
        return nil
    }

    static func encode(_ marker: ParsedMigrationMarker) throws -> Data {
        var dict: [String: Any] = [
            "v": currentFormatVersion,
            "writer_id": marker.writerID,
            "phase": marker.phase.rawValue
        ]
        if let runID = marker.runID { dict["run_id"] = runID }
        if let startedAtMs = marker.startedAtMs { dict["started_at_ms"] = startedAtMs }
        if let lastStepMs = marker.lastStepMs { dict["last_step_at_ms"] = lastStepMs }
        return try JSONSerialization.data(withJSONObject: dict, options: [.sortedKeys])
    }
}
