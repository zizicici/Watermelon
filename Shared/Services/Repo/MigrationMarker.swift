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
    case unsupportedVersion(raw: Int)
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
        let wire: MigrationMarkerWire
        do {
            wire = try MigrationMarkerWire(data: bytes)
        } catch let error as MigrationMarkerError {
            throw error
        } catch {
            throw MigrationMarkerError.malformedJSON
        }
        if let jsonWriter = wire.writerID {
            if jsonWriter != parsedName.writerID {
                throw MigrationMarkerError.writerIDMismatch(
                    filename: filename,
                    jsonWriter: jsonWriter
                )
            }
        }
        return ParsedMigrationMarker(
            writerID: parsedName.writerID,
            phase: wire.phase,
            runID: wire.runID,
            startedAtMs: wire.startedAtMs,
            lastStepMs: wire.lastStepMs
        )
    }

    static func encode(_ marker: ParsedMigrationMarker) throws -> Data {
        try MigrationMarkerWire(
            writerID: marker.writerID,
            phase: marker.phase,
            runID: marker.runID,
            startedAtMs: marker.startedAtMs,
            lastStepMs: marker.lastStepMs
        ).encode()
    }
}
