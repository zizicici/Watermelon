import Foundation

nonisolated struct VersionManifestWire: Sendable, Equatable {
    let formatVersion: Int
    let minAppVersion: String?
    let createdAtMs: Int64?
    let createdByWriter: String?

    init(formatVersion: Int, minAppVersion: String?, createdAtMs: Int64?, createdByWriter: String?) {
        self.formatVersion = formatVersion
        self.minAppVersion = minAppVersion
        self.createdAtMs = createdAtMs
        self.createdByWriter = createdByWriter
    }

    init(data: Data) throws {
        let dict = try repoMetadataJSONObject(from: data)
        self.formatVersion = try RepoWireValidator.requireInt(dict["format_version"], field: "format_version")
        self.minAppVersion = dict["min_app_version"] as? String
        self.createdAtMs = repoMetadataOptionalInt64(dict["created_at_ms"])
        self.createdByWriter = dict["created_by_writer"] as? String
    }

    func encode() throws -> Data {
        var dict: [String: Any] = [
            "format_version": formatVersion
        ]
        if let minAppVersion { dict["min_app_version"] = minAppVersion }
        if let createdAtMs { dict["created_at_ms"] = createdAtMs }
        if let createdByWriter { dict["created_by_writer"] = createdByWriter }
        return try repoMetadataJSONData(from: dict, prettyPrinted: true)
    }
}

nonisolated struct MigrationMarkerWire: Sendable, Equatable {
    static let currentVersion = 2

    let writerID: String?
    let phase: MigrationMarkerPhase
    let runID: String?
    let startedAtMs: Int64?
    let lastStepMs: Int64?

    init(writerID: String?, phase: MigrationMarkerPhase, runID: String?, startedAtMs: Int64?, lastStepMs: Int64?) {
        self.writerID = writerID
        self.phase = phase
        self.runID = runID
        self.startedAtMs = startedAtMs
        self.lastStepMs = lastStepMs
    }

    init(data: Data) throws {
        let dict = try repoMetadataJSONObject(from: data)
        let hasVersion = dict["v"] != nil
        if hasVersion {
            let version: Int
            do {
                version = try RepoWireValidator.requireInt(dict["v"], field: "v")
            } catch {
                throw MigrationMarkerError.malformedJSON
            }
            guard version == Self.currentVersion else {
                throw MigrationMarkerError.unsupportedVersion(raw: version)
            }
        }
        if let rawWriter = dict["writer_id"] {
            guard let writerID = rawWriter as? String else {
                throw MigrationMarkerError.writerIDWrongType
            }
            self.writerID = writerID
        } else {
            self.writerID = nil
        }
        if let rawPhase = dict["phase"] {
            let raw: Int
            do {
                raw = try RepoWireValidator.requireInt(rawPhase, field: "phase")
            } catch {
                throw MigrationMarkerError.phaseWrongType
            }
            guard let mapped = MigrationMarkerPhase(rawValue: raw) else {
                throw MigrationMarkerError.unknownPhase(raw: raw)
            }
            self.phase = mapped
        } else if hasVersion {
            throw MigrationMarkerError.phaseWrongType
        } else {
            self.phase = .phase1
        }
        self.runID = dict["run_id"] as? String
        self.startedAtMs = repoMetadataOptionalInt64(dict["started_at_ms"])
        self.lastStepMs = repoMetadataOptionalInt64(dict["last_step_at_ms"])
    }

    func encode() throws -> Data {
        var dict: [String: Any] = [
            "v": Self.currentVersion,
            "phase": phase.rawValue
        ]
        if let writerID { dict["writer_id"] = writerID }
        if let runID { dict["run_id"] = runID }
        if let startedAtMs { dict["started_at_ms"] = startedAtMs }
        if let lastStepMs { dict["last_step_at_ms"] = lastStepMs }
        return try repoMetadataJSONData(from: dict)
    }
}

nonisolated struct IdentityClaimWire: Sendable, Equatable {
    static let currentVersion = 1

    let repoID: String
    let createdAtMs: Int64
    let writerID: String

    init(repoID: String, createdAtMs: Int64, writerID: String) {
        self.repoID = repoID
        self.createdAtMs = createdAtMs
        self.writerID = writerID
    }

    init(data: Data) throws {
        let dict = try repoMetadataJSONObject(from: data)
        let version = try RepoWireValidator.requireInt(dict["v"], field: "v")
        guard version == Self.currentVersion else {
            throw WireValidationError.malformed("v unsupported")
        }
        self.repoID = try RepoWireValidator.validateRepoID(
            try RepoWireValidator.requireString(dict, "repo_id"),
            field: "repo_id"
        )
        self.writerID = try RepoWireValidator.requireNonEmptyString(dict, "writer_id")
        self.createdAtMs = try RepoWireValidator.validateNonNegativeInt64(dict["created_at_ms"], field: "created_at_ms")
    }

    func encode() throws -> Data {
        try repoMetadataJSONData(from: [
            "v": Self.currentVersion,
            "repo_id": repoID,
            "created_at_ms": createdAtMs,
            "writer_id": writerID
        ], prettyPrinted: true)
    }
}

nonisolated struct RepoIdentityFinalizationWire: Sendable, Equatable {
    static let currentVersion = 1

    let repoID: String
    let formatVersion: Int?
    let createdAtMs: Int64?
    let createdByWriter: String?

    init(repoID: String, formatVersion: Int?, createdAtMs: Int64?, createdByWriter: String?) {
        self.repoID = repoID
        self.formatVersion = formatVersion
        self.createdAtMs = createdAtMs
        self.createdByWriter = createdByWriter
    }

    init(data: Data) throws {
        let dict = try repoMetadataJSONObject(from: data)
        let version = try RepoWireValidator.requireInt(dict["v"], field: "v")
        guard version == Self.currentVersion else {
            throw WireValidationError.malformed("v unsupported")
        }
        self.repoID = try RepoWireValidator.validateRepoID(
            try RepoWireValidator.requireString(dict, "repo_id"),
            field: "repo_id"
        )
        self.formatVersion = try? RepoWireValidator.requireInt(dict["format_version"], field: "format_version")
        self.createdAtMs = repoMetadataOptionalInt64(dict["created_at_ms"])
        self.createdByWriter = dict["created_by_writer"] as? String
    }

    func encode() throws -> Data {
        var dict: [String: Any] = [
            "v": Self.currentVersion,
            "repo_id": repoID
        ]
        if let formatVersion { dict["format_version"] = formatVersion }
        if let createdAtMs { dict["created_at_ms"] = createdAtMs }
        if let createdByWriter { dict["created_by_writer"] = createdByWriter }
        return try repoMetadataJSONData(from: dict, prettyPrinted: true)
    }
}

private func repoMetadataJSONObject(from data: Data) throws -> [String: Any] {
    guard let dict = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
        throw WireValidationError.malformed("metadata document is not a JSON object")
    }
    return dict
}

private func repoMetadataJSONData(from dict: [String: Any], prettyPrinted: Bool = false) throws -> Data {
    var options: JSONSerialization.WritingOptions = [.sortedKeys]
    if prettyPrinted { options.insert(.prettyPrinted) }
    return try JSONSerialization.data(withJSONObject: dict, options: options)
}

private func repoMetadataOptionalInt64(_ raw: Any?) -> Int64? {
    try? RepoWireValidator.validateNonNegativeInt64(raw, field: "timestamp")
}
