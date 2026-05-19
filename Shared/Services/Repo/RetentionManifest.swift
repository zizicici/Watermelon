import Foundation

enum RetentionManifestError: Error, Equatable {
    case malformed(String)
    case unsupportedVersion(Int)
    case missingField(String)
}

struct RetentionManifestPolicy: Codable, Equatable, Sendable {
    var keepUncoveredCommits: Bool
    var keepCorruptOrUntrustedCommits: Bool
    var keepTombstones: Bool
    var snapshotKeepCount: Int

    enum CodingKeys: String, CodingKey {
        case keepUncoveredCommits = "keep_uncovered_commits"
        case keepCorruptOrUntrustedCommits = "keep_corrupt_or_untrusted_commits"
        case keepTombstones = "keep_tombstones"
        case snapshotKeepCount = "snapshot_keep_count"
    }
}

struct RetentionLivenessGate: Codable, Equatable, Sendable {
    var requiredCompleteView: Bool
    var requiredNoActiveNonSelfWriters: Bool
    var legacyClientGraceMs: Int64

    enum CodingKeys: String, CodingKey {
        case requiredCompleteView = "required_complete_view"
        case requiredNoActiveNonSelfWriters = "required_no_active_non_self_writers"
        case legacyClientGraceMs = "legacy_client_grace_ms"
    }
}

struct RetentionManifest: Equatable, Sendable {
    static let currentVersion = 1

    var version: Int
    private(set) var repoID: String
    var month: LibraryMonthKey
    private(set) var createdByWriterID: String
    var runID: UUID
    var createdAtMs: Int64
    var barrierLamport: UInt64
    var checkpointSnapshotName: String
    private(set) var checkpointSHA256Hex: String
    var coveredRanges: CoveredRanges
    var deletePrefixByWriter: [String: UInt64]
    var observedSeqHighByWriter: [String: UInt64]
    var policy: RetentionManifestPolicy
    var livenessGate: RetentionLivenessGate

    init(
        version: Int,
        repoID: String,
        month: LibraryMonthKey,
        createdByWriterID: String,
        runID: UUID,
        createdAtMs: Int64,
        barrierLamport: UInt64,
        checkpointSnapshotName: String,
        checkpointSHA256Hex: String,
        coveredRanges: CoveredRanges,
        deletePrefixByWriter: [String: UInt64],
        observedSeqHighByWriter: [String: UInt64],
        policy: RetentionManifestPolicy,
        livenessGate: RetentionLivenessGate
    ) {
        let canonicalRepoID = UUID(uuidString: repoID)?.uuidString.lowercased()
        precondition(canonicalRepoID != nil, "repoID must be a UUID")
        let canonicalWriterID = createdByWriterID.lowercased()
        precondition(RepoLayout.isValidWriterID(canonicalWriterID), "createdByWriterID must be a UUID")
        self.version = version
        self.repoID = canonicalRepoID!
        self.month = month
        self.createdByWriterID = canonicalWriterID
        self.runID = runID
        self.createdAtMs = createdAtMs
        self.barrierLamport = barrierLamport
        self.checkpointSnapshotName = checkpointSnapshotName
        self.checkpointSHA256Hex = checkpointSHA256Hex.lowercased()
        self.coveredRanges = coveredRanges
        self.deletePrefixByWriter = deletePrefixByWriter
        self.observedSeqHighByWriter = observedSeqHighByWriter
        self.policy = policy
        self.livenessGate = livenessGate
    }

    var ref: RetentionManifestRef {
        RetentionManifestRef(
            month: month,
            lamport: barrierLamport,
            writerID: createdByWriterID,
            runIDPrefix: RepoLayout.runIDPrefix(runID.uuidString)
        )
    }
}

extension RetentionManifest: Codable {
    enum CodingKeys: String, CodingKey {
        case version
        case repoID = "repo_id"
        case month
        case createdByWriterID = "created_by_writer_id"
        case runID = "run_id"
        case createdAtMs = "created_at_ms"
        case barrierLamport = "barrier_lamport"
        case checkpointSnapshotName = "checkpoint_snapshot"
        case checkpointSHA256Hex = "checkpoint_sha256"
        case coveredRanges = "covered_ranges"
        case deletePrefixByWriter = "delete_prefix_by_writer"
        case observedSeqHighByWriter = "observed_seq_high_by_writer"
        case policy
        case livenessGate = "liveness_gate"
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        let version = try c.decodeRequired(Int.self, forKey: .version)
        guard version == Self.currentVersion else {
            throw RetentionManifestError.unsupportedVersion(version)
        }

        let repoIDRaw = try c.decodeRequired(String.self, forKey: .repoID)
        guard let repoUUID = UUID(uuidString: repoIDRaw), !repoIDRaw.isEmpty else {
            throw RetentionManifestError.malformed("repo_id")
        }

        let monthRaw = try c.decodeRequired(String.self, forKey: .month)
        guard let month = CommitHeader.parseMonthScope("month:\(monthRaw)") else {
            throw RetentionManifestError.malformed("month")
        }

        let writerID = try c.decodeRequired(String.self, forKey: .createdByWriterID)
        guard RepoLayout.isValidWriterID(writerID) else {
            throw RetentionManifestError.malformed("created_by_writer_id")
        }

        let runIDRaw = try c.decodeRequired(String.self, forKey: .runID)
        guard let runID = UUID(uuidString: runIDRaw) else {
            throw RetentionManifestError.malformed("run_id")
        }

        let createdAtMs = try c.decodeRequired(Int64.self, forKey: .createdAtMs)
        guard createdAtMs >= 0 else {
            throw RetentionManifestError.malformed("created_at_ms")
        }

        let barrierRaw = try c.decodeRequired(String.self, forKey: .barrierLamport)
        guard barrierRaw.count == 16, let barrierLamport = UInt64(barrierRaw, radix: 16) else {
            throw RetentionManifestError.malformed("barrier_lamport")
        }
        guard barrierLamport < LamportClock.maxAdoptableValue else {
            throw RetentionManifestError.malformed("barrier_lamport")
        }

        let checkpointSnapshotName = try c.decodeRequired(String.self, forKey: .checkpointSnapshotName)
        guard checkpointSnapshotName.hasSuffix(".jsonl"),
              let checkpoint = RepoLayout.parseSnapshotFilename(checkpointSnapshotName),
              checkpoint.lamport < LamportClock.maxAdoptableValue,
              checkpoint.month == month,
              checkpoint.lamport == barrierLamport,
              checkpoint.writerID == writerID,
              checkpoint.runIDPrefix == RepoLayout.runIDPrefix(runID.uuidString) else {
            throw RetentionManifestError.malformed("checkpoint_snapshot")
        }

        let checkpointSHA256Hex = try c.decodeRequired(String.self, forKey: .checkpointSHA256Hex).lowercased()
        guard checkpointSHA256Hex.count == 64, Data(hexString: checkpointSHA256Hex) != nil else {
            throw RetentionManifestError.malformed("checkpoint_sha256")
        }

        let rawCovered = try c.decodeRequired([String: [[UInt64]]].self, forKey: .coveredRanges)
        let coveredRanges = try Self.validateCoveredRanges(rawCovered)

        let deletePrefixByWriter = try c.decodeRequired([String: UInt64].self, forKey: .deletePrefixByWriter)
        try Self.validateDeletePrefixes(deletePrefixByWriter, coveredRanges: coveredRanges)

        let observedSeqHighByWriter = try c.decodeRequired([String: UInt64].self, forKey: .observedSeqHighByWriter)
        try Self.validateWriterKeys(observedSeqHighByWriter.keys, field: "observed_seq_high_by_writer")

        let policy = try c.decodeRequired(RetentionManifestPolicy.self, forKey: .policy)
        guard policy.snapshotKeepCount >= 0 else {
            throw RetentionManifestError.malformed("policy.snapshot_keep_count")
        }

        let livenessGate = try c.decodeRequired(RetentionLivenessGate.self, forKey: .livenessGate)
        guard livenessGate.legacyClientGraceMs >= 0 else {
            throw RetentionManifestError.malformed("liveness_gate.legacy_client_grace_ms")
        }

        self.init(
            version: version,
            repoID: repoUUID.uuidString,
            month: month,
            createdByWriterID: writerID,
            runID: runID,
            createdAtMs: createdAtMs,
            barrierLamport: barrierLamport,
            checkpointSnapshotName: checkpointSnapshotName,
            checkpointSHA256Hex: checkpointSHA256Hex,
            coveredRanges: coveredRanges,
            deletePrefixByWriter: deletePrefixByWriter,
            observedSeqHighByWriter: observedSeqHighByWriter,
            policy: policy,
            livenessGate: livenessGate
        )
    }

    func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(version, forKey: .version)
        try c.encode(repoID.lowercased(), forKey: .repoID)
        try c.encode(month.text, forKey: .month)
        try c.encode(createdByWriterID, forKey: .createdByWriterID)
        try c.encode(runID.uuidString.lowercased(), forKey: .runID)
        try c.encode(createdAtMs, forKey: .createdAtMs)
        try c.encode(RepoLayout.format16Hex(barrierLamport), forKey: .barrierLamport)
        try c.encode(checkpointSnapshotName, forKey: .checkpointSnapshotName)
        try c.encode(checkpointSHA256Hex.lowercased(), forKey: .checkpointSHA256Hex)
        try c.encode(coveredRanges.encodedAsRangeArrayMap(), forKey: .coveredRanges)
        try c.encode(deletePrefixByWriter, forKey: .deletePrefixByWriter)
        try c.encode(observedSeqHighByWriter, forKey: .observedSeqHighByWriter)
        try c.encode(policy, forKey: .policy)
        try c.encode(livenessGate, forKey: .livenessGate)
    }

    private static func validateCoveredRanges(_ raw: [String: [[UInt64]]]) throws -> CoveredRanges {
        try validateWriterKeys(raw.keys, field: "covered_ranges")
        for (writerID, ranges) in raw {
            guard !ranges.isEmpty else {
                throw RetentionManifestError.malformed("covered_ranges[\(writerID)] empty")
            }
            for pair in ranges {
                guard pair.count == 2 else {
                    throw RetentionManifestError.malformed("covered_ranges[\(writerID)] pair")
                }
                let low = pair[0]
                let high = pair[1]
                guard low != 0, high >= low else {
                    throw RetentionManifestError.malformed("covered_ranges[\(writerID)] bounds")
                }
            }
        }
        return CoveredRanges.decode(raw)
    }

    private static func validateDeletePrefixes(_ prefixes: [String: UInt64], coveredRanges: CoveredRanges) throws {
        let contiguous = coveredRanges.conservativeContiguousPrefixByWriter()
        for (writerID, prefix) in prefixes {
            guard RepoLayout.isValidWriterID(writerID) else {
                throw RetentionManifestError.malformed("delete_prefix_by_writer[\(writerID)]")
            }
            guard coveredRanges.rangesByWriter[writerID] != nil else {
                throw RetentionManifestError.malformed("delete_prefix_by_writer[\(writerID)] missing covered")
            }
            guard prefix > 0, let coveredPrefix = contiguous[writerID], prefix <= coveredPrefix else {
                throw RetentionManifestError.malformed("delete_prefix_by_writer[\(writerID)] exceeds covered prefix")
            }
        }
    }

    private static func validateWriterKeys(_ keys: Dictionary<String, Any>.Keys, field: String) throws {
        try validateWriterKeys(Array(keys), field: field)
    }

    private static func validateWriterKeys<S: Sequence>(_ keys: S, field: String) throws where S.Element == String {
        for key in keys where !RepoLayout.isValidWriterID(key) {
            throw RetentionManifestError.malformed("\(field)[\(key)]")
        }
    }
}

private extension KeyedDecodingContainer {
    func decodeRequired<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
        guard contains(key) else {
            throw RetentionManifestError.missingField(key.stringValue)
        }
        return try decode(type, forKey: key)
    }
}

struct RetentionBarrierSet: Equatable, Sendable {
    var unsuperseded: [RetentionManifest]
    var unionCovered: CoveredRanges

    static func unsuperseded(manifests: [RetentionManifest]) -> RetentionBarrierSet {
        // Callers must pass already-validated manifests for one repo/month.
        var retained: [RetentionManifest] = []
        for manifest in manifests {
            let dominated = manifests.contains { candidate in
                candidate != manifest &&
                candidate.coveredRanges.superset(of: manifest.coveredRanges) &&
                candidate.ref > manifest.ref
            }
            if !dominated {
                retained.append(manifest)
            }
        }
        let union = retained.reduce(CoveredRanges.empty) { partial, manifest in
            partial.merging(manifest.coveredRanges)
        }
        return RetentionBarrierSet(unsuperseded: retained, unionCovered: union)
    }
}
