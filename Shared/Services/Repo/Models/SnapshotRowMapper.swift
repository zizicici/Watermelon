import Foundation

enum SnapshotWireError: Error, Equatable {
    case malformed(String)
    case unknownRowType(String)
    case unsupportedVersion(Int)
    case missingField(String)
    case unknownKeyType(String)
}

enum SnapshotRowMapper {
    static func encodeHeaderLine(_ header: SnapshotHeader) throws -> String {
        var dict: [String: Any] = [
            "t": "header",
            "v": header.version,
            "scope": header.scope,
            "writerID": header.writerID,
            "covered": header.covered.encodedAsRangeArrayMap()
        ]
        if !header.repoID.isEmpty {
            dict["repoID"] = header.repoID
        }
        return try CommitOpMapper.jsonLine(dict: dict)
    }

    static func encodeAssetLine(_ row: SnapshotAssetRow) throws -> String {
        var inner: [String: Any] = [
            "assetFingerprint": row.assetFingerprint.hexString,
            "backedUpAtMs": row.backedUpAtMs,
            "resourceCount": row.resourceCount,
            "totalFileSizeBytes": row.totalFileSizeBytes
        ]
        inner["creationDateMs"] = row.creationDateMs as Any? ?? NSNull()
        if let stamp = row.stamp {
            inner["lastWriterID"] = stamp.writerID
            inner["lastSeq"] = stamp.seq
            inner["lastClock"] = stamp.clock
        }
        let dict: [String: Any] = ["t": "asset", "r": inner]
        return try CommitOpMapper.jsonLine(dict: dict)
    }

    static func encodeResourceLine(_ row: SnapshotResourceRow) throws -> String {
        var inner: [String: Any] = [
            "physicalRemotePath": row.physicalRemotePath,
            "contentHash": row.contentHash.hexString,
            "fileSize": row.fileSize,
            "resourceType": row.resourceType,
            "backedUpAtMs": row.backedUpAtMs
        ]
        inner["creationDateMs"] = row.creationDateMs as Any? ?? NSNull()
        if let crypto = row.crypto {
            inner["crypto"] = CommitOpMapper.encodeCrypto(crypto)
        } else {
            inner["crypto"] = NSNull()
        }
        if let stamp = row.stamp {
            inner["lastWriterID"] = stamp.writerID
            inner["lastSeq"] = stamp.seq
            inner["lastClock"] = stamp.clock
        }
        let dict: [String: Any] = ["t": "resource", "r": inner]
        return try CommitOpMapper.jsonLine(dict: dict)
    }

    static func encodeAssetResourceLine(_ row: SnapshotAssetResourceRow) throws -> String {
        let inner: [String: Any] = [
            "assetFingerprint": row.assetFingerprint.hexString,
            "role": row.role,
            "slot": row.slot,
            "resourceHash": row.resourceHash.hexString,
            "logicalName": row.logicalName
        ]
        let dict: [String: Any] = ["t": "asset_resource", "r": inner]
        return try CommitOpMapper.jsonLine(dict: dict)
    }

    static func encodeDeletedKeyLine(_ row: SnapshotDeletedKeyRow) throws -> String {
        var inner: [String: Any] = [
            "keyType": row.keyType.rawValue,
            "keyValue": row.keyValue
        ]
        if let stamp = row.stamp {
            inner["lastWriterID"] = stamp.writerID
            inner["lastSeq"] = stamp.seq
            inner["lastClock"] = stamp.clock
        }
        let dict: [String: Any] = ["t": "deleted_key", "r": inner]
        return try CommitOpMapper.jsonLine(dict: dict)
    }

    static func encodeEndLine(sha256Hex: String, rowCount: Int) throws -> String {
        try CommitOpMapper.encodeEndLine(sha256Hex: sha256Hex, rowCount: rowCount)
    }

    static func decodeLine(_ raw: String) throws -> SnapshotRow {
        guard let data = raw.data(using: .utf8) else {
            throw SnapshotWireError.malformed("invalid utf8")
        }
        let any = try JSONSerialization.jsonObject(with: data, options: [])
        guard let dict = any as? [String: Any], let type = dict["t"] as? String else {
            throw SnapshotWireError.malformed("not an object or missing t")
        }
        switch type {
        case "header":
            return .header(try decodeHeader(dict))
        case "asset":
            return .asset(try decodeAsset(dict))
        case "resource":
            return .resource(try decodeResource(dict))
        case "asset_resource":
            return .assetResource(try decodeAssetResource(dict))
        case "deleted_key":
            return .deletedKey(try decodeDeletedKey(dict))
        case "end":
            let sha = try CommitOpMapper.requireString(dict, "sha256")
            let count = try CommitOpMapper.requireInt(dict, "rowCount")
            return .end(sha256Hex: sha, rowCount: count)
        default:
            throw SnapshotWireError.unknownRowType(type)
        }
    }

    private static func decodeHeader(_ dict: [String: Any]) throws -> SnapshotHeader {
        let version = try mapValidation { try RepoWireValidator.requireInt(dict["v"], field: "v") }
        if version != SnapshotHeader.currentVersion {
            throw SnapshotWireError.unsupportedVersion(version)
        }
        guard let coveredAny = dict["covered"] as? [String: Any] else {
            throw SnapshotWireError.missingField("covered")
        }
        // covered determines which seqs the materializer can skip — a silent decode-to-empty
        // would discard the safety boundary and replay commits that should be considered baked in.
        let normalized = try Self.normalizeCovered(coveredAny)
        let covered = CoveredRanges.decode(normalized)
        // Field-absent legacy snapshots are tolerated; explicit empty repoID is corruption.
        let repoID: String
        if dict["repoID"] == nil {
            repoID = ""
        } else {
            repoID = try mapValidation {
                let raw = try RepoWireValidator.requireString(dict, "repoID")
                return try RepoWireValidator.validateRepoID(raw, field: "repoID")
            }
        }
        let writerID = try mapValidation { try RepoWireValidator.requireNonEmptyString(dict, "writerID") }
        return SnapshotHeader(
            version: version,
            scope: try CommitOpMapper.requireString(dict, "scope"),
            writerID: writerID,
            repoID: repoID,
            covered: covered
        )
    }

    private static func normalizeCovered(_ raw: [String: Any]) throws -> [String: [[UInt64]]] {
        var result: [String: [[UInt64]]] = [:]
        result.reserveCapacity(raw.count)
        for (writer, value) in raw {
            guard let ranges = value as? [[Any]] else {
                throw SnapshotWireError.malformed("covered[\(writer)] not an array of pairs")
            }
            var converted: [[UInt64]] = []
            converted.reserveCapacity(ranges.count)
            for pair in ranges {
                guard pair.count == 2 else {
                    throw SnapshotWireError.malformed("covered[\(writer)] pair length != 2")
                }
                let lowOpt = Self.uint64FromJSON(pair[0])
                let highOpt = Self.uint64FromJSON(pair[1])
                guard let low = lowOpt, let high = highOpt, low > 0, low <= high else {
                    throw SnapshotWireError.malformed("covered[\(writer)] non-numeric, zero, or low>high")
                }
                converted.append([low, high])
            }
            result[writer] = converted
        }
        return result
    }

    private static func uint64FromJSON(_ value: Any) -> UInt64? {
        // Route through the shared validator so JSON `true`/`false` (which bridges
        // to `as? Int` / `as? UInt64` / `as? NSNumber` as 1/0) cannot smuggle in a
        // covered range that silently shadows real commits.
        return try? RepoWireValidator.requireUInt64(value, field: "covered")
    }

    private static func innerObject(_ dict: [String: Any]) throws -> [String: Any] {
        guard let r = dict["r"] as? [String: Any] else {
            throw SnapshotWireError.missingField("r")
        }
        return r
    }

    /// Mirrors CommitOpMapper.mapValidation but produces SnapshotWireError so the
    /// snapshot reader's existing throw shape is preserved.
    static func mapValidation<T>(_ block: () throws -> T) throws -> T {
        do {
            return try block()
        } catch let err as WireValidationError {
            switch err {
            case .missingField(let f): throw SnapshotWireError.missingField(f)
            case .wrongHashLength(let f, let n):
                throw SnapshotWireError.malformed("\(f) must be 32-byte hex (got \(n))")
            case .invalidHex(let f):
                throw SnapshotWireError.malformed("\(f) invalid hex")
            case .nonNegative(let f, _):
                throw SnapshotWireError.malformed("\(f) must be non-negative")
            case .uint64OutOfIntRange(let f, _):
                throw SnapshotWireError.malformed("\(f) exceeds Int.max")
            case .fractionalNumber(let f):
                throw SnapshotWireError.missingField(f)
            case .pathContainsTraversal(let p):
                throw SnapshotWireError.malformed("physicalRemotePath rejected: containsParentTraversal(\"\(p)\")")
            case .malformedMonthScope(let s):
                throw SnapshotWireError.malformed("malformed month scope: \(s)")
            case .malformed(let s):
                throw SnapshotWireError.malformed(s)
            }
        }
    }

    private static func decodeAsset(_ dict: [String: Any]) throws -> SnapshotAssetRow {
        let r = try innerObject(dict)
        let fp = try mapValidation {
            try RepoWireValidator.validateHash(
                RepoWireValidator.requireString(r, "assetFingerprint"),
                field: "assetFingerprint"
            )
        }
        let creation = try mapValidation { try RepoWireValidator.validateOptionalDateMillis(r["creationDateMs"], field: "creationDateMs") }
        let resourceCount = try mapValidation {
            try RepoWireValidator.validateNonNegativeInt(r["resourceCount"], field: "resourceCount")
        }
        let totalFileSizeBytes = try mapValidation {
            try RepoWireValidator.validateNonNegativeInt64(r["totalFileSizeBytes"], field: "totalFileSizeBytes")
        }
        let backedUpAtMs = try mapValidation { try RepoWireValidator.validateNonNegativeInt64(r["backedUpAtMs"], field: "backedUpAtMs") }
        let stamp = try decodeOptionalStamp(r)
        return SnapshotAssetRow(
            assetFingerprint: fp,
            creationDateMs: creation,
            backedUpAtMs: backedUpAtMs,
            resourceCount: resourceCount,
            totalFileSizeBytes: totalFileSizeBytes,
            stamp: stamp
        )
    }

    /// All-or-nothing on the stamp triple; partial-present is rejected so a
    /// half-stamp can't accidentally pass an LWW gate.
    private static func decodeOptionalStamp(_ r: [String: Any]) throws -> OpStamp? {
        let writerIDRaw = r["lastWriterID"]
        let seqRaw = r["lastSeq"]
        let clockRaw = r["lastClock"]
        let allMissing = (writerIDRaw == nil) && (seqRaw == nil) && (clockRaw == nil)
        if allMissing { return nil }
        let allPresent = (writerIDRaw != nil) && (seqRaw != nil) && (clockRaw != nil)
        guard allPresent, let writerID = writerIDRaw as? String else {
            throw SnapshotWireError.malformed("partial stamp triple")
        }
        return try mapValidation {
            try RepoWireValidator.validateOpStamp(writerID: writerID, seqRaw: seqRaw, clockRaw: clockRaw)
        }
    }

    private static func decodeResource(_ dict: [String: Any]) throws -> SnapshotResourceRow {
        let r = try innerObject(dict)
        let hash = try mapValidation {
            try RepoWireValidator.validateHash(
                RepoWireValidator.requireString(r, "contentHash"),
                field: "contentHash"
            )
        }
        let creation = try mapValidation { try RepoWireValidator.validateOptionalDateMillis(r["creationDateMs"], field: "creationDateMs") }
        let crypto: ResourceCryptoMetadata?
        do {
            crypto = try CommitOpMapper.decodeOptionalCrypto(r["crypto"])
        } catch {
            throw SnapshotWireError.malformed("crypto: \(error)")
        }
        let fileSize = try mapValidation {
            try RepoWireValidator.validateNonNegativeInt64(r["fileSize"], field: "fileSize")
        }
        let resourceType = try mapValidation {
            try RepoWireValidator.validateNonNegativeInt(r["resourceType"], field: "resourceType")
        }
        let physicalRemotePath = try mapValidation {
            try RepoWireValidator.validateRelativePath(
                RepoWireValidator.requireString(r, "physicalRemotePath")
            )
        }
        let backedUpAtMs = try mapValidation { try RepoWireValidator.validateNonNegativeInt64(r["backedUpAtMs"], field: "backedUpAtMs") }
        let stamp = try decodeOptionalStamp(r)
        return SnapshotResourceRow(
            physicalRemotePath: physicalRemotePath,
            contentHash: hash,
            fileSize: fileSize,
            resourceType: resourceType,
            creationDateMs: creation,
            backedUpAtMs: backedUpAtMs,
            crypto: crypto,
            stamp: stamp
        )
    }

    private static func decodeAssetResource(_ dict: [String: Any]) throws -> SnapshotAssetResourceRow {
        let r = try innerObject(dict)
        let fp = try mapValidation {
            try RepoWireValidator.validateHash(
                RepoWireValidator.requireString(r, "assetFingerprint"),
                field: "assetFingerprint"
            )
        }
        let hash = try mapValidation {
            try RepoWireValidator.validateHash(
                RepoWireValidator.requireString(r, "resourceHash"),
                field: "resourceHash"
            )
        }
        let role = try mapValidation {
            try RepoWireValidator.validateNonNegativeInt(r["role"], field: "role")
        }
        let slot = try mapValidation {
            try RepoWireValidator.validateNonNegativeInt(r["slot"], field: "slot")
        }
        let logicalName = try mapValidation {
            try RepoWireValidator.validateLogicalName(
                CommitOpMapper.requireString(r, "logicalName"),
                field: "logicalName"
            )
        }
        return SnapshotAssetResourceRow(
            assetFingerprint: fp,
            role: role,
            slot: slot,
            resourceHash: hash,
            logicalName: logicalName
        )
    }

    private static func decodeDeletedKey(_ dict: [String: Any]) throws -> SnapshotDeletedKeyRow {
        let r = try innerObject(dict)
        let raw = try CommitOpMapper.requireString(r, "keyType")
        guard let key = SnapshotDeletedKeyRow.KeyType(rawValue: raw) else {
            throw SnapshotWireError.unknownKeyType(raw)
        }
        let keyValue = try CommitOpMapper.requireString(r, "keyValue")
        if key == .asset {
            _ = try mapValidation {
                try RepoWireValidator.validateHash(keyValue, field: "keyValue")
            }
        }
        // Decode stamp on any keyType but ignore semantically on non-asset — V3 may
        // introduce resource/assetResource deletedKey rows; rejecting a future-format
        // stamp here would throw the whole snapshot away and force commit-log replay
        // when the materializer would have safely skipped that row anyway.
        let stamp = try decodeOptionalStamp(r)
        let effectiveStamp = key == .asset ? stamp : nil
        return SnapshotDeletedKeyRow(keyType: key, keyValue: keyValue, stamp: effectiveStamp)
    }
}
