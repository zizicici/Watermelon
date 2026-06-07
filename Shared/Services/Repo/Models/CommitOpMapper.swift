import Foundation

enum CommitWireError: Error, Equatable {
    case malformed(String)
    case unknownRowType(String)
    case unsupportedVersion(Int)
    case missingField(String)
    case unknownOpKind(String)
    case unknownReason(String)
}

enum CommitOpMapper {
    static func encodeHeaderLine(_ header: CommitHeader) throws -> String {
        let dict: [String: Any] = [
            "t": "header",
            "v": header.version,
            "repoID": header.repoID,
            "writerID": header.writerID,
            "seq": header.seq,
            "runID": header.runID,
            "scope": header.scope,
            "clockMin": header.clockMin,
            "clockMax": header.clockMax,
            "bodyKind": header.bodyKind
        ]
        return try jsonLine(dict: dict)
    }

    static func encodeOpLine(_ op: CommitOp) throws -> String {
        let body: [String: Any]
        let kind: String
        switch op.body {
        case .addAsset(let payload):
            kind = CommitOpKind.addAsset.rawValue
            body = encodeAddAssetBody(payload)
        case .tombstoneAsset(let payload):
            kind = CommitOpKind.tombstoneAsset.rawValue
            var dict: [String: Any] = [
                "assetFingerprint": payload.assetFingerprint.rawValue.hexString,
                "reason": payload.reason.rawValue
            ]
            var basisDict: [String: Any] = [
                "lamportWatermark": payload.observedBasis.lamportWatermark
            ]
            if !payload.observedBasis.perWriterMaxSeq.isEmpty {
                // Direct UInt64; Int64(bitPattern:) flipped values > Int64.max to
                // negatives which the decoder rejected on round-trip.
                basisDict["perWriterMaxSeq"] = payload.observedBasis.perWriterMaxSeq
            }
            dict["observedBasis"] = basisDict
            body = dict
        }
        let dict: [String: Any] = [
            "t": "op",
            "opSeq": op.opSeq,
            "clock": op.clock,
            "kind": kind,
            "body": body
        ]
        return try jsonLine(dict: dict)
    }

    static func encodeEndLine(sha256Hex: String, rowCount: Int) throws -> String {
        let dict: [String: Any] = [
            "t": "end",
            "sha256": sha256Hex,
            "rowCount": rowCount
        ]
        return try jsonLine(dict: dict)
    }

    static func decodeLine(_ raw: String) throws -> CommitWireRow {
        guard let data = raw.data(using: .utf8) else {
            throw CommitWireError.malformed("invalid utf8")
        }
        let any = try JSONSerialization.jsonObject(with: data, options: [])
        guard let dict = any as? [String: Any], let type = dict["t"] as? String else {
            throw CommitWireError.malformed("not an object or missing t")
        }
        switch type {
        case "header":
            return .header(try decodeHeader(dict))
        case "op":
            return .op(try decodeOp(dict))
        case "end":
            let parsed = try decodeEnd(dict)
            return .end(sha256: parsed.sha256, rowCount: parsed.rowCount)
        default:
            throw CommitWireError.unknownRowType(type)
        }
    }

    private static func decodeHeader(_ dict: [String: Any]) throws -> CommitHeader {
        let version = try requireInt(dict, "v")
        if version != CommitHeader.currentVersion {
            throw CommitWireError.unsupportedVersion(version)
        }
        let scope = try requireString(dict, "scope")
        _ = try mapValidation { try RepoWireValidator.validateMonthScope(scope) }
        let clockMin = try requireUInt64(dict, "clockMin")
        let clockMax = try requireUInt64(dict, "clockMax")
        guard clockMin > 0 else {
            throw CommitWireError.malformed("clockMin must be > 0")
        }
        guard clockMin <= clockMax else {
            throw CommitWireError.malformed("clockMin > clockMax (\(clockMin) > \(clockMax))")
        }
        let bodyKind = try requireString(dict, "bodyKind")
        guard bodyKind == CommitHeader.bodyKindPlain else {
            throw CommitWireError.malformed("unknown bodyKind: \(bodyKind)")
        }
        let seq = try requireUInt64(dict, "seq")
        guard seq > 0 else {
            throw CommitWireError.malformed("seq must be > 0")
        }
        return CommitHeader(
            version: version,
            repoID: try requireRepoID(dict, "repoID"),
            writerID: try requireNonEmptyString(dict, "writerID"),
            seq: seq,
            runID: try requireString(dict, "runID"),
            scope: scope,
            clockMin: clockMin,
            clockMax: clockMax,
            bodyKind: bodyKind
        )
    }

    private static func decodeOp(_ dict: [String: Any]) throws -> CommitOp {
        // JSONSerialization hands all numerics back as NSNumber; the prior `is UInt64`
        // belt-and-braces was dead code. validateNonNegativeInt routes through NSNumber
        // and rejects > Int.max as fractionalNumber (the stringValue round-trip catches it).
        let opSeq = try mapValidation {
            try RepoWireValidator.validateNonNegativeInt(dict["opSeq"], field: "opSeq")
        }
        let clock = try requireUInt64(dict, "clock")
        guard clock > 0 else {
            throw CommitWireError.malformed("clock must be > 0")
        }
        let kind = try requireString(dict, "kind")
        guard let bodyDict = dict["body"] as? [String: Any] else {
            throw CommitWireError.missingField("body")
        }
        let body: CommitOpBody
        switch kind {
        case CommitOpKind.addAsset.rawValue:
            body = .addAsset(try decodeAddAssetBody(bodyDict))
        case CommitOpKind.tombstoneAsset.rawValue:
            let fp = try mapValidation {
                try RepoWireValidator.validateAssetFingerprint(
                    RepoWireValidator.requireString(bodyDict, "assetFingerprint"),
                    field: "assetFingerprint"
                )
            }
            let reasonRaw = try requireString(bodyDict, "reason")
            guard let reason = CommitTombstoneBody.Reason(rawValue: reasonRaw) else {
                throw CommitWireError.unknownReason(reasonRaw)
            }
            guard let basisDict = bodyDict["observedBasis"] as? [String: Any] else {
                throw CommitWireError.missingField("observedBasis")
            }
            let watermark = try requireUInt64(basisDict, "lamportWatermark")
            var perWriter: [String: UInt64] = [:]
            // Present-but-not-an-object perWriterMaxSeq must fail closed, not decode to an empty (weaker)
            // basis — an empty basis makes every add look after-basis and would resurrect a tombstoned
            // asset on replay. Absent key stays empty (the encoder omits it when empty).
            if let raw = basisDict["perWriterMaxSeq"] {
                guard let rawMap = raw as? [String: Any] else {
                    throw CommitWireError.malformed("observedBasis.perWriterMaxSeq not an object")
                }
                for (writer, value) in rawMap {
                    perWriter[writer] = try mapValidation {
                        try RepoWireValidator.requireUInt64(value, field: "perWriterMaxSeq[\(writer)]")
                    }
                }
            }
            body = .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: fp,
                reason: reason,
                observedBasis: TombstoneObservationBasis(perWriterMaxSeq: perWriter, lamportWatermark: watermark)
            ))
        default:
            throw CommitWireError.unknownOpKind(kind)
        }
        return CommitOp(opSeq: opSeq, clock: clock, body: body)
    }

    private static func decodeEnd(_ dict: [String: Any]) throws -> (sha256: String, rowCount: Int) {
        let sha = try requireString(dict, "sha256")
        let count = try requireInt(dict, "rowCount")
        return (sha, count)
    }

    private static func encodeAddAssetBody(_ body: CommitAddAssetBody) -> [String: Any] {
        var dict: [String: Any] = [
            "assetFingerprint": body.assetFingerprint.rawValue.hexString,
            "backedUpAtMs": body.backedUpAtMs,
            "resources": body.resources.map(encodeResourceEntry)
        ]
        dict["creationDateMs"] = body.creationDateMs as Any? ?? NSNull()
        return dict
    }

    private static func encodeResourceEntry(_ r: CommitResourceEntry) -> [String: Any] {
        var dict: [String: Any] = [
            "physicalRemotePath": r.physicalRemotePath,
            "logicalName": r.logicalName,
            "contentHash": r.contentHash.hexString,
            "fileSize": r.fileSize,
            "resourceType": r.resourceType,
            "role": r.role,
            "slot": r.slot
        ]
        if let crypto = r.crypto {
            dict["crypto"] = encodeCrypto(crypto)
        } else {
            dict["crypto"] = NSNull()
        }
        return dict
    }

    private static func decodeAddAssetBody(_ dict: [String: Any]) throws -> CommitAddAssetBody {
        let fp = try mapValidation {
            try RepoWireValidator.validateAssetFingerprint(
                RepoWireValidator.requireString(dict, "assetFingerprint"),
                field: "assetFingerprint"
            )
        }
        let backedUpAtMs = try mapValidation { try RepoWireValidator.validateNonNegativeInt64(dict["backedUpAtMs"], field: "backedUpAtMs") }
        let creationDateMs = try mapValidation { try RepoWireValidator.validateOptionalDateMillis(dict["creationDateMs"], field: "creationDateMs") }
        guard let resourcesRaw = dict["resources"] as? [[String: Any]] else {
            throw CommitWireError.missingField("resources")
        }
        let resources = try resourcesRaw.map(decodeResourceEntry)
        return CommitAddAssetBody(
            assetFingerprint: fp,
            creationDateMs: creationDateMs,
            backedUpAtMs: backedUpAtMs,
            resources: resources
        )
    }

    private static func decodeResourceEntry(_ dict: [String: Any]) throws -> CommitResourceEntry {
        let hash = try mapValidation {
            try RepoWireValidator.validateHash(
                RepoWireValidator.requireString(dict, "contentHash"),
                field: "contentHash"
            )
        }
        let fileSize = try mapValidation {
            try RepoWireValidator.validateNonNegativeInt64(dict["fileSize"], field: "fileSize")
        }
        let resourceType = try mapValidation {
            try RepoWireValidator.validateNonNegativeInt(dict["resourceType"], field: "resourceType")
        }
        let role = try mapValidation {
            try RepoWireValidator.validateNonNegativeInt(dict["role"], field: "role")
        }
        let slot = try mapValidation {
            try RepoWireValidator.validateNonNegativeInt(dict["slot"], field: "slot")
        }
        let physicalRemotePath = try mapValidation {
            try RepoWireValidator.validateRelativePath(
                RepoWireValidator.requireString(dict, "physicalRemotePath")
            )
        }
        let logicalName = try mapValidation {
            try RepoWireValidator.validateLogicalName(
                RepoWireValidator.requireString(dict, "logicalName"),
                field: "logicalName"
            )
        }
        return CommitResourceEntry(
            physicalRemotePath: physicalRemotePath,
            logicalName: logicalName,
            contentHash: hash,
            fileSize: fileSize,
            resourceType: resourceType,
            role: role,
            slot: slot,
            crypto: try decodeOptionalCrypto(dict["crypto"])
        )
    }

    static func encodeCrypto(_ crypto: ResourceCryptoMetadata) -> [String: Any] {
        var dict: [String: Any] = ["scheme": crypto.scheme]
        if !crypto.payload.isEmpty {
            dict["payload"] = crypto.payload
        }
        return dict
    }

    static func decodeOptionalCrypto(_ raw: Any?) throws -> ResourceCryptoMetadata? {
        guard let raw, !(raw is NSNull) else { return nil }
        guard let dict = raw as? [String: Any] else {
            throw CommitWireError.malformed("crypto not object")
        }
        let scheme = try requireString(dict, "scheme")
        let payload = (dict["payload"] as? [String: String]) ?? [:]
        return ResourceCryptoMetadata(scheme: scheme, payload: payload)
    }

    static func jsonLine(dict: [String: Any]) throws -> String {
        let data = try JSONSerialization.data(withJSONObject: dict, options: [.sortedKeys])
        guard let raw = String(data: data, encoding: .utf8) else {
            throw CommitWireError.malformed("encoded utf8")
        }
        return raw
    }

    /// Map RepoWireValidator errors to CommitWireError so existing callers/tests see
    /// the same throw shape; new validation rules only need to land in the validator.
    static func mapValidation<T>(_ block: () throws -> T) throws -> T {
        do {
            return try block()
        } catch let err as WireValidationError {
            throw err.translated(
                missingField: CommitWireError.missingField,
                malformed: CommitWireError.malformed
            )
        }
    }

    static func requireString(_ dict: [String: Any], _ key: String) throws -> String {
        try mapValidation { try RepoWireValidator.requireString(dict, key) }
    }

    static func requireNonEmptyString(_ dict: [String: Any], _ key: String) throws -> String {
        try mapValidation { try RepoWireValidator.requireNonEmptyString(dict, key) }
    }

    static func requireRepoID(_ dict: [String: Any], _ key: String) throws -> String {
        try mapValidation {
            let raw = try RepoWireValidator.requireString(dict, key)
            return try RepoWireValidator.validateRepoID(raw, field: key)
        }
    }

    static func requireInt(_ dict: [String: Any], _ key: String) throws -> Int {
        try mapValidation { try RepoWireValidator.requireInt(dict[key], field: key) }
    }

    static func requireInt64(_ dict: [String: Any], _ key: String) throws -> Int64 {
        try mapValidation { try RepoWireValidator.requireInt64(dict[key], field: key) }
    }

    static func requireUInt64(_ dict: [String: Any], _ key: String) throws -> UInt64 {
        try mapValidation { try RepoWireValidator.requireUInt64(dict[key], field: key) }
    }
}

enum CommitWireRow: Equatable, Sendable {
    case header(CommitHeader)
    case op(CommitOp)
    case end(sha256: String, rowCount: Int)

    static func == (lhs: CommitWireRow, rhs: CommitWireRow) -> Bool {
        switch (lhs, rhs) {
        case (.header(let l), .header(let r)): return l == r
        case (.op(let l), .op(let r)): return l == r
        case (.end(let lSha, let lCount), .end(let rSha, let rCount)): return lSha == rSha && lCount == rCount
        default: return false
        }
    }
}
