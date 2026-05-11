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
                "assetFingerprint": payload.assetFingerprint.hexString,
                "reason": payload.reason.rawValue
            ]
            if let basis = payload.observedBasis {
                // Additive v2 field — older v2 readers ignore it and apply the
                // tombstone unconditionally (command-style). LWW gate against
                // stale adds covers them once snapshots carry stamps.
                var basisDict: [String: Any] = [
                    "lamportWatermark": basis.lamportWatermark
                ]
                if !basis.perWriterMaxSeq.isEmpty {
                    basisDict["perWriterMaxSeq"] = basis.perWriterMaxSeq.mapValues { Int64(bitPattern: $0) }
                }
                dict["observedBasis"] = basisDict
            }
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
        let version = (dict["v"] as? Int) ?? 0
        if version != CommitHeader.currentVersion {
            throw CommitWireError.unsupportedVersion(version)
        }
        let scope = try requireString(dict, "scope")
        _ = try mapValidation { try RepoWireValidator.validateMonthScope(scope) }
        let clockMin = try requireUInt64(dict, "clockMin")
        let clockMax = try requireUInt64(dict, "clockMax")
        guard clockMin <= clockMax else {
            throw CommitWireError.malformed("clockMin > clockMax (\(clockMin) > \(clockMax))")
        }
        let bodyKind = try requireString(dict, "bodyKind")
        guard bodyKind == CommitHeader.bodyKindPlain else {
            throw CommitWireError.malformed("unknown bodyKind: \(bodyKind)")
        }
        return CommitHeader(
            version: version,
            repoID: try requireNonEmptyString(dict, "repoID"),
            writerID: try requireNonEmptyString(dict, "writerID"),
            seq: try requireUInt64(dict, "seq"),
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
                try RepoWireValidator.validateHash(
                    RepoWireValidator.requireString(bodyDict, "assetFingerprint"),
                    field: "assetFingerprint"
                )
            }
            let reasonRaw = try requireString(bodyDict, "reason")
            guard let reason = CommitTombstoneBody.Reason(rawValue: reasonRaw) else {
                throw CommitWireError.unknownReason(reasonRaw)
            }
            var basis: TombstoneObservationBasis?
            if let basisDict = bodyDict["observedBasis"] as? [String: Any] {
                let watermark = try requireUInt64(basisDict, "lamportWatermark")
                var perWriter: [String: UInt64] = [:]
                if let raw = basisDict["perWriterMaxSeq"] as? [String: Any] {
                    for (writer, value) in raw {
                        // Each value is the max seq we'd seen for that writer at observation
                        // time. Strict UInt64 + Int64 acceptance — same semantics as covered
                        // ranges (negative / fractional values are not legal seqs).
                        perWriter[writer] = try mapValidation {
                            try RepoWireValidator.requireUInt64(value, field: "perWriterMaxSeq[\(writer)]")
                        }
                    }
                }
                basis = TombstoneObservationBasis(perWriterMaxSeq: perWriter, lamportWatermark: watermark)
            }
            body = .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: fp,
                reason: reason,
                observedBasis: basis
            ))
        default:
            throw CommitWireError.unknownOpKind(kind)
        }
        return CommitOp(opSeq: opSeq, clock: clock, body: body)
    }

    private static func decodeEnd(_ dict: [String: Any]) throws -> (sha256: String, rowCount: Int) {
        let sha = try requireString(dict, "sha256")
        let count: Int
        if let n = dict["rowCount"] as? Int {
            count = n
        } else if let n = dict["rowCount"] as? Int64 {
            count = Int(n)
        } else {
            throw CommitWireError.missingField("rowCount")
        }
        return (sha, count)
    }

    private static func encodeAddAssetBody(_ body: CommitAddAssetBody) -> [String: Any] {
        var dict: [String: Any] = [
            "assetFingerprint": body.assetFingerprint.hexString,
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
            try RepoWireValidator.validateHash(
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
            switch err {
            case .missingField(let f): throw CommitWireError.missingField(f)
            case .wrongHashLength(let f, let n):
                throw CommitWireError.malformed("\(f) must be 32-byte hex (got \(n))")
            case .invalidHex(let f):
                throw CommitWireError.malformed("\(f) invalid hex")
            case .nonNegative(let f, _):
                throw CommitWireError.malformed("\(f) must be non-negative")
            case .uint64OutOfIntRange(let f, _):
                throw CommitWireError.malformed("\(f) exceeds Int.max")
            case .fractionalNumber(let f):
                throw CommitWireError.missingField(f)
            case .pathContainsTraversal(let p):
                throw CommitWireError.malformed("physicalRemotePath rejected: containsParentTraversal(\"\(p)\")")
            case .malformedMonthScope(let s):
                throw CommitWireError.malformed("malformed month scope: \(s)")
            case .malformed(let s):
                throw CommitWireError.malformed(s)
            }
        }
    }

    static func requireString(_ dict: [String: Any], _ key: String) throws -> String {
        try mapValidation { try RepoWireValidator.requireString(dict, key) }
    }

    static func requireNonEmptyString(_ dict: [String: Any], _ key: String) throws -> String {
        try mapValidation { try RepoWireValidator.requireNonEmptyString(dict, key) }
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

