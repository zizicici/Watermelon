import Foundation

/// All commit/snapshot input is from a peer writer's bytes; "this can't happen"
/// assumptions become security holes when the peer is malicious or corrupt.
enum WireValidationError: Error, Equatable {
    case wrongHashLength(field: String, actual: Int)
    case invalidHex(field: String)
    case missingField(String)
    case nonNegative(field: String, actual: Int64)
    case uint64OutOfIntRange(field: String, actual: UInt64)
    case fractionalNumber(field: String)
    case pathContainsTraversal(String)
    case malformedMonthScope(String)
    case malformed(String)
}

enum RepoWireValidator {
    /// 32-byte enforcement: truncated hashes collide trivially → poisoned dedup keys.
    static func validateHash(_ raw: String, field: String) throws -> Data {
        guard let data = Data(hexString: raw) else {
            throw WireValidationError.invalidHex(field: field)
        }
        guard data.count == 32 else {
            throw WireValidationError.wrongHashLength(field: field, actual: data.count)
        }
        return data
    }

    /// Optional date-millis field — present + non-null must be a non-negative integer.
    /// Lenient `as? Int64 ?? Int.map(...)` would silently swallow a hostile peer's "abc"
    /// as nil, letting epoch-0 sort artifacts through; this rejects malformed values
    /// while still accepting NSNull / missing-key as nil.
    static func validateOptionalDateMillis(_ raw: Any?, field: String) throws -> Int64? {
        if raw == nil || raw is NSNull { return nil }
        return try validateNonNegativeInt64(raw, field: field)
    }

    /// Negatives flip sort orders and produce negative totalFileSizeBytes aggregates.
    static func validateNonNegativeInt(_ raw: Any?, field: String) throws -> Int {
        let n = try requireInt(raw, field: field)
        guard n >= 0 else { throw WireValidationError.nonNegative(field: field, actual: Int64(n)) }
        return n
    }

    static func validateNonNegativeInt64(_ raw: Any?, field: String) throws -> Int64 {
        let n = try requireInt64(raw, field: field)
        guard n >= 0 else { throw WireValidationError.nonNegative(field: field, actual: n) }
        return n
    }

    /// `Int(UInt64)` traps above Int.max — a corrupt opSeq=2^63 would crash the
    /// materializer instead of routing through the decode-failure skip path.
    static func validateUInt64InIntRange(_ raw: Any?, field: String) throws -> Int {
        let value = try requireUInt64(raw, field: field)
        guard value <= UInt64(Int.max) else {
            throw WireValidationError.uint64OutOfIntRange(field: field, actual: value)
        }
        return Int(value)
    }

    /// `..` segments escape basePath on server-side-resolving backends.
    static func validateRelativePath(_ raw: String) throws -> String {
        if let err = RemotePathBuilder.validateRelativePath(raw) {
            switch err {
            case .containsParentTraversal(let value):
                throw WireValidationError.pathContainsTraversal(value)
            }
        }
        return raw
    }

    /// Peer-writable `logicalName` flows into `PHAssetResourceCreationOptions.originalFilename`
    /// on restore. Slashes confuse Photos, control chars (NUL especially) terminate the
    /// underlying C-string, and a length over 255 UTF-8 bytes fails on APFS / ext4 / SMB /
    /// FAT32 (all 255-byte single-component limits) when restored to disk.
    static func validateLogicalName(_ raw: String, field: String) throws -> String {
        if raw.isEmpty { return raw }  // empty is legitimate — caller falls back to physical leaf
        let bytes = raw.utf8.count
        guard bytes <= 255 else {
            throw WireValidationError.malformed("\(field): \(bytes)-byte UTF-8 exceeds 255")
        }
        guard !raw.contains("/") && !raw.contains("\\") else {
            throw WireValidationError.malformed("\(field): contains path separator")
        }
        for scalar in raw.unicodeScalars {
            if scalar.value < 0x20 || scalar.value == 0x7F {
                throw WireValidationError.malformed("\(field): contains control character")
            }
        }
        return raw
    }

    static func validateMonthScope(_ raw: String) throws -> LibraryMonthKey {
        guard let key = CommitHeader.parseMonthScope(raw) else {
            throw WireValidationError.malformedMonthScope(raw)
        }
        return key
    }

    /// Caller must pass all three or none — partial-present is rejected so a
    /// half-stamp can't accidentally pass an LWW gate.
    static func validateOpStamp(writerID: String, seqRaw: Any?, clockRaw: Any?) throws -> OpStamp {
        guard !writerID.isEmpty else {
            throw WireValidationError.malformed("stamp.writerID empty")
        }
        let seq = try requireUInt64(seqRaw, field: "stamp.seq")
        let clock = try requireUInt64(clockRaw, field: "stamp.clock")
        return OpStamp(writerID: writerID, seq: seq, clock: clock)
    }

    static func requireString(_ dict: [String: Any], _ key: String) throws -> String {
        guard let v = dict[key] as? String else {
            throw WireValidationError.missingField(key)
        }
        return v
    }

    /// Empty repoID silently disables identity filtering; foreign commits would
    /// leak into our cache.
    static func requireNonEmptyString(_ dict: [String: Any], _ key: String) throws -> String {
        let v = try requireString(dict, key)
        guard !v.isEmpty else {
            throw WireValidationError.malformed("\(key) empty")
        }
        return v
    }

    static func requireInt(_ raw: Any?, field: String) throws -> Int {
        if let n = raw as? Int { return n }
        if let n = raw as? Int64 { return Int(n) }
        if let n = raw as? NSNumber {
            // Stringify-roundtrip: only true integers round-trip; rejects fractional NSNumber.
            let candidate = n.intValue
            if n.stringValue == String(candidate) { return candidate }
            throw WireValidationError.fractionalNumber(field: field)
        }
        throw WireValidationError.missingField(field)
    }

    static func requireInt64(_ raw: Any?, field: String) throws -> Int64 {
        if let n = raw as? Int64 { return n }
        if let n = raw as? Int { return Int64(n) }
        if let n = raw as? NSNumber {
            let candidate = n.int64Value
            if n.stringValue == String(candidate) { return candidate }
            throw WireValidationError.fractionalNumber(field: field)
        }
        throw WireValidationError.missingField(field)
    }

    /// JSONSerialization can hand back NSNumber for values that don't fit Int64 cleanly.
    /// Must be non-negative AND integral — `uint64Value` wraps negatives to ~UInt64.max
    /// and truncates fractionals (1.9 → 1), both of which silently change semantics.
    static func requireUInt64(_ raw: Any?, field: String) throws -> UInt64 {
        if let n = raw as? UInt64 { return n }
        if let n = raw as? Int64, n >= 0 { return UInt64(n) }
        if let n = raw as? Int, n >= 0 { return UInt64(n) }
        if let n = raw as? NSNumber {
            let value = n.uint64Value
            if n.int64Value >= 0 && n.stringValue == String(value) {
                return value
            }
        }
        throw WireValidationError.missingField(field)
    }
}
