import Foundation

enum RepoCrossRepoIndexWireError: Error, Equatable {
    case malformed(String)
    case unknownRowType(String)
    case unsupportedVersion(Int)
    case missingField(String)
    case unknownKeyType(String)
}

enum RepoCrossRepoIndexRowMapper {
    // MARK: - Encode

    static func encodeHeaderLine(_ header: RepoCrossRepoIndexHeader) throws -> String {
        var coveredEncoded: [[Any]] = []
        coveredEncoded.reserveCapacity(header.coveredByMonth.count)
        for month in header.coveredByMonth.keys.sorted(by: monthSortAscending) {
            guard let covered = header.coveredByMonth[month] else { continue }
            coveredEncoded.append([month.text, covered.encodedAsRangeArrayMap()])
        }
        let dict: [String: Any] = [
            "t": "crossRepoIndexHeader",
            "v": header.schemaVersion,
            "repoID": header.repoID,
            "writerID": header.writerID,
            "lamport": header.lamport,
            "runIDPrefix": header.runIDPrefix,
            "observedClock": header.observedClock,
            "coveredByMonth": coveredEncoded
        ]
        return try CommitOpMapper.jsonLine(dict: dict)
    }

    static func encodeMonthBeginLine(_ month: LibraryMonthKey) throws -> String {
        let dict: [String: Any] = ["t": "crossRepoMonthBegin", "month": month.text]
        return try CommitOpMapper.jsonLine(dict: dict)
    }

    static func encodeMonthEndLine(_ month: LibraryMonthKey) throws -> String {
        let dict: [String: Any] = ["t": "crossRepoMonthEnd", "month": month.text]
        return try CommitOpMapper.jsonLine(dict: dict)
    }

    static func encodeTailLine(_ tail: RepoCrossRepoIndexTail) throws -> String {
        var observedSeq: [[Any]] = []
        observedSeq.reserveCapacity(tail.observedSeqByWriter.count)
        for writer in tail.observedSeqByWriter.keys.sorted() {
            guard let seq = tail.observedSeqByWriter[writer] else { continue }
            observedSeq.append([writer, seq])
        }
        var baselines: [[Any]] = []
        baselines.reserveCapacity(tail.acceptedSnapshotBaselinesByMonthAtIndexTime.count)
        for month in tail.acceptedSnapshotBaselinesByMonthAtIndexTime.keys.sorted(by: monthSortAscending) {
            guard let info = tail.acceptedSnapshotBaselinesByMonthAtIndexTime[month] else { continue }
            let entry: [String: Any] = [
                "filename": info.filename,
                "lamport": info.lamport,
                "writerID": info.writerID,
                "runIDPrefix": info.runIDPrefix,
                "covered": info.covered.encodedAsRangeArrayMap()
            ]
            baselines.append([month.text, entry])
        }
        let corrupted = tail.corruptedSnapshotMonthsAtIndexTime
            .sorted(by: monthSortAscending)
            .map { $0.text }
        let dict: [String: Any] = [
            "t": "crossRepoIndexTail",
            "observedSeqByWriter": observedSeq,
            "acceptedSnapshotBaselinesByMonthAtIndexTime": baselines,
            "corruptedSnapshotMonthsAtIndexTime": corrupted
        ]
        return try CommitOpMapper.jsonLine(dict: dict)
    }

    static func encodeAssetLine(_ row: SnapshotAssetRow) throws -> String {
        try SnapshotRowMapper.encodeAssetLine(row)
    }

    static func encodeResourceLine(_ row: SnapshotResourceRow) throws -> String {
        try SnapshotRowMapper.encodeResourceLine(row)
    }

    static func encodeAssetResourceLine(_ row: SnapshotAssetResourceRow) throws -> String {
        try SnapshotRowMapper.encodeAssetResourceLine(row)
    }

    static func encodeDeletedKeyLine(_ row: SnapshotDeletedKeyRow) throws -> String {
        try SnapshotRowMapper.encodeDeletedKeyLine(row)
    }

    static func encodeEndLine(sha256Hex: String, rowCount: Int) throws -> String {
        try CommitOpMapper.encodeEndLine(sha256Hex: sha256Hex, rowCount: rowCount)
    }

    // MARK: - Decode

    static func decodeLine(_ raw: String) throws -> RepoCrossRepoIndexRow {
        guard let data = raw.data(using: .utf8) else {
            throw RepoCrossRepoIndexWireError.malformed("invalid utf8")
        }
        let any = try JSONSerialization.jsonObject(with: data, options: [])
        guard let dict = any as? [String: Any], let type = dict["t"] as? String else {
            throw RepoCrossRepoIndexWireError.malformed("not an object or missing t")
        }
        switch type {
        case "crossRepoIndexHeader":
            return .header(try decodeHeader(dict))
        case "crossRepoMonthBegin":
            return .monthBegin(try decodeMonth(dict))
        case "crossRepoMonthEnd":
            return .monthEnd(try decodeMonth(dict))
        case "asset", "resource", "asset_resource", "deleted_key":
            // Body rows reuse SnapshotRowMapper's existing wire format verbatim.
            let snapshotRow = try mapSnapshotWireError {
                try SnapshotRowMapper.decodeLine(raw)
            }
            switch snapshotRow {
            case .asset(let a): return .asset(a)
            case .resource(let r): return .resource(r)
            case .assetResource(let r): return .assetResource(r)
            case .deletedKey(let k): return .deletedKey(k)
            case .header, .end:
                throw RepoCrossRepoIndexWireError.malformed("snapshot wire row of unexpected kind inside cross-repo index body")
            }
        case "crossRepoIndexTail":
            return .tail(try decodeTail(dict))
        case "end":
            let sha = try requireString(dict, "sha256")
            let count = try mapValidation { try RepoWireValidator.requireInt(dict["rowCount"], field: "rowCount") }
            return .end(sha256Hex: sha, rowCount: count)
        default:
            throw RepoCrossRepoIndexWireError.unknownRowType(type)
        }
    }

    private static func decodeHeader(_ dict: [String: Any]) throws -> RepoCrossRepoIndexHeader {
        let version = try mapValidation { try RepoWireValidator.requireInt(dict["v"], field: "v") }
        if version != RepoCrossRepoIndexSchema.currentVersion {
            throw RepoCrossRepoIndexWireError.unsupportedVersion(version)
        }
        let repoID = try mapValidation {
            let raw = try RepoWireValidator.requireString(dict, "repoID")
            return try RepoWireValidator.validateRepoID(raw, field: "repoID")
        }
        let writerID = try mapValidation { try RepoWireValidator.requireNonEmptyString(dict, "writerID") }
        let lamport = try mapValidation { try RepoWireValidator.requireUInt64(dict["lamport"], field: "lamport") }
        guard lamport > 0 else {
            throw RepoCrossRepoIndexWireError.malformed("lamport must be > 0")
        }
        let runIDPrefix = try mapValidation { try RepoWireValidator.requireNonEmptyString(dict, "runIDPrefix") }
        let observedClock = try mapValidation {
            try RepoWireValidator.requireUInt64(dict["observedClock"], field: "observedClock")
        }
        guard let coveredAny = dict["coveredByMonth"] as? [[Any]] else {
            throw RepoCrossRepoIndexWireError.missingField("coveredByMonth")
        }
        var coveredByMonth: [LibraryMonthKey: CoveredRanges] = [:]
        for entry in coveredAny {
            guard entry.count == 2,
                  let monthText = entry[0] as? String,
                  let coveredDict = entry[1] as? [String: Any] else {
                throw RepoCrossRepoIndexWireError.malformed("coveredByMonth entry must be [monthText, coveredDict]")
            }
            guard let month = parseMonthText(monthText) else {
                throw RepoCrossRepoIndexWireError.malformed("coveredByMonth invalid month: \(monthText)")
            }
            let normalized = try normalizeCovered(coveredDict, field: "coveredByMonth[\(monthText)]")
            coveredByMonth[month] = CoveredRanges.decode(normalized)
        }
        return RepoCrossRepoIndexHeader(
            schemaVersion: version,
            repoID: repoID,
            writerID: writerID,
            lamport: lamport,
            runIDPrefix: runIDPrefix,
            observedClock: observedClock,
            coveredByMonth: coveredByMonth
        )
    }

    private static func decodeMonth(_ dict: [String: Any]) throws -> LibraryMonthKey {
        let monthText = try requireString(dict, "month")
        guard let month = parseMonthText(monthText) else {
            throw RepoCrossRepoIndexWireError.malformed("invalid month: \(monthText)")
        }
        return month
    }

    private static func decodeTail(_ dict: [String: Any]) throws -> RepoCrossRepoIndexTail {
        guard let observedSeqArray = dict["observedSeqByWriter"] as? [[Any]] else {
            throw RepoCrossRepoIndexWireError.missingField("observedSeqByWriter")
        }
        var observedSeqByWriter: [String: UInt64] = [:]
        for pair in observedSeqArray {
            guard pair.count == 2,
                  let writer = pair[0] as? String,
                  !writer.isEmpty else {
                throw RepoCrossRepoIndexWireError.malformed("observedSeqByWriter entry malformed")
            }
            let seq = try mapValidation {
                try RepoWireValidator.requireUInt64(pair[1], field: "observedSeqByWriter[\(writer)]")
            }
            observedSeqByWriter[writer] = seq
        }
        guard let baselinesArray = dict["acceptedSnapshotBaselinesByMonthAtIndexTime"] as? [[Any]] else {
            throw RepoCrossRepoIndexWireError.missingField("acceptedSnapshotBaselinesByMonthAtIndexTime")
        }
        var baselines: [LibraryMonthKey: RepoCrossRepoIndexAcceptedSnapshotInfo] = [:]
        for entry in baselinesArray {
            guard entry.count == 2,
                  let monthText = entry[0] as? String,
                  let body = entry[1] as? [String: Any] else {
                throw RepoCrossRepoIndexWireError.malformed("acceptedSnapshotBaselinesByMonthAtIndexTime entry malformed")
            }
            guard let month = parseMonthText(monthText) else {
                throw RepoCrossRepoIndexWireError.malformed("acceptedSnapshotBaselines invalid month: \(monthText)")
            }
            let filename = try requireString(body, "filename")
            let lamport = try mapValidation { try RepoWireValidator.requireUInt64(body["lamport"], field: "lamport") }
            let writerID = try mapValidation { try RepoWireValidator.requireNonEmptyString(body, "writerID") }
            let runIDPrefix = try mapValidation { try RepoWireValidator.requireNonEmptyString(body, "runIDPrefix") }
            guard let coveredDict = body["covered"] as? [String: Any] else {
                throw RepoCrossRepoIndexWireError.missingField("acceptedSnapshotBaselines covered")
            }
            let normalized = try normalizeCovered(coveredDict, field: "acceptedSnapshotBaselines[\(monthText)].covered")
            let covered = CoveredRanges.decode(normalized)
            baselines[month] = RepoCrossRepoIndexAcceptedSnapshotInfo(
                filename: filename,
                lamport: lamport,
                writerID: writerID,
                runIDPrefix: runIDPrefix,
                covered: covered
            )
        }
        guard let corruptedArray = dict["corruptedSnapshotMonthsAtIndexTime"] as? [String] else {
            throw RepoCrossRepoIndexWireError.missingField("corruptedSnapshotMonthsAtIndexTime")
        }
        var corrupted: Set<LibraryMonthKey> = []
        for monthText in corruptedArray {
            guard let month = parseMonthText(monthText) else {
                throw RepoCrossRepoIndexWireError.malformed("corruptedSnapshotMonths invalid month: \(monthText)")
            }
            corrupted.insert(month)
        }
        return RepoCrossRepoIndexTail(
            observedSeqByWriter: observedSeqByWriter,
            acceptedSnapshotBaselinesByMonthAtIndexTime: baselines,
            corruptedSnapshotMonthsAtIndexTime: corrupted
        )
    }

    // MARK: - Helpers

    private static func parseMonthText(_ text: String) -> LibraryMonthKey? {
        let parts = text.split(separator: "-")
        guard parts.count == 2,
              let year = Int(parts[0]),
              let month = Int(parts[1]),
              (1...12).contains(month) else {
            return nil
        }
        return LibraryMonthKey(year: year, month: month)
    }

    private static func monthSortAscending(_ lhs: LibraryMonthKey, _ rhs: LibraryMonthKey) -> Bool {
        if lhs.year != rhs.year { return lhs.year < rhs.year }
        return lhs.month < rhs.month
    }

    private static func normalizeCovered(_ raw: [String: Any], field: String) throws -> [String: [[UInt64]]] {
        var result: [String: [[UInt64]]] = [:]
        result.reserveCapacity(raw.count)
        for (writer, value) in raw {
            guard let ranges = value as? [[Any]] else {
                throw RepoCrossRepoIndexWireError.malformed("\(field)[\(writer)] not an array of pairs")
            }
            var converted: [[UInt64]] = []
            converted.reserveCapacity(ranges.count)
            for pair in ranges {
                guard pair.count == 2 else {
                    throw RepoCrossRepoIndexWireError.malformed("\(field)[\(writer)] pair length != 2")
                }
                let lowOpt = try? RepoWireValidator.requireUInt64(pair[0], field: "\(field)")
                let highOpt = try? RepoWireValidator.requireUInt64(pair[1], field: "\(field)")
                guard let low = lowOpt, let high = highOpt, low > 0, low <= high else {
                    throw RepoCrossRepoIndexWireError.malformed("\(field)[\(writer)] non-numeric, zero, or low>high")
                }
                converted.append([low, high])
            }
            result[writer] = converted
        }
        return result
    }

    private static func requireString(_ dict: [String: Any], _ key: String) throws -> String {
        try mapValidation { try RepoWireValidator.requireString(dict, key) }
    }

    private static func mapValidation<T>(_ block: () throws -> T) throws -> T {
        do {
            return try block()
        } catch let err as WireValidationError {
            throw err.translated(
                missingField: RepoCrossRepoIndexWireError.missingField,
                malformed: RepoCrossRepoIndexWireError.malformed
            )
        }
    }

    private static func mapSnapshotWireError<T>(_ block: () throws -> T) throws -> T {
        do {
            return try block()
        } catch let err as SnapshotWireError {
            switch err {
            case .malformed(let s): throw RepoCrossRepoIndexWireError.malformed(s)
            case .unknownRowType(let s): throw RepoCrossRepoIndexWireError.unknownRowType(s)
            case .unsupportedVersion(let v): throw RepoCrossRepoIndexWireError.unsupportedVersion(v)
            case .missingField(let s): throw RepoCrossRepoIndexWireError.missingField(s)
            case .unknownKeyType(let s): throw RepoCrossRepoIndexWireError.unknownKeyType(s)
            }
        }
    }
}
