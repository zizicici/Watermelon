import Foundation
import os.log

private let repoStateAuthorityLog = Logger(subsystem: "com.zizicici.watermelon", category: "RepoStateAuthority")

enum RepoStateAuthority {
    static let maxPersistableSeq = UInt64(Int64.max)

    struct RepoStateCounters: Equatable, Sendable {
        let lastSeq: UInt64
        let lastClock: UInt64
    }

    struct RepoCounterObservation: Equatable, Sendable {
        let writerID: String
        let sameWriterSeqMax: UInt64?
    }

    enum RepoCounterSanitization: Equatable, Sendable {
        case accepted(UInt64)
        case repaired(UInt64)
        case ignoredAsUntrusted(UInt64)

        var value: UInt64 {
            switch self {
            case .accepted(let value), .repaired(let value), .ignoredAsUntrusted(let value):
                return value
            }
        }
    }

    static func counters(from row: RepoStateRecord) -> RepoStateCounters {
        RepoStateCounters(
            lastSeq: decodePersistedSeq(row.lastSeq).value,
            lastClock: decodePersistedClock(row.lastClock).value
        )
    }

    static func decodePersistedSeq(_ stored: Int64) -> RepoCounterSanitization {
        guard stored >= 0 else {
            repoStateAuthorityLog.warning("repaired negative persisted seq to 0 stored=\(stored, privacy: .public)")
            return .repaired(0)
        }
        return .accepted(UInt64(stored))
    }

    static func decodePersistedClock(_ stored: Int64) -> RepoCounterSanitization {
        let decoded = UInt64(bitPattern: stored)
        guard decoded < LamportClock.maxObservableValue else {
            repoStateAuthorityLog.warning("repaired overflowed persisted clock decoded=\(decoded, privacy: .public)")
            return .repaired(decoded)
        }
        return .accepted(decoded)
    }

    static func encodeSeq(_ seq: UInt64) throws -> Int64 {
        guard seq <= maxPersistableSeq else {
            throw SeqAllocator.SeqAllocatorError.exhausted
        }
        return Int64(seq)
    }

    static func sanitizeInitialSeq(_ seq: UInt64) -> RepoCounterSanitization {
        guard seq <= maxPersistableSeq else {
            return .repaired(0)
        }
        return .accepted(seq)
    }

    static func isTrustedFallbackSeq(_ stored: Int64) -> Bool {
        guard case .accepted(let seq) = decodePersistedSeq(stored) else {
            return false
        }
        return seq < maxPersistableSeq
    }

    static func observeSameWriterSeq(
        writerID: String,
        observedSeqByWriter: [String: UInt64],
        allocator: SeqAllocator
    ) async throws {
        let observation = RepoCounterObservation(
            writerID: writerID,
            sameWriterSeqMax: observedSeqByWriter[writerID]
        )
        try await observeSameWriterSeq(observation, allocator: allocator)
    }

    static func observeSameWriterSeq(
        _ observation: RepoCounterObservation,
        allocator: SeqAllocator
    ) async throws {
        if let sameWriterSeqMax = observation.sameWriterSeqMax {
            switch sanitizeRemoteSeqObservation(sameWriterSeqMax, writerID: observation.writerID) {
            case .accepted(let seq):
                try await allocator.observeRemoteMax(seq)
            case .ignoredAsUntrusted:
                break
            case .repaired:
                break
            }
        }
    }

    static func sanitizeRemoteSeqObservation(_ remoteSeq: UInt64, writerID: String) -> RepoCounterSanitization {
        guard remoteSeq <= maxPersistableSeq else {
            repoStateAuthorityLog.warning("ignore same-writer remote seq above persistable ceiling writerID=\(writerID, privacy: .public) seq=\(remoteSeq, privacy: .public)")
            return .ignoredAsUntrusted(remoteSeq)
        }
        return .accepted(remoteSeq)
    }
}
