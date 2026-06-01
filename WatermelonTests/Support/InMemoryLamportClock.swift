import Foundation
@testable import Watermelon

/// Test-only DB-free Lamport clock. Production allocates via `PersistedLamportClock`;
/// this mirrors the in-memory ceiling/exhaustion semantics so checkpoint/materialize
/// tests can drive a clock without a database.
actor InMemoryLamportClock {
    private var current: UInt64

    init(initial: UInt64 = 0) {
        self.current = initial
    }

    func value() -> UInt64 {
        current
    }

    func observe(_ external: UInt64) {
        guard external < LamportClock.maxAdoptableValue else { return }
        if external > current {
            current = external
        }
    }

    func tick() throws -> UInt64 {
        try ensureHeadroom(current: current, count: 1)
        let (next, _) = current.addingReportingOverflow(1)
        current = next
        return current
    }

    func tickRange(count: Int) throws -> LamportClock.Range {
        precondition(count > 0, "tickRange count must be positive")
        try ensureHeadroom(current: current, count: UInt64(count))
        let (low, _) = current.addingReportingOverflow(1)
        let (high, _) = current.addingReportingOverflow(UInt64(count))
        current = high
        return LamportClock.Range(low: low, high: high)
    }

    private func ensureHeadroom(current: UInt64, count: UInt64) throws {
        guard count > 0 else { return }
        if count >= LamportClock.maxAdoptableValue
            || current >= LamportClock.maxAdoptableValue
            || current >= LamportClock.maxAdoptableValue - count {
            throw LamportClockError.advanceExhausted(current: current, requested: Int(min(count, UInt64(Int.max))))
        }
    }
}

enum LamportClockError: Error, Equatable {
    case advanceExhausted(current: UInt64, requested: Int)
}

extension InMemoryLamportClock: RepoCheckpointClock {
    func observeForCheckpoint(_ external: UInt64) async throws {
        observe(external)
    }

    func tickRangeForCheckpoint(count: Int) async throws -> LamportClock.Range {
        try tickRange(count: count)
    }
}
