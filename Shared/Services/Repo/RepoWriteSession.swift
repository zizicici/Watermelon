import Foundation

protocol RepoWriteSession: Sendable {
    func begin() async
    func release() async
    // Recoverable writes may reuse bounded lease confidence because failure can only leave reclaimable state.
    func assertWriteAllowed(now: Date) async throws
    // Destructive writes normally prove remotely; selected attended backends may reuse bounded confidence.
    func assertDestructiveWriteAllowed(now: Date) async throws
}

// Both gates, carried as one value. Kept as a struct rather than two closure parameters so a caller cannot
// wire up the recoverable gate and forget the destructive one — the erased single closure this replaced could
// not express the difference at all, which is why every site conservatively took the strongest gate.
struct RepoOwnershipGates: Sendable {
    let assertWrite: @Sendable () async throws -> Void
    let assertDestructive: @Sendable () async throws -> Void
}

struct AnyRepoWriteSession: RepoWriteSession {
    private let beginSession: @Sendable () async -> Void
    private let releaseSession: @Sendable () async -> Void
    private let assertWrite: @Sendable (Date) async throws -> Void
    private let assertDestructiveWrite: @Sendable (Date) async throws -> Void

    init<Session: RepoWriteSession>(_ session: Session) {
        beginSession = { await session.begin() }
        releaseSession = { await session.release() }
        assertWrite = { try await session.assertWriteAllowed(now: $0) }
        assertDestructiveWrite = { try await session.assertDestructiveWriteAllowed(now: $0) }
    }

    func begin() async {
        await beginSession()
    }

    func release() async {
        await releaseSession()
    }

    func assertWriteAllowed(now: Date = Date()) async throws {
        try await assertWrite(now)
    }

    func assertDestructiveWriteAllowed(now: Date = Date()) async throws {
        try await assertDestructiveWrite(now)
    }
}

enum RepoWriteGuard {
    static func assertWriteAllowed(_ mode: RepoWriteMode, now: Date = Date()) async throws {
        try await mode.session.assertWriteAllowed(now: now)
    }

    static func assertDestructiveWriteAllowed(_ mode: RepoWriteMode, now: Date = Date()) async throws {
        try await mode.session.assertDestructiveWriteAllowed(now: now)
    }

    static func ownershipGates<Session: RepoWriteSession>(
        _ session: Session?
    ) -> RepoOwnershipGates? {
        guard let session else { return nil }
        return RepoOwnershipGates(
            assertWrite: { try await session.assertWriteAllowed(now: Date()) },
            assertDestructive: { try await session.assertDestructiveWriteAllowed(now: Date()) }
        )
    }
}
