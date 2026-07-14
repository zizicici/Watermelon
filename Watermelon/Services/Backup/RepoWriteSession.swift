import Foundation

protocol RepoWriteSession: Sendable {
    func begin() async
    func release() async
    func assertDataWriteAllowed(now: Date) async throws
    func assertControlWriteAllowed(now: Date) async throws
}

struct AnyRepoWriteSession: RepoWriteSession {
    private let beginSession: @Sendable () async -> Void
    private let releaseSession: @Sendable () async -> Void
    private let assertDataWrite: @Sendable (Date) async throws -> Void
    private let assertControlWrite: @Sendable (Date) async throws -> Void

    init<Session: RepoWriteSession>(_ session: Session) {
        beginSession = { await session.begin() }
        releaseSession = { await session.release() }
        assertDataWrite = { try await session.assertDataWriteAllowed(now: $0) }
        assertControlWrite = { try await session.assertControlWriteAllowed(now: $0) }
    }

    func begin() async {
        await beginSession()
    }

    func release() async {
        await releaseSession()
    }

    func assertDataWriteAllowed(now: Date = Date()) async throws {
        try await assertDataWrite(now)
    }

    func assertControlWriteAllowed(now: Date = Date()) async throws {
        try await assertControlWrite(now)
    }
}

enum RepoWriteGuard {
    static func assertDataWriteAllowed(_ mode: RepoWriteMode, now: Date = Date()) async throws {
        try await mode.session.assertDataWriteAllowed(now: now)
    }

    static func assertControlWriteAllowed(_ mode: RepoWriteMode, now: Date = Date()) async throws {
        try await mode.session.assertControlWriteAllowed(now: now)
    }

    static func controlWriteAssertion<Session: RepoWriteSession>(
        _ session: Session?
    ) -> MonthManifestOwnershipAssertion? {
        guard let session else { return nil }
        return { try await session.assertControlWriteAllowed(now: Date()) }
    }
}
