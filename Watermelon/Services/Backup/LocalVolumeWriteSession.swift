import Foundation

final class LocalVolumeWriteSession: RepoWriteSession, @unchecked Sendable {
    private static let lock = NSLock()
    private static var activeTokens: [String: UUID] = [:]
    private let key: String
    private let token: UUID

    var authorID: String {
        token.uuidString.lowercased()
    }

    private init(key: String, token: UUID) {
        self.key = key
        self.token = token
    }

    static func claim(key: String) -> LocalVolumeWriteSession? {
        lock.withLock {
            guard activeTokens[key] == nil else { return nil }
            let token = UUID()
            activeTokens[key] = token
            return LocalVolumeWriteSession(key: key, token: token)
        }
    }

    func stop() {
        Self.lock.withLock {
            if Self.activeTokens[key] == token {
                Self.activeTokens.removeValue(forKey: key)
            }
        }
    }

    func assertActive() throws {
        guard Self.lock.withLock({ Self.activeTokens[key] == token }) else {
            throw LiteRepoError.ownershipLost
        }
    }

    func stopAndRelease() async {
        stop()
    }

    func begin() async {}

    func release() async {
        stop()
    }

    func assertDataWriteAllowed(now: Date) async throws {
        try assertActive()
    }

    func assertControlWriteAllowed(now: Date) async throws {
        try assertActive()
    }

    deinit {
        stop()
    }
}
