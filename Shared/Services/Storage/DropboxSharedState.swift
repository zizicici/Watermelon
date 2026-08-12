import Foundation

actor DropboxThrottleGate {
    struct Key: Hashable, Sendable {
        let appKey: String
        let accountID: String
    }

    private var blockedUntilByKey: [Key: Date] = [:]

    func record(retryAfter: Date, for key: Key) {
        if let blockedUntil = blockedUntilByKey[key], blockedUntil >= retryAfter { return }
        blockedUntilByKey[key] = retryAfter
    }

    func waitForPermit(for key: Key) async throws {
        while let deadline = blockedUntilByKey[key] {
            let delay = deadline.timeIntervalSinceNow
            guard delay > 0 else {
                blockedUntilByKey.removeValue(forKey: key)
                return
            }
            try await Task.sleep(for: .seconds(delay))
        }
    }
}

nonisolated final class DropboxSharedState: Sendable {
    let throttleGate = DropboxThrottleGate()
}
