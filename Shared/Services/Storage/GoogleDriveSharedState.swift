import Foundation

actor GoogleDriveThrottleGate {
    struct Key: Hashable, Sendable {
        let clientID: String
        let accountSubject: String
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

actor GoogleDriveDirectoryMutationGate {
    struct Key: Hashable, Sendable {
        let accountSubject: String
        let rootFolderID: String
    }

    private struct Waiter {
        let id: UUID
        let continuation: CheckedContinuation<Void, Error>
    }

    private var lockedKeys = Set<Key>()
    private var waitersByKey: [Key: [Waiter]] = [:]

    func acquire(_ key: Key) async throws {
        if lockedKeys.insert(key).inserted { return }
        let id = UUID()
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                if Task.isCancelled {
                    continuation.resume(throwing: CancellationError())
                } else {
                    waitersByKey[key, default: []].append(Waiter(id: id, continuation: continuation))
                }
            }
        } onCancel: {
            Task { await self.cancelWaiter(id, for: key) }
        }
    }

    func release(_ key: Key) {
        if var waiters = waitersByKey[key], !waiters.isEmpty {
            let next = waiters.removeFirst()
            if waiters.isEmpty {
                waitersByKey.removeValue(forKey: key)
            } else {
                waitersByKey[key] = waiters
            }
            next.continuation.resume()
        } else {
            lockedKeys.remove(key)
        }
    }

    private func cancelWaiter(_ id: UUID, for key: Key) {
        guard var waiters = waitersByKey[key],
              let index = waiters.firstIndex(where: { $0.id == id }) else { return }
        let waiter = waiters.remove(at: index)
        if waiters.isEmpty {
            waitersByKey.removeValue(forKey: key)
        } else {
            waitersByKey[key] = waiters
        }
        waiter.continuation.resume(throwing: CancellationError())
    }
}

nonisolated final class GoogleDriveSharedState: Sendable {
    let throttleGate = GoogleDriveThrottleGate()
    let directoryMutationGate = GoogleDriveDirectoryMutationGate()
    let writeSession = GoogleDriveWriteSessionState()
}
