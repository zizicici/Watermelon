import Foundation

/// Stays true from `enter()` until `exit()` — not until `phase == .completed/.failed`.
/// Non-MainActor so `DependencyContainer` stays instantiable from background tasks.
final class AppRuntimeFlags: @unchecked Sendable {
    @TaskLocal private static var profileMutationLeaseToken: UUID?

    private static let lock = NSLock()
    private static var executionOwner: ObjectIdentifier?
    private static var activeProfileMutationToken: UUID?
    private static var profileMutationDepth = 0
    private static var connectingProfileID: Int64?
    private static var connectingEphemeralID: String?
    private static var connectingOwner: ObjectIdentifier?

    var isExecuting: Bool {
        Self.lock.withLock { Self.executionOwner != nil }
    }

    func isConnecting(profileID: Int64?) -> Bool {
        guard let profileID else { return false }
        return Self.lock.withLock { Self.connectingProfileID == profileID }
    }

    @discardableResult
    func tryEnterExecution() -> Bool {
        let owner = ObjectIdentifier(self)
        let didEnter: Bool = Self.lock.withLock {
            guard Self.executionOwner == nil,
                  Self.activeProfileMutationToken == nil,
                  Self.connectingOwner == nil else { return false }
            Self.executionOwner = owner
            return true
        }
        guard didEnter else { return false }
        NotificationCenter.default.post(
            name: .ExecutionLifecycleDidChange,
            object: self
        )
        return true
    }

    // Scope form of the execution mutex for self-contained one-shot work: claim, run, release (even on throw).
    // Returns nil — body NOT run — when another owner holds the mutex, so the caller can surface "task in
    // progress". Prefer this over raw tryEnterExecution/exitExecution so a claim can't be leaked or skipped.
    func withExecutionLease<T>(_ body: () async throws -> T) async rethrows -> T? {
        guard tryEnterExecution() else { return nil }
        defer { exitExecution() }
        return try await body()
    }

    func exitExecution() {
        let owner = ObjectIdentifier(self)
        let didChange: Bool = Self.lock.withLock {
            guard Self.executionOwner == owner else { return false }
            Self.executionOwner = nil
            return true
        }
        guard didChange else { return }
        NotificationCenter.default.post(
            name: .ExecutionLifecycleDidChange,
            object: self
        )
    }

    func withProfileMutationLease<T>(profileID: Int64?, _ body: () throws -> T) rethrows -> T? {
        let token = Self.profileMutationLeaseToken ?? UUID()
        guard Self.acquireProfileMutationLease(token: token, profileID: profileID) else { return nil }
        defer { Self.releaseProfileMutationLease(token: token) }
        return try Self.$profileMutationLeaseToken.withValue(token) {
            try body()
        }
    }

    func withAsyncProfileMutationLease<T>(
        profileID: Int64?,
        _ body: () async throws -> T
    ) async rethrows -> T? {
        let token = Self.profileMutationLeaseToken ?? UUID()
        guard Self.acquireProfileMutationLease(token: token, profileID: profileID) else { return nil }
        defer { Self.releaseProfileMutationLease(token: token) }
        return try await Self.$profileMutationLeaseToken.withValue(token) {
            try await body()
        }
    }

    @discardableResult
    func tryBeginConnecting(profileID: Int64?) -> Bool {
        guard let profileID else { return false }
        let owner = ObjectIdentifier(self)
        let didChange = Self.lock.withLock {
            guard Self.executionOwner == nil,
                  Self.activeProfileMutationToken == nil,
                  Self.connectingOwner == nil ||
                    (Self.connectingOwner == owner && Self.connectingEphemeralID == nil) else { return false }
            Self.connectingProfileID = profileID
            Self.connectingEphemeralID = nil
            Self.connectingOwner = owner
            return true
        }
        if didChange {
            NotificationCenter.default.post(name: .ConnectionLifecycleDidChange, object: self)
        }
        return didChange
    }

    @discardableResult
    func tryBeginEphemeralConnecting(sessionID: String) -> Bool {
        guard !sessionID.isEmpty else { return false }
        let owner = ObjectIdentifier(self)
        let didChange = Self.lock.withLock {
            guard Self.executionOwner == nil,
                  Self.activeProfileMutationToken == nil,
                  Self.connectingOwner == nil else { return false }
            Self.connectingProfileID = nil
            Self.connectingEphemeralID = sessionID
            Self.connectingOwner = owner
            return true
        }
        if didChange {
            NotificationCenter.default.post(name: .ConnectionLifecycleDidChange, object: self)
        }
        return didChange
    }

    func endConnecting(profileID: Int64?) {
        guard let profileID else { return }
        let owner = ObjectIdentifier(self)
        let didChange = Self.lock.withLock {
            guard Self.connectingProfileID == profileID, Self.connectingOwner == owner else { return false }
            Self.connectingProfileID = nil
            Self.connectingEphemeralID = nil
            Self.connectingOwner = nil
            return true
        }
        if didChange {
            NotificationCenter.default.post(name: .ConnectionLifecycleDidChange, object: self)
        }
    }

    func endEphemeralConnecting(sessionID: String) {
        let owner = ObjectIdentifier(self)
        let didChange = Self.lock.withLock {
            guard Self.connectingEphemeralID == sessionID, Self.connectingOwner == owner else { return false }
            Self.connectingProfileID = nil
            Self.connectingEphemeralID = nil
            Self.connectingOwner = nil
            return true
        }
        if didChange {
            NotificationCenter.default.post(name: .ConnectionLifecycleDidChange, object: self)
        }
    }

    deinit {
        let connection = Self.lock.withLock {
            Self.connectingOwner == ObjectIdentifier(self)
                ? (Self.connectingProfileID, Self.connectingEphemeralID)
                : (nil, nil)
        }
        if let profileID = connection.0 { endConnecting(profileID: profileID) }
        if let sessionID = connection.1 { endEphemeralConnecting(sessionID: sessionID) }
        exitExecution()
    }

    private static func acquireProfileMutationLease(token: UUID, profileID: Int64?) -> Bool {
        lock.withLock {
            guard executionOwner == nil,
                  connectingEphemeralID == nil,
                  profileID == nil || connectingProfileID != profileID else { return false }
            if let activeProfileMutationToken {
                guard activeProfileMutationToken == token else { return false }
                profileMutationDepth += 1
            } else {
                activeProfileMutationToken = token
                profileMutationDepth = 1
            }
            return true
        }
    }

    private static func releaseProfileMutationLease(token: UUID) {
        lock.withLock {
            guard activeProfileMutationToken == token, profileMutationDepth > 0 else { return }
            profileMutationDepth -= 1
            if profileMutationDepth == 0 {
                activeProfileMutationToken = nil
            }
        }
    }

    #if DEBUG
    static func _testReset() {
        lock.withLock {
            executionOwner = nil
            activeProfileMutationToken = nil
            profileMutationDepth = 0
            connectingProfileID = nil
            connectingEphemeralID = nil
            connectingOwner = nil
        }
    }
    #endif
}
