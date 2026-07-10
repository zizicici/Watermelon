import Foundation

/// Stays true from `enter()` until `exit()` — not until `phase == .completed/.failed`.
/// Non-MainActor so `DependencyContainer` stays instantiable from background tasks.
final class AppRuntimeFlags: @unchecked Sendable {
    private static let lock = NSLock()
    private static var executionOwner: ObjectIdentifier?
    private static var profileMutationInProgress = false
    private static var connectingProfileID: Int64?
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
            guard Self.executionOwner == nil, !Self.profileMutationInProgress else { return false }
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
        let acquired = Self.lock.withLock {
            guard Self.executionOwner == nil,
                  !Self.profileMutationInProgress,
                  profileID == nil || Self.connectingProfileID != profileID else { return false }
            Self.profileMutationInProgress = true
            return true
        }
        guard acquired else { return nil }
        defer {
            Self.lock.withLock {
                Self.profileMutationInProgress = false
            }
        }
        return try body()
    }

    @discardableResult
    func tryBeginConnecting(profileID: Int64?) -> Bool {
        guard let profileID else { return false }
        let owner = ObjectIdentifier(self)
        let didChange = Self.lock.withLock {
            guard Self.executionOwner == nil,
                  !Self.profileMutationInProgress,
                  Self.connectingOwner == nil || Self.connectingOwner == owner else { return false }
            Self.connectingProfileID = profileID
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
            Self.connectingOwner = nil
            return true
        }
        if didChange {
            NotificationCenter.default.post(name: .ConnectionLifecycleDidChange, object: self)
        }
    }

    deinit {
        let connectingID = Self.lock.withLock {
            Self.connectingOwner == ObjectIdentifier(self) ? Self.connectingProfileID : nil
        }
        endConnecting(profileID: connectingID)
        exitExecution()
    }

    #if DEBUG
    static func _testReset() {
        lock.withLock {
            executionOwner = nil
            profileMutationInProgress = false
            connectingProfileID = nil
            connectingOwner = nil
        }
    }
    #endif
}
