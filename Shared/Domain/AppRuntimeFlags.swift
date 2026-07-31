import Foundation

private final class WeakExecutionCancellationTarget<Value: AnyObject>:
    @unchecked Sendable {
    weak var value: Value?

    init(_ value: Value) {
        self.value = value
    }
}

/// Stays true from `enter()` until `exit()` — not until `phase == .completed/.failed`.
/// Non-MainActor so `DependencyContainer` stays instantiable from background tasks.
final class AppRuntimeFlags: @unchecked Sendable {
    struct ExecutionClaim: Sendable {
        fileprivate let token: UUID
    }

    @TaskLocal private static var profileMutationLeaseToken: UUID?

    private static let lock = NSLock()
    nonisolated(unsafe) private static var executionOwner:
        ObjectIdentifier?
    nonisolated(unsafe) private static var executionClaimToken: UUID?
    nonisolated(unsafe) private static var executionCancellationHandler:
        (@Sendable () -> Void)?
    nonisolated(unsafe) private static var activeProfileMutationToken:
        UUID?
    nonisolated(unsafe) private static var profileMutationDepth = 0
    nonisolated(unsafe) private static var connectingProfileID: Int64?
    nonisolated(unsafe) private static var connectingEphemeralID: String?
    nonisolated(unsafe) private static var connectingOwner:
        ObjectIdentifier?

    var isExecuting: Bool {
        Self.lock.withLock { Self.executionOwner != nil }
    }

    func isConnecting(profileID: Int64?) -> Bool {
        guard let profileID else { return false }
        return Self.lock.withLock { Self.connectingProfileID == profileID }
    }

    @discardableResult
    func tryEnterExecution() -> ExecutionClaim? {
        let owner = ObjectIdentifier(self)
        let claim: ExecutionClaim? = Self.lock.withLock {
            guard Self.executionOwner == nil,
                  Self.activeProfileMutationToken == nil,
                  Self.connectingOwner == nil else { return nil }
            let token = UUID()
            Self.executionOwner = owner
            Self.executionCancellationHandler = nil
            Self.executionClaimToken = token
            return ExecutionClaim(token: token)
        }
        guard let claim else { return nil }
        NotificationCenter.default.post(
            name: .ExecutionLifecycleDidChange,
            object: self
        )
        return claim
    }

    // Scope form of the execution mutex for self-contained one-shot work: claim, run, release (even on throw).
    // Returns nil — body NOT run — when another owner holds the mutex, so the caller can surface "task in
    // progress". Prefer this over raw tryEnterExecution/exitExecution so a claim can't be leaked or skipped.
    func withExecutionLease<T: Sendable>(
        cancellationHandler: (@Sendable () -> Void)? = nil,
        _ body: @isolated(any) () async throws -> T
    ) async rethrows -> T? {
        guard let claim = tryEnterExecution() else { return nil }
        if let cancellationHandler {
            setExecutionCancellationHandler(
                cancellationHandler,
                claim: claim
            )
        }
        defer { exitExecution(claim) }
        return try await body()
    }

    func setExecutionCancellationHandler(
        _ handler: @escaping @Sendable () -> Void,
        claim: ExecutionClaim
    ) {
        let owner = ObjectIdentifier(self)
        Self.lock.withLock {
            guard Self.executionOwner == owner,
                  Self.executionClaimToken == claim.token else {
                return
            }
            Self.executionCancellationHandler = handler
        }
    }

    func setExecutionCancellationHandler<Value: AnyObject>(
        for target: Value,
        claim: ExecutionClaim,
        _ action: @escaping @MainActor @Sendable (Value) -> Void
    ) {
        setExecutionCancellationHandler(
            makeExecutionCancellationHandler(
                for: target,
                action
            ),
            claim: claim
        )
    }

    func makeExecutionCancellationHandler<Value: AnyObject>(
        for target: Value,
        _ action: @escaping @MainActor @Sendable (Value) -> Void
    ) -> @Sendable () -> Void {
        let target = WeakExecutionCancellationTarget(target)
        return {
            Task { @MainActor in
                guard let value = target.value else { return }
                action(value)
            }
        }
    }

    @discardableResult
    func requestExecutionCancellation() -> Bool {
        let owner = ObjectIdentifier(self)
        let handler: (@Sendable () -> Void)? = Self.lock.withLock {
            guard Self.executionOwner == owner else { return nil }
            return Self.executionCancellationHandler
        }
        handler?()
        return handler != nil
    }

    func exitExecution(_ claim: ExecutionClaim) {
        let owner = ObjectIdentifier(self)
        let didChange: Bool = Self.lock.withLock {
            guard Self.executionOwner == owner,
                  Self.executionClaimToken == claim.token else { return false }
            Self.executionOwner = nil
            Self.executionCancellationHandler = nil
            Self.executionClaimToken = nil
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
        let owner = ObjectIdentifier(self)
        let released = Self.lock.withLock {
            let releasedConnection = Self.connectingOwner == owner
            if releasedConnection {
                Self.connectingProfileID = nil
                Self.connectingEphemeralID = nil
                Self.connectingOwner = nil
            }

            let releasedExecution = Self.executionOwner == owner
            if releasedExecution {
                Self.executionOwner = nil
                Self.executionClaimToken = nil
                Self.executionCancellationHandler = nil
            }
            return (connection: releasedConnection, execution: releasedExecution)
        }
        if released.connection {
            NotificationCenter.default.post(name: .ConnectionLifecycleDidChange, object: nil)
        }
        if released.execution {
            NotificationCenter.default.post(name: .ExecutionLifecycleDidChange, object: nil)
        }
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
            executionCancellationHandler = nil
            executionClaimToken = nil
            activeProfileMutationToken = nil
            profileMutationDepth = 0
            connectingProfileID = nil
            connectingEphemeralID = nil
            connectingOwner = nil
        }
    }
    #endif
}
