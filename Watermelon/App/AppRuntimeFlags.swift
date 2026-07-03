import Foundation

/// Stays true from `enter()` until `exit()` — not until `phase == .completed/.failed`.
/// Non-MainActor so `DependencyContainer` stays instantiable from background tasks.
final class AppRuntimeFlags: @unchecked Sendable {
    private static let lock = NSLock()
    private static var executionOwner: ObjectIdentifier?

    var isExecuting: Bool {
        Self.lock.withLock { Self.executionOwner != nil }
    }

    @discardableResult
    func tryEnterExecution() -> Bool {
        let owner = ObjectIdentifier(self)
        let didEnter: Bool = Self.lock.withLock {
            guard Self.executionOwner == nil else { return false }
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

    deinit {
        exitExecution()
    }

    #if DEBUG
    static func _testReset() {
        lock.withLock {
            executionOwner = nil
        }
    }
    #endif
}
