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
