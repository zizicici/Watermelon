import Foundation

/// Stays true from `enter()` until `exit()` — not until `phase == .completed/.failed`.
/// Non-MainActor so `DependencyContainer` stays instantiable from background tasks.
///
/// The `shared` singleton is the process-wide source of truth: foreground (scene)
/// DependencyContainer, background-task DependencyContainer, and any multi-scene
/// container construction all hand back the same instance so a BGProcessingTask
/// can see a foreground run's `isExecuting` / `isVerifying` and skip.
final class AppRuntimeFlags: @unchecked Sendable {
    static let shared = AppRuntimeFlags()

    private let lock = NSLock()
    private var _isExecuting: Bool = false
    private var _isVerifying: Bool = false

    var isExecuting: Bool {
        lock.withLock { _isExecuting }
    }

    var isVerifying: Bool {
        lock.withLock { _isVerifying }
    }

    func setExecuting(_ value: Bool) {
        let didChange: Bool = lock.withLock {
            guard _isExecuting != value else { return false }
            _isExecuting = value
            return true
        }
        guard didChange else { return }
        NotificationCenter.default.post(
            name: .ExecutionLifecycleDidChange,
            object: self
        )
    }

    func setVerifying(_ value: Bool) {
        let didChange: Bool = lock.withLock {
            guard _isVerifying != value else { return false }
            _isVerifying = value
            return true
        }
        guard didChange else { return }
        NotificationCenter.default.post(
            name: .ExecutionLifecycleDidChange,
            object: self
        )
    }

    /// Atomic check-and-set: claim the execution lease only when no foreground / background run
    /// and no manual verify is active. Returns `true` if the caller now owns the lease; `false`
    /// if another owner already holds it. Pair with `setExecuting(false)` (or `endExecution()`)
    /// at every exit path. Required so the BG → V2-open window cannot race a foreground claim.
    @discardableResult
    func tryBeginExecution() -> Bool {
        let acquired: Bool = lock.withLock {
            guard !_isExecuting, !_isVerifying else { return false }
            _isExecuting = true
            return true
        }
        guard acquired else { return false }
        NotificationCenter.default.post(
            name: .ExecutionLifecycleDidChange,
            object: self
        )
        return true
    }

    /// Atomic check-and-set for the verify lease. Refuses to start while any execution or
    /// another verify is in flight. Pair with `setVerifying(false)` at every exit path.
    @discardableResult
    func tryBeginVerifying() -> Bool {
        let acquired: Bool = lock.withLock {
            guard !_isExecuting, !_isVerifying else { return false }
            _isVerifying = true
            return true
        }
        guard acquired else { return false }
        NotificationCenter.default.post(
            name: .ExecutionLifecycleDidChange,
            object: self
        )
        return true
    }
}
