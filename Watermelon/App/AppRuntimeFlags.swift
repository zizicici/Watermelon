import Foundation

/// Stays true from `enter()` until `exit()` — not until `phase == .completed/.failed`.
/// Non-MainActor so `DependencyContainer` stays instantiable from background tasks.
final class AppRuntimeFlags: @unchecked Sendable {
    private let lock = NSLock()
    private var _isExecuting: Bool = false

    var isExecuting: Bool {
        lock.withLock { _isExecuting }
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
}
