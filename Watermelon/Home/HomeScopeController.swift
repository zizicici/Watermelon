import Foundation
import os.log

private let scopeLog = Logger(subsystem: "com.zizicici.watermelon", category: "HomeScope")

@MainActor
final class HomeScopeController {
    private(set) var activeScope: HomeLocalLibraryScope = .allPhotos
    private(set) var loadedScope: HomeLocalLibraryScope?
    private(set) var isReloading: Bool = false
    private(set) var pendingScope: HomeLocalLibraryScope?

    var onChange: (() -> Void)?

    enum SetActiveResult {
        case applied
        case noChange
        case deferred
    }

    func setActive(_ scope: HomeLocalLibraryScope, isExecuting: Bool) -> SetActiveResult {
        if isExecuting {
            pendingScope = scope
            scopeLog.info("[HomeScope] setActive deferred (execution): \(String(describing: scope), privacy: .public)")
            onChange?()
            return .deferred
        }
        guard scope != activeScope else { return .noChange }
        activeScope = scope
        isReloading = true
        scopeLog.info("[HomeScope] setActive applied: \(String(describing: scope), privacy: .public)")
        onChange?()
        return .applied
    }

    func completeReload(loaded: HomeLocalLibraryScope, hasMoreReloadPending: Bool) {
        loadedScope = loaded
        if !hasMoreReloadPending {
            isReloading = false
        }
        scopeLog.info("[HomeScope] completeReload loaded=\(String(describing: loaded), privacy: .public), morePending=\(hasMoreReloadPending)")
        onChange?()
    }

    func resumeFromDeferred() -> HomeLocalLibraryScope? {
        guard let pending = pendingScope else { return nil }
        pendingScope = nil
        if pending != activeScope {
            activeScope = pending
            isReloading = true
        }
        scopeLog.info("[HomeScope] resumeFromDeferred: \(String(describing: pending), privacy: .public)")
        onChange?()
        return pending
    }

    @discardableResult
    func setActiveFromNormalize(_ scope: HomeLocalLibraryScope) -> Bool {
        guard scope != activeScope else { return false }
        activeScope = scope
        scopeLog.info("[HomeScope] setActiveFromNormalize: \(String(describing: scope), privacy: .public)")
        onChange?()
        return true
    }

    /// Stash the *current* active album scope as pending so the post-execution
    /// flow re-runs normalization against PhotoKit. Unlike `setActive(_:isExecuting: true)`
    /// (which stashes a *different* user-requested scope), this is a no-op when a
    /// pending scope already exists or when active is `.allPhotos`.
    func requestPostExecutionRenormalization() {
        guard pendingScope == nil, case .albums = activeScope else { return }
        pendingScope = activeScope
        scopeLog.info("[HomeScope] requestPostExecutionRenormalization")
        onChange?()
    }
}
