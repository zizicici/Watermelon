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

    /// Used by the deferred-normalize path: stash the current active scope as pending so
    /// the post-execution flow re-runs normalization. No-op if a pending scope is already
    /// queued (it implicitly covers any later normalization too).
    func deferActiveScopeForReevaluation() {
        guard pendingScope == nil, case .albums = activeScope else { return }
        pendingScope = activeScope
        scopeLog.info("[HomeScope] deferActiveScopeForReevaluation")
        onChange?()
    }
}
