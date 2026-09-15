import Foundation
import MoreKit

enum ShortcutsAccess {
    static func run<T: Sendable>(
        operation: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        try ShortcutsAccessStore.shared.checkEnabled()
        let isPro = await ProStatus.verifyEntitlement()
        return try await ShortcutsAccessStore.shared.run(isPro: isPro, operation: operation)
    }
}

final class ShortcutsAccessStore: @unchecked Sendable {
    static let shared = ShortcutsAccessStore(defaults: ShortcutsSetting.userDefaults)
    static let trialLimit = 5
    private static let completedRunsKey = "com.zizicici.watermelon.ShortcutsCompletedTrialRuns"

    private let defaults: UserDefaults
    private let lock = NSLock()
    private var activeTrialRuns = 0

    init(defaults: UserDefaults) {
        self.defaults = defaults
    }

    var remainingTrialRuns: Int {
        lock.withLock { remainingTrialRunsLocked }
    }

    func checkEnabled() throws {
        if defaults.object(forKey: ShortcutsSetting.getKey()) as? Int == ShortcutsSetting.disable.rawValue {
            throw ShortcutsAccessError.disabled
        }
    }

    func run<T: Sendable>(
        isPro: Bool,
        operation: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        try Task.checkCancellation()
        let usesTrial = try lock.withLock {
            try checkEnabled()
            guard !isPro else { return false }
            guard remainingTrialRunsLocked > 0 else { throw ShortcutsAccessError.trialEnded }
            guard remainingTrialRunsLocked > activeTrialRuns else {
                throw BackgroundBackupRunError(message: String(localized: "mediaBrowser.action.taskInProgress"))
            }
            activeTrialRuns += 1
            return true
        }
        var succeeded = false
        defer {
            if usesTrial {
                lock.withLock {
                    activeTrialRuns -= 1
                    if succeeded {
                        defaults.set(Self.trialLimit - remainingTrialRunsLocked + 1, forKey: Self.completedRunsKey)
                    }
                }
                if succeeded {
                    DispatchQueue.main.async {
                        NotificationCenter.default.post(name: .SettingsUpdate, object: nil)
                    }
                }
            }
        }
        let result = try await operation()
        try Task.checkCancellation()
        succeeded = true
        return result
    }

    private var remainingTrialRunsLocked: Int {
        Self.trialLimit - min(Self.trialLimit, max(0, defaults.integer(forKey: Self.completedRunsKey)))
    }
}

enum ShortcutsAccessError: LocalizedError, Equatable {
    case disabled
    case trialEnded

    var errorDescription: String? {
        switch self {
        case .disabled:
            String(format: String(localized: "settings.shortcuts.error.disabled"), AppName.localized)
        case .trialEnded:
            String(format: String(localized: "settings.shortcuts.error.trialEnded"), AppName.localized)
        }
    }
}
