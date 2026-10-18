import Foundation

enum WatermelonProFeature: String, CaseIterable {
    case storageNodes, pip, backgroundBackup, shortcuts, transferCount, computerSpeed, focusMode

    var title: String {
        switch self {
        case .storageNodes: String(localized: "paywall.benefit.storageNodes")
        case .transferCount: String(localized: "paywall.benefit.transferCount")
        case .computerSpeed: String(localized: "paywall.benefit.transferSpeed")
        case .backgroundBackup: String(localized: "paywall.benefit.backgroundBackup")
        case .shortcuts: String(localized: "paywall.benefit.shortcuts")
        case .pip: PiPProgressSetting.getTitle()
        case .focusMode: String(localized: "paywall.benefit.focusMode")
        }
    }

    var upgradeMessage: String {
        switch self {
        case .storageNodes: String(localized: "home.alert.upgradeNodeLimitMessage")
        case .transferCount: String(localized: "transfer.limit.free")
        case .computerSpeed: String(localized: "settings.browserLinkRateLimit.requiresPro")
        case .backgroundBackup: String(localized: "settings.background.requiresPro")
        case .shortcuts: String(localized: "paywall.shortcuts.upgrade")
        case .pip: String(localized: "settings.pipProgress.requiresPro")
        case .focusMode: String(localized: "log.focusMode.upgradeMessage")
        }
    }

    var comparison: WatermelonPaywallConfiguration.Feature {
        let free: String
        let pro: String
        switch self {
        case .storageNodes:
            free = String(format: String(localized: "paywall.nodeCount"), ProStatus.freeMaximumStorageNodes)
            pro = String(localized: "paywall.unlimited")
        case .transferCount:
            free = String(format: String(localized: "paywall.itemsPerTransfer"), InboxTransferAccessPolicy.freeMaximumItemCount)
            pro = String(localized: "paywall.unlimited")
        case .computerSpeed:
            free = BrowserLinkRateLimitSetting.standard.getName()
            pro = BrowserLinkRateLimitSetting.unlimited.getName()
        case .shortcuts:
            free = String(format: String(localized: "paywall.shortcutTrials"), ShortcutsAccessStore.trialLimit)
            pro = String(localized: "paywall.unlimited")
        case .backgroundBackup, .pip, .focusMode:
            free = "—"
            pro = String(localized: "paywall.included")
        }
        return .init(title: title, freeValue: free, proValue: pro, id: rawValue, explanation: explanation, isSubfeature: self == .computerSpeed)
    }

    var explanation: String {
        switch self {
        case .storageNodes: String(localized: "paywall.info.storageNodes")
        case .transferCount: String(localized: "paywall.info.mediaDrop")
        case .computerSpeed: String(localized: "paywall.info.transferSpeed")
        case .backgroundBackup: String(localized: "paywall.info.backgroundBackup")
        case .shortcuts: String(localized: "paywall.info.shortcuts")
        case .pip: String(localized: "paywall.info.pip")
        case .focusMode: String(localized: "paywall.info.focusBackup")
        }
    }

    static func requiringUpgrade(for error: Error) -> Self? {
        switch error {
        case is BrowserLinkRateLimitSettingError: .computerSpeed
        case is BackgroundBackupSettingError: .backgroundBackup
        case is PiPProgressSettingError: .pip
        case ShortcutsAccessError.trialEnded: .shortcuts
        default: nil
        }
    }
}

enum WatermelonProBenefits {
    static var title: String {
        String(format: String(localized: "paywall.title"), AppName.localized)
    }

    static var features: [WatermelonPaywallConfiguration.Feature] {
        WatermelonProFeature.allCases.filter {
            if $0 == .shortcuts {
                if #available(iOS 27.0, *) { return true }
                return false
            }
            return true
        }.map(\.comparison)
    }
}
