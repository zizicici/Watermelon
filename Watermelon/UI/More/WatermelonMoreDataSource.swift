//
//  WatermelonMoreDataSource.swift
//  Watermelon
//

import Foundation
import MoreKit
import UIKit

class WatermelonMoreDataSource: MoreViewControllerDataSource {
    private enum ItemID {
        static let manageProfiles = "manageProfiles"
        static let storageUsage = "storageUsage"
        static let defaultDeviceScope = "defaultDeviceScope"
        static let workerCount = "workerCount"
        static let iCloudPhotoBackup = "iCloudPhotoBackup"
        static let inboxTransferTutorial = "inboxTransferTutorial"
        static let inboxTransferLivePhotoVideo = "inboxTransferLivePhotoVideo"
        static let inboxTransferOriginalPhoto = "inboxTransferOriginalPhoto"
        static let inboxTransferOriginalVideo = "inboxTransferOriginalVideo"
        static let inboxTransferRemoveLocation = "inboxTransferRemoveLocation"
        static let browserLinkRateLimit = "browserLinkRateLimit"
        static let monthGroupingTimeZone = "monthGroupingTimeZone"
        static let backgroundBackup = "backgroundBackup"
        static let backgroundBackupNodes = "backgroundBackupNodes"
        static let shortcuts = "shortcuts"
        static let language = "language"
        static let photosPermission = "photosPermission"
        static let notificationsPermission = "notificationsPermission"
        static let diagnosticLogs = "diagnosticLogs"
        static let pipProgress = "pipProgress"
        static let pipSound = "pipSound"
        static let testCrash = "testCrash"
    }

    private static let proBadge = MoreBadgeStyle(
        text: "PRO",
        textColor: .materialOnPrimary(dark: .Material.Green._800),
        backgroundColor: .materialPrimary(light: .Material.Green._600, dark: .Material.Green._200)
    )

    private let dependencies: DependencyContainer?
    private let permissions: PermissionSettings
    private let onProfilesChanged: (() -> Void)?
    private let isMonthGroupingTimeZoneChangeBlocked: () -> Bool

    @MainActor
    init(
        dependencies: DependencyContainer?,
        onProfilesChanged: (() -> Void)?,
        isMonthGroupingTimeZoneChangeBlocked: @escaping () -> Bool = { false }
    ) {
        self.dependencies = dependencies
        self.permissions = PermissionSettings()
        self.onProfilesChanged = onProfilesChanged
        self.isMonthGroupingTimeZoneChangeBlocked = isMonthGroupingTimeZoneChangeBlocked
    }

    func sections(for controller: MoreViewController) -> [MoreSectionType] {
        let permissionValues = MainActor.assumeIsolated { (permissions.photosValue, permissions.notificationsValue) }
        var sections: [MoreSectionType] = [.membership]

        sections.append(.custom(MoreCustomSection(
            id: "general",
            header: String(localized: "more.section.general"),
            items: [
                MoreCustomItem(
                    id: ItemID.language,
                    title: String(localized: "more.item.settings.language"),
                    value: String(localized: "more.item.settings.language.value")
                ),
                MoreCustomItem(
                    id: ItemID.photosPermission,
                    title: String(localized: "settings.permission.photos.title"),
                    value: permissionValues.0
                ),
                MoreCustomItem(
                    id: ItemID.notificationsPermission,
                    title: String(localized: "settings.permission.notifications.title"),
                    value: permissionValues.1
                )
            ]
        )))

        if let dependencies {
            sections.append(.custom(MoreCustomSection(
                id: "remoteStorage",
                header: String(localized: "more.section.remoteStorage"),
                items: [
                    MoreCustomItem(id: ItemID.manageProfiles, title: String(localized: "more.item.manageStorage")),
                ]
            )))
            sections.append(.custom(MoreCustomSection(
                id: "backup",
                header: String(localized: "more.section.backup"),
                items: [
                    MoreCustomItem(
                        id: ItemID.defaultDeviceScope,
                        title: DefaultDeviceMediaScopeSetting.getTitle(),
                        value: LocalDataSourceStore.shared.defaultSource.title
                    ),
                    MoreCustomItem(
                        id: ItemID.workerCount,
                        title: String(localized: "settings.worker.default.title"),
                        value: BackupWorkerCountMode.getValue().getName()
                    ),
                    MoreCustomItem(
                        id: ItemID.iCloudPhotoBackup,
                        title: String(localized: "more.item.iCloudAccess"),
                        value: ICloudPhotoBackupMode.getValue().getName()
                    ),
                    MoreCustomItem(
                        id: ItemID.monthGroupingTimeZone,
                        title: String(localized: "settings.monthGroupingTimeZone.title", defaultValue: "Local Photo Grouping Time Zone"),
                        value: MonthGroupingTimeZoneFormatter.title(for: MonthGroupingTimeZonePreference.current)
                    )
                ]
            )))
            sections.append(.custom(MoreCustomSection(
                id: "browserLink",
                header: String(localized: "link.node.backupToComputer"),
                items: [
                    MoreCustomItem(
                        id: ItemID.browserLinkRateLimit,
                        title: String(localized: "settings.browserLinkRateLimit.header"),
                        value: BrowserLinkRateLimitSetting.getValue().getName(),
                        badge: Self.proBadge
                    )
                ]
            )))
            let bgEligible = ((try? dependencies.databaseManager.fetchServerProfiles()) ?? [])
                .filter { $0.resolvedStorageType != .externalVolume }
            let bgEnabledCount = bgEligible.filter { $0.backgroundBackupEnabled }.count
            sections.append(.custom(MoreCustomSection(
                id: "backgroundBackup",
                header: String(localized: "more.section.backgroundBackup"),
                items: [
                    MoreCustomItem(
                        id: ItemID.backgroundBackup,
                        title: String(localized: "more.item.backgroundBackup"),
                        value: BackgroundBackupSetting.getValue().getName(),
                        badge: Self.proBadge
                    ),
                    MoreCustomItem(
                        id: ItemID.backgroundBackupNodes,
                        title: String(localized: "more.item.backgroundBackup.nodes"),
                        value: "\(bgEnabledCount)/\(bgEligible.count)"
                    )
                ]
            )))
            if #available(iOS 27.0, *) {
                sections.append(.custom(MoreCustomSection(
                    id: "shortcuts",
                    header: String(localized: "more.section.shortcuts"),
                    footer: ShortcutsSetting.sectionFooter,
                    items: [
                        MoreCustomItem(
                            id: ItemID.shortcuts,
                            title: ShortcutsSetting.getTitle(),
                            value: ShortcutsSetting.displayValue,
                            badge: Self.proBadge
                        )
                    ]
                )))
            }
            let isPiPProgressActive = PiPProgressSetting.getValue() == .enable
                && MainActor.assumeIsolated { ProStatus.isPro }
            var pipItems = [
                MoreCustomItem(
                    id: ItemID.pipProgress,
                    title: String(localized: "more.item.pipProgress"),
                    value: PiPProgressSetting.getValue().getName(),
                    badge: Self.proBadge
                )
            ]
            if isPiPProgressActive {
                pipItems.append(MoreCustomItem(
                    id: ItemID.pipSound,
                    title: String(localized: "settings.pipSound.header"),
                    value: PiPProgressSoundSetting.getValue().getName()
                ))
            }
            sections.append(.custom(MoreCustomSection(
                id: "pip",
                header: String(localized: "more.section.pip"),
                items: pipItems
            )))
            sections.append(.custom(MoreCustomSection(
                id: "transfer",
                header: String(localized: "transfer.settings.title"),
                items: [
                    MoreCustomItem(
                        id: ItemID.inboxTransferTutorial,
                        title: String(localized: "transfer.settings.tutorial")
                    ),
                    MoreCustomItem(
                        id: ItemID.inboxTransferLivePhotoVideo,
                        title: InboxTransferLivePhotoVideoSetting.getTitle(),
                        value: InboxTransferLivePhotoVideoSetting.current.getName()
                    ),
                    MoreCustomItem(
                        id: ItemID.inboxTransferOriginalPhoto,
                        title: InboxTransferOriginalPhotoSetting.getTitle(),
                        value: InboxTransferOriginalPhotoSetting.current.getName()
                    ),
                    MoreCustomItem(
                        id: ItemID.inboxTransferOriginalVideo,
                        title: InboxTransferOriginalVideoSetting.getTitle(),
                        value: InboxTransferOriginalVideoSetting.current.getName()
                    ),
                    MoreCustomItem(
                        id: ItemID.inboxTransferRemoveLocation,
                        title: InboxTransferRemoveLocationSetting.getTitle(),
                        value: InboxTransferRemoveLocationSetting.current.getName()
                    ),
                ]
            )))
            sections.append(.custom(MoreCustomSection(
                id: "storageUsage",
                header: String(localized: "more.item.storageUsage"),
                items: [
                    MoreCustomItem(id: ItemID.storageUsage, title: String(localized: "more.item.storageUsage")),
                ]
            )))
        }

        sections.append(contentsOf: [.contact, .appjun, .about])

        var diagnosticsItems: [MoreCustomItem] = [
            MoreCustomItem(
                id: ItemID.diagnosticLogs,
                title: String(localized: "more.item.diagnosticLogs")
            )
        ]
        #if DEBUG
        diagnosticsItems.append(
            MoreCustomItem(id: ItemID.testCrash, title: "Test Crash (Debug)")
        )
        #endif
        sections.append(.custom(MoreCustomSection(
            id: "diagnostics",
            header: String(localized: "more.section.diagnostics"),
            items: diagnosticsItems
        )))

        return sections
    }

    func additionalReloadNotifications() -> [Notification.Name] {
        [.BackgroundBackupProfileChanged, .ProfileListChanged, .MonthGroupingTimeZonePreferenceDidChange]
    }

    func moreViewController(_ controller: MoreViewController, didSelectCustomItem item: MoreCustomItem) {
        MainActor.assumeIsolated {
            switch item.id {
            case ItemID.manageProfiles:
                guard let dependencies else { return }
                let vc = ManageStorageProfilesViewController(dependencies: dependencies) { [weak self] in
                    self?.onProfilesChanged?()
                    NotificationCenter.default.post(name: .ProfileListChanged, object: nil)
                }
                controller.pushViewController(vc)
            case ItemID.storageUsage:
                controller.pushViewController(StorageUsageViewController())
            case ItemID.workerCount:
                controller.enterSettings(BackupWorkerCountMode.self)
            case ItemID.defaultDeviceScope:
                guard let dependencies else { return }
                controller.pushViewController(DefaultDataSourceViewController(service: dependencies.photoLibraryService))
            case ItemID.iCloudPhotoBackup:
                controller.enterSettings(ICloudPhotoBackupMode.self)
            case ItemID.inboxTransferTutorial:
                let tutorial = MediaDropTutorialViewController(allowsDismissal: true)
                let container = UINavigationController(rootViewController: tutorial)
                if let sheet = container.sheetPresentationController {
                    sheet.detents = [.large()]
                    sheet.prefersGrabberVisible = true
                }
                tutorial.onCompleted = { [weak container] in
                    MediaDropTutorialViewController.CompletionGate.markCompleted()
                    container?.dismiss(animated: ConsideringUser.animated)
                }
                controller.present(container, animated: ConsideringUser.animated)
            case ItemID.inboxTransferLivePhotoVideo:
                controller.enterSettings(InboxTransferLivePhotoVideoSetting.self)
            case ItemID.inboxTransferOriginalPhoto:
                controller.enterSettings(InboxTransferOriginalPhotoSetting.self)
            case ItemID.inboxTransferOriginalVideo:
                controller.enterSettings(InboxTransferOriginalVideoSetting.self)
            case ItemID.inboxTransferRemoveLocation:
                controller.enterSettings(InboxTransferRemoveLocationSetting.self)
            case ItemID.browserLinkRateLimit:
                controller.enterSettings(BrowserLinkRateLimitSetting.self)
            case ItemID.monthGroupingTimeZone:
                let isMonthGroupingTimeZoneChangeBlocked = isMonthGroupingTimeZoneChangeBlocked
                controller.pushViewController(MonthGroupingTimeZoneSettingsViewController(
                    canChangePreference: { !isMonthGroupingTimeZoneChangeBlocked() }
                ))
            case ItemID.backgroundBackup:
                controller.enterSettings(BackgroundBackupSetting.self)
            case ItemID.shortcuts:
                controller.enterSettings(ShortcutsSetting.self)
            case ItemID.backgroundBackupNodes:
                guard let dependencies else { return }
                let vc = BackgroundBackupNodesViewController(dependencies: dependencies) { [weak self] in
                    self?.onProfilesChanged?()
                    NotificationCenter.default.post(name: .ProfileListChanged, object: nil)
                }
                controller.pushViewController(vc)
            case ItemID.pipProgress:
                controller.enterSettings(PiPProgressSetting.self)
            case ItemID.pipSound:
                controller.enterSettings(PiPProgressSoundSetting.self)
            case ItemID.language:
                controller.jumpToSettings()
            case ItemID.photosPermission:
                Task { await permissions.openPhotos() }
            case ItemID.notificationsPermission:
                Task { await permissions.openNotifications(from: controller) }
            case ItemID.diagnosticLogs:
                controller.pushViewController(ExecutionLogHistoryViewController())
            #if DEBUG
            case ItemID.testCrash:
                fatalError("Test crash for Crashlytics activation")
            #endif
            default:
                break
            }
        }
    }

}
