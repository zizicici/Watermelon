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
        static let workerCount = "workerCount"
        static let iCloudPhotoBackup = "iCloudPhotoBackup"
        static let backgroundBackup = "backgroundBackup"
        static let language = "language"
        static let diagnosticLogs = "diagnosticLogs"
    }

    private let dependencies: DependencyContainer?
    private let onProfilesChanged: (() -> Void)?

    init(dependencies: DependencyContainer?, onProfilesChanged: (() -> Void)?) {
        self.dependencies = dependencies
        self.onProfilesChanged = onProfilesChanged
    }

    func sections(for controller: MoreViewController) -> [MoreSectionType] {
        var sections: [MoreSectionType] = [.membership]

        sections.append(.custom(MoreCustomSection(
            id: "general",
            header: String(localized: "more.section.general"),
            items: [
                MoreCustomItem(
                    id: ItemID.language,
                    title: String(localized: "more.item.settings.language"),
                    value: String(localized: "more.item.settings.language.value")
                )
            ]
        )))

        if dependencies != nil {
            sections.append(.custom(MoreCustomSection(
                id: "remoteStorage",
                header: String(localized: "more.section.remoteStorage"),
                items: [MoreCustomItem(id: ItemID.manageProfiles, title: String(localized: "more.item.manageStorage"))]
            )))
            sections.append(.custom(MoreCustomSection(
                id: "backup",
                header: String(localized: "more.section.backup"),
                items: [
                    MoreCustomItem(
                        id: ItemID.workerCount,
                        title: String(localized: "more.item.workerCount"),
                        value: BackupWorkerCountMode.getValue().getName()
                    ),
                    MoreCustomItem(
                        id: ItemID.iCloudPhotoBackup,
                        title: String(localized: "more.item.iCloudAccess"),
                        value: ICloudPhotoBackupMode.getValue().getName()
                    ),
                    MoreCustomItem(
                        id: ItemID.backgroundBackup,
                        title: String(localized: "more.item.backgroundBackup"),
                        value: BackgroundBackupSetting.getValue().getName()
                    )
                ]
            )))
        }

        sections.append(.custom(MoreCustomSection(
            id: "diagnostics",
            header: String(localized: "more.section.diagnostics"),
            items: [
                MoreCustomItem(
                    id: ItemID.diagnosticLogs,
                    title: String(localized: "more.item.diagnosticLogs")
                )
            ]
        )))

        sections.append(contentsOf: [.contact, .appjun, .about])
        return sections
    }

    func moreViewController(_ controller: MoreViewController, didSelectCustomItem item: MoreCustomItem) {
        MainActor.assumeIsolated {
            switch item.id {
            case ItemID.manageProfiles:
                guard let dependencies else { return }
                let vc = ManageStorageProfilesViewController(dependencies: dependencies) { [weak self] in
                    self?.onProfilesChanged?()
                }
                controller.pushViewController(vc)
            case ItemID.workerCount:
                controller.enterSettings(BackupWorkerCountMode.self)
            case ItemID.iCloudPhotoBackup:
                controller.enterSettings(ICloudPhotoBackupMode.self)
            case ItemID.backgroundBackup:
                controller.enterSettings(BackgroundBackupSetting.self)
            case ItemID.language:
                controller.jumpToSettings()
            case ItemID.diagnosticLogs:
                controller.pushViewController(ExecutionLogHistoryViewController())
            default:
                break
            }
        }
    }

}
