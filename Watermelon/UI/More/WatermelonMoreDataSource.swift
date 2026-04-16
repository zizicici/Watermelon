//
//  WatermelonMoreDataSource.swift
//  Watermelon
//

import Foundation
import MoreKit

class WatermelonMoreDataSource: MoreViewControllerDataSource {
    private enum ItemID {
        static let manageProfiles = "manageProfiles"
        static let hashIndex = "hashIndex"
        static let workerCount = "workerCount"
        static let iCloudPhotoBackup = "iCloudPhotoBackup"
        static let language = "language"
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
                header: "远端存储",
                items: [MoreCustomItem(id: ItemID.manageProfiles, title: "管理存储")]
            )))
            sections.append(.custom(MoreCustomSection(
                id: "localData",
                header: "本地数据",
                items: [MoreCustomItem(id: ItemID.hashIndex, title: "本地 Hash 索引")]
            )))
            sections.append(.custom(MoreCustomSection(
                id: "backup",
                header: String(localized: "more.section.backup"),
                items: [
                    MoreCustomItem(
                        id: ItemID.workerCount,
                        title: "上传并发",
                        value: BackupWorkerCountMode.getValue().getName()
                    ),
                    MoreCustomItem(
                        id: ItemID.iCloudPhotoBackup,
                        title: "允许访问 iCloud 原件",
                        value: ICloudPhotoBackupMode.getValue().getName()
                    )
                ]
            )))
        }

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
            case ItemID.hashIndex:
                guard let dependencies else { return }
                let vc = LocalHashIndexManagerViewController(dependencies: dependencies)
                controller.pushViewController(vc)
            case ItemID.workerCount:
                controller.enterSettings(BackupWorkerCountMode.self)
            case ItemID.iCloudPhotoBackup:
                controller.enterSettings(ICloudPhotoBackupMode.self)
            case ItemID.language:
                controller.jumpToSettings()
            default:
                break
            }
        }
    }

}
