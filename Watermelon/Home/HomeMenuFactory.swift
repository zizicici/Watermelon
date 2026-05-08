import UIKit

enum NewStorageDestination {
    case smb
    case smbDiscovery
    case webdav
    case externalVolume
    case s3
    case sftp
}

@MainActor
struct HomeMenuFactory {
    struct Hooks {
        var refreshLocalLibraryMenu: () -> Void
        var openLocalAlbumPicker: () -> Void
        var openNewStorageFlow: (NewStorageDestination) -> Void
        var openManageProfiles: () -> Void
        var openCurrentProfileSettings: () -> Void
        var scrollToMonth: (LibraryMonthKey) -> Void
        var openLocalIndex: () -> Void
        var openDuplicates: () -> Void
    }

    private static let monthFormatter: DateFormatter = {
        let f = DateFormatter()
        f.setLocalizedDateFormatFromTemplate("MMM")
        return f
    }()

    private static let yearFormatter: DateFormatter = {
        let f = DateFormatter()
        f.setLocalizedDateFormatFromTemplate("yyyy")
        return f
    }()

    let store: HomeScreenStore
    let hooks: Hooks

    func buildLocalLibrary(isPad: Bool) -> UIMenu {
        let isSpecificAlbums = store.localLibraryScope.isSpecificAlbums
        let attributes: UIMenuElement.Attributes = store.executionState != nil ? .disabled : []
        let allPhotosSymbol = isPad ? "ipad" : "iphone"
        let allPhotosAction = UIAction(
            title: String(localized: "home.localSource.allPhotos"),
            image: UIImage(systemName: allPhotosSymbol),
            attributes: attributes,
            state: isSpecificAlbums ? .off : .on
        ) { [store, hooks] _ in
            store.setLocalLibraryScope(.allPhotos)
            hooks.refreshLocalLibraryMenu()
        }

        let specificAlbumsAction = UIAction(
            title: String(localized: "home.localSource.specificAlbums"),
            image: UIImage(systemName: "photo.stack"),
            attributes: attributes,
            state: isSpecificAlbums ? .on : .off
        ) { [hooks] _ in
            hooks.openLocalAlbumPicker()
        }

        let localIndexAction = UIAction(
            title: String(localized: "home.localIndex.title"),
            image: UIImage(systemName: "square.stack.3d.up"),
            attributes: attributes
        ) { [hooks] _ in
            hooks.openLocalIndex()
        }

        let duplicatesAction = UIAction(
            title: String(localized: "home.menu.duplicates"),
            image: UIImage(systemName: "rectangle.on.rectangle.slash"),
            attributes: attributes
        ) { [hooks] _ in
            hooks.openDuplicates()
        }

        let toolsSection = UIMenu(
            title: "",
            options: .displayInline,
            children: [localIndexAction, duplicatesAction]
        )

        return UIMenu(children: [allPhotosAction, specificAlbumsAction, toolsSection])
    }

    func buildDestination() -> UIMenu {
        let activeProfile = store.connectionState.isConnected ? store.connectionState.activeProfile : nil
        let busyAttributes: UIMenuElement.Attributes =
            (store.executionState != nil || store.isRemoteMaintenanceActive) ? .disabled : []

        let typeOrder: [StorageType] = [.smb, .webdav, .s3, .sftp, .externalVolume]
        var profilesByType: [StorageType: [UIAction]] = [:]
        for profile in store.savedProfiles {
            if let active = activeProfile, active.id == profile.id { continue }
            let storageType = profile.storageProfile.storageType
            var subtitle = profile.storageProfile.displaySubtitle
            if let id = profile.id, store.reachability(for: id) == .unreachable {
                subtitle = String(localized: "home.menu.offlineMarker") + subtitle
            }
            let action = UIAction(
                title: profile.name,
                subtitle: subtitle,
                image: UIImage(systemName: storageType.symbolName),
                attributes: busyAttributes
            ) { [store] _ in
                store.connectProfile(profile)
            }
            profilesByType[storageType, default: []].append(action)
        }

        var connectionChildren: [UIMenuElement] = []
        if activeProfile != nil {
            let currentProfileAction = UIAction(
                title: String(localized: "home.menu.currentProfileSettings"),
                image: UIImage(systemName: "slider.horizontal.3"),
                attributes: busyAttributes
            ) { [hooks] _ in
                hooks.openCurrentProfileSettings()
            }
            let disconnectAction = UIAction(
                title: String(localized: "home.menu.disconnect"),
                image: UIImage(systemName: "xmark.circle"),
                attributes: busyAttributes
            ) { [store] _ in
                store.disconnect()
            }
            connectionChildren.append(
                UIMenu(title: "", options: .displayInline, children: [currentProfileAction, disconnectAction])
            )
        }
        for type in typeOrder {
            guard let actions = profilesByType[type], !actions.isEmpty else { continue }
            connectionChildren.append(
                UIMenu(title: typeSectionTitle(for: type), options: .displayInline, children: actions)
            )
        }

        let connectionTitle = activeProfile?.name ?? String(localized: "home.menu.selectNode")
        let connectionImage = UIImage(
            systemName: activeProfile.map { $0.storageProfile.storageType.symbolName } ?? "link"
        )
        let connectionMenu = UIMenu(
            title: connectionTitle,
            subtitle: activeProfile?.storageProfile.displaySubtitle,
            image: connectionImage,
            children: connectionChildren
        )
        let addStorageMenu = UIMenu(
            title: String(localized: "home.menu.addStorage"),
            image: UIImage(systemName: "plus.circle"),
            children: [
                UIMenu(
                    title: "SMB",
                    image: UIImage(systemName: StorageType.smb.symbolName),
                    children: [
                        UIAction(title: String(localized: "home.menu.smbManual")) { [hooks] _ in
                            hooks.openNewStorageFlow(.smb)
                        },
                        UIAction(title: String(localized: "home.menu.smbDiscovery"), image: UIImage(systemName: "bonjour")) { [hooks] _ in
                            hooks.openNewStorageFlow(.smbDiscovery)
                        }
                    ]
                ),
                UIAction(title: "WebDAV", image: UIImage(systemName: StorageType.webdav.symbolName)) { [hooks] _ in
                    hooks.openNewStorageFlow(.webdav)
                },
                UIAction(title: "S3", image: UIImage(systemName: StorageType.s3.symbolName)) { [hooks] _ in
                    hooks.openNewStorageFlow(.s3)
                },
                UIAction(title: "SFTP", image: UIImage(systemName: StorageType.sftp.symbolName)) { [hooks] _ in
                    hooks.openNewStorageFlow(.sftp)
                },
                UIAction(title: String(localized: "home.menu.externalStorage"), image: UIImage(systemName: StorageType.externalVolume.symbolName)) { [hooks] _ in
                    hooks.openNewStorageFlow(.externalVolume)
                }
            ]
        )
        let manageAction = UIAction(
            title: String(localized: "more.item.manageStorage"),
            image: UIImage(systemName: "list.bullet")
        ) { [hooks] _ in
            hooks.openManageProfiles()
        }

        let manageSection = UIMenu(title: "", options: .displayInline, children: [addStorageMenu, manageAction])
        let connectionSection = UIMenu(title: "", options: .displayInline, children: [connectionMenu])
        return UIMenu(children: [manageSection, connectionSection])
    }

    private func typeSectionTitle(for type: StorageType) -> String {
        switch type {
        case .smb: return "SMB"
        case .webdav: return "WebDAV"
        case .s3: return "S3"
        case .sftp: return "SFTP"
        case .externalVolume: return String(localized: "home.menu.externalStorage")
        }
    }

    func buildCategory(for intent: MonthIntent) -> UIMenu {
        let months = store.selection.months(for: intent)
        var byYear: [Int: [LibraryMonthKey]] = [:]
        for month in months { byYear[month.year, default: []].append(month) }

        let yearMenus = byYear.keys.sorted().map { year -> UIMenu in
            let actions = (byYear[year] ?? []).map { month -> UIAction in
                let row = store.rowLookup[month]
                let monthDate = Calendar.current.date(from: DateComponents(year: 2000, month: month.month))
                let title = monthDate.map(Self.monthFormatter.string(from:)) ?? String(format: "%02d", month.month)
                var parts: [String] = []
                if let lc = row?.local?.assetCount { parts.append(String(format: String(localized: "home.data.localCount"), lc)) }
                if let rc = row?.remote?.assetCount { parts.append(String(format: String(localized: "home.data.remoteCount"), rc)) }
                let subtitle = parts.isEmpty ? nil : parts.joined(separator: " · ")
                return UIAction(title: title, subtitle: subtitle) { [hooks] _ in
                    hooks.scrollToMonth(month)
                }
            }
            let yearDate = Calendar.current.date(from: DateComponents(year: year))
            let yearTitle = yearDate.map(Self.yearFormatter.string(from:)) ?? String(year)
            return UIMenu(title: yearTitle, options: .displayInline, children: actions)
        }
        return UIMenu(children: yearMenus)
    }
}
