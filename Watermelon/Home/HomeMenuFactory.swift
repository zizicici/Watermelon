import UIKit

enum NewStorageDestination {
    case smb
    case smbDiscovery
    case webdav
    case externalVolume
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

        return UIMenu(children: [allPhotosAction, specificAlbumsAction])
    }

    func buildDestination() -> UIMenu {
        let disconnected = !store.connectionState.isConnected
        let busyAttributes: UIMenuElement.Attributes =
            (store.executionState != nil || store.isRemoteMaintenanceActive) ? .disabled : []

        let disconnectAction = UIAction(
            title: String(localized: "home.menu.notConnected"),
            attributes: busyAttributes,
            state: disconnected ? .on : .off
        ) { [store] _ in
            store.disconnect()
        }

        var profileActions: [UIAction] = []
        for profile in store.savedProfiles {
            let isActive = store.connectionState.activeProfile?.id == profile.id
            let action = UIAction(
                title: profile.name,
                subtitle: profile.storageProfile.displaySubtitle,
                attributes: busyAttributes,
                state: isActive ? .on : .off
            ) { [store] _ in
                store.connectProfile(profile)
            }
            profileActions.append(action)
        }

        let profileSection = UIMenu(title: "", options: .displayInline, children: profileActions)
        let addStorageMenu = UIMenu(
            title: String(localized: "home.menu.addStorage"),
            image: UIImage(systemName: "plus.circle"),
            children: [
                UIMenu(
                    title: "SMB",
                    image: UIImage(systemName: "server.rack"),
                    children: [
                        UIAction(title: String(localized: "home.menu.smbManual")) { [hooks] _ in
                            hooks.openNewStorageFlow(.smb)
                        },
                        UIAction(title: String(localized: "home.menu.smbDiscovery"), image: UIImage(systemName: "bonjour")) { [hooks] _ in
                            hooks.openNewStorageFlow(.smbDiscovery)
                        }
                    ]
                ),
                UIAction(title: "WebDAV", image: UIImage(systemName: "network")) { [hooks] _ in
                    hooks.openNewStorageFlow(.webdav)
                },
                UIAction(title: String(localized: "home.menu.externalStorage"), image: UIImage(systemName: "externaldrive")) { [hooks] _ in
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

        var topItems: [UIMenuElement] = [addStorageMenu, manageAction]
        // `activeProfile` is also non-nil mid-connect; the detail page reads
        // `appSession.activeProfile`, which is only set after a successful reload.
        if store.connectionState.isConnected, let active = store.connectionState.activeProfile {
            let currentProfileAction = UIAction(
                title: String(localized: "home.menu.currentProfileSettings"),
                subtitle: active.name,
                image: UIImage(systemName: "slider.horizontal.3"),
                attributes: busyAttributes
            ) { [hooks] _ in
                hooks.openCurrentProfileSettings()
            }
            topItems.append(currentProfileAction)
        }
        let disconnectSection = UIMenu(title: "", options: .displayInline, children: [disconnectAction])
        return UIMenu(children: topItems + [profileSection, disconnectSection])
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
