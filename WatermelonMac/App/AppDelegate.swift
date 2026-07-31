import AppKit
import MoreKit

private enum MacMembershipConfiguration {
    static let productID = "com.zizicici.watermelon.pro"
    static let membershipKey =
        "com.zizicici.watermelon.membership.lifetime"
}

enum MacToolsMenuCommand: String, Equatable {
    case manageProfiles = "openProfileManagement:"
    case localIndex = "openLocalIndex:"
    case duplicates = "openDuplicates:"
    case repositoryMaintenance = "openRepositoryMaintenance:"
    case logs = "openLogs:"

    init?(action: Selector?) {
        guard let action else { return nil }
        self.init(rawValue: NSStringFromSelector(action))
    }
}

enum MacToolsMenuAvailabilityPolicy {
    static func isEnabled(
        command: MacToolsMenuCommand,
        executionActive: Bool,
        hasVisibleWindow: Bool
    ) -> Bool {
        switch command {
        case .manageProfiles, .logs:
            return true
        case .localIndex, .duplicates, .repositoryMaintenance:
            return !executionActive || hasVisibleWindow
        }
    }
}

@MainActor
final class AppDelegate:
    NSObject,
    NSApplicationDelegate,
    NSMenuItemValidation
{
    private var container: MacDependencyContainer?
    private var mainWindowController: MainWindowController?
    private var executionPowerActivity:
        MacExecutionPowerActivity?
    nonisolated(unsafe) private var executionLifecycleObserver: NSObjectProtocol?
    private var isWaitingForSafeTermination = false
    private var didStart = false

    deinit {
        if let executionLifecycleObserver {
            NotificationCenter.default.removeObserver(
                executionLifecycleObserver
            )
        }
    }

    func applicationDidFinishLaunching(_ notification: Notification) {
        start()
    }

    func applicationWillTerminate(_ notification: Notification) {
        executionPowerActivity?.invalidate()
    }

    func start() {
        guard !didStart else { return }
        didStart = true
        MoreKit.configure(
            productID: MacMembershipConfiguration.productID,
            membershipKey: MacMembershipConfiguration.membershipKey
        )
        let container = MacDependencyContainer()
        let windowController = MainWindowController(container: container)
        self.container = container
        self.mainWindowController = windowController
        executionPowerActivity = MacExecutionPowerActivity(
            appRuntimeFlags: container.appRuntimeFlags
        )
        executionLifecycleObserver = NotificationCenter.default
            .addObserver(
                forName: .ExecutionLifecycleDidChange,
                object: container.appRuntimeFlags,
                queue: .main
            ) { [weak self] _ in
                MainActor.assumeIsolated {
                    self?.finishPendingTerminationIfPossible()
                }
            }
        installMainMenu()
        windowController.showWindow(nil)
        let arguments = ProcessInfo.processInfo.arguments
        let isDemoLaunch = arguments.contains {
            $0.hasPrefix("--demo-")
        }
        if arguments.contains("--demo-onboarding")
            || (!isDemoLaunch
                && !MacOnboardingCompletionGate.hasCompleted) {
            DispatchQueue.main.async {
                windowController.presentOnboarding(
                    isFirstLaunch: !arguments.contains(
                        "--demo-onboarding"
                    )
                )
            }
        }
        #if DEBUG
        if ProcessInfo.processInfo.arguments.contains("--demo-settings")
            || ProcessInfo.processInfo.arguments.contains("--demo-timezone") {
            windowController.openSettings()
        }
        if ProcessInfo.processInfo.arguments.contains("--demo-profiles")
            || ProcessInfo.processInfo.arguments.contains(
                "--demo-local-profile"
            )
            || ProcessInfo.processInfo.arguments.contains(
                "--demo-smb-profile"
            )
            || ProcessInfo.processInfo.arguments.contains(
                "--demo-webdav-profile"
            )
            || ProcessInfo.processInfo.arguments.contains(
                "--demo-s3-profile"
            )
            || ProcessInfo.processInfo.arguments.contains(
                "--demo-sftp-profile"
            )
            || ProcessInfo.processInfo.arguments.contains(
                "--demo-onedrive-profile"
            ) {
            windowController.openProfileManagement()
        }
        if ProcessInfo.processInfo.arguments.contains("--demo-maintenance") {
            windowController.openRepositoryMaintenance()
        }
        if ProcessInfo.processInfo.arguments.contains(
            "--demo-photo-browser"
        ) || ProcessInfo.processInfo.arguments.contains(
            "--demo-photo-metadata"
        ) {
            windowController.openPhotoBrowser(
                request: MacPhotoBrowserRequest(
                    initialMonth: LibraryMonthKey(
                        year: 2026,
                        month: 7
                    ),
                    initialSide: .local,
                    localQuery: .allAssets,
                    title: String(
                        localized: "home.photoLibrary"
                    ),
                    monthGroupingTimeZone: .frozenCurrent()
                )
            )
        }
        if ProcessInfo.processInfo.arguments.contains("--demo-logs") {
            windowController.openExecutionLogHistory()
        }
        if ProcessInfo.processInfo.arguments.contains(
            "--demo-execution-log"
        ) {
            windowController.openCurrentExecutionLog()
        }
        if ProcessInfo.processInfo.arguments.contains(
            "--demo-local-index"
        ) {
            windowController.openLocalIndex()
        }
        if ProcessInfo.processInfo.arguments.contains(
            "--demo-duplicates"
        ) {
            windowController.openDuplicates()
        }
        if ProcessInfo.processInfo.arguments.contains(
            "--demo-album-picker"
        ) {
            DispatchQueue.main.async {
                windowController.showDemoAlbumPicker()
            }
        }
        #endif
        NSApp.activate(ignoringOtherApps: true)
    }

    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        true
    }

    func applicationShouldTerminate(
        _ sender: NSApplication
    ) -> NSApplication.TerminateReply {
        if isWaitingForSafeTermination {
            return .terminateLater
        }
        guard mainWindowController?.hasActiveExecution == true else {
            return .terminateNow
        }
        guard mainWindowController?
            .requestSafeStopForTermination() == true else {
            return .terminateCancel
        }
        guard mainWindowController?.hasActiveExecution == true else {
            return .terminateNow
        }
        isWaitingForSafeTermination = true
        return .terminateLater
    }

    private func finishPendingTerminationIfPossible() {
        guard isWaitingForSafeTermination,
              mainWindowController?.hasActiveExecution == false else {
            return
        }
        isWaitingForSafeTermination = false
        NSApp.reply(toApplicationShouldTerminate: true)
    }

    func applicationShouldHandleReopen(
        _ sender: NSApplication,
        hasVisibleWindows flag: Bool
    ) -> Bool {
        if !flag {
            mainWindowController?.showWindow(nil)
        }
        return true
    }

    func application(
        _ application: NSApplication,
        open urls: [URL]
    ) {
        for url in urls where OneDriveMSALService.handleRedirect(
            url: url,
            sourceApplication: nil
        ) {
            return
        }
    }

    func validateMenuItem(_ menuItem: NSMenuItem) -> Bool {
        guard let command = MacToolsMenuCommand(
            action: menuItem.action
        ) else {
            return true
        }
        return mainWindowController?
            .isToolsMenuCommandEnabled(command) ?? false
    }

    @objc private func showMainWindow(_ sender: Any?) {
        mainWindowController?.showWindow(sender)
    }

    @objc private func showAbout(_ sender: Any?) {
        NSApp.orderFrontStandardAboutPanel(
            options: [
                .applicationName: AppName.localized
            ]
        )
    }

    @objc private func openProfileManagement(_ sender: Any?) {
        mainWindowController?.openProfileManagement()
    }

    @objc private func openSettings(_ sender: Any?) {
        mainWindowController?.openSettings()
    }

    @objc private func openRepositoryMaintenance(_ sender: Any?) {
        mainWindowController?.openRepositoryMaintenance()
    }

    @objc private func openLogs(_ sender: Any?) {
        mainWindowController?.openExecutionLogHistory()
    }

    @objc private func openLocalIndex(_ sender: Any?) {
        mainWindowController?.openLocalIndex()
    }

    @objc private func openDuplicates(_ sender: Any?) {
        mainWindowController?.openDuplicates()
    }

    @objc private func showWelcome(_ sender: Any?) {
        mainWindowController?.showWindow(sender)
        mainWindowController?.presentOnboarding(isFirstLaunch: false)
    }

    @objc private func openHelpDestination(_ sender: NSMenuItem) {
        guard let rawValue = sender.representedObject as? NSNumber else {
            return
        }
        let index = rawValue.intValue
        guard MacHelpDestination.allCases.indices.contains(index) else {
            return
        }
        NSWorkspace.shared.open(
            MacHelpDestination.allCases[index].url
        )
    }

    private func installMainMenu() {
        let mainMenu = NSMenu()
        mainMenu.addItem(makeApplicationMenu())
        mainMenu.addItem(makeFileMenu())
        mainMenu.addItem(makeEditMenu())
        mainMenu.addItem(makeToolsMenu())
        mainMenu.addItem(makeWindowMenu())
        mainMenu.addItem(makeHelpMenu())
        NSApp.mainMenu = mainMenu
    }

    private func makeApplicationMenu() -> NSMenuItem {
        let appName = AppName.localized

        let root = NSMenuItem()
        let menu = NSMenu(title: appName)
        root.submenu = menu

        let about = menu.addItem(
            withTitle: String.localizedStringWithFormat(
                String(
                    localized: "mac.menu.about",
                    defaultValue: "About %@"
                ),
                appName
            ),
            action: #selector(showAbout(_:)),
            keyEquivalent: ""
        )
        about.target = self
        menu.addItem(.separator())
        let settings = menu.addItem(
            withTitle: String(
                localized: "controller.more.title",
                defaultValue: "Settings"
            ) + "…",
            action: #selector(openSettings(_:)),
            keyEquivalent: ","
        )
        settings.target = self
        menu.addItem(.separator())

        let services = NSMenuItem(
            title: String(
                localized: "mac.menu.services",
                defaultValue: "Services"
            ),
            action: nil,
            keyEquivalent: ""
        )
        let servicesMenu = NSMenu()
        services.submenu = servicesMenu
        NSApp.servicesMenu = servicesMenu
        menu.addItem(services)
        menu.addItem(.separator())

        menu.addItem(
            withTitle: String.localizedStringWithFormat(
                String(
                    localized: "mac.menu.hide",
                    defaultValue: "Hide %@"
                ),
                appName
            ),
            action: #selector(NSApplication.hide(_:)),
            keyEquivalent: "h"
        )
        let hideOthers = menu.addItem(
            withTitle: String(
                localized: "mac.menu.hideOthers",
                defaultValue: "Hide Others"
            ),
            action: #selector(NSApplication.hideOtherApplications(_:)),
            keyEquivalent: "h"
        )
        hideOthers.keyEquivalentModifierMask = [.command, .option]
        menu.addItem(
            withTitle: String(
                localized: "log.showAll",
                defaultValue: "Show All"
            ),
            action: #selector(NSApplication.unhideAllApplications(_:)),
            keyEquivalent: ""
        )
        menu.addItem(.separator())
        menu.addItem(
            withTitle: String.localizedStringWithFormat(
                String(
                    localized: "mac.menu.quit",
                    defaultValue: "Quit %@"
                ),
                appName
            ),
            action: #selector(NSApplication.terminate(_:)),
            keyEquivalent: "q"
        )
        return root
    }

    private func makeFileMenu() -> NSMenuItem {
        let root = NSMenuItem()
        let menu = NSMenu(
            title: String(
                localized: "mediaMetadata.section.file",
                defaultValue: "File"
            )
        )
        root.submenu = menu
        menu.addItem(
            withTitle: String(
                localized: "mac.menu.showMainWindow",
                defaultValue: "Show Watermelon Backup"
            ),
            action: #selector(showMainWindow(_:)),
            keyEquivalent: "1"
        ).target = self
        menu.addItem(.separator())
        menu.addItem(
            withTitle: String(localized: "common.close", defaultValue: "Close"),
            action: #selector(NSWindow.performClose(_:)),
            keyEquivalent: "w"
        )
        return root
    }

    private func makeEditMenu() -> NSMenuItem {
        let root = NSMenuItem()
        let menu = NSMenu(
            title: String(localized: "common.edit")
        )
        root.submenu = menu

        addResponderMenuItem(
            to: menu,
            title: String(localized: "common.undo"),
            action: Selector(("undo:")),
            keyEquivalent: "z"
        )
        addResponderMenuItem(
            to: menu,
            title: String(localized: "common.redo"),
            action: Selector(("redo:")),
            keyEquivalent: "z",
            modifiers: [.command, .shift]
        )
        menu.addItem(.separator())
        addResponderMenuItem(
            to: menu,
            title: String(localized: "common.cut"),
            action: #selector(NSText.cut(_:)),
            keyEquivalent: "x"
        )
        addResponderMenuItem(
            to: menu,
            title: String(localized: "common.copy"),
            action: #selector(NSText.copy(_:)),
            keyEquivalent: "c"
        )
        addResponderMenuItem(
            to: menu,
            title: String(localized: "common.paste"),
            action: #selector(NSText.paste(_:)),
            keyEquivalent: "v"
        )
        addResponderMenuItem(
            to: menu,
            title: String(localized: "common.delete"),
            action: #selector(NSText.delete(_:)),
            keyEquivalent: ""
        )
        menu.addItem(.separator())
        addResponderMenuItem(
            to: menu,
            title: String(localized: "common.selectAll"),
            action: #selector(NSResponder.selectAll(_:)),
            keyEquivalent: "a"
        )
        return root
    }

    private func addResponderMenuItem(
        to menu: NSMenu,
        title: String,
        action: Selector,
        keyEquivalent: String,
        modifiers: NSEvent.ModifierFlags = [.command]
    ) {
        let item = menu.addItem(
            withTitle: title,
            action: action,
            keyEquivalent: keyEquivalent
        )
        item.keyEquivalentModifierMask = modifiers
    }

    private func makeToolsMenu() -> NSMenuItem {
        let root = NSMenuItem()
        let menu = NSMenu(
            title: String(localized: "mac.menu.tools", defaultValue: "Tools")
        )
        root.submenu = menu

        let profiles = menu.addItem(
            withTitle: String(
                localized: "more.item.manageStorage",
                defaultValue: "Manage Nodes"
            ) + "…",
            action: #selector(openProfileManagement(_:)),
            keyEquivalent: ""
        )
        profiles.target = self
        menu.addItem(.separator())

        let localIndex = menu.addItem(
            withTitle: String(
                localized: "home.localIndex.title",
                defaultValue: "Local Photo Index…"
            ),
            action: #selector(openLocalIndex(_:)),
            keyEquivalent: ""
        )
        localIndex.target = self

        let duplicates = menu.addItem(
            withTitle: String(
                localized: "home.duplicates.title",
                defaultValue: "Duplicate Photos…"
            ),
            action: #selector(openDuplicates(_:)),
            keyEquivalent: ""
        )
        duplicates.target = self
        menu.addItem(.separator())

        let maintenance = menu.addItem(
            withTitle: String(
                localized: "mac.menu.repositoryMaintenance",
                defaultValue: "Repository Maintenance…"
            ),
            action: #selector(openRepositoryMaintenance(_:)),
            keyEquivalent: ""
        )
        maintenance.target = self
        menu.addItem(.separator())

        let logs = menu.addItem(
            withTitle: String(
                localized: "more.item.diagnosticLogs",
                defaultValue: "Diagnostic Logs"
            ) + "…",
            action: #selector(openLogs(_:)),
            keyEquivalent: "l"
        )
        logs.keyEquivalentModifierMask = [.command, .shift]
        logs.target = self
        return root
    }

    private func makeWindowMenu() -> NSMenuItem {
        let root = NSMenuItem()
        let menu = NSMenu(
            title: String(localized: "mac.menu.window", defaultValue: "Window")
        )
        root.submenu = menu
        menu.addItem(
            withTitle: String(
                localized: "mac.menu.minimize",
                defaultValue: "Minimize"
            ),
            action: #selector(NSWindow.performMiniaturize(_:)),
            keyEquivalent: "m"
        )
        menu.addItem(
            withTitle: String(localized: "mac.menu.zoom", defaultValue: "Zoom"),
            action: #selector(NSWindow.performZoom(_:)),
            keyEquivalent: ""
        )
        menu.addItem(.separator())
        menu.addItem(
            withTitle: String(
                localized: "mac.menu.bringAllToFront",
                defaultValue: "Bring All to Front"
            ),
            action: #selector(NSApplication.arrangeInFront(_:)),
            keyEquivalent: ""
        )
        NSApp.windowsMenu = menu
        return root
    }

    func makeHelpMenu() -> NSMenuItem {
        let root = NSMenuItem()
        let menu = NSMenu(
            title: String(localized: "mac.menu.help", defaultValue: "Help")
        )
        root.submenu = menu
        let welcome = menu.addItem(
            withTitle: String(
                localized: "mac.menu.welcome",
                defaultValue: "Welcome to Watermelon Backup"
            ),
            action: #selector(showWelcome(_:)),
            keyEquivalent: ""
        )
        welcome.target = self
        menu.addItem(.separator())
        let destinations = MacHelpDestination.allCases
        for (index, destination) in destinations.enumerated() {
            let item = menu.addItem(
                withTitle: destination.title,
                action: #selector(openHelpDestination(_:)),
                keyEquivalent: ""
            )
            item.target = self
            item.representedObject = NSNumber(value: index)
        }
        NSApp.helpMenu = menu
        return root
    }
}
