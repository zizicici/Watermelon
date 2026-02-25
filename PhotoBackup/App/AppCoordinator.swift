import UIKit

final class AppCoordinator {
    private let window: UIWindow
    private let dependencies: DependencyContainer
    private let rootNavigationController = UINavigationController()

    init(window: UIWindow, dependencies: DependencyContainer = DependencyContainer()) {
        self.window = window
        self.dependencies = dependencies
    }

    func start() {
        window.rootViewController = rootNavigationController
        window.makeKeyAndVisible()

        showLogin(allowAutoLogin: true)
    }

    private func showLogin(allowAutoLogin: Bool) {
        rootNavigationController.setNavigationBarHidden(false, animated: false)
        rootNavigationController.setToolbarHidden(true, animated: false)
        dependencies.appSession.clear()

        let loginVC = ServerSelectionViewController(
            dependencies: dependencies,
            autoLoginOnAppear: allowAutoLogin
        ) { [weak self] _, _ in
            self?.showMainAlbum()
        }
        rootNavigationController.setViewControllers([loginVC], animated: false)
    }

    private func showMainAlbum() {
        rootNavigationController.setNavigationBarHidden(false, animated: false)

        let albumVC = AlbumViewController(dependencies: dependencies, onOpenSettings: { [weak self] in
            self?.showSettings()
        })
        albumVC.title = "Album"
        rootNavigationController.setViewControllers([albumVC], animated: true)
    }

    private func showSettings() {
        let settingsVC = SettingsViewController(dependencies: dependencies, onSwitchServer: { [weak self] in
            self?.showLogin(allowAutoLogin: false)
        })
        settingsVC.title = "Settings"
        rootNavigationController.pushViewController(settingsVC, animated: true)
    }
}
