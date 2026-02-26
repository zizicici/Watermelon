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

        showHome()
    }

    private func showHome() {
        rootNavigationController.setNavigationBarHidden(false, animated: false)
        rootNavigationController.setToolbarHidden(false, animated: false)
        let homeVC = HomeViewController(dependencies: dependencies)
        rootNavigationController.setViewControllers([homeVC], animated: false)
    }
}
