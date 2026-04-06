import UIKit

final class AppCoordinator {
    private let window: UIWindow
    private let dependencies: DependencyContainer

    init(window: UIWindow, dependencies: DependencyContainer = DependencyContainer()) {
        self.window = window
        self.dependencies = dependencies
    }

    func start() {
        window.rootViewController = NewHomeViewController(dependencies: dependencies)
        window.makeKeyAndVisible()
    }
}
