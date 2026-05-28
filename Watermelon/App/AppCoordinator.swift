import UIKit

@MainActor
final class AppCoordinator {
    private let window: UIWindow
    private let dependencies: DependencyContainer

    init(window: UIWindow, dependencies: DependencyContainer = DependencyContainer()) {
        self.window = window
        self.dependencies = dependencies
    }

    private weak var homeViewController: HomeViewController?

    func start() {
        let home = HomeViewController(dependencies: dependencies)
        homeViewController = home
        window.rootViewController = home
        window.makeKeyAndVisible()

        if !OnboardingViewController.CompletionGate.hasCompleted {
            presentOnboarding(over: home)
        }
    }

    func handleSceneDisconnect() {
        // Forward to Home so the in-flight execution / verify can be cancelled and the shared
        // AppRuntimeFlags lease released before the scene's container is reclaimed.
        homeViewController?.handleSceneDisconnect()
    }

    private func presentOnboarding(over presenter: UIViewController) {
        let onboarding = OnboardingViewController()
        let nav = UINavigationController(rootViewController: onboarding)
        nav.modalPresentationStyle = .pageSheet
        nav.isModalInPresentation = true
        onboarding.onCompleted = { [weak nav] in
            OnboardingViewController.CompletionGate.markCompleted()
            nav?.dismiss(animated: ConsideringUser.animated)
        }
        DispatchQueue.main.async {
            presenter.present(nav, animated: ConsideringUser.animated)
        }
    }
}
