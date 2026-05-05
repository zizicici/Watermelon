import UIKit

@MainActor
final class AppCoordinator {
    private let window: UIWindow
    private let dependencies: DependencyContainer

    init(window: UIWindow, dependencies: DependencyContainer = DependencyContainer()) {
        self.window = window
        self.dependencies = dependencies
    }

    func start() {
        let home = HomeViewController(dependencies: dependencies)
        window.rootViewController = home
        window.makeKeyAndVisible()

        if !OnboardingViewController.CompletionGate.hasCompleted {
            presentOnboarding(over: home)
        }
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
