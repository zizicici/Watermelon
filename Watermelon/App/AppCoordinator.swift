import UIKit

@MainActor
final class AppCoordinator {
    private let window: UIWindow
    private let dependencies: DependencyContainer
    private weak var homeViewController: HomeViewController?
    private var pendingBrowserLinkURL: URL?

    init(window: UIWindow, dependencies: DependencyContainer = DependencyContainer()) {
        self.window = window
        self.dependencies = dependencies
    }

    func start(initialUniversalLinkURL: URL? = nil) {
        let home = HomeViewController(dependencies: dependencies)
        homeViewController = home
        if let initialUniversalLinkURL {
            _ = home.prepareForIncomingBrowserLink(initialUniversalLinkURL)
            pendingBrowserLinkURL = initialUniversalLinkURL
        }
        window.rootViewController = home
        window.makeKeyAndVisible()

        if !OnboardingViewController.CompletionGate.hasCompleted {
            presentOnboarding(over: home)
        } else if let url = pendingBrowserLinkURL {
            pendingBrowserLinkURL = nil
            DispatchQueue.main.async { [weak home] in
                home?.handleBrowserLinkURL(url)
            }
        }
    }

    func handleUniversalLink(_ userActivity: NSUserActivity) {
        guard userActivity.activityType == NSUserActivityTypeBrowsingWeb,
              let url = userActivity.webpageURL,
              BrowserLinkPairing.isCandidateURL(url) else { return }
        _ = homeViewController?.prepareForIncomingBrowserLink(url)
        if !OnboardingViewController.CompletionGate.hasCompleted {
            pendingBrowserLinkURL = url
            return
        }
        DispatchQueue.main.async { [weak self] in
            self?.homeViewController?.handleBrowserLinkURL(url)
        }
    }

    func sceneDidEnterBackground() {
        homeViewController?.endBrowserLinkForBackground()
    }

    private func presentOnboarding(over presenter: UIViewController) {
        let onboarding = OnboardingViewController()
        let nav = UINavigationController(rootViewController: onboarding)
        nav.modalPresentationStyle = .pageSheet
        nav.isModalInPresentation = true
        onboarding.onCompleted = { [weak self, weak nav] in
            OnboardingViewController.CompletionGate.markCompleted()
            nav?.dismiss(animated: ConsideringUser.animated) {
                guard let self, let url = self.pendingBrowserLinkURL else { return }
                self.pendingBrowserLinkURL = nil
                self.homeViewController?.handleBrowserLinkURL(url)
            }
        }
        DispatchQueue.main.async {
            presenter.present(nav, animated: ConsideringUser.animated)
        }
    }
}
