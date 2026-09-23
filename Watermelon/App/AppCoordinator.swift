import UIKit

@MainActor
final class AppCoordinator {
    private let window: UIWindow
    private let dependencies: DependencyContainer
    private weak var homeViewController: HomeViewController?
    private var pendingDeepLink: AppDeepLink?
    private var isPresentingOnboarding = false

    init(window: UIWindow, dependencies: DependencyContainer = DependencyContainer()) {
        self.window = window
        self.dependencies = dependencies
    }

    func start(initialDeepLink: AppDeepLink? = nil) {
        let home = HomeViewController(dependencies: dependencies)
        homeViewController = home
        pendingDeepLink = initialDeepLink
        if case .browserLink(let url) = initialDeepLink {
            _ = home.prepareForIncomingBrowserLink(url)
        }
        window.rootViewController = home
        window.makeKeyAndVisible()

        if !OnboardingViewController.CompletionGate.hasCompleted {
            presentOnboarding(over: home)
        } else {
            DispatchQueue.main.async { [weak self] in
                self?.openPendingDeepLink()
            }
        }
    }

    func handleUniversalLink(_ userActivity: NSUserActivity) {
        guard userActivity.activityType == NSUserActivityTypeBrowsingWeb,
              let url = userActivity.webpageURL else { return }
        handleURL(url)
    }

    @discardableResult
    func handleURL(_ url: URL) -> Bool {
        guard let deepLink = AppDeepLink(url: url) else { return false }
        if case .browserLink(let url) = deepLink {
            _ = homeViewController?.prepareForIncomingBrowserLink(url)
        }
        pendingDeepLink = deepLink
        guard !isPresentingOnboarding else { return true }
        DispatchQueue.main.async { [weak self] in
            self?.openPendingDeepLink()
        }
        return true
    }

    private func openPendingDeepLink() {
        guard !isPresentingOnboarding, let deepLink = pendingDeepLink else { return }
        pendingDeepLink = nil
        switch deepLink {
        case .shortcuts:
            homeViewController?.presentShortcutsSettings()
        case .browserLink(let url):
            homeViewController?.handleBrowserLinkURL(url)
        }
    }

    private func presentOnboarding(over presenter: UIViewController) {
        isPresentingOnboarding = true
        let onboarding = OnboardingViewController()
        let nav = UINavigationController(rootViewController: onboarding)
        nav.modalPresentationStyle = .pageSheet
        nav.isModalInPresentation = true
        onboarding.onCompleted = { [weak self, weak nav] in
            OnboardingViewController.CompletionGate.markCompleted()
            nav?.dismiss(animated: ConsideringUser.animated) {
                guard let self else { return }
                self.isPresentingOnboarding = false
                self.openPendingDeepLink()
            }
        }
        DispatchQueue.main.async {
            presenter.present(nav, animated: ConsideringUser.animated)
        }
    }
}
