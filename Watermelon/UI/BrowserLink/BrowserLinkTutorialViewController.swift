import UIKit

@MainActor
final class BrowserLinkTutorialViewController: ProgressiveTutorialViewController {
    enum CompletionGate {
        private static let key = "browser-link-tutorial.completed.v1"

        static var hasCompleted: Bool {
            UserDefaults.standard.bool(forKey: key)
        }

        static func markCompleted() {
            UserDefaults.standard.set(true, forKey: key)
        }
    }

    init(allowsDismissal: Bool) {
        super.init(
            tutorialTitle: String(localized: "link.tutorial.title"),
            items: [
                Item(
                    title: String(localized: "link.tutorial.computer.title"),
                    subtitle: String(localized: "link.tutorial.computer.subtitle"),
                    symbolName: "desktopcomputer"
                ),
                Item(
                    title: String(localized: "link.tutorial.folder.title"),
                    subtitle: String(localized: "link.tutorial.folder.subtitle"),
                    symbolName: "folder.badge.plus"
                ),
                Item(
                    title: String(localized: "link.tutorial.network.title"),
                    subtitle: String(localized: "link.connection.sameNetworkHint"),
                    symbolName: "point.3.connected.trianglepath.dotted"
                ),
                Item(
                    title: String(localized: "link.scanner.title"),
                    subtitle: String(localized: "link.scanner.instruction"),
                    symbolName: "qrcode.viewfinder"
                ),
            ],
            completionButtonTitle: String(localized: "link.tutorial.button.scan"),
            allowsDismissal: allowsDismissal
        )
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }
}
