import UIKit

@MainActor
final class MediaDropTutorialViewController: ProgressiveTutorialViewController {
    enum CompletionGate {
        private static let key = "media-drop-tutorial.completed.v1"

        static var hasCompleted: Bool {
            UserDefaults.standard.bool(forKey: key)
        }

        static func markCompleted() {
            UserDefaults.standard.set(true, forKey: key)
        }
    }

    init(allowsDismissal: Bool) {
        super.init(
            tutorialTitle: String(localized: "transfer.settings.title"),
            items: [
                Item(
                    title: String(localized: "transfer.tutorial.noDeduplication.title"),
                    subtitle: String(localized: "transfer.tutorial.noDeduplication.subtitle"),
                    symbolName: "exclamationmark.triangle"
                ),
                Item(
                    title: String(localized: "transfer.tutorial.sources.title"),
                    subtitle: String(localized: "transfer.tutorial.sources.subtitle"),
                    symbolName: "square.grid.2x2"
                ),
                Item(
                    title: String(localized: "transfer.tutorial.destination.title"),
                    subtitle: String(localized: "transfer.tutorial.destination.subtitle"),
                    symbolName: "paperplane"
                ),
                Item(
                    title: String(localized: "transfer.tutorial.export.title"),
                    subtitle: String(localized: "transfer.tutorial.export.subtitle"),
                    symbolName: "slider.horizontal.3"
                ),
            ],
            completionButtonTitle: String(localized: "transfer.tutorial.button.start"),
            allowsDismissal: allowsDismissal
        )
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }
}
