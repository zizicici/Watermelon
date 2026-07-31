import Foundation

enum MacHelpDestination: CaseIterable {
    case contactSupport
    case privacyPolicy

    var title: String {
        switch self {
        case .contactSupport:
            String(
                localized: "mac.menu.contactSupport",
                defaultValue: "Contact Support"
            )
        case .privacyPolicy:
            String(
                localized: "mac.menu.privacyPolicy",
                defaultValue: "Privacy Policy"
            )
        }
    }

    var url: URL {
        switch self {
        case .contactSupport:
            URL(string: "mailto:watermelon@zi.ci")!
        case .privacyPolicy:
            URL(
                string: "https://watermelonbackup.com/privacy.html"
            )!
        }
    }
}
