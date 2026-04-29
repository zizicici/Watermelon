import Foundation

enum AppName {
    static var localized: String {
        if let name = Bundle.main.localizedInfoDictionary?["CFBundleDisplayName"] as? String, !name.isEmpty {
            return name
        }
        if let name = Bundle.main.infoDictionary?["CFBundleDisplayName"] as? String, !name.isEmpty {
            return name
        }
        return "Watermelon"
    }
}
