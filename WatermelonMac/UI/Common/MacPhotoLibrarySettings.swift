import AppKit

@MainActor
enum MacPhotoLibrarySettings {
    static func open() {
        guard let url = URL(
            string: "x-apple.systempreferences:com.apple.preference.security?Privacy_Photos"
        ) else { return }
        NSWorkspace.shared.open(url)
    }
}
