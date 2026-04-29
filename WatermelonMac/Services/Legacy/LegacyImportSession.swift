import Foundation

/// Owns the security-scoped access for the legacy source folder during a commit run.
/// Destination bookmarks are managed inside the storage client (LocalVolumeClient).
final class LegacyImportSession {
    private let sourceURL: URL
    private var didStartSourceAccess = false

    init(sourceURL: URL) {
        self.sourceURL = sourceURL
    }

    func begin() {
        if !didStartSourceAccess {
            didStartSourceAccess = sourceURL.startAccessingSecurityScopedResource()
        }
    }

    func end() {
        if didStartSourceAccess {
            sourceURL.stopAccessingSecurityScopedResource()
            didStartSourceAccess = false
        }
    }

    deinit {
        end()
    }
}
