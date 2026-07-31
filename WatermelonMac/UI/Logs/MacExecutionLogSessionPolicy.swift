import Foundation

enum MacExecutionLogSessionPolicy {
    static func preferredIndex(
        sessionURLs: [URL?],
        preferredURL: URL?
    ) -> Int? {
        guard !sessionURLs.isEmpty else { return nil }
        guard let preferredURL else { return 0 }
        return sessionURLs.firstIndex {
            $0?.standardizedFileURL
                == preferredURL.standardizedFileURL
        } ?? 0
    }

    static func canDelete(
        sessionURL: URL?,
        activeSessionURL: URL?
    ) -> Bool {
        guard let sessionURL else { return false }
        return sessionURL.standardizedFileURL
            != activeSessionURL?.standardizedFileURL
    }
}
