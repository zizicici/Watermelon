import Foundation

enum AppDeepLink: Equatable {
    case shortcuts
    case browserLink(URL)

    init?(url: URL) {
        if url.scheme?.lowercased() == "watermelonbackup",
           url.host?.lowercased() == "shortcuts",
           url.path.isEmpty || url.path == "/",
           url.user == nil,
           url.password == nil,
           url.port == nil {
            self = .shortcuts
        } else if BrowserLinkPairing.isCandidateURL(url) {
            self = .browserLink(url)
        } else {
            return nil
        }
    }
}
