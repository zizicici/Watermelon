import Foundation

final class SecurityScopedBookmarkStore {
    struct ResolvedBookmark {
        let url: URL
        let refreshedBookmarkData: Data?
    }

    func makeBookmarkData(for directoryURL: URL) throws -> Data {
        try directoryURL.bookmarkData(
            options: [],
            includingResourceValuesForKeys: nil,
            relativeTo: nil
        )
    }

    func resolveBookmarkData(_ bookmarkData: Data) throws -> ResolvedBookmark {
        var isStale = false
        let url = try URL(
            resolvingBookmarkData: bookmarkData,
            options: [.withoutUI],
            relativeTo: nil,
            bookmarkDataIsStale: &isStale
        )
        if isStale {
            let refreshed = try makeBookmarkData(for: url)
            return ResolvedBookmark(url: url, refreshedBookmarkData: refreshed)
        }
        return ResolvedBookmark(url: url, refreshedBookmarkData: nil)
    }
}
