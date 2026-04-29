import Foundation

final class SecurityScopedBookmarkStore {
    struct ResolvedBookmark {
        let url: URL
        let refreshedBookmarkData: Data?
    }

    func makeBookmarkData(for directoryURL: URL) throws -> Data {
        try directoryURL.bookmarkData(
            options: Self.creationOptions,
            includingResourceValuesForKeys: nil,
            relativeTo: nil
        )
    }

    func resolveBookmarkData(_ bookmarkData: Data) throws -> ResolvedBookmark {
        var isStale = false
        let url = try URL(
            resolvingBookmarkData: bookmarkData,
            options: Self.resolutionOptions,
            relativeTo: nil,
            bookmarkDataIsStale: &isStale
        )
        if isStale {
            let refreshed = try makeBookmarkData(for: url)
            return ResolvedBookmark(url: url, refreshedBookmarkData: refreshed)
        }
        return ResolvedBookmark(url: url, refreshedBookmarkData: nil)
    }

    #if os(macOS)
    private static let creationOptions: URL.BookmarkCreationOptions = [.withSecurityScope]
    private static let resolutionOptions: URL.BookmarkResolutionOptions = [.withSecurityScope, .withoutUI]
    #else
    private static let creationOptions: URL.BookmarkCreationOptions = []
    private static let resolutionOptions: URL.BookmarkResolutionOptions = [.withoutUI]
    #endif
}
