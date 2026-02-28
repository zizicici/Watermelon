import Foundation

final class SecurityScopedBookmarkStore {
    func makeBookmarkData(for directoryURL: URL) throws -> Data {
        try directoryURL.bookmarkData(
            options: [],
            includingResourceValuesForKeys: nil,
            relativeTo: nil
        )
    }

    func resolveBookmarkData(_ bookmarkData: Data) throws -> URL {
        var isStale = false
        let url = try URL(
            resolvingBookmarkData: bookmarkData,
            options: [],
            relativeTo: nil,
            bookmarkDataIsStale: &isStale
        )
        return url
    }
}
