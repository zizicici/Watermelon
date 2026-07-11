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

    func currentLocation(for bookmarkData: Data) throws -> ExternalVolumeCurrentLocation {
        let resolved = try resolveBookmarkData(bookmarkData)
        let scoped = resolved.url.startAccessingSecurityScopedResource()
        defer {
            if scoped {
                resolved.url.stopAccessingSecurityScopedResource()
            }
        }
        return ExternalVolumeCurrentLocation(
            ephemeralIdentity: ephemeralLocationIdentity(for: resolved.url),
            standardizedURL: resolved.url.standardizedFileURL
        )
    }

    func ephemeralLocationIdentity(for url: URL) -> Data? {
        guard let values = try? url.resourceValues(forKeys: [.volumeIdentifierKey, .fileResourceIdentifierKey]),
              let volumeIdentifier = values.volumeIdentifier,
              let fileResourceIdentifier = values.fileResourceIdentifier else { return nil }
        return Self.ephemeralLocationIdentity(
            volumeIdentifier: volumeIdentifier,
            fileResourceIdentifier: fileResourceIdentifier
        )
    }

    static func ephemeralLocationIdentity(
        volumeIdentifier: Any,
        fileResourceIdentifier: Any
    ) -> Data? {
        guard let volume = stableResourceComponent(volumeIdentifier),
              let file = stableResourceComponent(fileResourceIdentifier) else { return nil }
        var payload = Data()
        appendLengthPrefixed(volume, to: &payload)
        appendLengthPrefixed(file, to: &payload)
        return payload
    }

    private static func stableResourceComponent(_ value: Any) -> Data? {
        if let value = value as? Data {
            return Data("data\u{0}".utf8) + value
        }
        if let value = value as? String {
            return Data("string\u{0}\(value)".utf8)
        }
        if let value = value as? UUID {
            return Data("uuid\u{0}\(value.uuidString.lowercased())".utf8)
        }
        if let value = value as? NSNumber {
            let type = String(cString: value.objCType)
            return Data("number\u{0}\(type)\u{0}\(value.stringValue)".utf8)
        }
        return nil
    }

    private static func appendLengthPrefixed(_ component: Data, to payload: inout Data) {
        var length = UInt64(component.count).bigEndian
        withUnsafeBytes(of: &length) { payload.append(contentsOf: $0) }
        payload.append(component)
    }

    #if os(macOS)
    private static let creationOptions: URL.BookmarkCreationOptions = [.withSecurityScope]
    private static let resolutionOptions: URL.BookmarkResolutionOptions = [.withSecurityScope, .withoutUI]
    #else
    private static let creationOptions: URL.BookmarkCreationOptions = []
    private static let resolutionOptions: URL.BookmarkResolutionOptions = [.withoutUI]
    #endif
}
