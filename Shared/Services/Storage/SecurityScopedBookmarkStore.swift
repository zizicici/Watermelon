import Foundation

final class SecurityScopedBookmarkStore {
    struct EphemeralLocationIdentities: Equatable, Sendable {
        let fullIdentity: Data?
        let volumePathIdentity: Data?
    }

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
        let identities = ephemeralLocationIdentities(for: resolved.url)
        return ExternalVolumeCurrentLocation(
            fullIdentity: identities.fullIdentity,
            volumePathIdentity: identities.volumePathIdentity,
            standardizedURL: resolved.url.standardizedFileURL
        )
    }

    func ephemeralLocationIdentities(for url: URL) -> EphemeralLocationIdentities {
        guard let values = try? url.resourceValues(forKeys: [.volumeIdentifierKey, .fileResourceIdentifierKey]) else {
            return EphemeralLocationIdentities(fullIdentity: nil, volumePathIdentity: nil)
        }
        return Self.ephemeralLocationIdentities(
            volumeIdentifier: values.volumeIdentifier,
            fileResourceIdentifier: values.fileResourceIdentifier,
            standardizedURL: url.standardizedFileURL
        )
    }

    nonisolated static func ephemeralLocationIdentities(
        volumeIdentifier: Any?,
        fileResourceIdentifier: Any?,
        standardizedURL: URL
    ) -> EphemeralLocationIdentities {
        guard let volumeIdentifier,
              let volume = stableResourceComponent(volumeIdentifier) else {
            return EphemeralLocationIdentities(fullIdentity: nil, volumePathIdentity: nil)
        }
        let path = stableResourceComponent(standardizedURL.standardizedFileURL.path)
        let volumePathIdentity = path.map {
            makeIdentity(namespace: "volume-path", components: [volume, $0])
        }
        let fullIdentity = fileResourceIdentifier
            .flatMap(stableResourceComponent)
            .map { makeIdentity(namespace: "full", components: [volume, $0]) }
        return EphemeralLocationIdentities(
            fullIdentity: fullIdentity,
            volumePathIdentity: volumePathIdentity
        )
    }

    nonisolated private static func makeIdentity(namespace: String, components: [Data]) -> Data {
        var payload = Data("external-location-v1\u{0}\(namespace)\u{0}".utf8)
        for component in components {
            appendLengthPrefixed(component, to: &payload)
        }
        return payload
    }

    nonisolated private static func stableResourceComponent(_ value: Any) -> Data? {
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

    nonisolated private static func appendLengthPrefixed(_ component: Data, to payload: inout Data) {
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
