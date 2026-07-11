import Foundation

// `nonisolated` keeps these helpers isolation-neutral — WatermelonMac uses default-MainActor isolation, otherwise actor callers cross actors.
nonisolated enum RemotePathBuilder {
    static func normalizeRelativePath(_ value: String) -> String {
        value.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
    }

    static func absolutePath(basePath: String, remoteRelativePath: String) -> String {
        let normalizedBase = normalizePath(basePath)
        let normalizedPath = normalizePath(remoteRelativePath)
        if normalizedPath == normalizedBase || normalizedPath.hasPrefix(normalizedBase + "/") {
            return normalizedPath
        }
        let relative = normalizeRelativePath(remoteRelativePath)
        if relative.isEmpty {
            return normalizedBase
        }
        return normalizePath("\(normalizedBase)/\(relative)")
    }

    static func normalizePath(_ value: String) -> String {
        let trimmed = value.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
        return "/" + trimmed
    }

    static func sanitizeFilename(_ filename: String) -> String {
        let invalid = CharacterSet(charactersIn: "\\/:*?\"<>|")
        return filename.components(separatedBy: invalid).joined(separator: "_")
    }
}

nonisolated enum WebDAVPathCanonicalizer {
    static func canonicalRawPath(_ rawPath: String) throws -> String {
        let components = RemotePathBuilder.normalizePath(rawPath)
            .split(separator: "/", omittingEmptySubsequences: true)
            .map(String.init)
        if components.isEmpty {
            return "/"
        }
        for component in components {
            guard component != ".", component != ".." else {
                throw RemoteStorageClientError.invalidConfiguration
            }
        }
        return "/" + components.joined(separator: "/")
    }

    static func percentEncodedRequestPath(fromRawPath rawPath: String) throws -> String {
        let canonical = try canonicalRawPath(rawPath)
        guard canonical != "/" else { return "/" }
        return "/" + canonical
            .dropFirst()
            .split(separator: "/", omittingEmptySubsequences: false)
            .map { encodeRawPathComponent(String($0)) }
            .joined(separator: "/")
    }

    static func rawPath(fromPercentEncodedHrefPath encodedPath: String) throws -> String {
        let components = RemotePathBuilder.normalizePath(encodedPath)
            .split(separator: "/", omittingEmptySubsequences: true)
            .map(String.init)
        if components.isEmpty {
            return "/"
        }
        var rawComponents: [String] = []
        rawComponents.reserveCapacity(components.count)
        for component in components {
            guard let decoded = component.removingPercentEncoding,
                  decoded != ".",
                  decoded != "..",
                  !decoded.contains("/") else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            rawComponents.append(decoded)
        }
        return try canonicalRawPath("/" + rawComponents.joined(separator: "/"))
    }

    static func uppercasedPercentEscapes(in value: String) -> String {
        var result = ""
        result.reserveCapacity(value.count)
        var index = value.startIndex
        while index < value.endIndex {
            if value[index] == "%" {
                let first = value.index(after: index)
                if first < value.endIndex {
                    let second = value.index(after: first)
                    if second < value.endIndex,
                       isHexDigit(value[first]),
                       isHexDigit(value[second]) {
                        result.append("%")
                        result.append(contentsOf: value[first ... second].uppercased())
                        index = value.index(after: second)
                        continue
                    }
                }
            }
            result.append(value[index])
            index = value.index(after: index)
        }
        return result
    }

    private static func isHexDigit(_ character: Character) -> Bool {
        guard character.unicodeScalars.count == 1, let value = character.unicodeScalars.first?.value else {
            return false
        }
        return (48 ... 57).contains(value) || (65 ... 70).contains(value) || (97 ... 102).contains(value)
    }

    private static func encodeRawPathComponent(_ component: String) -> String {
        let allowed = CharacterSet.urlPathAllowed.subtracting(CharacterSet(charactersIn: "/"))
        let encoded = component.addingPercentEncoding(withAllowedCharacters: allowed) ?? component
        return uppercasedPercentEscapes(in: encoded)
    }
}
