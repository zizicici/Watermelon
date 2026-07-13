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
        var invalid = CharacterSet(charactersIn: "\\/:*?\"<>|")
        invalid.formUnion(.controlCharacters)
        return filename.components(separatedBy: invalid).joined(separator: "_")
    }

    static func isSafePathComponent(_ value: String) -> Bool {
        guard !value.isEmpty,
              value != ".",
              value != "..",
              value.utf8.count <= 1_024,
              !value.contains("/"),
              !value.contains("\\") else {
            return false
        }
        return value.unicodeScalars.allSatisfy { !CharacterSet.controlCharacters.contains($0) }
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
        var canonicalComponents: [String] = []
        canonicalComponents.reserveCapacity(components.count)
        for component in components {
            if component == "." { continue }
            guard component != ".." else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            canonicalComponents.append(component)
        }
        return canonicalComponents.isEmpty ? "/" : "/" + canonicalComponents.joined(separator: "/")
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

    static func effectiveRootRawPath(mountPath: String, basePath: String) throws -> String {
        let mount = try canonicalRawPath(mountPath)
        let base = try canonicalRawPath(basePath)
        if mount == "/" { return base }
        if base == "/" { return mount }
        return mount + base
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

nonisolated enum SMBPathCanonicalizer {
    static func canonicalShareName(_ rawName: String) throws -> String {
        let path = try canonicalRawPath(rawName)
        let components = path.split(separator: "/", omittingEmptySubsequences: true)
        guard components.count == 1 else { throw RemoteStorageClientError.invalidConfiguration }
        return String(components[0])
    }

    static func canonicalRawPath(_ rawPath: String) throws -> String {
        let components = rawPath
            .split(whereSeparator: { $0 == "/" || $0 == "\\" })
            .map(String.init)
        var canonical: [String] = []
        canonical.reserveCapacity(components.count)
        for component in components {
            if component == "." { continue }
            guard component != ".." else { throw RemoteStorageClientError.invalidConfiguration }
            canonical.append(component)
        }
        return canonical.isEmpty ? "/" : "/" + canonical.joined(separator: "/")
    }
}

nonisolated enum SFTPPathCanonicalizer {
    static func canonicalRawPath(_ rawPath: String) throws -> String {
        let components = RemotePathBuilder.normalizePath(rawPath)
            .split(separator: "/", omittingEmptySubsequences: true)
            .map(String.init)
        if components.isEmpty {
            return "/"
        }
        var canonicalComponents: [String] = []
        canonicalComponents.reserveCapacity(components.count)
        for component in components {
            if component == "." { continue }
            guard component != ".." else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            canonicalComponents.append(component)
        }
        return canonicalComponents.isEmpty ? "/" : "/" + canonicalComponents.joined(separator: "/")
    }
}
