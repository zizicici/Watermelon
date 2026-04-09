import Foundation

enum RemotePathBuilder {
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
