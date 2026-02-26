import Foundation

enum RemotePathBuilder {
    private static let calendar = Calendar(identifier: .gregorian)

    static func buildRelativePath(
        originalFilename: String,
        creationDate: Date?,
        duplicateIndex: Int = 0
    ) -> String {
        let date = creationDate ?? Date()
        let comps = calendar.dateComponents([.year, .month], from: date)
        let year = String(comps.year ?? 1970)
        let month = String(format: "%02d", comps.month ?? 1)

        let safeName = sanitizeFilename(originalFilename)
        let finalName = duplicateIndex == 0 ? safeName : makeDuplicatedFilename(base: safeName, index: duplicateIndex)

        return normalizeRelativePath("\(year)/\(month)/\(finalName)")
    }

    static func normalizeRelativePath(_ value: String) -> String {
        value.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
    }

    static func storedPathToRelative(basePath: String, storedPath: String) -> String {
        let normalizedBase = normalizePath(basePath)
        let normalizedStored = normalizePath(storedPath)
        if normalizedStored.hasPrefix(normalizedBase + "/") {
            return normalizeRelativePath(String(normalizedStored.dropFirst(normalizedBase.count + 1)))
        }
        return normalizeRelativePath(storedPath)
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

    static func directory(of remotePath: String) -> String {
        let nsPath = remotePath as NSString
        let directory = nsPath.deletingLastPathComponent
        if directory.isEmpty { return "/" }
        return normalizePath(directory)
    }

    private static func makeDuplicatedFilename(base: String, index: Int) -> String {
        let ns = base as NSString
        let ext = ns.pathExtension
        let stem = ns.deletingPathExtension
        if ext.isEmpty {
            return "\(stem)_\(index)"
        }
        return "\(stem)_\(index).\(ext)"
    }

    static func sanitizeFilename(_ filename: String) -> String {
        let invalid = CharacterSet(charactersIn: "\\/:*?\"<>|")
        return filename.components(separatedBy: invalid).joined(separator: "_")
    }
}
