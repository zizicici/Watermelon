import Foundation

// `nonisolated` keeps these helpers isolation-neutral — WatermelonMac uses default-MainActor isolation, otherwise actor callers cross actors.
nonisolated enum RemotePathBuilder {
    /// Rejects peer-supplied traversal that could escape the configured base path.
    enum PathValidationError: Error, Equatable {
        case containsParentTraversal(String)
    }

    static func normalizeRelativePath(_ value: String) -> String {
        value.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
    }

    /// Absolute inputs are tolerated because callers rebase through absolutePath(basePath:remoteRelativePath:).
    static func validateRelativePath(_ remoteRelativePath: String) -> PathValidationError? {
        let trimmed = normalizeRelativePath(remoteRelativePath)
        guard !trimmed.isEmpty else { return nil }
        for segment in trimmed.split(separator: "/", omittingEmptySubsequences: false) {
            if segment == ".." {
                return .containsParentTraversal(remoteRelativePath)
            }
        }
        return nil
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
        var sanitized = filename.components(separatedBy: invalid).joined(separator: "_")
        // Strip control chars (NUL truncates C-strings; others confuse Photos / FS).
        if sanitized.unicodeScalars.contains(where: { $0.value < 0x20 || $0.value == 0x7F }) {
            var rebuilt = String.UnicodeScalarView()
            for scalar in sanitized.unicodeScalars {
                if scalar.value < 0x20 || scalar.value == 0x7F {
                    rebuilt.append(Unicode.Scalar(UInt8(ascii: "_")))
                } else {
                    rebuilt.append(scalar)
                }
            }
            sanitized = String(rebuilt)
        }
        // Cap to 255 UTF-8 bytes — APFS / ext4 / SMB / FAT32 single-component limit.
        while sanitized.utf8.count > 255 && !sanitized.isEmpty {
            sanitized.removeLast()
        }
        return sanitized
    }
}
