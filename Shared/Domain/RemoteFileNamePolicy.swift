import Foundation

nonisolated enum RemoteFileNamePolicy: Sendable {
    case standard
    case oneDrive

    var maximumComponentLength: Int? {
        self == .oneDrive ? 255 : nil
    }

    func sanitize(_ filename: String) -> String {
        switch self {
        case .standard:
            return filename
        case .oneDrive:
            var result = RemotePathBuilder.sanitizeFilename(filename)
            while result.first == " " {
                result.replaceSubrange(result.startIndex ... result.startIndex, with: "_")
            }
            if result.hasPrefix("~$") {
                result.replaceSubrange(result.startIndex ... result.startIndex, with: "_")
            }
            while result.last == "." || result.last == " " {
                result.replaceSubrange(result.index(before: result.endIndex) ..< result.endIndex, with: "_")
            }
            while let range = result.range(of: "_vti_", options: .caseInsensitive) {
                result.replaceSubrange(range, with: "_vti-")
            }
            if Self.isReservedOneDriveName(result) {
                result.insert("_", at: result.startIndex)
            }
            if let maximumComponentLength, result.count > maximumComponentLength {
                result = Self.truncatePreservingExtension(
                    result,
                    maximumLength: maximumComponentLength
                )
                while result.last == "." || result.last == " " {
                    result.replaceSubrange(result.index(before: result.endIndex) ..< result.endIndex, with: "_")
                }
            }
            return result
        }
    }

    func isValid(_ filename: String) -> Bool {
        guard RemotePathBuilder.isSafePathComponent(filename) else { return false }
        switch self {
        case .standard:
            return true
        case .oneDrive:
            let forbidden = CharacterSet(charactersIn: "\\/:*?\"<>|").union(.controlCharacters)
            return filename.count <= 255
                && filename.rangeOfCharacter(from: forbidden) == nil
                && !filename.hasPrefix("~$")
                && filename.first != " "
                && filename.last != "."
                && filename.last != " "
                && !Self.isReservedOneDriveName(filename)
                && filename.range(of: "_vti_", options: .caseInsensitive) == nil
        }
    }

    private static func isReservedOneDriveName(_ filename: String) -> Bool {
        let lowercase = filename.lowercased()
        if lowercase == ".lock" || lowercase == "desktop.ini" { return true }
        let baseName = lowercase.split(separator: ".", maxSplits: 1, omittingEmptySubsequences: false).first.map(String.init) ?? lowercase
        if ["con", "prn", "aux", "nul"].contains(baseName) { return true }
        guard baseName.count == 4 else { return false }
        let prefix = baseName.prefix(3)
        let suffix = baseName.suffix(1)
        return (prefix == "com" || prefix == "lpt") && suffix.allSatisfy(\.isNumber)
    }

    private static func truncatePreservingExtension(
        _ filename: String,
        maximumLength: Int
    ) -> String {
        let name = filename as NSString
        let ext = name.pathExtension
        guard !ext.isEmpty else { return String(filename.prefix(maximumLength)) }
        let stem = name.deletingPathExtension
        let maximumExtensionLength = max(0, maximumLength - 2)
        let boundedExtension = String(ext.prefix(maximumExtensionLength))
        let extensionPart = boundedExtension.isEmpty ? "" : "." + boundedExtension
        let stemLength = max(1, maximumLength - extensionPart.count)
        return String(stem.prefix(stemLength)) + extensionPart
    }
}

extension StorageType {
    var remoteFileNamePolicy: RemoteFileNamePolicy {
        self == .onedrive ? .oneDrive : .standard
    }
}
