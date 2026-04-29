import Foundation

struct LegacyScannedFile {
    let url: URL
    let fileSize: Int64
    let mtime: Date?
}

enum LegacyMediaExtensions {
    static let imageExtensions: Set<String> = [
        "heic", "heif", "jpg", "jpeg", "png", "gif",
        "tiff", "tif", "webp", "dng", "raw",
        "cr2", "cr3", "nef", "arw", "rw2", "orf", "raf", "srw"
    ]
    static let videoExtensions: Set<String> = [
        "mp4", "mov", "m4v", "hevc"
    ]

    static func kind(forExtension lowercasedExt: String) -> LegacyMediaKind? {
        if imageExtensions.contains(lowercasedExt) { return .image }
        if videoExtensions.contains(lowercasedExt) { return .video }
        return nil
    }

    static func isMedia(_ lowercasedExt: String) -> Bool {
        kind(forExtension: lowercasedExt) != nil
    }
}

final class LegacyFolderScanner {
    func enumerate(at root: URL) throws -> [LegacyScannedFile] {
        let resourceKeys: [URLResourceKey] = [
            .isRegularFileKey,
            .fileSizeKey,
            .contentModificationDateKey,
            .nameKey
        ]
        guard let enumerator = FileManager.default.enumerator(
            at: root,
            includingPropertiesForKeys: resourceKeys,
            options: [.skipsHiddenFiles]
        ) else {
            return []
        }

        var results: [LegacyScannedFile] = []
        for case let fileURL as URL in enumerator {
            let values = try fileURL.resourceValues(forKeys: Set(resourceKeys))
            guard values.isRegularFile == true else { continue }
            let ext = fileURL.pathExtension.lowercased()
            guard LegacyMediaExtensions.isMedia(ext) else { continue }
            let size = Int64(values.fileSize ?? 0)
            results.append(
                LegacyScannedFile(
                    url: fileURL,
                    fileSize: size,
                    mtime: values.contentModificationDate
                )
            )
        }
        return results
    }
}
