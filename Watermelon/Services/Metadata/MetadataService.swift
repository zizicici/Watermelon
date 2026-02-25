import Foundation
import ImageIO
import UniformTypeIdentifiers

struct FileMetadata {
    let fileSize: Int64
    let pixelWidth: Int?
    let pixelHeight: Int?
    let uti: String?
}

final class MetadataService {
    func metadata(for fileURL: URL) -> FileMetadata {
        let fileSize = (try? FileManager.default.attributesOfItem(atPath: fileURL.path)[.size] as? NSNumber)?.int64Value ?? 0

        guard let source = CGImageSourceCreateWithURL(fileURL as CFURL, nil),
              let properties = CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [CFString: Any] else {
            return FileMetadata(fileSize: fileSize, pixelWidth: nil, pixelHeight: nil, uti: nil)
        }

        let width = properties[kCGImagePropertyPixelWidth] as? Int
        let height = properties[kCGImagePropertyPixelHeight] as? Int
        let uti = CGImageSourceGetType(source) as String?

        return FileMetadata(fileSize: fileSize, pixelWidth: width, pixelHeight: height, uti: uti)
    }

    func isImage(uti: String?) -> Bool {
        guard let uti else { return false }
        guard let type = UTType(uti) else { return false }
        return type.conforms(to: .image)
    }
}
