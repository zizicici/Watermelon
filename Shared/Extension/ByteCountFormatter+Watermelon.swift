import Foundation

extension ByteCountFormatter {
    static func fileSizeString(_ bytes: Int64) -> String {
        ByteCountFormatter.string(fromByteCount: bytes, countStyle: .file)
    }
}
