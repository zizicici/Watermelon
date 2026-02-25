import CryptoKit
import Foundation

enum FingerprintBuilder {
    static func makeFingerprint(
        assetLocalIdentifier: String,
        resourceType: String,
        originalFilename: String,
        fileSize: Int64,
        uti: String?,
        resourceModificationDate: Date?
    ) -> String {
        let payload = [
            assetLocalIdentifier,
            resourceType,
            originalFilename,
            String(fileSize),
            uti ?? "",
            String(resourceModificationDate?.timeIntervalSince1970 ?? 0)
        ].joined(separator: "|")

        let digest = SHA256.hash(data: Data(payload.utf8))
        return digest.map { String(format: "%02x", $0) }.joined()
    }

    static func shortHash(_ value: String, length: Int = 12) -> String {
        let digest = SHA256.hash(data: Data(value.utf8))
        let full = digest.map { String(format: "%02x", $0) }.joined()
        return String(full.prefix(length))
    }
}
