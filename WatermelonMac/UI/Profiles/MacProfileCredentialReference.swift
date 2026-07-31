import CryptoKit
import Foundation

enum MacProfileCredentialReference {
    static func make(
        for identity: ProfileDuplicateIdentity
    ) -> String {
        var payload = Data()
        for field in identity.components {
            let fieldData = Data(field.utf8)
            var length = UInt64(fieldData.count).bigEndian
            withUnsafeBytes(of: &length) {
                payload.append(contentsOf: $0)
            }
            payload.append(fieldData)
        }
        let digest = SHA256.hash(data: payload)
            .map { String(format: "%02x", $0) }
            .joined()
        return "v2|\(identity.storageType.rawValue)|\(digest)"
    }
}
