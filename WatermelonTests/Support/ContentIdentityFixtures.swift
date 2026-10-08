import CryptoKit
import Foundation
@testable import Watermelon

enum ContentIdentityFixtures {
    static func adjustment(timestamp: Double = 1, payload: Data = Data("edit-recipe".utf8),
                           flags: Int = 16384, format: String = "com.apple.photo", version: String = "1.6",
                           extra: [String: Any] = [:], encoding: PropertyListSerialization.PropertyListFormat = .binary) -> Data {
        var plist: [String: Any] = [
            "adjustmentBaseVersion": 0, "adjustmentData": payload,
            "adjustmentEditorBundleID": "com.apple.mobileslideshow",
            "adjustmentFormatIdentifier": format, "adjustmentFormatVersion": version,
            "adjustmentRenderTypes": flags, "adjustmentTimestamp": Date(timeIntervalSince1970: timestamp)
        ]
        plist.merge(extra) { _, new in new }
        return try! PropertyListSerialization.data(fromPropertyList: plist, format: encoding, options: 0)
    }

    static func hash(_ data: Data) -> Data { Data(SHA256.hash(data: data)) }

    static func resources(adjustment: Data, media: UInt8 = 1, roles: [Int] = [2, 5, 6, 7]) -> [AssetContentFingerprint.Resource] {
        roles.map { .init(role: $0, slot: 0, hash: $0 == 7 ? hash(adjustment) : Data(repeating: UInt8($0) &+ media, count: 32)) }
    }
}
