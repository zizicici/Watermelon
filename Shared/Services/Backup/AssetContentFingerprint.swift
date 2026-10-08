import CoreFoundation
import CryptoKit
import Foundation

enum AssetContentFingerprint {
    static let version = 1
    static let maximumAdjustmentBytes = 16 * 1024 * 1024

    struct Resource: Sendable {
        let role: Int
        let slot: Int
        let hash: Data
    }

    static func fingerprint(resources: [Resource], adjustmentData: [Data: Data]) throws -> Data {
        let hashes = try resourceHashes(resources: resources, adjustmentData: adjustmentData)
        return BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: hashes.map { ($0.role, $0.slot, $0.hash) })
    }

    static func resourceHashes(resources: [Resource], adjustmentData: [Data: Data]) throws -> [Resource] {
        let roles = Set(resources.map(\.role))
        return try resources.map { resource in
            guard resource.role == ResourceTypeCode.adjustmentData, resource.hash.count == 32 else { return resource }
            guard let data = adjustmentData[resource.hash], Data(SHA256.hash(data: data)) == resource.hash else {
                throw CocoaError(.fileReadCorruptFile)
            }
            return Resource(role: resource.role, slot: resource.slot, hash: adjustmentHash(data, roles: roles))
        }
    }

    static func adjustmentHash(_ data: Data, roles: Set<Int>) -> Data {
        let originalHash = Data(SHA256.hash(data: data))
        guard data.count <= maximumAdjustmentBytes,
              var plist = (try? PropertyListSerialization.propertyList(from: data, format: nil)) as? [String: Any],
              plist["adjustmentData"] is Data,
              let format = plist["adjustmentFormatIdentifier"] as? String, !format.isEmpty,
              let version = plist["adjustmentFormatVersion"] as? String, !version.isEmpty,
              plist["adjustmentTimestamp"] is Date else { return originalHash }
        plist.removeValue(forKey: "adjustmentTimestamp")
        // PhotoKit adds these render flags for this original/rendered video resource combination.
        if roles == [2, 5, 6, 7], format == "com.apple.photo", version == "1.6",
           let flags = plist["adjustmentRenderTypes"] as? NSNumber,
           CFGetTypeID(flags) != CFBooleanGetTypeID(),
           flags == 16384 || flags == 18944 {
            plist["adjustmentRenderTypes"] = NSNumber(value: 16384)
        }
        var canonical = Data("watermelon-adjustment-content-v1\n".utf8)
        guard append(plist, to: &canonical) else { return originalHash }
        return Data(SHA256.hash(data: canonical))
    }

    private static func append(_ value: Any, to output: inout Data) -> Bool {
        if let dictionary = value as? [String: Any] {
            output.append(0x64)
            appendLength(dictionary.count, to: &output)
            for key in dictionary.keys.sorted(by: { $0.utf8.lexicographicallyPrecedes($1.utf8) }) {
                guard append(key, to: &output), append(dictionary[key]!, to: &output) else { return false }
            }
        } else if let array = value as? [Any] {
            output.append(0x61)
            appendLength(array.count, to: &output)
            for item in array {
                guard append(item, to: &output) else { return false }
            }
        } else if let data = value as? Data {
            output.append(0x62)
            appendLength(data.count, to: &output)
            output.append(data)
        } else if let date = value as? Date {
            output.append(0x74)
            appendBytes(date.timeIntervalSinceReferenceDate.bitPattern, to: &output)
        } else if let number = value as? NSNumber {
            if CFGetTypeID(number) == CFBooleanGetTypeID() {
                output.append(number.boolValue ? 0x31 : 0x30)
            } else if ["f", "d"].contains(String(cString: number.objCType)) {
                guard number.doubleValue.isFinite else { return false }
                output.append(0x72)
                appendBytes(number.doubleValue.bitPattern, to: &output)
            } else {
                output.append(0x69)
                appendBytes(UInt64(bitPattern: number.int64Value), to: &output)
            }
        } else if let string = value as? String {
            output.append(0x73)
            let bytes = Data(string.utf8)
            appendLength(bytes.count, to: &output)
            output.append(bytes)
        } else { return false }
        return true
    }

    private static func appendLength(_ count: Int, to output: inout Data) {
        appendBytes(UInt64(count), to: &output)
    }

    private static func appendBytes(_ value: UInt64, to output: inout Data) {
        var bigEndian = value.bigEndian
        withUnsafeBytes(of: &bigEndian) { output.append(contentsOf: $0) }
    }
}

extension RemoteAssetResourceInstance {
    var contentIdentityResource: AssetContentFingerprint.Resource {
        .init(role: role, slot: slot, hash: resourceHash)
    }
}
