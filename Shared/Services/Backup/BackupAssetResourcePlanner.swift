import CryptoKit
import Foundation
#if os(iOS)
import Photos
#endif

#if os(iOS)
struct BackupSelectedResource {
    let resourceIndex: Int
    let resource: PHAssetResource
    let role: Int
    let slot: Int
}
#endif

enum BackupAssetResourcePlanner {
    static func assetFingerprint<Values: Collection>(
        resourceRoleSlotHashes: Values
    ) -> Data where Values.Element == (role: Int, slot: Int, contentHash: Data) {
        switch resourceRoleSlotHashes.count {
        case 0:
            return sha256(Data())
        case 1:
            let value = resourceRoleSlotHashes[resourceRoleSlotHashes.startIndex]
            return sha256(canonicalToken(
                role: value.role,
                slot: value.slot,
                contentHash: value.contentHash
            ))
        case 2:
            let firstIndex = resourceRoleSlotHashes.startIndex
            let secondIndex = resourceRoleSlotHashes.index(after: firstIndex)
            let lhs = resourceRoleSlotHashes[firstIndex]
            let rhs = resourceRoleSlotHashes[secondIndex]
            var first = canonicalToken(role: lhs.role, slot: lhs.slot, contentHash: lhs.contentHash)
            var second = canonicalToken(role: rhs.role, slot: rhs.slot, contentHash: rhs.contentHash)
            if second.lexicographicallyPrecedes(first) {
                swap(&first, &second)
            }
            var canonical = Data(capacity: first.count + 1 + second.count)
            canonical.append(first)
            canonical.append(0x0A)
            canonical.append(second)
            return sha256(canonical)
        default:
            var tokens: [Data] = []
            tokens.reserveCapacity(resourceRoleSlotHashes.count)
            for value in resourceRoleSlotHashes {
                tokens.append(canonicalToken(
                    role: value.role,
                    slot: value.slot,
                    contentHash: value.contentHash
                ))
            }
            tokens.sort { $0.lexicographicallyPrecedes($1) }
            let byteCount = tokens.reduce(tokens.count - 1) { $0 + $1.count }
            var canonical = Data(capacity: byteCount)
            for index in tokens.indices {
                if index != tokens.startIndex {
                    canonical.append(0x0A)
                }
                canonical.append(tokens[index])
            }
            return sha256(canonical)
        }
    }

    private static let lowercaseHexDigits = Array("0123456789abcdef".utf8)

    private static func canonicalToken(role: Int, slot: Int, contentHash: Data) -> Data {
        let roleText = String(role)
        let slotText = String(slot)
        let byteCount = roleText.utf8.count + 1 + slotText.utf8.count + 1 + contentHash.count * 2
        var token = Data(count: byteCount)
        token.withUnsafeMutableBytes { (output: UnsafeMutableRawBufferPointer) in
            var outputIndex = 0
            for byte in roleText.utf8 {
                output[outputIndex] = byte
                outputIndex += 1
            }
            output[outputIndex] = 0x7C
            outputIndex += 1
            for byte in slotText.utf8 {
                output[outputIndex] = byte
                outputIndex += 1
            }
            output[outputIndex] = 0x7C
            outputIndex += 1
            for byte in contentHash {
                output[outputIndex] = lowercaseHexDigits[Int(byte >> 4)]
                output[outputIndex + 1] = lowercaseHexDigits[Int(byte & 0x0F)]
                outputIndex += 2
            }
        }
        return token
    }

    private static func sha256(_ data: Data) -> Data {
        Data(SHA256.hash(data: data))
    }

    #if os(iOS)
    static func orderedResourcesWithRoleSlot(from resources: [PHAssetResource]) -> [BackupSelectedResource] {
        let filtered = resources.enumerated().filter { _, resource in
            !shouldExcludeFromBackup(resource: resource)
        }
        let sorted = Array(filtered).sorted { lhs, rhs in
            let lhsRole = PhotoLibraryService.resourceTypeCode(lhs.1.type)
            let rhsRole = PhotoLibraryService.resourceTypeCode(rhs.1.type)
            if lhsRole != rhsRole { return lhsRole < rhsRole }

            let lhsName = PhotoLibraryService.safeOriginalFilename(for: lhs.1).lowercased()
            let rhsName = PhotoLibraryService.safeOriginalFilename(for: rhs.1).lowercased()
            if lhsName != rhsName { return lhsName < rhsName }
            return lhs.0 < rhs.0
        }

        var roleCounters: [Int: Int] = [:]
        var result: [BackupSelectedResource] = []
        result.reserveCapacity(sorted.count)

        for (resourceIndex, resource) in sorted {
            let role = PhotoLibraryService.resourceTypeCode(resource.type)
            let slot = roleCounters[role, default: 0]
            roleCounters[role] = slot + 1

            result.append(
                BackupSelectedResource(
                    resourceIndex: resourceIndex,
                    resource: resource,
                    role: role,
                    slot: slot
                )
            )
        }

        return result
    }

    // iOS 17+ returns .photoProxy as a low-res stand-in alongside the real .photo for iCloud assets.
    private static func shouldExcludeFromBackup(resource: PHAssetResource) -> Bool {
        if #available(iOS 17, *), resource.type == .photoProxy {
            return true
        }
        return false
    }

    static func assetDisplayName(asset: PHAsset, selectedResources: [BackupSelectedResource]) -> String {
        if let first = selectedResources.first {
            return PhotoLibraryService.safeOriginalFilename(for: first.resource)
        }
        return "asset_\(LibraryCreationDate.normalized(asset.creationDate).milliseconds)"
    }
    #endif
}
