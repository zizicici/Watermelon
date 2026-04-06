import CryptoKit
import Foundation
import Photos

struct BackupSelectedResource {
    let resourceIndex: Int
    let resource: PHAssetResource
    let role: Int
    let slot: Int
}

enum BackupAssetResourcePlanner {
    static func orderedResourcesWithRoleSlot(from resources: [PHAssetResource]) -> [BackupSelectedResource] {
        let sorted = Array(resources.enumerated()).sorted { lhs, rhs in
            let lhsRole = PhotoLibraryService.resourceTypeCode(lhs.1.type)
            let rhsRole = PhotoLibraryService.resourceTypeCode(rhs.1.type)
            if lhsRole != rhsRole { return lhsRole < rhsRole }

            let lhsName = lhs.1.originalFilename.lowercased()
            let rhsName = rhs.1.originalFilename.lowercased()
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

    static func assetFingerprint(resourceRoleSlotHashes: [(role: Int, slot: Int, contentHash: Data)]) -> Data {
        let tokens = resourceRoleSlotHashes
            .map { token in
                let hashHex = token.contentHash.hexString
                return "\(token.role)|\(token.slot)|\(hashHex)"
            }
            .sorted()
            .joined(separator: "\n")

        let digest = SHA256.hash(data: Data(tokens.utf8))
        return Data(digest)
    }

    static func assetDisplayName(asset: PHAsset, selectedResources: [BackupSelectedResource]) -> String {
        if let first = selectedResources.first {
            return first.resource.originalFilename
        }
        return "asset_\(asset.creationDate?.nanosecondsSinceEpoch ?? 0)"
    }
}
