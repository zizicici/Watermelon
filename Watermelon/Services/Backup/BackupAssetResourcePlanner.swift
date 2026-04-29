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
        let filtered = resources.enumerated().filter { _, resource in
            !shouldExcludeFromBackup(resource: resource)
        }
        let sorted = Array(filtered).sorted { lhs, rhs in
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

    // iOS 17+ returns .photoProxy as a low-res stand-in alongside the real .photo for iCloud assets.
    private static func shouldExcludeFromBackup(resource: PHAssetResource) -> Bool {
        if #available(iOS 17, *), resource.type == .photoProxy {
            return true
        }
        return false
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
        return "asset_\(asset.creationDate?.millisecondsSinceEpoch ?? 0)"
    }
}
