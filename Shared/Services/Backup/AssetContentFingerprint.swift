import Foundation

enum AssetContentFingerprint {
    static let version = 1

    struct Resource: Sendable {
        let role: Int
        let slot: Int
        let hash: Data
    }

    static func fingerprint(resources: [Resource]) -> Data {
        BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: resources.map { ($0.role, $0.slot, $0.hash) })
    }
}

extension RemoteAssetResourceInstance {
    var contentIdentityResource: AssetContentFingerprint.Resource {
        .init(role: role, slot: slot, hash: resourceHash)
    }
}
