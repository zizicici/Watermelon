import Foundation

// Process-local key for tracking a single PHAssetResource within one backup
// run. Never serialized, never written to remote storage. The strongly-typed
// shape replaces a composed string that read like a stable identifier.
struct InProcessResourceKey: Hashable, Sendable {
    let assetID: PhotoKitLocalIdentifier
    let role: Int
    let slot: Int
}
