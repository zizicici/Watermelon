import Foundation

struct LocalAlbumDescriptor: Hashable, Sendable {
    // PHCollection.localIdentifier (album id) — not the asset-id boundary.
    let localIdentifier: String
    let title: String
    let assetCount: Int
    // PHAsset.localIdentifier of the chosen thumbnail asset.
    let thumbnailAssetIdentifier: PhotoKitLocalIdentifier?
}
