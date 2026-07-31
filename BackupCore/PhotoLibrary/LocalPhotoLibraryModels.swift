import Foundation

struct LocalAlbumDescriptor: Hashable, Sendable {
    let localIdentifier: String
    let title: String
    let assetCount: Int
    let thumbnailAssetIdentifier: String?
}
