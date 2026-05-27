import Foundation
import Photos

struct LocalPhotoResource {
    let asset: PHAsset
    let resource: PHAssetResource
    let assetLocalIdentifier: PhotoKitLocalIdentifier
    let inProcessResourceKey: InProcessResourceKey
    let preferredRemoteFileName: String
    let resourceRole: Int
    let resourceSlot: Int
    let resourceType: String
    let resourceTypeCode: Int
    let uti: String?
    let originalFilename: String
    let fileSize: Int64
    let resourceModificationDate: Date?
}
