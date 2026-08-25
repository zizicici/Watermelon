import XCTest
@testable import Watermelon

final class RestoreContentTypeTests: XCTestCase {
    private func instance(fileName: String) -> RemoteAssetResourceInstance {
        RemoteAssetResourceInstance(
            role: ResourceTypeCode.photo,
            slot: 0,
            resourceHash: Data([1]),
            fileName: fileName,
            fileSize: 100,
            remoteRelativePath: "2026/08/\(fileName)",
            creationDateMs: nil
        )
    }

    func testDNGUsesRAWContentTypeFromExtension() {
        let resource = instance(fileName: "third-party.DNG")
        let identifier = RestoreService.restoreContentTypeIdentifier(
            for: resource,
            fileURL: URL(fileURLWithPath: "/tmp/restore.dng")
        )
        XCTAssertEqual(identifier, "com.adobe.raw-image")
    }

    func testJPEGLeavesContentTypeInferenceToPhotoKit() {
        let resource = instance(fileName: "photo.jpg")
        let identifier = RestoreService.restoreContentTypeIdentifier(
            for: resource,
            fileURL: URL(fileURLWithPath: "/tmp/restore.jpg")
        )
        XCTAssertNil(identifier)
    }
}
