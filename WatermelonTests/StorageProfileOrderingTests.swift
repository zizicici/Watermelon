import XCTest
@testable import Watermelon

final class StorageProfileOrderingTests: XCTestCase {
    func testNodeTypeDisplayOrderKeepsCloudProvidersLast() {
        XCTAssertEqual(
            StorageType.nodeTypeDisplayOrder,
            [.externalVolume, .smb, .webdav, .sftp, .s3, .onedrive, .dropbox]
        )
    }
}
