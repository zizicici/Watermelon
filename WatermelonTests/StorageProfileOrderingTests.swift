import XCTest
@testable import Watermelon

final class StorageProfileOrderingTests: XCTestCase {
    func testNodeTypeDisplayOrderKeepsOneDriveLast() {
        XCTAssertEqual(
            StorageType.nodeTypeDisplayOrder,
            [.externalVolume, .smb, .webdav, .sftp, .s3, .onedrive]
        )
    }
}
