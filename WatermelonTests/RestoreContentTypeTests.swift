import XCTest
@testable import Watermelon

final class RestoreContentTypeTests: XCTestCase {
    private func instance(
        fileName: String,
        role: Int = ResourceTypeCode.photo
    ) -> RemoteAssetResourceInstance {
        RemoteAssetResourceInstance(
            role: role,
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

    func testExtensionlessImageUsesContentDetectedType() throws {
        let fileURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("restore_content_type_\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: fileURL) }
        let png = Data(base64Encoded: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=")!
        try png.write(to: fileURL)

        let identifier = RestoreService.restoreContentTypeIdentifier(
            for: instance(fileName: "extensionless"),
            fileURL: fileURL
        )

        XCTAssertEqual(identifier, "public.png")
    }

    func testExtensionlessInvalidContentDoesNotGuessAType() throws {
        let fileURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("restore_content_type_\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: fileURL) }
        try Data("not an image".utf8).write(to: fileURL)

        let identifier = RestoreService.restoreContentTypeIdentifier(
            for: instance(fileName: "extensionless"),
            fileURL: fileURL
        )

        XCTAssertNil(identifier)
    }

    func testExtensionlessVideoDoesNotUseImageDetection() throws {
        let fileURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("restore_content_type_\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: fileURL) }
        let png = Data(base64Encoded: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=")!
        try png.write(to: fileURL)

        let identifier = RestoreService.restoreContentTypeIdentifier(
            for: instance(fileName: "extensionless", role: ResourceTypeCode.video),
            fileURL: fileURL
        )

        XCTAssertNil(identifier)
    }

    func testExtensionlessDNGStagesTypedPhotoKitURL() throws {
        let fileURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("restore_content_type_\(UUID().uuidString)")
        let data = Data("dng bytes".utf8)
        try data.write(to: fileURL)
        defer { try? FileManager.default.removeItem(at: fileURL) }

        let importURL = try RestoreService.makePhotoKitImportURL(
            fileURL: fileURL,
            contentTypeIdentifier: "com.adobe.raw-image"
        )
        defer { try? FileManager.default.removeItem(at: importURL) }

        XCTAssertEqual(importURL.pathExtension, "dng")
        XCTAssertEqual(try Data(contentsOf: importURL), data)
        XCTAssertTrue(FileManager.default.fileExists(atPath: fileURL.path))
    }

    func testPhotoKitImportURLKeepsExistingExtension() throws {
        let fileURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("restore_content_type_\(UUID().uuidString).dng")

        let importURL = try RestoreService.makePhotoKitImportURL(
            fileURL: fileURL,
            contentTypeIdentifier: "com.adobe.raw-image"
        )

        XCTAssertEqual(importURL, fileURL)
    }
}
