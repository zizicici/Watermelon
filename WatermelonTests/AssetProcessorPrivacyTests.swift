import XCTest
@testable import Watermelon

final class AssetProcessorPrivacyTests: XCTestCase {
    func testPrivacySafeDisplayNamesDoNotUseOriginalFilenames() {
        XCTAssertEqual(
            AssetProcessor.resourceDisplayName(
                originalFilename: "IMG_MEDICAL_2026.HEIC",
                resourcePosition: 2,
                hidesOriginalFilename: true
            ),
            "resource 2"
        )
        XCTAssertEqual(AssetProcessor.privacySafeAssetDisplayName(assetPosition: 7), "asset 7")
    }

    func testPlaintextDisplayNameKeepsOriginalFilename() {
        XCTAssertEqual(
            AssetProcessor.resourceDisplayName(
                originalFilename: "IMG_0001.HEIC",
                resourcePosition: 1,
                hidesOriginalFilename: false
            ),
            "IMG_0001.HEIC"
        )
    }

    func testInlineThumbnailPolicySkipsPlaintextWriteWhenRemoteIsEncryptedWithoutContext() {
        XCTAssertNil(AssetProcessor.inlineThumbnailSidecarPolicy(
            encryptionContext: nil,
            remoteManifestIsEncrypted: true
        ))
    }

    func testInlineThumbnailPolicySkipsPlaintextRemoteWhenLocalContextIsEncrypted() {
        let context = RepoEncryptionContext(
            repoID: "repo",
            activeKeyID: "key",
            contentKey: Data(repeating: 0x11, count: RepoEncryptionKeyMaterial.byteCount)
        )

        XCTAssertNil(AssetProcessor.inlineThumbnailSidecarPolicy(
            encryptionContext: context,
            remoteManifestIsEncrypted: false
        ))
    }

    func testInlineThumbnailPolicyUsesEncryptedSidecarForEncryptedRemote() {
        let context = RepoEncryptionContext(
            repoID: "repo",
            activeKeyID: "key",
            contentKey: Data(repeating: 0x11, count: RepoEncryptionKeyMaterial.byteCount)
        )

        let policy = AssetProcessor.inlineThumbnailSidecarPolicy(
            encryptionContext: context,
            remoteManifestIsEncrypted: true
        )

        XCTAssertEqual(policy?.storageCodec, RemoteManifestResource.encryptedStorageCodec)
        XCTAssertEqual(policy?.encryptionContext, context)
    }
}
