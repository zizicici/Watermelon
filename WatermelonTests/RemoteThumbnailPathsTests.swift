import XCTest
@testable import Watermelon

// Locks in the dedup-critical sidecar layout: content-addressed (no month segment), prefix-sharded,
// one path per fingerprint+codec. Guards against reintroducing month-bucketed paths (cross-month duplicates).
final class RemoteThumbnailPathsTests: XCTestCase {
    private let fp = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

    func testRelativePathIsContentAddressedAndShardedByPrefix() {
        XCTAssertEqual(
            RemoteThumbnailPaths.relativePath(fingerprintHex: fp),
            ".watermelon/thumbs/ab/\(fp).jpg"
        )
    }

    func testEncryptedRelativePathUsesSeparateObjectSlot() {
        XCTAssertEqual(
            RemoteThumbnailPaths.relativePath(
                fingerprintHex: fp,
                storageCodec: RemoteManifestResource.encryptedStorageCodec
            ),
            ".watermelon/thumbs/ab/\(fp).wmenc"
        )
    }

    func testRelativePathHasNoMonthSegment() {
        let path = RemoteThumbnailPaths.relativePath(fingerprintHex: fp)
        // A YYYY-MM or YYYY/MM segment would let the same content duplicate across months.
        XCTAssertFalse(path.contains("-"))
        XCTAssertEqual(path.split(separator: "/").count, 4) // .watermelon / thumbs / <shard> / <file>
    }

    func testSameFingerprintAlwaysMapsToSamePath() {
        XCTAssertEqual(
            RemoteThumbnailPaths.relativePath(fingerprintHex: fp),
            RemoteThumbnailPaths.relativePath(fingerprintHex: fp)
        )
    }

    func testShardIsFirstTwoHexCharacters() {
        XCTAssertEqual(RemoteThumbnailPaths.shard(forFingerprintHex: fp), "ab")
        XCTAssertEqual(RemoteThumbnailPaths.shardDirectoryRelativePath(fingerprintHex: fp), ".watermelon/thumbs/ab")
    }

    func testAbsolutePathComposesWithBasePath() {
        XCTAssertEqual(
            RemoteThumbnailPaths.absolutePath(basePath: "/backup", fingerprintHex: fp),
            "/backup/.watermelon/thumbs/ab/\(fp).jpg"
        )
        XCTAssertEqual(
            RemoteThumbnailPaths.absolutePath(
                basePath: "/backup",
                fingerprintHex: fp,
                storageCodec: RemoteManifestResource.encryptedStorageCodec
            ),
            "/backup/.watermelon/thumbs/ab/\(fp).wmenc"
        )
        XCTAssertEqual(
            RemoteThumbnailPaths.rootAbsolutePath(basePath: "/backup"),
            "/backup/.watermelon/thumbs"
        )
    }

    func testShortFingerprintDoesNotCrashSharding() {
        XCTAssertEqual(RemoteThumbnailPaths.shard(forFingerprintHex: ""), "_")
        XCTAssertEqual(RemoteThumbnailPaths.shard(forFingerprintHex: "a"), "a")
    }

    func testEncryptedRepoThumbnailPolicyEncryptsLegacyPlaintextResourceSidecar() {
        let context = RepoEncryptionContext(
            repoID: "repo-thumbs",
            activeKeyID: "key-thumbs",
            contentKey: Data(repeating: 0x5B, count: RepoEncryptionKeyMaterial.byteCount)
        )

        let policy = RemoteThumbnailService.thumbnailCachePolicy(
            storageCodec: RemoteManifestResource.plaintextStorageCodec,
            encryptionKeyID: nil,
            state: .encrypted(context)
        )

        XCTAssertTrue(policy.canUseSharedCaches)
        XCTAssertEqual(policy.storageCodec, RemoteManifestResource.encryptedStorageCodec)
        XCTAssertEqual(policy.encryptionKeyID, context.activeKeyID)
        XCTAssertEqual(policy.sidecarEncryptionContext, context)
    }

    func testUnknownThumbnailEncryptionStateDoesNotWriteSharedPlaintextSidecar() {
        let policy = RemoteThumbnailService.thumbnailCachePolicy(
            storageCodec: RemoteManifestResource.plaintextStorageCodec,
            encryptionKeyID: nil,
            state: nil
        )

        XCTAssertFalse(policy.canUseSharedCaches)
    }
}
