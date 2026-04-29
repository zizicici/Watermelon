import XCTest
@testable import Watermelon

final class RemoteFileNamingTests: XCTestCase {

    // MARK: - sanitizedFileStem

    func testSanitizedFileStem_stripsExtension() {
        XCTAssertEqual(RemoteFileNaming.sanitizedFileStem(from: "IMG_1234.HEIC"), "IMG_1234")
        XCTAssertEqual(RemoteFileNaming.sanitizedFileStem(from: "movie.MOV"), "movie")
    }

    func testSanitizedFileStem_replacesInvalidPathCharacters() {
        XCTAssertEqual(RemoteFileNaming.sanitizedFileStem(from: "weird/name:bad?file.jpg"), "weird_name_bad_file")
    }

    func testSanitizedFileStem_dotPrefixedFileKeepsFullName() {
        // NSString.deletingPathExtension treats a leading dot as part of the stem,
        // so ".hidden" has no extension to strip. Pin this Cocoa behavior.
        XCTAssertEqual(RemoteFileNaming.sanitizedFileStem(from: ".hidden"), ".hidden")
    }

    // MARK: - collisionKey

    func testCollisionKey_isCaseInsensitive() {
        XCTAssertEqual(
            RemoteFileNaming.collisionKey(for: "IMG_1234.HEIC"),
            RemoteFileNaming.collisionKey(for: "img_1234.heic")
        )
    }

    func testCollisionKey_isDiacriticInsensitive() {
        XCTAssertEqual(
            RemoteFileNaming.collisionKey(for: "café.jpg"),
            RemoteFileNaming.collisionKey(for: "cafe.jpg")
        )
    }

    func testCollisionKey_normalizesUnicodeComposition() {
        // "é" composed (U+00E9) vs decomposed (U+0065 + U+0301) must collapse.
        let composed = "caf\u{00E9}.jpg"
        let decomposed = "cafe\u{0301}.jpg"
        XCTAssertEqual(
            RemoteFileNaming.collisionKey(for: composed),
            RemoteFileNaming.collisionKey(for: decomposed)
        )
    }

    // MARK: - resolveNextAvailableName

    func testResolveNextAvailableName_returnsBaseWhenNoCollision() {
        let result = RemoteFileNaming.resolveNextAvailableName(
            baseName: "IMG_1.heic",
            collisionKeys: []
        )
        XCTAssertEqual(result, "IMG_1.heic")
    }

    func testResolveNextAvailableName_appendsUnderscoreCounter() {
        // Lock the suffix format: stem_1, stem_2, NOT stem_(2)
        let occupied: Set<String> = ["IMG_1.heic"]
        let collisionKeys = RemoteFileNaming.collisionKeySet(from: occupied)
        let first = RemoteFileNaming.resolveNextAvailableName(
            baseName: "IMG_1.heic",
            collisionKeys: collisionKeys
        )
        XCTAssertEqual(first, "IMG_1_1.heic")

        let next = RemoteFileNaming.resolveNextAvailableName(
            baseName: "IMG_1.heic",
            collisionKeys: RemoteFileNaming.collisionKeySet(from: ["IMG_1.heic", "IMG_1_1.heic"])
        )
        XCTAssertEqual(next, "IMG_1_2.heic")
    }

    func testResolveNextAvailableName_handlesNoExtension() {
        let result = RemoteFileNaming.resolveNextAvailableName(
            baseName: "RAW",
            collisionKeys: RemoteFileNaming.collisionKeySet(from: ["RAW"])
        )
        XCTAssertEqual(result, "RAW_1")
    }

    func testResolveNextAvailableName_collisionMatchIsCaseInsensitive() {
        // Existing file is uppercase; new candidate is lowercase — must still collide.
        let result = RemoteFileNaming.resolveNextAvailableName(
            baseName: "img_1.heic",
            collisionKeys: RemoteFileNaming.collisionKeySet(from: ["IMG_1.HEIC"])
        )
        XCTAssertEqual(result, "img_1_1.heic")
    }

    // MARK: - fallbackResourceLabel

    func testFallbackResourceLabel_titleCasesKnownRoles() {
        XCTAssertEqual(RemoteFileNaming.fallbackResourceLabel(forRole: 1), "Photo")
        XCTAssertEqual(RemoteFileNaming.fallbackResourceLabel(forRole: 2), "Video")
        XCTAssertEqual(RemoteFileNaming.fallbackResourceLabel(forRole: 9), "PairedVideo")
        XCTAssertEqual(RemoteFileNaming.fallbackResourceLabel(forRole: 4), "AlternatePhoto")
        XCTAssertEqual(RemoteFileNaming.fallbackResourceLabel(forRole: 7), "AdjustmentData")
    }

    func testFallbackResourceLabel_unknownRoleReturnsUnknown() {
        XCTAssertEqual(RemoteFileNaming.fallbackResourceLabel(forRole: 999), "Unknown")
    }

    // MARK: - preferredAssetNameStem

    func testPreferredAssetNameStem_prefersPhotoOverPairedVideo() {
        let resources: [RemoteFileNaming.ResourceIdentity] = [
            .init(role: 9, slot: 0, originalFilename: "IMG_1234.MOV"),
            .init(role: 1, slot: 0, originalFilename: "IMG_1234.HEIC")
        ]
        let stem = RemoteFileNaming.preferredAssetNameStem(orderedResources: resources, fallbackTimestampMs: 0)
        XCTAssertEqual(stem, "IMG_1234")
    }

    func testPreferredAssetNameStem_fallsBackToFirstWhenNoPriorityRoleHasName() {
        // Adjustment-data only — none of the priority roles match, so fall back to the first resource's stem.
        let resources: [RemoteFileNaming.ResourceIdentity] = [
            .init(role: 7, slot: 0, originalFilename: "adj.plist")
        ]
        let stem = RemoteFileNaming.preferredAssetNameStem(orderedResources: resources, fallbackTimestampMs: 0)
        XCTAssertEqual(stem, "adj")
    }

    func testPreferredAssetNameStem_usesTimestampFallbackWhenAllNamesEmpty() {
        let resources: [RemoteFileNaming.ResourceIdentity] = [
            .init(role: 1, slot: 0, originalFilename: "")
        ]
        let stem = RemoteFileNaming.preferredAssetNameStem(orderedResources: resources, fallbackTimestampMs: 12345)
        XCTAssertEqual(stem, "asset_12345")
    }

    func testPreferredAssetNameStem_usesZeroWhenTimestampNil() {
        let resources: [RemoteFileNaming.ResourceIdentity] = []
        let stem = RemoteFileNaming.preferredAssetNameStem(orderedResources: resources, fallbackTimestampMs: nil)
        XCTAssertEqual(stem, "asset_0")
    }

    // MARK: - preferredRemoteFileName

    func testPreferredRemoteFileName_primaryPhotoUsesAssetStem() {
        let name = RemoteFileNaming.preferredRemoteFileName(
            preferredAssetNameStem: "IMG_1234",
            resource: .init(role: 1, slot: 0, originalFilename: "IMG_1234.HEIC")
        )
        XCTAssertEqual(name, "IMG_1234.HEIC")
    }

    func testPreferredRemoteFileName_pairedVideoSlotZeroIsTreatedAsPrimary() {
        // pairedVideo at slot 0 is primary per the existing iOS rule (not detail-suffixed).
        let name = RemoteFileNaming.preferredRemoteFileName(
            preferredAssetNameStem: "IMG_1234",
            resource: .init(role: 9, slot: 0, originalFilename: "IMG_1234.MOV")
        )
        XCTAssertEqual(name, "IMG_1234.MOV")
    }

    func testPreferredRemoteFileName_alternatePhotoGetsRoleSuffix() {
        // alternatePhoto (role 4) is not in the primary set → suffixed with detail stem.
        let name = RemoteFileNaming.preferredRemoteFileName(
            preferredAssetNameStem: "IMG_1234",
            resource: .init(role: 4, slot: 0, originalFilename: "ALT_5678.jpg")
        )
        XCTAssertEqual(name, "IMG_1234_ALT_5678.jpg")
    }

    func testPreferredRemoteFileName_detailSameAsBaseFallsBackToRoleLabel() {
        // When original stem == asset stem (case-insensitive), use the role's fallback label.
        let name = RemoteFileNaming.preferredRemoteFileName(
            preferredAssetNameStem: "IMG_1234",
            resource: .init(role: 4, slot: 0, originalFilename: "img_1234.jpg")
        )
        XCTAssertEqual(name, "IMG_1234_AlternatePhoto.jpg")
    }

    func testPreferredRemoteFileName_detailHasBasePrefixIsKeptVerbatim() {
        // If the detail stem already starts with "{base}_" or "{base}-", do NOT double-prefix.
        let name = RemoteFileNaming.preferredRemoteFileName(
            preferredAssetNameStem: "IMG_1234",
            resource: .init(role: 4, slot: 0, originalFilename: "IMG_1234_extra.jpg")
        )
        XCTAssertEqual(name, "IMG_1234_extra.jpg")
    }

    func testPreferredRemoteFileName_emptyAssetStemFallsBackToOriginal() {
        let name = RemoteFileNaming.preferredRemoteFileName(
            preferredAssetNameStem: "",
            resource: .init(role: 1, slot: 0, originalFilename: "fallback.jpg")
        )
        XCTAssertEqual(name, "fallback.jpg")
    }

    func testPreferredRemoteFileName_originalWithoutExtensionDropsExtSegment() {
        let name = RemoteFileNaming.preferredRemoteFileName(
            preferredAssetNameStem: "IMG_1234",
            resource: .init(role: 1, slot: 0, originalFilename: "IMG_1234")
        )
        XCTAssertEqual(name, "IMG_1234")
    }
}
