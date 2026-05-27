import XCTest
@testable import Watermelon

final class LocalHashIndexTrustTests: XCTestCase {
    private let baseUpdatedAt = Date(timeIntervalSince1970: 1_700_000_000)
    private let currentSignature = Data([0xAA, 0xBB, 0xCC, 0xDD])
    private let staleSignature = Data([0x11, 0x22, 0x33, 0x44])
    private var currentVersion: Int { BackupAssetResourcePlanner.currentSelectionVersion }

    private func cache(
        updatedAt: Date? = nil,
        selectionVersion: Int? = nil,
        resourceSignature: Data?? = nil
    ) -> LocalHashIndexTrust.CacheFields {
        LocalHashIndexTrust.CacheFields(
            updatedAt: updatedAt ?? baseUpdatedAt,
            selectionVersion: selectionVersion ?? currentVersion,
            resourceSignature: resourceSignature ?? currentSignature
        )
    }

    private func shape(
        modificationDate: Date? = nil,
        currentResourceSignature: Data? = nil
    ) -> LocalHashIndexTrust.AssetShape {
        LocalHashIndexTrust.AssetShape(
            modificationDate: modificationDate,
            currentResourceSignature: currentResourceSignature ?? currentSignature
        )
    }

    // MARK: canTrust

    func testCanTrustReturnsTrueWhenAllConditionsSatisfied() {
        XCTAssertTrue(LocalHashIndexTrust.canTrust(cache(), for: shape()))
    }

    func testCanTrustReturnsTrueWhenModificationDateIsNil() {
        XCTAssertTrue(LocalHashIndexTrust.canTrust(cache(), for: shape(modificationDate: nil)))
    }

    func testCanTrustReturnsTrueWhenMtimeEqualToUpdatedAt() {
        XCTAssertTrue(LocalHashIndexTrust.canTrust(cache(), for: shape(modificationDate: baseUpdatedAt)))
    }

    func testCanTrustReturnsTrueWhenMtimeBeforeUpdatedAt() {
        let mtime = baseUpdatedAt.addingTimeInterval(-60)
        XCTAssertTrue(LocalHashIndexTrust.canTrust(cache(), for: shape(modificationDate: mtime)))
    }

    func testCanTrustReturnsFalseWhenMtimeAfterUpdatedAt() {
        let mtime = baseUpdatedAt.addingTimeInterval(1)
        XCTAssertFalse(LocalHashIndexTrust.canTrust(cache(), for: shape(modificationDate: mtime)))
    }

    func testCanTrustReturnsFalseWhenSelectionVersionStale() {
        XCTAssertFalse(LocalHashIndexTrust.canTrust(cache(selectionVersion: currentVersion - 1), for: shape()))
    }

    func testCanTrustReturnsTrueWhenSelectionVersionAtCurrent() {
        XCTAssertTrue(LocalHashIndexTrust.canTrust(cache(selectionVersion: currentVersion), for: shape()))
    }

    func testCanTrustReturnsTrueWhenSelectionVersionAboveCurrent() {
        XCTAssertTrue(LocalHashIndexTrust.canTrust(cache(selectionVersion: currentVersion + 1), for: shape()))
    }

    func testCanTrustReturnsFalseWhenResourceSignatureIsNil() {
        XCTAssertFalse(LocalHashIndexTrust.canTrust(cache(resourceSignature: .some(nil)), for: shape()))
    }

    func testCanTrustReturnsFalseWhenResourceSignatureMismatch() {
        XCTAssertFalse(LocalHashIndexTrust.canTrust(cache(resourceSignature: .some(staleSignature)), for: shape()))
    }

    // MARK: cacheFieldsPassCheapChecks

    func testCacheFieldsPassCheapChecksReturnsTrueWhenAllConditionsSatisfied() {
        XCTAssertTrue(LocalHashIndexTrust.cacheFieldsPassCheapChecks(cache(), modificationDate: nil))
        XCTAssertTrue(LocalHashIndexTrust.cacheFieldsPassCheapChecks(cache(), modificationDate: baseUpdatedAt))
    }

    func testCacheFieldsPassCheapChecksReturnsFalseWhenMtimeAfterUpdatedAt() {
        XCTAssertFalse(
            LocalHashIndexTrust.cacheFieldsPassCheapChecks(cache(), modificationDate: baseUpdatedAt.addingTimeInterval(1))
        )
    }

    func testCacheFieldsPassCheapChecksReturnsFalseWhenSelectionVersionStale() {
        XCTAssertFalse(
            LocalHashIndexTrust.cacheFieldsPassCheapChecks(
                cache(selectionVersion: currentVersion - 1),
                modificationDate: nil
            )
        )
    }

    func testCacheFieldsPassCheapChecksReturnsFalseWhenResourceSignatureNil() {
        XCTAssertFalse(
            LocalHashIndexTrust.cacheFieldsPassCheapChecks(
                cache(resourceSignature: .some(nil)),
                modificationDate: nil
            )
        )
    }

    func testCacheFieldsPassCheapChecksIgnoresActualSignatureValue() {
        XCTAssertTrue(
            LocalHashIndexTrust.cacheFieldsPassCheapChecks(
                cache(resourceSignature: .some(staleSignature)),
                modificationDate: nil
            )
        )
    }

    // MARK: signatureMatches

    func testSignatureMatchesIgnoresMtime() {
        let staleMtimeShape = shape(modificationDate: baseUpdatedAt.addingTimeInterval(3600))
        XCTAssertFalse(LocalHashIndexTrust.canTrust(cache(), for: staleMtimeShape))
        XCTAssertTrue(LocalHashIndexTrust.signatureMatches(cache(), currentSignature: currentSignature))
    }

    func testSignatureMatchesReturnsFalseWhenSelectionVersionStale() {
        XCTAssertFalse(
            LocalHashIndexTrust.signatureMatches(
                cache(selectionVersion: currentVersion - 1),
                currentSignature: currentSignature
            )
        )
    }

    func testSignatureMatchesReturnsTrueWhenSelectionVersionAtCurrent() {
        XCTAssertTrue(
            LocalHashIndexTrust.signatureMatches(
                cache(selectionVersion: currentVersion),
                currentSignature: currentSignature
            )
        )
    }

    func testSignatureMatchesReturnsFalseWhenResourceSignatureIsNil() {
        XCTAssertFalse(
            LocalHashIndexTrust.signatureMatches(
                cache(resourceSignature: .some(nil)),
                currentSignature: currentSignature
            )
        )
    }

    func testSignatureMatchesReturnsFalseWhenResourceSignatureMismatch() {
        XCTAssertFalse(
            LocalHashIndexTrust.signatureMatches(
                cache(resourceSignature: .some(staleSignature)),
                currentSignature: currentSignature
            )
        )
    }

    // MARK: trustFields adapter equivalence

    func testTrustFieldsExtensionsAgreeAcrossRecordTypes() {
        let updatedAt = baseUpdatedAt
        let version = currentVersion
        let signature = currentSignature

        let fingerprintRecord = LocalAssetFingerprintRecord(
            fingerprint: TestFixtures.assetFingerprint(0x01),
            updatedAt: updatedAt,
            selectionVersion: version,
            resourceSignature: signature
        )
        let hashCache = LocalAssetHashCache(
            assetFingerprint: TestFixtures.assetFingerprint(0x01),
            resourceCount: 1,
            totalFileSizeBytes: 0,
            updatedAt: updatedAt,
            hashesByRoleSlot: [:],
            selectionVersion: version,
            resourceSignature: signature
        )
        let indexedRow = IndexedAssetRow(
            assetLocalIdentifier: "id",
            assetFingerprint: TestFixtures.assetFingerprint(0x01),
            totalFileSizeBytes: 0,
            updatedAt: updatedAt,
            selectionVersion: version,
            resourceSignature: signature
        )
        let duplicateRow = DuplicateIndexedAssetRow(
            assetLocalIdentifier: "id",
            assetFingerprint: TestFixtures.assetFingerprint(0x01),
            updatedAt: updatedAt,
            selectionVersion: version,
            resourceSignature: signature
        )

        let fields = [
            fingerprintRecord.trustFields,
            hashCache.trustFields,
            indexedRow.trustFields,
            duplicateRow.trustFields,
        ]
        for left in fields {
            for right in fields {
                XCTAssertEqual(left, right)
            }
        }
        for cacheFields in fields {
            XCTAssertTrue(LocalHashIndexTrust.canTrust(cacheFields, for: shape()))
            XCTAssertTrue(LocalHashIndexTrust.signatureMatches(cacheFields, currentSignature: currentSignature))
        }
    }
}
