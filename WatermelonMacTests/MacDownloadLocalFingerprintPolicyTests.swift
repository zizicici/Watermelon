import Foundation
import XCTest
@testable import WatermelonMac

final class MacDownloadLocalFingerprintPolicyTests: XCTestCase {
    func testDeletedAndEditedAssetsDoNotSuppressRemoteRestore() {
        let indexedAt = Date(timeIntervalSince1970: 1_000)
        let current = Data([0x01])
        let deleted = Data([0x02])
        let edited = Data([0x03])
        let records = [
            "current": LocalAssetFingerprintRecord(
                fingerprint: current,
                updatedAt: indexedAt
            ),
            "deleted": LocalAssetFingerprintRecord(
                fingerprint: deleted,
                updatedAt: indexedAt
            ),
            "edited": LocalAssetFingerprintRecord(
                fingerprint: edited,
                updatedAt: indexedAt
            ),
        ]
        let snapshots = [
            makeSnapshot(
                id: "current",
                modificationDate: indexedAt
            ),
            makeSnapshot(
                id: "edited",
                modificationDate: indexedAt.addingTimeInterval(1)
            ),
        ]

        XCTAssertEqual(
            MacDownloadLocalFingerprintPolicy.freshFingerprints(
                snapshots: snapshots,
                records: records
            ),
            [current]
        )
    }

    func testAssetWithoutModificationDateKeepsIndexedFingerprint() {
        let fingerprint = Data([0x04])
        let records = [
            "asset": LocalAssetFingerprintRecord(
                fingerprint: fingerprint,
                updatedAt: Date(timeIntervalSince1970: 1_000)
            ),
        ]

        XCTAssertEqual(
            MacDownloadLocalFingerprintPolicy.freshFingerprints(
                snapshots: [
                    makeSnapshot(
                        id: "asset",
                        modificationDate: nil
                    ),
                ],
                records: records
            ),
            [fingerprint]
        )
    }

    private func makeSnapshot(
        id: String,
        modificationDate: Date?
    ) -> LibraryAssetSnapshot {
        LibraryAssetSnapshot(
            localIdentifier: id,
            creationDate: nil,
            modificationDate: modificationDate,
            mediaKind: .photo
        )
    }
}
