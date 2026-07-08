import XCTest
@testable import Watermelon

// The single presence derivation every source, the viewer, and upload success now share. (The index's
// refresh/profile-gating touches a real hash index + snapshot and is manually regressed.)
final class LibraryPresenceIndexTests: XCTestCase {
    func testPresenceDerivation() {
        XCTAssertEqual(MediaPresence.of(onDevice: true, onRemote: true), .both)
        XCTAssertEqual(MediaPresence.of(onDevice: true, onRemote: false), .localOnly)
        XCTAssertEqual(MediaPresence.of(onDevice: false, onRemote: true), .remoteOnly)
        // Neither side present is degenerate (such an item shouldn't exist); it resolves to remoteOnly
        // because there is no local handle to prefer.
        XCTAssertEqual(MediaPresence.of(onDevice: false, onRemote: false), .remoteOnly)
    }

    func testStaleHashRowRule() {
        // Home's staleness rule, now shared by the browser: a row older than the asset's Photos edit no
        // longer fingerprints the current bytes — the item must not read backed-up nor render as that
        // fingerprint (shared L1 / L2 sidecar poisoning).
        let rowAt = Date(timeIntervalSince1970: 1000)
        XCTAssertTrue(LibraryPresenceIndex.isRowCurrent(recordUpdatedAt: rowAt, assetModificationDate: nil))
        XCTAssertTrue(LibraryPresenceIndex.isRowCurrent(recordUpdatedAt: rowAt, assetModificationDate: Date(timeIntervalSince1970: 900)))
        XCTAssertTrue(LibraryPresenceIndex.isRowCurrent(recordUpdatedAt: rowAt, assetModificationDate: rowAt))
        XCTAssertFalse(LibraryPresenceIndex.isRowCurrent(recordUpdatedAt: rowAt, assetModificationDate: Date(timeIntervalSince1970: 1001)))
    }

    func testCurrentFingerprintsDropRules() {
        // The batch validator behind remote/merged handle binding and the upload success verdicts: a stale
        // row must not bind a device handle to its pre-edit fingerprint (`.both` projection, local-first
        // full-size/share, Delete-from-Device) nor count a skipped upload as backed up; an unfetchable
        // asset (deleted, or outside a limited-access selection) proves nothing and is dropped too.
        let fp1 = Data([0x01]), fp2 = Data([0x02])
        let rowAt = Date(timeIntervalSince1970: 1000)
        let records = [
            "current": LocalAssetFingerprintRecord(fingerprint: fp1, updatedAt: rowAt),
            "stale": LocalAssetFingerprintRecord(fingerprint: fp2, updatedAt: rowAt),
            "unfetched": LocalAssetFingerprintRecord(fingerprint: fp1, updatedAt: rowAt),
            "noModificationDate": LocalAssetFingerprintRecord(fingerprint: fp2, updatedAt: rowAt),
        ]
        let modificationDates: [String: Date?] = [
            "current": Date(timeIntervalSince1970: 900),
            "stale": Date(timeIntervalSince1970: 1001),
            "noModificationDate": nil,
            // "unfetched" absent: PHAsset fetch miss
        ]
        let result = LibraryPresenceIndex.currentFingerprints(records: records, modificationDateByAssetID: modificationDates)
        XCTAssertEqual(result, ["current": fp1, "noModificationDate": fp2])
    }

    func testSelectCurrentHandlesFallback() {
        // The reverse map keeps one arbitrary row per fingerprint. After downloading an edited-after-backup
        // asset back, the older stale row can be the map's candidate while the fresh import's current row
        // exists — the validator must bind the current row instead of dropping the handle (else the record
        // keeps offering Download for bytes already on device and every re-tap imports a duplicate).
        let shadowed = Data([0x0A]), healthy = Data([0x0B]), gone = Data([0x0C]), moved = Data([0x0D])
        let mapHits = [shadowed: "staleOld", healthy: "ok", gone: "staleOnly", moved: "rehashed"]
        let alternatives = [
            shadowed: ["freshImport", "staleTwin"],
            gone: ["alsoStale"],
        ]
        let current = [
            "ok": healthy,           // candidate row current → candidate wins
            "freshImport": shadowed, // candidate stale, alternative current → alternative binds
            "rehashed": Data([0x0E]),   // row moved to another fingerprint → candidate identity check fails
            // "staleOld"/"staleTwin"/"alsoStale" absent: stale or unfetchable
        ]
        let result = LibraryPresenceIndex.selectCurrentHandles(
            mapHits: mapHits,
            alternativesByFingerprint: alternatives,
            currentFingerprintsByAssetID: current
        )
        XCTAssertEqual(result, [healthy: "ok", shadowed: "freshImport"])
    }
}
