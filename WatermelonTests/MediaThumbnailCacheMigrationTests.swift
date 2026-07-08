import XCTest
@testable import Watermelon

// Pins the one-time L1 drop: entries stored before remote-derived thumbnail writes were manifest-hash
// gated are undetectable at read time (fingerprint-only keys, no hash metadata), so the first launch
// after the gates must clear the cache once — and only once.
final class MediaThumbnailCacheMigrationTests: XCTestCase {
    private let migrationKey = "com.zizicici.common.migration.browserThumbnailWritersVerified"

    override func setUp() {
        super.setUp()
        MediaThumbnailCache.configureIfNeeded()
    }

    override func tearDown() {
        // Leave the migration in its post-run state so the host app doesn't re-clear on next launch.
        UserDefaults.standard.set(true, forKey: migrationKey)
        super.tearDown()
    }

    private func makeImage() -> UIImage {
        UIGraphicsImageRenderer(size: CGSize(width: 4, height: 4)).image { context in
            UIColor.red.setFill()
            context.fill(CGRect(x: 0, y: 0, width: 4, height: 4))
        }
    }

    // The launch task runs the purge unawaited, so the read path itself must be the barrier: a first
    // read with the migration pending must drop pre-gate entries rather than serve them.
    func testReadPurgesPreGateEntriesWithoutExplicitMigrationCall() async {
        let fingerprint = Data(UUID().uuidString.utf8)
        UserDefaults.standard.removeObject(forKey: migrationKey)

        MediaThumbnailCache.store(makeImage(), for: fingerprint)
        let firstRead = await MediaThumbnailCache.cached(for: fingerprint)
        XCTAssertNil(firstRead, "a read must not serve pre-gate entries before the one-time purge")
        XCTAssertTrue(UserDefaults.standard.bool(forKey: migrationKey))

        MediaThumbnailCache.store(makeImage(), for: fingerprint)
        let secondRead = await MediaThumbnailCache.cached(for: fingerprint)
        XCTAssertNotNil(secondRead, "post-migration reads must serve gated entries")
    }

    func testPurgeDropsPreGateEntriesExactlyOnce() async {
        let fingerprint = Data(UUID().uuidString.utf8)
        UserDefaults.standard.removeObject(forKey: migrationKey)

        MediaThumbnailCache.store(makeImage(), for: fingerprint)
        await MediaThumbnailCache.purgeUnverifiedLegacyEntriesIfNeeded()
        let afterFirstRun = await MediaThumbnailCache.cached(for: fingerprint)
        XCTAssertNil(afterFirstRun, "pre-gate entries must be dropped on the first run")

        MediaThumbnailCache.store(makeImage(), for: fingerprint)
        await MediaThumbnailCache.purgeUnverifiedLegacyEntriesIfNeeded()
        let afterSecondRun = await MediaThumbnailCache.cached(for: fingerprint)
        XCTAssertNotNil(afterSecondRun, "the drop must be one-time; later runs keep gated entries")
    }
}
