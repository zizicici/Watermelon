import XCTest
@testable import Watermelon

final class RestoredAssetFingerprintVerifierTests: XCTestCase {
    private let assetID = PhotoKitLocalIdentifier(rawValue: "ABC-123/L0/001")

    func testHappyPath_readyOnFirstAttempt_returnsTrue() async throws {
        let fingerprint = TestFixtures.assetFingerprint(0x01)
        let buildCalls = Counter()
        let fetchCalls = Counter()

        let verifier = RestoredAssetFingerprintVerifier(
            buildIndex: { [assetID] ids in
                buildCalls.bump()
                return Self.makeBuildResult(ready: ids.contains(assetID) ? [assetID] : [])
            },
            fetchRecords: { [assetID] _ in
                fetchCalls.bump()
                return [assetID: Self.makeRecord(fingerprint: fingerprint)]
            },
            delays: []
        )

        let verified = try await verifier.verifyDurableBinding(
            assetLocalIdentifier: assetID,
            expectedFingerprint: fingerprint
        )

        XCTAssertTrue(verified)
        XCTAssertEqual(buildCalls.value, 1)
        XCTAssertEqual(fetchCalls.value, 1)
    }

    func testEventuallyReady_returnsTrueAfterRetries() async throws {
        let fingerprint = TestFixtures.assetFingerprint(0xAA)
        let buildCalls = Counter()
        let fetchCalls = Counter()

        let verifier = RestoredAssetFingerprintVerifier(
            buildIndex: { [assetID] _ in
                let n = buildCalls.bump()
                let ready: Set<PhotoKitLocalIdentifier> = (n >= 3) ? [assetID] : []
                return Self.makeBuildResult(ready: ready)
            },
            fetchRecords: { [assetID] _ in
                fetchCalls.bump()
                return [assetID: Self.makeRecord(fingerprint: fingerprint)]
            },
            delays: [.zero, .zero, .zero, .zero]
        )

        let verified = try await verifier.verifyDurableBinding(
            assetLocalIdentifier: assetID,
            expectedFingerprint: fingerprint
        )

        XCTAssertTrue(verified)
        XCTAssertEqual(buildCalls.value, 3)
        XCTAssertEqual(fetchCalls.value, 1)
    }

    func testReadyButFingerprintMismatch_exhaustsRetryBudget() async throws {
        let expected = TestFixtures.assetFingerprint(0x01)
        let actual = TestFixtures.assetFingerprint(0x02)
        let buildCalls = Counter()
        let fetchCalls = Counter()
        let delays: [Duration] = [.zero, .zero, .zero]

        let verifier = RestoredAssetFingerprintVerifier(
            buildIndex: { [assetID] _ in
                buildCalls.bump()
                return Self.makeBuildResult(ready: [assetID])
            },
            fetchRecords: { [assetID] _ in
                fetchCalls.bump()
                return [assetID: Self.makeRecord(fingerprint: actual)]
            },
            delays: delays
        )

        let verified = try await verifier.verifyDurableBinding(
            assetLocalIdentifier: assetID,
            expectedFingerprint: expected
        )

        XCTAssertFalse(verified)
        XCTAssertEqual(buildCalls.value, delays.count + 1)
        XCTAssertEqual(fetchCalls.value, delays.count + 1)
    }

    func testReadyButTransientMismatch_eventuallyMatchesAfterRetries() async throws {
        let expected = TestFixtures.assetFingerprint(0x01)
        let transient = TestFixtures.assetFingerprint(0x02)
        let buildCalls = Counter()
        let fetchCalls = Counter()

        let verifier = RestoredAssetFingerprintVerifier(
            buildIndex: { [assetID] _ in
                buildCalls.bump()
                return Self.makeBuildResult(ready: [assetID])
            },
            fetchRecords: { [assetID] _ in
                let n = fetchCalls.bump()
                return [assetID: Self.makeRecord(fingerprint: n >= 3 ? expected : transient)]
            },
            delays: [.zero, .zero, .zero, .zero]
        )

        let verified = try await verifier.verifyDurableBinding(
            assetLocalIdentifier: assetID,
            expectedFingerprint: expected
        )

        XCTAssertTrue(verified)
        XCTAssertGreaterThanOrEqual(buildCalls.value, 3)
        XCTAssertGreaterThanOrEqual(fetchCalls.value, 3)
    }

    func testReadyButRecordMissing_exhaustsRetryBudget() async throws {
        let buildCalls = Counter()
        let fetchCalls = Counter()
        let delays: [Duration] = [.zero, .zero]

        let verifier = RestoredAssetFingerprintVerifier(
            buildIndex: { [assetID] _ in
                buildCalls.bump()
                return Self.makeBuildResult(ready: [assetID])
            },
            fetchRecords: { _ in
                fetchCalls.bump()
                return [:]
            },
            delays: delays
        )

        let verified = try await verifier.verifyDurableBinding(
            assetLocalIdentifier: assetID,
            expectedFingerprint: TestFixtures.assetFingerprint(0xFF)
        )

        XCTAssertFalse(verified)
        XCTAssertEqual(buildCalls.value, delays.count + 1)
        XCTAssertEqual(fetchCalls.value, delays.count + 1)
    }

    func testBudgetExhausted_returnsFalseAfterAllAttempts() async throws {
        let buildCalls = Counter()
        let fetchCalls = Counter()
        let delays: [Duration] = [.zero, .zero, .zero]

        let verifier = RestoredAssetFingerprintVerifier(
            buildIndex: { _ in
                buildCalls.bump()
                return Self.makeBuildResult(ready: [])
            },
            fetchRecords: { _ in
                fetchCalls.bump()
                return [:]
            },
            delays: delays
        )

        let verified = try await verifier.verifyDurableBinding(
            assetLocalIdentifier: assetID,
            expectedFingerprint: TestFixtures.assetFingerprint(0x01)
        )

        XCTAssertFalse(verified)
        XCTAssertEqual(buildCalls.value, delays.count + 1)
        XCTAssertEqual(fetchCalls.value, 0)
    }

    /// Post-import cancellation must NOT collapse the settle retry: the asset is already in Photos,
    /// so the bounded retry has to run until the durable binding is written (or the budget is spent).
    func testCancellation_doesNotAbortSettleRetry_writesBindingWhenReadyOnLaterAttempt() async throws {
        let fingerprint = TestFixtures.assetFingerprint(0x55)
        let buildCalls = Counter()
        let fetchCalls = Counter()

        // Not ready on attempt 1 (PhotoKit settle window), ready on attempt 2.
        let verifier = RestoredAssetFingerprintVerifier(
            buildIndex: { [assetID] _ in
                let n = buildCalls.bump()
                return Self.makeBuildResult(ready: n >= 2 ? [assetID] : [])
            },
            fetchRecords: { [assetID] _ in
                fetchCalls.bump()
                return [assetID: Self.makeRecord(fingerprint: fingerprint)]
            },
            delays: [.zero, .zero]
        )

        let assetID = assetID
        let task = Task<Bool, Error> {
            try await verifier.verifyDurableBinding(
                assetLocalIdentifier: assetID,
                expectedFingerprint: fingerprint
            )
        }
        task.cancel()

        let verified = try await task.value
        XCTAssertTrue(verified,
                      "cancellation after import must not prevent the durable binding once the asset settles")
        XCTAssertGreaterThanOrEqual(buildCalls.value, 2)
        XCTAssertEqual(fetchCalls.value, 1)
    }

    /// When the asset never becomes ready, a cancelled verify still exhausts the bounded budget and
    /// returns false rather than throwing — the caller surfaces cancellation, not a torn retry.
    func testCancellation_neverReady_exhaustsBudgetWithoutThrowing() async throws {
        let buildCalls = Counter()
        let delays: [Duration] = [.zero, .zero]
        let verifier = RestoredAssetFingerprintVerifier(
            buildIndex: { _ in
                buildCalls.bump()
                return Self.makeBuildResult(ready: [])
            },
            fetchRecords: { _ in [:] },
            delays: delays
        )

        let assetID = assetID
        let task = Task<Bool, Error> {
            try await verifier.verifyDurableBinding(
                assetLocalIdentifier: assetID,
                expectedFingerprint: TestFixtures.assetFingerprint(0x00)
            )
        }
        task.cancel()

        let verified = try await task.value
        XCTAssertFalse(verified)
        XCTAssertEqual(buildCalls.value, delays.count + 1,
                       "every bounded attempt must run despite cancellation")
    }

    func testDefaultDelays_matchProductionConstants() {
        XCTAssertEqual(
            RestoredAssetFingerprintVerifier.defaultDelays,
            [
                .milliseconds(500),
                .milliseconds(750),
                .milliseconds(1_000),
                .milliseconds(1_500),
                .milliseconds(2_000),
                .milliseconds(2_500),
                .milliseconds(3_000)
            ]
        )
    }

    private static func makeBuildResult(ready: Set<PhotoKitLocalIdentifier>) -> LocalHashIndexBuildResult {
        LocalHashIndexBuildResult(
            requestedAssetIDs: ready,
            readyAssetIDs: ready,
            unavailableAssetIDs: [],
            failedAssetIDs: [],
            missingAssetIDs: []
        )
    }

    private static func makeRecord(fingerprint: AssetFingerprint) -> LocalAssetFingerprintRecord {
        LocalAssetFingerprintRecord(
            fingerprint: fingerprint,
            updatedAt: Date(timeIntervalSince1970: 0),
            selectionVersion: 1,
            resourceSignature: nil
        )
    }
}

private final class Counter: @unchecked Sendable {
    private let lock = NSLock()
    private var n = 0

    @discardableResult
    func bump() -> Int {
        lock.lock()
        defer { lock.unlock() }
        n += 1
        return n
    }

    var value: Int {
        lock.lock()
        defer { lock.unlock() }
        return n
    }
}
