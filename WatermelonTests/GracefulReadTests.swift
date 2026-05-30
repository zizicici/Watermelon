import Foundation
import XCTest
@testable import Watermelon

/// Arch-VII A-I: GracefulRead must reproduce the hand-written read-after-write loop semantics.
final class GracefulReadTests: XCTestCase {

    private func zeroGrace() -> InMemoryRemoteStorageClient {
        InMemoryRemoteStorageClient(graceSeconds: 0)
    }
    private func withGrace() -> InMemoryRemoteStorageClient {
        InMemoryRemoteStorageClient(graceSeconds: 1)
    }

    func testZeroGraceAbsenceIsGenuineOnFirstMiss() async throws {
        var attempts = 0
        let result: GracefulReadResult<Int> = try await GracefulRead.read(
            client: zeroGrace(), floorSeconds: 0
        ) {
            attempts += 1
            return nil
        }
        XCTAssertEqual(attempts, 1, "zero-grace must not retry")
        if case .absent(let kind) = result { XCTAssertEqual(kind, .genuinelyAbsent) }
        else { XCTFail("expected absent") }
    }

    func testFoundOnFirstAttemptReturnsValue() async throws {
        let result: GracefulReadResult<Int> = try await GracefulRead.read(
            client: withGrace(), floorSeconds: 0
        ) { 42 }
        XCTAssertEqual(result.value, 42)
    }

    func testVisibilityLagThenFoundWithinGrace() async throws {
        var attempts = 0
        let result: GracefulReadResult<Int> = try await GracefulRead.read(
            client: withGrace(), floorSeconds: 1, pollIntervalMs: 50
        ) {
            attempts += 1
            return attempts >= 2 ? 7 : nil
        }
        XCTAssertEqual(result.value, 7)
        XCTAssertGreaterThanOrEqual(attempts, 2)
    }

    func testGraceDeadlineExhaustedYieldsGenuinelyAbsent() async throws {
        let result: GracefulReadResult<Int> = try await GracefulRead.read(
            client: withGrace(), floorSeconds: 0, pollIntervalMs: 50
        ) { nil }
        if case .absent(let kind) = result { XCTAssertEqual(kind, .genuinelyAbsent) }
        else { XCTFail("expected genuinelyAbsent after deadline") }
    }

    func testNonNotFoundErrorPropagatesFromFirstAttempt() async {
        struct Boom: Error {}
        do {
            _ = try await GracefulRead.read(client: withGrace(), floorSeconds: 0) { () -> Int? in
                throw Boom()
            }
            XCTFail("non-not-found error must propagate")
        } catch is Boom {
        } catch {
            XCTFail("unexpected error \(error)")
        }
    }

    func testNotFoundErrorOnFirstAttemptTreatedAsAbsence() async throws {
        let result: GracefulReadResult<Int> = try await GracefulRead.read(
            client: zeroGrace(), floorSeconds: 0,
            isNotFound: { _ in true }
        ) { () -> Int? in
            throw NSError(domain: "x", code: 1)
        }
        if case .absent = result {} else { XCTFail("not-found error must be absence") }
    }

    func testRetryWithinGraceZeroGraceAttemptsOnce() async throws {
        var attempts = 0
        let value: Int? = try await GracefulRead.retryWithinGrace(client: zeroGrace(), floorSeconds: 0) {
            attempts += 1
            return nil
        }
        XCTAssertNil(value)
        XCTAssertEqual(attempts, 1)
    }

    func testRetryWithinGraceReturnsFirstNonNil() async throws {
        var attempts = 0
        let value: Int? = try await GracefulRead.retryWithinGrace(
            client: withGrace(), floorSeconds: 1, backoff: .fixed(ms: 50)
        ) {
            attempts += 1
            return attempts >= 2 ? 9 : nil
        }
        XCTAssertEqual(value, 9)
    }

    func testRetryWithinGracePropagatesThrownError() async {
        struct Boom: Error {}
        do {
            _ = try await GracefulRead.retryWithinGrace(client: withGrace(), floorSeconds: 0) { () -> Int? in
                throw Boom()
            }
            XCTFail("retryWithinGrace must propagate attempt errors")
        } catch is Boom {
        } catch {
            XCTFail("unexpected error \(error)")
        }
    }

    func testExponentialBackoffDelaySchedule() {
        let b = GracefulRead.Backoff.exponential(baseMs: 200, maxShift: 3)
        XCTAssertEqual(b.delayMs(attempt: 0), 200)
        XCTAssertEqual(b.delayMs(attempt: 1), 400)
        XCTAssertEqual(b.delayMs(attempt: 2), 800)
        XCTAssertEqual(b.delayMs(attempt: 3), 1600)
        XCTAssertEqual(b.delayMs(attempt: 9), 1600, "capped at maxShift doublings")
    }

    func testFixedBackoffDelaySchedule() {
        let b = GracefulRead.Backoff.fixed(ms: 150)
        XCTAssertEqual(b.delayMs(attempt: 0), 150)
        XCTAssertEqual(b.delayMs(attempt: 5), 150)
    }

    func testAbsenceKindIsExhaustive() {
        for kind in [AbsenceKind.genuinelyAbsent, .visibilityLag] {
            switch kind {
            case .genuinelyAbsent, .visibilityLag: break
            }
        }
    }
}
