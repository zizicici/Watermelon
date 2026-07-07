import XCTest
@testable import Watermelon

// The runtime probe that decides whether a backend's MOVE is independent, exercised against the in-memory
// client's alias model (which mirrors 123pan-style content aliasing).
final class RemoteMoveIndependenceProbeTests: XCTestCase {
    private let basePath = "/photos"

    func testProbeClassifiesIndependentMoveBackend() async {
        let client = InMemoryRemoteStorageClient()
        let nonIndependent = await RemoteMoveIndependenceProbe.detectNonIndependentMove(client: client, basePath: basePath)
        XCTAssertFalse(nonIndependent, "an independent-MOVE backend probes as independent")
    }

    func testProbeClassifiesAliasingMoveBackendAsNonIndependent() async {
        let client = InMemoryRemoteStorageClient(moveMayNotBeIndependent: true)
        let nonIndependent = await RemoteMoveIndependenceProbe.detectNonIndependentMove(client: client, basePath: basePath)
        XCTAssertTrue(nonIndependent, "deleting the moved-from source destroys the destination → non-independent")
    }

    func testProbeFailsSafeToNonIndependentOnFault() async {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueMoveError(RemoteErrorFixtures.retryable)
        let nonIndependent = await RemoteMoveIndependenceProbe.detectNonIndependentMove(client: client, basePath: basePath)
        XCTAssertTrue(nonIndependent, "an inconclusive probe fails safe to non-independent (direct publish)")
    }

    func testProbeFailsSafeToNonIndependentWhenSourceDeleteFaults() async {
        let client = InMemoryRemoteStorageClient(moveMayNotBeIndependent: true)
        await client.enqueueDeleteError(RemoteErrorFixtures.retryable)   // source delete faults before applying
        let nonIndependent = await RemoteMoveIndependenceProbe.detectNonIndependentMove(client: client, basePath: basePath)
        XCTAssertTrue(nonIndependent, "a source-delete fault must fail safe — the surviving alias is not proof of independence")
    }

    func testProbeIgnoresConfirmedNotFoundOnSourceDelete() async {
        let client = InMemoryRemoteStorageClient()   // independent
        await client.enqueueDeleteError(RemoteErrorFixtures.notFound)   // source already gone after an independent move
        let nonIndependent = await RemoteMoveIndependenceProbe.detectNonIndependentMove(client: client, basePath: basePath)
        XCTAssertFalse(nonIndependent, "a confirmed not-found on the source delete is ignorable — still independent")
    }

    // A good WebDAV/NAS with independent MOVE that rejects dot-prefixed files must NOT be misclassified: the probe
    // scratch is non-dot, so the probe still completes and reports independent (no needless direct-PUT downgrade).
    func testProbeIsIndependentOnDotRejectingBackendWithIndependentMove() async {
        let client = InMemoryRemoteStorageClient()   // independent MOVE
        await client.rejectDotPrefixedFiles()
        let nonIndependent = await RemoteMoveIndependenceProbe.detectNonIndependentMove(client: client, basePath: basePath)
        XCTAssertFalse(nonIndependent, "a dot-file-rejecting but independent-MOVE backend must probe as independent")
    }

    func testProbeCleansUpItsScratchFiles() async {
        let client = InMemoryRemoteStorageClient()
        _ = await RemoteMoveIndependenceProbe.detectNonIndependentMove(client: client, basePath: basePath)
        let repoDir = RepoLayoutLite.repoDirectoryPath(basePath: basePath)
        let leftovers = ((try? await client.list(path: repoDir)) ?? []).filter { $0.name.hasPrefix("movecheck_") }
        XCTAssertTrue(leftovers.isEmpty, "the probe removes its scratch files")
    }

    func testProbeScratchMatcherIsRestrictedToUUIDShapedSrcDst() {
        let uuid = UUID().uuidString
        XCTAssertTrue(RepoLayoutLite.isMoveProbeScratchFileName("movecheck_\(uuid).src"))
        XCTAssertTrue(RepoLayoutLite.isMoveProbeScratchFileName("movecheck_\(uuid).dst"))
        XCTAssertFalse(RepoLayoutLite.isMoveProbeScratchFileName("movecheck_foreign"), "a non-probe shape must stay an unknown child")
        XCTAssertFalse(RepoLayoutLite.isMoveProbeScratchFileName("movecheck_\(uuid).txt"))
        XCTAssertFalse(RepoLayoutLite.isMoveProbeScratchFileName("movecheck_notauuid.src"))
        XCTAssertFalse(RepoLayoutLite.isMoveProbeScratchFileName(".movecheck_\(uuid).src"), "the probe scratch is non-dot; a dot-prefixed name is foreign")
    }
}
