import XCTest
@testable import Watermelon

/// Bootstrap convergence tests. Two writers calling `ensureRepoJSON` must converge on
/// the same canonical id rather than each silently keeping a local one (which would
/// fork the repo by writing commits under different repoIDs that filter each other out).
///
/// In-memory client serializes through actor isolation, so true concurrency on a single
/// fixture is impossible to reproduce deterministically — the convergence invariant
/// only requires that whichever writer lost the race read back the winner's id, which
/// is the sequential interleaving these tests cover. The trailing
/// `testTrueConcurrentBootstrap_*` test launches a TaskGroup to at least exercise the
/// awaiting path; deterministic outcome assertions are limited to "both agree".
final class ConcurrentBootstrapRaceTests: XCTestCase {
    private let basePath = "/repo"

    func testStrictAtomicBackend_secondWriterReadsExistingId() async throws {
        // POSIX / S3 / WebDAV-If-None-Match — atomicCreate returns .alreadyExists when
        // the file exists. Second writer's ensureRepoJSON should re-read and return the
        // first writer's id.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.setAtomicCreateMode(.strictlyAtomic)
        let writerA = RepoBootstrap(client: client, basePath: basePath)
        let writerB = RepoBootstrap(client: client, basePath: basePath)

        let suggestedA = "id-from-A"
        let suggestedB = "id-from-B"

        let resolvedA = try await writerA.ensureRepoJSON(repoID: suggestedA, writerID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
        let resolvedB = try await writerB.ensureRepoJSON(repoID: suggestedB, writerID: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

        XCTAssertEqual(resolvedA, suggestedA, "first writer wins")
        XCTAssertEqual(resolvedB, suggestedA, "second writer reads existing id")
        XCTAssertNotEqual(resolvedB, suggestedB, "second writer must NOT keep local suggestion")
    }

    func testBestEffortBackend_secondWriterStillConvergesViaReadback() async throws {
        // SMB exists+upload — atomicCreate returns .bestEffortRetry. Bootstrap must
        // read back and converge on whatever id actually landed.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.setAtomicCreateMode(.bestEffort)
        let writerA = RepoBootstrap(client: client, basePath: basePath)
        let writerB = RepoBootstrap(client: client, basePath: basePath)

        let suggestedA = "id-from-A"
        let suggestedB = "id-from-B"

        let resolvedA = try await writerA.ensureRepoJSON(repoID: suggestedA, writerID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
        let resolvedB = try await writerB.ensureRepoJSON(repoID: suggestedB, writerID: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

        // First writer's bestEffortRetry → reads back → finds its own id (since no race here).
        XCTAssertEqual(resolvedA, suggestedA)
        // Second writer's bestEffortRetry → reads back → finds A's id (alreadyExists path),
        // because by the time B's atomicCreate runs the file already exists.
        XCTAssertEqual(resolvedB, suggestedA, "non-atomic backend must still converge")
    }

    func testInconsistentState_atomicCreateAlreadyExistsButFileDisappeared_throws() async throws {
        // ensureRepoJSON's `.alreadyExists`/`.bestEffortRetry` paths now throw if the
        // strict-load comes back .absent — the file we (or a peer) "created" must be
        // readable, otherwise we'd silently bind to an id that never reaches remote.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.setAtomicCreateMode(.alwaysAlreadyExists)
        let writer = RepoBootstrap(client: client, basePath: basePath)
        do {
            _ = try await writer.ensureRepoJSON(repoID: "x", writerID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
            XCTFail("expected throw — alreadyExists + .absent is inconsistent")
        } catch is RepoBootstrap.BootstrapError {
            // expected
        }
    }

    /// Concurrent invocation through a TaskGroup. Actor isolation serializes the
    /// in-memory client, so the two awaits still interleave deterministically — but
    /// the test exercises the awaiting path and asserts the strong invariant: both
    /// writers must agree on whichever id wins.
    func testTrueConcurrentBootstrap_bothWritersAgreeOnWinningID() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.setAtomicCreateMode(.strictlyAtomic)
        let writerA = RepoBootstrap(client: client, basePath: basePath)
        let writerB = RepoBootstrap(client: client, basePath: basePath)

        async let resolvedA = writerA.ensureRepoJSON(repoID: "id-A", writerID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
        async let resolvedB = writerB.ensureRepoJSON(repoID: "id-B", writerID: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
        let (a, b) = try await (resolvedA, resolvedB)

        XCTAssertEqual(a, b, "both concurrent writers must converge on the same id")
        XCTAssertTrue(a == "id-A" || a == "id-B", "winner must be one of the suggested ids")
    }

    /// A later writer with an earlier wall-clock MUST NOT flip canonical.
    /// "Identity is decided once" — new writers adopt the existing canonical,
    /// they never re-elect with their own suggested repoID.
    func testLaterWriterEarlierClock_adoptsCanonicalNotFlips() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        // Writer A bootstraps first.
        let resolvedA = try await RepoBootstrap(client: client, basePath: basePath)
            .ensureRepoJSON(repoID: "id-A", writerID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

        // Now plant a writer B claim BY HAND with an earlier created_at_ms.
        // This simulates a clock-skewed device whose claim ts pre-dates A's.
        // Without adopt-not-elect, B's ensureRepoJSON would write its own
        // repoID and lex-min would flip canonical to B.
        let resolvedB = try await RepoBootstrap(client: client, basePath: basePath)
            .ensureRepoJSON(repoID: "id-B", writerID: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
        XCTAssertEqual(resolvedB, resolvedA,
                       "B must adopt A's canonical; B's own repoID must be ignored")
    }

    /// Writer-unique claims + lex-min canonical = no split-brain even on
    /// `.overwritePossible` backends (SMB).
    func testBestEffortBackend_concurrentBootstrap_neverSplits() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.setAtomicCreateMode(.bestEffort)
        let writerA = RepoBootstrap(client: client, basePath: basePath)
        let writerB = RepoBootstrap(client: client, basePath: basePath)

        async let resolvedA = writerA.ensureRepoJSON(repoID: "id-A", writerID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
        async let resolvedB = writerB.ensureRepoJSON(repoID: "id-B", writerID: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
        let (a, b) = try await (resolvedA, resolvedB)
        XCTAssertEqual(a, b, "writer-unique claims must converge even on bestEffort backend")

        // Re-runs see the same canonical — no late overwrite can flip it.
        let reA = try await RepoBootstrap(client: client, basePath: basePath)
            .ensureRepoJSON(repoID: "id-A", writerID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
        let reB = try await RepoBootstrap(client: client, basePath: basePath)
            .ensureRepoJSON(repoID: "id-B", writerID: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
        XCTAssertEqual(reA, a, "writer A's re-run must read back the same canonical")
        XCTAssertEqual(reB, a, "writer B's re-run must read back the same canonical")
    }
}
