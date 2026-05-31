import XCTest
@testable import Watermelon

final class BackupErrorChainTests: XCTestCase {

    // MARK: - Traversal coverage

    func testWalkVisitsRootOnlyForUnwrappableError() {
        let root = NSError(domain: "Unwrappable", code: 1)
        var visited: [String] = []
        BackupErrorChain.walk(root) { node in
            visited.append(describe(node))
            return .continue
        }
        XCTAssertEqual(visited.count, 1)
    }

    func testWalkDescendsIntoV2MonthSessionFlushError() {
        let inner = CancellationError()
        let root = V2MonthSession.FlushError.postCommitFailed(underlying: inner)
        var kinds: [String] = []
        BackupErrorChain.walk(root) { node in
            kinds.append(describe(node))
            return .continue
        }
        XCTAssertEqual(kinds, ["FlushError", "CancellationError"])
    }

    func testWalkDescendsIntoSnapshotWriterIoFailure() {
        let leaf = NSError(domain: "leaf", code: 1)
        let root = SnapshotWriter.WriteError.ioFailure(leaf)
        var nodeCount = 0
        BackupErrorChain.walk(root) { _ in
            nodeCount += 1
            return .continue
        }
        XCTAssertEqual(nodeCount, 2)
    }

    func testWalkDescendsIntoSnapshotWriterFinalizationFailed() {
        let leaf = NSError(domain: "leaf", code: 1)
        let root = SnapshotWriter.WriteError.finalizationFailed(leaf)
        var nodeCount = 0
        BackupErrorChain.walk(root) { _ in
            nodeCount += 1
            return .continue
        }
        XCTAssertEqual(nodeCount, 2)
    }

    func testWalkSkipsSnapshotWriterVerificationFailed() {
        let root = SnapshotWriter.WriteError.verificationFailed(IntegrityResult.ok)
        var nodeCount = 0
        BackupErrorChain.walk(root) { _ in
            nodeCount += 1
            return .continue
        }
        XCTAssertEqual(nodeCount, 1, "verificationFailed must be terminal — IntegrityResult is not an Error wrapper")
    }

    func testWalkDescendsIntoCommitLogWriterIoFailure() {
        let leaf = NSError(domain: "leaf", code: 1)
        let root = CommitLogWriter.WriteError.ioFailure(leaf)
        var nodeCount = 0
        BackupErrorChain.walk(root) { _ in
            nodeCount += 1
            return .continue
        }
        XCTAssertEqual(nodeCount, 2)
    }

    func testWalkSkipsCommitLogWriterAlreadyExistsAndEncodingFailed() {
        var nodeCount = 0
        BackupErrorChain.walk(CommitLogWriter.WriteError.alreadyExists) { _ in
            nodeCount += 1
            return .continue
        }
        XCTAssertEqual(nodeCount, 1)

        nodeCount = 0
        let encoding = CommitLogWriter.WriteError.encodingFailed(NSError(domain: "encode", code: 1))
        BackupErrorChain.walk(encoding) { _ in
            nodeCount += 1
            return .continue
        }
        XCTAssertEqual(nodeCount, 1, "encodingFailed is terminal even though it carries an Error — by current design")
    }

    func testWalkDescendsIntoMetadataCreateGateStagingVerificationFailedWhenUnderlyingNonNil() {
        let leaf = NSError(domain: "leaf", code: 1)
        let root = MetadataCreateGate.Error.stagingVerificationFailed(remotePath: "/x", underlying: leaf)
        var nodeCount = 0
        BackupErrorChain.walk(root) { _ in
            nodeCount += 1
            return .continue
        }
        XCTAssertEqual(nodeCount, 2)
    }

    func testWalkSkipsMetadataCreateGateStagingVerificationFailedWhenUnderlyingNil() {
        let root = MetadataCreateGate.Error.stagingVerificationFailed(remotePath: "/x", underlying: nil)
        var nodeCount = 0
        BackupErrorChain.walk(root) { _ in
            nodeCount += 1
            return .continue
        }
        XCTAssertEqual(nodeCount, 1)
    }

    func testWalkSkipsMetadataCreateGateNonExclusiveFinalization() {
        let root = MetadataCreateGate.Error.nonExclusiveFinalization(remotePath: "/x")
        var nodeCount = 0
        BackupErrorChain.walk(root) { _ in
            nodeCount += 1
            return .continue
        }
        XCTAssertEqual(nodeCount, 1)
    }

    func testWalkDescendsIntoRemoteStorageClientErrorUnderlying() {
        let leaf = NSError(domain: "leaf", code: 1)
        let root = RemoteStorageClientError.underlying(leaf)
        var nodeCount = 0
        BackupErrorChain.walk(root) { _ in
            nodeCount += 1
            return .continue
        }
        XCTAssertEqual(nodeCount, 2)

        // Terminal cases: still yielded once but no descent.
        for terminal: RemoteStorageClientError in [
            .notConnected,
            .unavailable,
            .invalidConfiguration,
            .externalStorageUnavailable,
            .unsupportedStorageType("smb")
        ] {
            var count = 0
            BackupErrorChain.walk(terminal) { _ in
                count += 1
                return .continue
            }
            XCTAssertEqual(count, 1, "terminal RemoteStorageClientError case must visit only itself: \(terminal)")
        }
    }

    func testWalkDescendsIntoNSUnderlyingErrorKey() {
        let leaf = NSError(domain: "leaf", code: 1)
        let mid = NSError(domain: "mid", code: 2, userInfo: [NSUnderlyingErrorKey: leaf])
        let root = NSError(domain: "root", code: 3, userInfo: [NSUnderlyingErrorKey: mid])
        var nodeCount = 0
        BackupErrorChain.walk(root) { _ in
            nodeCount += 1
            return .continue
        }
        XCTAssertEqual(nodeCount, 3)
    }

    func testWalkTerminatesOnNSErrorIdentityCycle() {
        // SelfReferencingNSError overrides userInfo to return [NSUnderlyingErrorKey: self],
        // producing a real self-cycle in the NSUnderlyingErrorKey graph. Without
        // ObjectIdentifier dedup the walker would loop forever; with dedup, exactly one visit.
        let cyclic: NSError = SelfReferencingNSError()
        var visits = 0
        BackupErrorChain.walk(cyclic) { _ in
            visits += 1
            return .continue
        }
        XCTAssertEqual(visits, 1, "self-cycling NSError must be visited exactly once via ObjectIdentifier dedup")
    }

    func testWalkTerminatesOnExternalCallerStop() {
        let leaf = NSError(domain: "leaf", code: 1)
        let root = V2MonthSession.FlushError.postCommitFailed(underlying: SnapshotWriter.WriteError.ioFailure(leaf))
        var visited: [String] = []
        BackupErrorChain.walk(root) { node in
            visited.append(describe(node))
            return .stop
        }
        XCTAssertEqual(visited, ["FlushError"], "body returning .stop on the root must end traversal before any descent")
    }

    // MARK: - Predicate convenience

    func testContainsMatchesNestedCancellationError() {
        let root = V2MonthSession.FlushError.postCommitFailed(underlying: CancellationError())
        XCTAssertTrue(BackupErrorChain.contains(root) { $0 is CancellationError })
    }

    func testFirstSatisfyingReturnsTheMatchingNode() {
        let inner = CancellationError()
        let root = V2MonthSession.FlushError.postCommitFailed(underlying: inner)
        let match = BackupErrorChain.firstSatisfying(root) { $0 is CancellationError }
        XCTAssertNotNil(match)
        XCTAssertTrue(match is CancellationError)
    }

    func testFirstOfTypeReturnsFirstMatchingNode() {
        let gate = MetadataCreateGate.Error.stagingVerificationFailed(
            remotePath: "/x",
            underlying: CancellationError()
        )
        let root = V2MonthSession.FlushError.postCommitFailed(underlying: SnapshotWriter.WriteError.finalizationFailed(gate))
        let match = BackupErrorChain.firstOfType(root, as: MetadataCreateGate.Error.self)
        XCTAssertNotNil(match)
        if case .stagingVerificationFailed(let path, _)? = match {
            XCTAssertEqual(path, "/x")
        } else {
            XCTFail("expected staging gate error")
        }
    }

    func testFirstOfTypeReturnsNilWhenNoMatch() {
        let root: Error = NSError(domain: "x", code: 1)
        let match = BackupErrorChain.firstOfType(root, as: V2MonthSession.FlushError.self)
        XCTAssertNil(match)
    }

    // MARK: - Documented dedup widening (vs old domain#code#desc dedup)

    func testDistinctNSErrorsWithSameDomainCodeDescriptionAreNotCollapsed() {
        // Chain: outerNS -> NSUnderlyingErrorKey -> innerNS -> NSUnderlyingErrorKey -> leaf.
        // Both outerNS and innerNS have identical (domain, code, localizedDescription).
        // Under the new ObjectIdentifier dedup, both NSErrors are distinct instances so
        // both are visited and the leaf is reached. Under the old Set<String> dedup used
        // by the legacy V2MonthSession walkers, innerNS would collide on the key
        // "Dup#42#same" and be skipped, making the leaf unreachable.
        let leaf = RemoteStorageClientError.externalStorageUnavailable
        let innerNS = NSError(
            domain: "Dup", code: 42,
            userInfo: [NSLocalizedDescriptionKey: "same", NSUnderlyingErrorKey: leaf]
        )
        let outerNS = NSError(
            domain: "Dup", code: 42,
            userInfo: [NSLocalizedDescriptionKey: "same", NSUnderlyingErrorKey: innerNS]
        )

        XCTAssertEqual(outerNS.localizedDescription, innerNS.localizedDescription,
                       "fixture sanity: both wrappers must share the dedup key")
        XCTAssertFalse(outerNS === innerNS, "fixture sanity: must be distinct NSError instances")

        var leafHits = 0
        var visits = 0
        BackupErrorChain.walk(outerNS) { node in
            visits += 1
            if case RemoteStorageClientError.externalStorageUnavailable = node {
                leafHits += 1
            }
            return .continue
        }
        // Under new dedup: outerNS, innerNS, leaf -> visits=3, leafHits=1.
        // Under old string-key dedup: outerNS, then innerNS collapsed -> visits=1, leafHits=0.
        XCTAssertEqual(visits, 3,
                       "ObjectIdentifier dedup must visit both same-keyed NSError wrappers and the leaf")
        XCTAssertEqual(leafHits, 1,
                       "leaf must be reachable past the second same-keyed NSError wrapper")
    }

    // MARK: - helpers

    private func describe(_ error: Error) -> String {
        switch error {
        case is CancellationError: return "CancellationError"
        case is V2MonthSession.FlushError: return "FlushError"
        case is SnapshotWriter.WriteError: return "WriteError"
        case is CommitLogWriter.WriteError: return "CommitWriteError"
        case is MetadataCreateGate.Error: return "GateError"
        case is RemoteStorageClientError: return "StorageError"
        default: return "NSError(\((error as NSError).domain))"
        }
    }
}

private final class SelfReferencingNSError: NSError, @unchecked Sendable {
    init() {
        super.init(domain: "BackupErrorChainTests.SelfCycle", code: 1, userInfo: nil)
    }
    required init?(coder: NSCoder) { super.init(coder: coder) }
    override var userInfo: [String: Any] { [NSUnderlyingErrorKey: self] }
}
