import XCTest
@testable import Watermelon

final class RetentionManifestTests: XCTestCase {
    func testEncodeDecodeRoundTripPreservesRequiredValues() throws {
        let manifest = makeManifest()
        let data = try RetentionManifestStore.encode(manifest)
        let decoded = try RetentionManifestStore.decode(data)
        XCTAssertEqual(decoded, manifest)

        let object = try XCTUnwrap(JSONSerialization.jsonObject(with: data) as? [String: Any])
        XCTAssertEqual(object["repo_id"] as? String, repoID)
        XCTAssertEqual(object["month"] as? String, month.text)
        XCTAssertEqual(object["barrier_lamport"] as? String, "000000000000002a")
        XCTAssertNotNil(object["policy"])
        XCTAssertNotNil(object["liveness_gate"])
    }

    func testManifestInitializerCanonicalizesCase() {
        let manifest = RetentionManifest(
            version: RetentionManifest.currentVersion,
            repoID: repoID.uppercased(),
            month: month,
            createdByWriterID: writerA.uppercased(),
            runID: UUID(uuidString: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")!,
            createdAtMs: 1_700_000_000_000,
            barrierLamport: 42,
            checkpointSnapshotName: RepoLayout.snapshotFileName(
                month: month,
                lamport: 42,
                writerID: writerA,
                runID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            ),
            checkpointSHA256Hex: String(repeating: "A", count: 64),
            coveredRanges: covered([writerA: [(1, 5)]]),
            deletePrefixByWriter: [writerA: 5],
            observedSeqHighByWriter: [writerA: 5],
            policy: RetentionManifestPolicy(
                keepUncoveredCommits: true,
                keepCorruptOrUntrustedCommits: true,
                keepTombstones: true,
                snapshotKeepCount: 2
            ),
            livenessGate: RetentionLivenessGate(
                requiredCompleteView: true,
                requiredNoActiveNonSelfWriters: true,
                legacyClientGraceMs: 604_800_000
            )
        )

        XCTAssertEqual(manifest.repoID, repoID)
        XCTAssertEqual(manifest.createdByWriterID, writerA)
        XCTAssertEqual(manifest.checkpointSHA256Hex, String(repeating: "a", count: 64))
    }

    func testDecodeIgnoresUnknownFieldsAfterRequiredFieldsValidate() throws {
        var json = try manifestJSON(makeManifest())
        json["unknown_future_field"] = ["x": 1]
        XCTAssertNoThrow(try decode(json))

        json.removeValue(forKey: "repo_id")
        XCTAssertThrowsError(try decode(json))
    }

    func testDecodeRejectsEachMissingRequiredFieldEvenWithUnknownFieldPresent() throws {
        let requiredKeys = [
            "version",
            "repo_id",
            "month",
            "created_by_writer_id",
            "run_id",
            "created_at_ms",
            "barrier_lamport",
            "checkpoint_snapshot",
            "checkpoint_sha256",
            "covered_ranges",
            "delete_prefix_by_writer",
            "observed_seq_high_by_writer",
            "policy",
            "liveness_gate"
        ]

        for key in requiredKeys {
            var json = try manifestJSON(makeManifest())
            json["unknown_future_field"] = ["x": 1]
            json.removeValue(forKey: key)
            XCTAssertThrowsError(try decode(json), "missing \(key) should fail")
        }
    }

    func testDecodeRejectsInvalidShapes() throws {
        try assertDecodeRejects(mutating: { $0["version"] = RetentionManifest.currentVersion + 1 })
        try assertDecodeRejects(mutating: { $0["repo_id"] = "" })
        try assertDecodeRejects(mutating: { $0["repo_id"] = "not-a-uuid" })
        try assertDecodeRejects(mutating: { $0["month"] = "2026-13" })
        try assertDecodeRejects(mutating: { $0["created_by_writer_id"] = "writer" })
        try assertDecodeRejects(mutating: { $0["created_at_ms"] = -1 })
        try assertDecodeRejects(mutating: { $0["barrier_lamport"] = RepoLayout.format16Hex(LamportClock.maxAdoptableValue) })
        try assertDecodeRejects(mutating: { $0["checkpoint_snapshot"] = String(($0["checkpoint_snapshot"] as! String).dropLast(".jsonl".count)) })
        try assertDecodeRejects(mutating: {
            $0["checkpoint_snapshot"] = RepoLayout.snapshotFileName(
                month: LibraryMonthKey(year: 2026, month: 4),
                lamport: 42,
                writerID: writerA,
                runID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            )
        })
        try assertDecodeRejects(mutating: {
            $0["checkpoint_snapshot"] = RepoLayout.snapshotFileName(
                month: month,
                lamport: 43,
                writerID: writerA,
                runID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            )
        })
        try assertDecodeRejects(mutating: {
            $0["checkpoint_snapshot"] = RepoLayout.snapshotFileName(
                month: month,
                lamport: 42,
                writerID: writerB,
                runID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            )
        })
        try assertDecodeRejects(mutating: {
            $0["checkpoint_snapshot"] = RepoLayout.snapshotFileName(
                month: month,
                lamport: 42,
                writerID: writerA,
                runID: "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
            )
        })
        try assertDecodeRejects(mutating: { $0["checkpoint_sha256"] = String(repeating: "a", count: 63) })
        try assertDecodeRejects(mutating: { $0["checkpoint_sha256"] = String(repeating: "z", count: 64) })
        try assertDecodeRejects(mutating: { $0["covered_ranges"] = [writerA: [[0, 1]]] })
        try assertDecodeRejects(mutating: { $0["covered_ranges"] = [writerA: [[10, 9]]] })
        try assertDecodeRejects(mutating: { $0["covered_ranges"] = ["writer": [[1, 2]]] })
        try assertDecodeRejects(mutating: {
            $0["policy"] = [
                "keep_uncovered_commits": true,
                "keep_corrupt_or_untrusted_commits": true,
                "keep_tombstones": true,
                "snapshot_keep_count": -1
            ]
        })
        try assertDecodeRejects(mutating: {
            $0["liveness_gate"] = [
                "required_complete_view": true,
                "required_no_active_non_self_writers": true,
                "legacy_client_grace_ms": -1
            ]
        })
    }

    func testDecodeRejectsDeletePrefixOutsideCoveredContiguousPrefix() throws {
        try assertDecodeRejects(mutating: { $0["delete_prefix_by_writer"] = [writerA: 6] })
        try assertDecodeRejects(mutating: { $0["delete_prefix_by_writer"] = [writerB: 1] })
        try assertDecodeRejects(mutating: {
            $0["covered_ranges"] = [writerA: [[5, 10]]]
            $0["delete_prefix_by_writer"] = [writerA: 0]
        })
    }

    func testRetentionManifestRefOrdering() {
        let lowLamport = RetentionManifestRef(month: month, lamport: 1, writerID: writerB, runIDPrefix: "zzzzzz")
        let highLamport = RetentionManifestRef(month: month, lamport: 2, writerID: writerA, runIDPrefix: "aaaaaa")
        XCTAssertLessThan(lowLamport, highLamport)

        let lowWriter = RetentionManifestRef(month: month, lamport: 2, writerID: writerA, runIDPrefix: "zzzzzz")
        let highWriter = RetentionManifestRef(month: month, lamport: 2, writerID: writerB, runIDPrefix: "aaaaaa")
        XCTAssertLessThan(lowWriter, highWriter)

        let lowRun = RetentionManifestRef(month: month, lamport: 2, writerID: writerB, runIDPrefix: "aaaaaa")
        let highRun = RetentionManifestRef(month: month, lamport: 2, writerID: writerB, runIDPrefix: "bbbbbb")
        XCTAssertLessThan(lowRun, highRun)
    }

    func testRetentionManifestFilenameRoundTripAndRejectsMalformedNames() {
        let ref = RetentionManifestRef(month: month, lamport: 0x2a, writerID: writerA, runIDPrefix: "abcdef")
        let filename = RetentionManifestStore.filename(for: ref)
        XCTAssertEqual(filename, "2026-05--000000000000002a--\(writerA)--abcdef.json")
        XCTAssertEqual(RetentionManifestStore.parseFilename(filename), ref)

        XCTAssertNil(RetentionManifestStore.parseFilename("2026-05--000000000000002a--\(writerA)--abcdef.jsonl"))
        XCTAssertNil(RetentionManifestStore.parseFilename("2026-05--000000000000002a--\(writerA).json"))
        XCTAssertNil(RetentionManifestStore.parseFilename("2026-5--000000000000002a--\(writerA)--abcdef.json"))
        XCTAssertNil(RetentionManifestStore.parseFilename("02026-05--000000000000002a--\(writerA)--abcdef.json"))
        XCTAssertNil(RetentionManifestStore.parseFilename("2026-05--zzzzzzzzzzzzzzzz--\(writerA)--abcdef.json"))
        XCTAssertNil(RetentionManifestStore.parseFilename("2026-05--000000000000002A--\(writerA)--abcdef.json"))
        XCTAssertNil(RetentionManifestStore.parseFilename("2026-05--000000000000002a--writer--abcdef.json"))
        XCTAssertNil(RetentionManifestStore.parseFilename("2026-05--000000000000002a--\(writerA)--.json"))
        XCTAssertNil(RetentionManifestStore.parseFilename("2026-05--000000000000002a--\(writerA)--ABCDEF.json"))
        XCTAssertNil(RetentionManifestStore.parseFilename("2026-05--000000000000002a--\(writerA)--abcdeg.json"))
        XCTAssertNil(RetentionManifestStore.parseFilename("2026-05--000000000000002a--\(writerA)--abcde.json"))
        XCTAssertNil(RetentionManifestStore.parseFilename("2026-05--\(RepoLayout.format16Hex(LamportClock.maxAdoptableValue))--\(writerA)--abcdef.json"))
    }

    func testBarrierSetKeepsSingleManifest() {
        let manifest = makeManifest()
        let set = RetentionBarrierSet.unsuperseded(manifests: [manifest])
        XCTAssertEqual(set.unsuperseded, [manifest])
        XCTAssertEqual(set.unionCovered, manifest.coveredRanges)
    }

    func testBarrierSetDropsCoveredSubsetWithIncreasingSortKey() {
        let smaller = makeManifest(lamport: 1, covered: covered([writerA: [(1, 10)]]))
        let wider = makeManifest(lamport: 2, covered: covered([writerA: [(1, 20)]]), deletePrefix: [writerA: 20])
        let set = RetentionBarrierSet.unsuperseded(manifests: [smaller, wider])
        XCTAssertEqual(set.unsuperseded, [wider])
        XCTAssertEqual(set.unionCovered, wider.coveredRanges)
    }

    func testBarrierSetRetainsWiderBarrierWithDecreasingSortKey() {
        let narrower = makeManifest(lamport: 3, covered: covered([writerA: [(1, 10)]]))
        let widerStale = makeManifest(lamport: 2, covered: covered([writerA: [(1, 20)]]), deletePrefix: [writerA: 20])
        let set = RetentionBarrierSet.unsuperseded(manifests: [narrower, widerStale])
        XCTAssertEqual(Set(set.unsuperseded.map(\.barrierLamport)), [2, 3])
        XCTAssertEqual(set.unionCovered, covered([writerA: [(1, 20)]]))
    }

    func testBarrierSetRetainsDisjointCoveredRangesAndUnions() {
        let left = makeManifest(lamport: 1, covered: covered([writerA: [(1, 10)]]))
        let right = makeManifest(lamport: 2, covered: covered([writerB: [(1, 5)]]), deletePrefix: [writerB: 5])
        let set = RetentionBarrierSet.unsuperseded(manifests: [left, right])
        XCTAssertEqual(Set(set.unsuperseded.map(\.barrierLamport)), [1, 2])
        XCTAssertEqual(set.unionCovered, covered([writerA: [(1, 10)], writerB: [(1, 5)]]))
    }

    func testBarrierSetDropsEqualCoveredLowerSortKey() {
        let low = makeManifest(lamport: 1)
        let high = makeManifest(lamport: 2)
        let set = RetentionBarrierSet.unsuperseded(manifests: [low, high])
        XCTAssertEqual(set.unsuperseded, [high])
    }

    func testBarrierSetRetainsDistinctManifestsWithEqualRef() {
        let first = makeManifest(lamport: 7)
        var second = first
        second.createdAtMs += 1

        let set = RetentionBarrierSet.unsuperseded(manifests: [first, second])
        XCTAssertEqual(set.unsuperseded, [first, second])
        XCTAssertEqual(set.unionCovered, first.coveredRanges)
    }

    func testBarrierSetChainKeepsMiddleWhenEndpointDoesNotDominateIt() {
        let low = makeManifest(lamport: 1, covered: covered([writerA: [(1, 10)]]))
        let middle = makeManifest(lamport: 3, covered: covered([writerB: [(1, 10)]]), deletePrefix: [writerB: 10])
        let high = makeManifest(lamport: 4, covered: covered([writerA: [(1, 20)]]), deletePrefix: [writerA: 20])
        let set = RetentionBarrierSet.unsuperseded(manifests: [low, middle, high])
        XCTAssertEqual(Set(set.unsuperseded.map(\.barrierLamport)), [3, 4])
        XCTAssertEqual(set.unionCovered, covered([writerA: [(1, 20)], writerB: [(1, 10)]]))
    }

    private func assertDecodeRejects(mutating block: (inout [String: Any]) -> Void) throws {
        var json = try manifestJSON(makeManifest())
        block(&json)
        XCTAssertThrowsError(try decode(json))
    }

    private func decode(_ json: [String: Any]) throws -> RetentionManifest {
        let data = try JSONSerialization.data(withJSONObject: json, options: [])
        return try RetentionManifestStore.decode(data)
    }

    private func manifestJSON(_ manifest: RetentionManifest) throws -> [String: Any] {
        let data = try RetentionManifestStore.encode(manifest)
        return try XCTUnwrap(JSONSerialization.jsonObject(with: data) as? [String: Any])
    }

    private func makeManifest(
        lamport: UInt64 = 42,
        createdByWriterID: String? = nil,
        covered: CoveredRanges? = nil,
        deletePrefix: [String: UInt64]? = nil
    ) -> RetentionManifest {
        let coveredRanges = covered ?? self.covered([writerA: [(1, 5), (10, 12)]])
        let writerID = createdByWriterID ?? writerA
        return RetentionManifest(
            version: RetentionManifest.currentVersion,
            repoID: repoID,
            month: month,
            createdByWriterID: writerID,
            runID: UUID(uuidString: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")!,
            createdAtMs: 1_700_000_000_000,
            barrierLamport: lamport,
            checkpointSnapshotName: RepoLayout.snapshotFileName(
                month: month,
                lamport: lamport,
                writerID: writerID,
                runID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            ),
            checkpointSHA256Hex: String(repeating: "a", count: 64),
            coveredRanges: coveredRanges,
            deletePrefixByWriter: deletePrefix ?? coveredRanges.conservativeContiguousPrefixByWriter(),
            observedSeqHighByWriter: observedHigh(coveredRanges),
            policy: RetentionManifestPolicy(
                keepUncoveredCommits: true,
                keepCorruptOrUntrustedCommits: true,
                keepTombstones: true,
                snapshotKeepCount: 2
            ),
            livenessGate: RetentionLivenessGate(
                requiredCompleteView: true,
                requiredNoActiveNonSelfWriters: true,
                legacyClientGraceMs: 604_800_000
            )
        )
    }

    private func observedHigh(_ covered: CoveredRanges) -> [String: UInt64] {
        covered.rangesByWriter.mapValues { ranges in ranges.map(\.high).max() ?? 0 }
    }

    private func covered(_ ranges: [String: [(UInt64, UInt64)]]) -> CoveredRanges {
        CoveredRanges(rangesByWriter: ranges.mapValues { pairs in
            pairs.map { ClosedSeqRange(low: $0.0, high: $0.1) }
        })
    }

    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let writerB = "22222222-2222-2222-2222-bbbbbbbbbbbb"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let month = LibraryMonthKey(year: 2026, month: 5)
}
