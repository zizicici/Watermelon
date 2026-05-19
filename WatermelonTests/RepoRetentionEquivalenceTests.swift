import XCTest
@testable import Watermelon

enum RepoRetentionEquivalence {
    enum Mode {
        case strict
        case retentionSuperset
    }

    static func matches(
        _ before: RepoMaterializer.MaterializeOutput,
        _ after: RepoMaterializer.MaterializeOutput,
        month: LibraryMonthKey,
        mode: Mode
    ) -> Bool {
        guard before.repoID == after.repoID else { return false }

        switch mode {
        case .strict:
            return before.state.months[month] == after.state.months[month] &&
            before.coveredByMonth[month, default: .empty] == after.coveredByMonth[month, default: .empty] &&
            before.observedSeqByWriter == after.observedSeqByWriter &&
            before.state.observedClock == after.state.observedClock
        case .retentionSuperset:
            guard let beforeMonth = before.state.months[month] else {
                return after.coveredByMonth[month, default: .empty].superset(of: before.coveredByMonth[month, default: .empty]) &&
                observedSeqDoesNotRegress(before: before.observedSeqByWriter, after: after.observedSeqByWriter) &&
                after.state.observedClock >= before.state.observedClock
            }
            guard let afterMonth = after.state.months[month] else { return false }
            return monthStateIsSuperset(before: beforeMonth, after: afterMonth) &&
            after.coveredByMonth[month, default: .empty].superset(of: before.coveredByMonth[month, default: .empty]) &&
            observedSeqDoesNotRegress(before: before.observedSeqByWriter, after: after.observedSeqByWriter) &&
            after.state.observedClock >= before.state.observedClock
        }
    }

    private static func monthStateIsSuperset(before: RepoMonthState, after: RepoMonthState) -> Bool {
        dictionaryIsSuperset(before: before.assets, after: after.assets) &&
        dictionaryIsSuperset(before: before.resources, after: after.resources) &&
        dictionaryIsSuperset(before: before.assetResources, after: after.assetResources) &&
        after.deletedAssetFingerprints.isSuperset(of: before.deletedAssetFingerprints) &&
        dictionaryIsSuperset(before: before.deletedAssetStamps, after: after.deletedAssetStamps)
    }

    private static func dictionaryIsSuperset<K: Hashable, V: Equatable>(before: [K: V], after: [K: V]) -> Bool {
        for (key, value) in before where after[key] != value {
            return false
        }
        return true
    }

    private static func observedSeqDoesNotRegress(before: [String: UInt64], after: [String: UInt64]) -> Bool {
        for (writerID, seq) in before where (after[writerID] ?? 0) < seq {
            return false
        }
        return true
    }
}

final class RepoRetentionEquivalenceTests: XCTestCase {
    func testStrictAcceptsIdenticalFixtures() {
        let output = makeOutput()
        XCTAssertTrue(RepoRetentionEquivalence.matches(output, output, month: month, mode: .strict))
    }

    func testStrictRejectsAssetStampDifference() {
        let before = makeOutput()
        var after = before
        var monthState = after.state.months[month]!
        monthState.assets[fp] = asset(stamp: OpStamp(writerID: Self.writerA, seq: 2, clock: 100))
        after = replacingMonth(after, with: monthState)
        XCTAssertFalse(RepoRetentionEquivalence.matches(before, after, month: month, mode: .strict))
    }

    func testRejectsDroppedDeletedAssetStamp() {
        let before = makeOutput()
        var after = before
        var monthState = after.state.months[month]!
        monthState.deletedAssetStamps = [:]
        after = replacingMonth(after, with: monthState)
        XCTAssertFalse(RepoRetentionEquivalence.matches(before, after, month: month, mode: .strict))
        XCTAssertFalse(RepoRetentionEquivalence.matches(before, after, month: month, mode: .retentionSuperset))
    }

    func testRejectsRemovedResourceEvenWhenAssetIsTombstoned() {
        let before = makeOutput()
        var after = before
        var monthState = after.state.months[month]!
        monthState.resources.removeValue(forKey: resourcePath)
        monthState.deletedAssetFingerprints.insert(fp)
        after = replacingMonth(after, with: monthState)
        XCTAssertFalse(RepoRetentionEquivalence.matches(before, after, month: month, mode: .retentionSuperset))
    }

    func testStrictAcceptsLegacyUnstampedRowsOnBothSides() {
        let output = makeOutput(assetStamp: nil, resourceStamp: nil)
        XCTAssertTrue(RepoRetentionEquivalence.matches(output, output, month: month, mode: .strict))
    }

    func testSupersetAcceptsExtraRowsButRejectsMissingRows() {
        let before = makeOutput()
        var after = before
        var monthState = after.state.months[month]!
        let extraFP = TestFixtures.fingerprint(0xB2)
        monthState.assets[extraFP] = SnapshotAssetRow(
            assetFingerprint: extraFP,
            creationDateMs: nil,
            backedUpAtMs: 2,
            resourceCount: 0,
            totalFileSizeBytes: 0,
            stamp: OpStamp(writerID: writerB, seq: 1, clock: 200)
        )
        after = replacingMonth(after, with: monthState, observedClock: 200, observedSeq: [Self.writerA: 5, writerB: 1])
        XCTAssertTrue(RepoRetentionEquivalence.matches(before, after, month: month, mode: .retentionSuperset))

        monthState.assets.removeValue(forKey: fp)
        after = replacingMonth(after, with: monthState, observedClock: 200, observedSeq: [Self.writerA: 5, writerB: 1])
        XCTAssertFalse(RepoRetentionEquivalence.matches(before, after, month: month, mode: .retentionSuperset))
    }

    func testSupersetRejectsCoveredRangeRegression() {
        let before = makeOutput(covered: covered([(1, 5)]))
        let after = makeOutput(covered: covered([(1, 4)]), observedClock: 200)
        XCTAssertFalse(RepoRetentionEquivalence.matches(before, after, month: month, mode: .retentionSuperset))
    }

    private func makeOutput(
        assetStamp: OpStamp? = OpStamp(writerID: RepoRetentionEquivalenceTests.writerA, seq: 5, clock: 100),
        resourceStamp: OpStamp? = OpStamp(writerID: RepoRetentionEquivalenceTests.writerA, seq: 5, clock: 100),
        covered: CoveredRanges? = nil,
        observedClock: UInt64 = 100,
        observedSeq: [String: UInt64]? = nil
    ) -> RepoMaterializer.MaterializeOutput {
        let monthState = RepoMonthState(
            assets: [fp: asset(stamp: assetStamp)],
            resources: [resourcePath: resource(stamp: resourceStamp)],
            assetResources: [
                AssetResourceKey(assetFingerprint: fp, role: 1, slot: 0): SnapshotAssetResourceRow(
                    assetFingerprint: fp,
                    role: 1,
                    slot: 0,
                    resourceHash: resourceHash,
                    logicalName: "a.jpg"
                )
            ],
            deletedAssetFingerprints: [deletedFP],
            deletedAssetStamps: [deletedFP: OpStamp(writerID: Self.writerA, seq: 4, clock: 90)]
        )
        return RepoMaterializer.MaterializeOutput(
            state: RepoSnapshotState(months: [month: monthState], observedClock: observedClock),
            observedSeqByWriter: observedSeq ?? [Self.writerA: 5],
            coveredByMonth: [month: covered ?? self.covered([(1, 5)])],
            corruptedSnapshotMonths: [],
            repoID: repoID
        )
    }

    private func replacingMonth(
        _ output: RepoMaterializer.MaterializeOutput,
        with monthState: RepoMonthState,
        observedClock: UInt64? = nil,
        observedSeq: [String: UInt64]? = nil
    ) -> RepoMaterializer.MaterializeOutput {
        var months = output.state.months
        months[month] = monthState
        return RepoMaterializer.MaterializeOutput(
            state: RepoSnapshotState(months: months, observedClock: observedClock ?? output.state.observedClock),
            observedSeqByWriter: observedSeq ?? output.observedSeqByWriter,
            coveredByMonth: output.coveredByMonth,
            corruptedSnapshotMonths: output.corruptedSnapshotMonths,
            repoID: output.repoID
        )
    }

    private func asset(stamp: OpStamp?) -> SnapshotAssetRow {
        SnapshotAssetRow(
            assetFingerprint: fp,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resourceCount: 1,
            totalFileSizeBytes: 100,
            stamp: stamp
        )
    }

    private func resource(stamp: OpStamp?) -> SnapshotResourceRow {
        SnapshotResourceRow(
            physicalRemotePath: resourcePath,
            contentHash: resourceHash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 1,
            crypto: nil,
            stamp: stamp
        )
    }

    private func covered(_ ranges: [(UInt64, UInt64)]) -> CoveredRanges {
        CoveredRanges(rangesByWriter: [
            Self.writerA: ranges.map { ClosedSeqRange(low: $0.0, high: $0.1) }
        ])
    }

    private static let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let writerB = "22222222-2222-2222-2222-bbbbbbbbbbbb"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let month = LibraryMonthKey(year: 2026, month: 5)
    private let fp = TestFixtures.fingerprint(0xA1)
    private let deletedFP = TestFixtures.fingerprint(0xA2)
    private let resourceHash = TestFixtures.fingerprint(0xB1)
    private let resourcePath = "2026/05/a.jpg"
}
