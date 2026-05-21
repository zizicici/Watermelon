import XCTest
@testable import Watermelon

final class AssetResourceLinkSetPredicateTests: XCTestCase {
    private func hash(_ byte: UInt8) -> Data {
        Data([byte])
    }

    private func key(_ byte: UInt8, role: Int = 1, slot: Int = 0) -> AssetResourceLinkKey {
        AssetResourceLinkKey(role: role, slot: slot, hash: hash(byte))
    }

    private func link(_ byte: UInt8, role: Int = 1, slot: Int = 0) -> RemoteAssetResourceLink {
        RemoteAssetResourceLink(
            year: 2026,
            month: 1,
            assetFingerprint: hash(0xF0),
            resourceHash: hash(byte),
            role: role,
            slot: slot,
            logicalName: "\(byte).jpg"
        )
    }

    func testEqualSetsAreNotStrictSubsets() {
        let candidate: Set<AssetResourceLinkKey> = [key(0xA1)]
        let incoming: Set<AssetResourceLinkKey> = [key(0xA1)]

        XCTAssertFalse(AssetResourceLinkSetPredicate.isStrictSubset(candidate, of: incoming))
    }

    func testProperSubsetIsStrictSubset() {
        let candidate: Set<AssetResourceLinkKey> = [key(0xA1)]
        let incoming: Set<AssetResourceLinkKey> = [key(0xA1), key(0xA2, role: 2)]

        XCTAssertTrue(AssetResourceLinkSetPredicate.isStrictSubset(candidate, of: incoming))
    }

    func testUnrelatedHashIsNotSubset() {
        let candidate: Set<AssetResourceLinkKey> = [key(0xFF)]
        let incoming: Set<AssetResourceLinkKey> = [key(0xA1), key(0xA2, role: 2)]

        XCTAssertFalse(AssetResourceLinkSetPredicate.isStrictSubset(candidate, of: incoming))
    }

    func testSameHashDifferentRoleIsNotSubset() {
        let candidate: Set<AssetResourceLinkKey> = [key(0xA1, role: 2)]
        let incoming: Set<AssetResourceLinkKey> = [key(0xA1, role: 1), key(0xA2, role: 2)]

        XCTAssertFalse(AssetResourceLinkSetPredicate.isStrictSubset(candidate, of: incoming))
    }

    func testSameHashDifferentSlotIsNotSubset() {
        let candidate: Set<AssetResourceLinkKey> = [key(0xA1, slot: 1)]
        let incoming: Set<AssetResourceLinkKey> = [key(0xA1, slot: 0), key(0xA2, role: 2)]

        XCTAssertFalse(AssetResourceLinkSetPredicate.isStrictSubset(candidate, of: incoming))
    }

    func testDuplicateTuplesAndLinksDoNotInflateCardinality() {
        let candidate = AssetResourceLinkSetPredicate.keys(fromLinks: [
            link(0xA1),
            link(0xA1)
        ])
        let incoming = AssetResourceLinkSetPredicate.keys(fromTuples: [
            (role: 1, slot: 0, hash: hash(0xA1)),
            (role: 1, slot: 0, hash: hash(0xA1))
        ])

        XCTAssertEqual(candidate.count, 1)
        XCTAssertEqual(incoming.count, 1)
        XCTAssertFalse(AssetResourceLinkSetPredicate.isStrictSubset(candidate, of: incoming))
    }

    func testEmptyCandidateIsNotStrictSubset() {
        XCTAssertFalse(AssetResourceLinkSetPredicate.isStrictSubset([], of: [key(0xA1)]))
    }

    func testEmptyIncomingDoesNotEnclose() {
        XCTAssertFalse(AssetResourceLinkSetPredicate.isStrictSubset([key(0xA1)], of: []))
        XCTAssertFalse(AssetResourceLinkSetPredicate.isSuperset([key(0xA1)], of: []))
    }

    func testSupersetAcceptsExistingSetWithExtraRoleSlotEntries() {
        let candidate: Set<AssetResourceLinkKey> = [
            key(0xA1, role: ResourceTypeCode.photo, slot: 0),
            key(0xA2, role: ResourceTypeCode.adjustmentData, slot: 0)
        ]
        let needed: Set<AssetResourceLinkKey> = [
            key(0xA1, role: ResourceTypeCode.photo, slot: 0)
        ]

        XCTAssertTrue(AssetResourceLinkSetPredicate.isSuperset(candidate, of: needed))
    }

    func testSupersetRejectsMissingHash() {
        let candidate: Set<AssetResourceLinkKey> = [key(0xA1)]
        let needed: Set<AssetResourceLinkKey> = [key(0xA2)]

        XCTAssertFalse(AssetResourceLinkSetPredicate.isSuperset(candidate, of: needed))
    }
}
