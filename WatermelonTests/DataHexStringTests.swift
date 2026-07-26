import CryptoKit
import Foundation
import XCTest
@testable import Watermelon

final class DataHexStringTests: XCTestCase {
    func testLowercaseEncodingMatchesCanonicalFormattingForEveryByte() {
        let data = Data((0 ... 255).map(UInt8.init))
        let canonical = data.map { String(format: "%02x", $0) }.joined()

        XCTAssertEqual(data.hexString, canonical)
        XCTAssertEqual(Data().hexString, "")
    }
}

final class AssetFingerprintTests: XCTestCase {
    private typealias Input = [(role: Int, slot: Int, contentHash: Data)]

    private func legacyFingerprint(_ input: Input) -> Data {
        let tokens = input
            .map { "\($0.role)|\($0.slot)|\($0.contentHash.hexString)" }
            .sorted()
            .joined(separator: "\n")
        return Data(SHA256.hash(data: Data(tokens.utf8)))
    }

    func testKnownCanonicalDigestsRemainByteStable() {
        XCTAssertEqual(
            BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: []).hexString,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )
        XCTAssertEqual(
            BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: [
                (role: 1, slot: 0, contentHash: Data([0x00, 0xFF]))
            ]).hexString,
            "d32b575438a16ce20771515d7abcc5175a1c172c695404f31d918c1b7c922ecb"
        )
        XCTAssertEqual(
            BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: [
                (role: 2, slot: 2, contentHash: Data([0xAB])),
                (role: 10, slot: 0, contentHash: Data([0x01])),
                (role: 2, slot: 0, contentHash: Data([0xFF])),
                (role: 2, slot: 10, contentHash: Data([0x00])),
            ]).hexString,
            "130e7b5ac2c687dadc4270d328ab1c226509e224512187a8d7693c9c37e9dfa8"
        )
    }

    func testOptimizedEncodingMatchesLegacyForBoundariesAndDuplicates() {
        let everyByte = Data((0 ... 255).map(UInt8.init))
        let samples: [Input] = [
            [],
            [(role: 0, slot: 0, contentHash: Data())],
            [
                (role: -1, slot: -10, contentHash: Data([0x00])),
                (role: 19, slot: 2, contentHash: everyByte),
            ],
            [
                (role: 10, slot: 0, contentHash: Data([0x01])),
                (role: 2, slot: 10, contentHash: Data([0x00])),
                (role: 2, slot: 2, contentHash: Data([0xAB])),
                (role: 10, slot: 0, contentHash: Data([0x01])),
            ],
        ]

        for sample in samples {
            let reversed = Array(sample.reversed())
            XCTAssertEqual(
                BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: sample),
                legacyFingerprint(sample)
            )
            XCTAssertEqual(
                BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: reversed),
                legacyFingerprint(sample)
            )
        }
    }
}
