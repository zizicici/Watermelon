import CryptoKit
import ImageIO
import UniformTypeIdentifiers
import XCTest
@testable import Watermelon

final class LeftoverAdoptionCandidateMatcherTests: XCTestCase {
    private let month = LibraryMonthKey(year: 2024, month: 3)

    private func check(
        _ name: String,
        hashByte: UInt8,
        kind: LeftoverMediaKind,
        identifier: String? = nil,
        modificationDateMs: Int64? = 1_700_000_000_000,
        mediaCreationDateMs: Int64? = nil,
        presence: LeftoverLocalPresence = .absent,
        status: LeftoverHashCheckStatus? = nil
    ) -> LeftoverDownloadedCheck {
        let file = LeftoverFile(
            month: month,
            fileName: name,
            path: "/backup/2024/03/\(name)",
            size: 100,
            modificationDateMs: modificationDateMs
        )
        return LeftoverDownloadedCheck(
            file: file,
            status: status ?? .noMatch(hashHex: String(format: "%02x", hashByte)),
            inspection: LeftoverFileInspection(
                contentHash: Data([hashByte]),
                actualSize: 100,
                mediaKind: kind,
                livePhotoContentIdentifier: identifier,
                mediaCreationDateMs: mediaCreationDateMs,
                localPresence: presence
            )
        )
    }

    func testExactIdentifierPairBecomesOneLivePhotoAsset() {
        let photo = check("IMG_0001.HEIC", hashByte: 1, kind: .image, identifier: "pair")
        let video = check("IMG_0001.MOV", hashByte: 2, kind: .video, identifier: "pair")

        let candidates = LeftoverAdoptionCandidateMatcher.makeCandidates(from: [photo, video])

        XCTAssertEqual(candidates.count, 1)
        XCTAssertEqual(candidates[0].resources.map(\.role), [
            ResourceTypeCode.photo,
            ResourceTypeCode.pairedVideo
        ])
        XCTAssertEqual(Set(candidates[0].resources.map(\.file.path)), Set([photo.file.path, video.file.path]))
        XCTAssertEqual(candidates[0].assetFingerprint, BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [
                (role: ResourceTypeCode.photo, slot: 0, contentHash: Data([1])),
                (role: ResourceTypeCode.pairedVideo, slot: 0, contentHash: Data([2]))
            ]
        ))
    }

    func testExactIdentifierPairLeavesUnidentifiedVideoStandalone() {
        let photo = check("IMG_0001.HEIC", hashByte: 1, kind: .image, identifier: "pair")
        let video = check("IMG_0001.MOV", hashByte: 2, kind: .video, identifier: "pair")
        let standalone = check("other.MOV", hashByte: 3, kind: .video)

        let candidates = LeftoverAdoptionCandidateMatcher.makeCandidates(from: [photo, video, standalone])

        XCTAssertEqual(candidates.count, 2)
        XCTAssertTrue(candidates.contains { $0.resources.count == 2 })
        XCTAssertTrue(candidates.contains { $0.resources.map(\.role) == [ResourceTypeCode.video] })
    }

    func testChecksFromSeparateBatchesMergeIntoLivePhoto() {
        let photo = check("IMG_0002.HEIC", hashByte: 1, kind: .image, identifier: "pair")
        let video = check("IMG_0002.MOV", hashByte: 2, kind: .video, identifier: "pair")
        let merged = LeftoverHashCheckMerger.merge(
            existing: [photo],
            current: [video],
            failedPaths: []
        )

        let candidates = LeftoverAdoptionCandidateMatcher.makeCandidates(from: merged)
        XCTAssertEqual(candidates.count, 1)
        XCTAssertEqual(candidates[0].resources.count, 2)
    }

    func testDuplicateResidualHashesDoNotProduceCandidates() {
        let checks = [
            check("one.jpg", hashByte: 1, kind: .image),
            check("two.jpg", hashByte: 1, kind: .image)
        ]

        XCTAssertTrue(LeftoverAdoptionCandidateMatcher.makeCandidates(from: checks).isEmpty)
    }

    func testAmbiguousLivePhotoIdentifierIsNotAdoptable() {
        let checks = [
            check("IMG_0001.HEIC", hashByte: 1, kind: .image, identifier: "pair"),
            check("IMG_0001.MOV", hashByte: 2, kind: .video, identifier: "pair"),
            check("IMG_0001_1.MOV", hashByte: 3, kind: .video, identifier: "pair")
        ]

        XCTAssertTrue(LeftoverAdoptionCandidateMatcher.makeCandidates(from: checks).isEmpty)
    }

    func testUnpairedMediaWithoutIdentifierBecomesStandaloneAssets() {
        let checks = [
            check("photo.jpg", hashByte: 1, kind: .image),
            check("video.mov", hashByte: 2, kind: .video)
        ]

        let candidates = LeftoverAdoptionCandidateMatcher.makeCandidates(from: checks)

        XCTAssertEqual(candidates.count, 2)
        XCTAssertEqual(Set(candidates.flatMap { $0.resources.map(\.role) }), Set([
            ResourceTypeCode.photo,
            ResourceTypeCode.video
        ]))
    }

    func testRemoteMatchAndUnprovenLocalAbsenceAreExcluded() {
        let checks = [
            check(
                "remote.jpg",
                hashByte: 1,
                kind: .image,
                status: .matched(hashHex: "01", resources: [])
            ),
            check("local.jpg", hashByte: 2, kind: .image, presence: .present),
            check("unknown.jpg", hashByte: 3, kind: .image, presence: .unknown)
        ]

        XCTAssertTrue(LeftoverAdoptionCandidateMatcher.makeCandidates(from: checks).isEmpty)
    }

    func testEmbeddedCreationDateWinsAndLivePhotoUsesStillDate() {
        let photoDate: Int64 = 1_650_000_000_000
        let photo = check(
            "IMG_0001.HEIC",
            hashByte: 1,
            kind: .image,
            identifier: "pair",
            mediaCreationDateMs: photoDate
        )
        let video = check(
            "IMG_0001.MOV",
            hashByte: 2,
            kind: .video,
            identifier: "pair",
            mediaCreationDateMs: photoDate + 500
        )

        let candidate = LeftoverAdoptionCandidateMatcher.makeCandidates(from: [photo, video])[0]

        XCTAssertEqual(candidate.creationDateMs, photoDate)
        XCTAssertEqual(Set(candidate.resources.map(\.creationDateMs)), [photoDate])
    }

    func testImageInspectorReadsEmbeddedCreationDate() async throws {
        let colorSpace = CGColorSpaceCreateDeviceRGB()
        let context = try XCTUnwrap(CGContext(
            data: nil,
            width: 1,
            height: 1,
            bitsPerComponent: 8,
            bytesPerRow: 4,
            space: colorSpace,
            bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
        ))
        let image = try XCTUnwrap(context.makeImage())
        let data = NSMutableData()
        let destination = try XCTUnwrap(CGImageDestinationCreateWithData(
            data,
            UTType.jpeg.identifier as CFString,
            1,
            nil
        ))
        CGImageDestinationAddImage(destination, image, [
            kCGImagePropertyExifDictionary: [
                kCGImagePropertyExifDateTimeOriginal: "2024:03:04 05:06:07",
                "OffsetTimeOriginal": "+00:00"
            ]
        ] as CFDictionary)
        XCTAssertTrue(CGImageDestinationFinalize(destination))
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("leftover-date-\(UUID().uuidString).jpg")
        defer { try? FileManager.default.removeItem(at: url) }
        try (data as Data).write(to: url)

        let inspection = await LeftoverMediaInspector.inspect(url)
        let expected = ISO8601DateFormatter().date(from: "2024-03-04T05:06:07Z")!

        XCTAssertEqual(inspection.kind, .image)
        XCTAssertEqual(inspection.creationDateMs, expected.millisecondsSinceEpoch)
    }

    func testResidualHashCounterIncludesUnindexedDuplicates() async throws {
        let client = InMemoryRemoteStorageClient()
        let data = Data("same remote bytes".utf8)
        let hash = Data(SHA256.hash(data: data))
        let files = ["one.jpg", "two.jpg"].map { name in
            LeftoverFile(
                month: month,
                fileName: name,
                path: "/backup/2024/03/\(name)",
                size: Int64(data.count)
            )
        }
        for file in files {
            await client.seedFile(path: file.path, data: data)
        }

        let result = try await LeftoverResidualHashCounter.count(
            targetHashes: [hash],
            targetSizes: [Int64(data.count)],
            leftovers: files,
            client: client
        )

        XCTAssertEqual(result.counts[hash], 2)
        XCTAssertEqual(result.hashByPath[files[0].path], hash)
        XCTAssertEqual(result.hashByPath[files[1].path], hash)
    }

    func testResidualCounterFreshlyHashesEverySelectedPath() async throws {
        let client = InMemoryRemoteStorageClient()
        let oldFirstHash = Data(SHA256.hash(data: Data("aaaa".utf8)))
        let currentSharedData = Data("bbbb".utf8)
        let sharedHash = Data(SHA256.hash(data: currentSharedData))
        let files = ["first.jpg", "second.jpg"].map { name in
            LeftoverFile(
                month: month,
                fileName: name,
                path: "/backup/2024/03/\(name)",
                size: Int64(currentSharedData.count)
            )
        }
        for file in files {
            await client.seedFile(path: file.path, data: currentSharedData)
        }

        let result = try await LeftoverResidualHashCounter.count(
            targetHashes: [oldFirstHash, sharedHash],
            targetSizes: [Int64(currentSharedData.count)],
            leftovers: files,
            client: client
        )

        XCTAssertNil(result.counts[oldFirstHash])
        XCTAssertEqual(result.counts[sharedHash], 2)
        let downloadAttemptPaths = await client.downloadAttemptPaths
        XCTAssertEqual(downloadAttemptPaths, files.map(\.path))
    }
}
