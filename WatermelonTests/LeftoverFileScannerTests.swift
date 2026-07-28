import CryptoKit
import XCTest
@testable import Watermelon

final class LeftoverFileScannerTests: XCTestCase {
    private let base = "/base"
    private let month = LibraryMonthKey(year: 2024, month: 3)

    private func monthPath() -> String {
        LeftoverFileScanner.monthDataPath(basePath: base, month: month)
    }

    private func target(_ name: String, size: Int64 = 10) -> LeftoverFile {
        target(month, name, size: size)
    }

    private func target(_ month: LibraryMonthKey, _ name: String, size: Int64 = 10) -> LeftoverFile {
        let path = LeftoverFileScanner.monthDataPath(basePath: base, month: month) + "/" + name
        return LeftoverFile(month: month, fileName: name, path: path, size: size)
    }

    private func seed(_ client: InMemoryRemoteStorageClient, _ month: LibraryMonthKey, _ name: String) async {
        await client.seedFile(path: LeftoverFileScanner.monthDataPath(basePath: base, month: month) + "/" + name, data: Data(count: 10))
    }

    private actor CallCounter {
        private(set) var count = 0
        func bump() { count += 1 }
    }

    private actor ProgressRecorder {
        private(set) var values: [Int] = []
        func append(_ value: Int) { values.append(value) }
    }

    private func makeScanner(
        client: InMemoryRemoteStorageClient,
        months: [LibraryMonthKey],
        manifestNames: @escaping LeftoverFileScanner.ManifestNamesProvider
    ) -> LeftoverFileScanner {
        LeftoverFileScanner(client: client, basePath: base, months: months, manifestNames: manifestNames)
    }

    // listing − manifest = leftover
    func testScanReportsFilesAbsentFromManifest() async throws {
        let client = InMemoryRemoteStorageClient()
        for name in ["a.jpg", "b.jpg", "c.jpg"] {
            await client.seedFile(path: monthPath() + "/" + name, data: Data(count: 10))
        }
        let scanner = makeScanner(client: client, months: [month]) { _ in ["a.jpg", "b.jpg"] }

        let result = try await scanner.scan()

        XCTAssertEqual(result.totalCount, 1)
        XCTAssertEqual(result.allFiles.map(\.fileName), ["c.jpg"])
    }

    func testScanReportsNothingWhenManifestCoversListing() async throws {
        let client = InMemoryRemoteStorageClient()
        for name in ["a.jpg", "b.jpg"] {
            await client.seedFile(path: monthPath() + "/" + name, data: Data(count: 10))
        }
        let scanner = makeScanner(client: client, months: [month]) { _ in ["a.jpg", "b.jpg"] }

        let result = try await scanner.scan()

        XCTAssertEqual(result.totalCount, 0)
        XCTAssertTrue(result.groups.isEmpty)
    }

    func testScanDataCollectsFingerprintsFromTheSameManifestLoad() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: monthPath() + "/keep.jpg", data: Data(count: 10))
        await client.seedFile(path: monthPath() + "/leftover.jpg", data: Data(count: 10))
        let counter = CallCounter()
        let knownResource = LeftoverKnownResource(
            month: month,
            fileName: "keep.jpg",
            fileSize: 10,
            contentHash: Data([1, 2, 3])
        )
        let scanner = LeftoverFileScanner(
            client: client,
            basePath: base,
            months: [month],
            manifestSnapshots: { _ in
                await counter.bump()
                return LeftoverManifestSnapshot(
                    fileNames: ["keep.jpg"],
                    assetFingerprintHexes: ["aa", "bb"],
                    knownResources: [knownResource]
                )
            }
        )

        let result = try await scanner.scanData()

        XCTAssertEqual(result.groups.flatMap(\.files).map(\.fileName), ["leftover.jpg"])
        XCTAssertEqual(result.liveFingerprintHexes, ["aa", "bb"])
        XCTAssertEqual(result.knownResources, [knownResource])
        let calls = await counter.count
        XCTAssertEqual(calls, 1, "file names and fingerprints must come from one manifest load")
    }

    func testKnownResourceFilterKeepsOnlyPossibleHashMatchSizes() {
        let file = target("leftover.jpg", size: 42)
        let sameSize = LeftoverKnownResource(
            month: month,
            fileName: "same.jpg",
            fileSize: 42,
            contentHash: Data([1])
        )
        let differentSize = LeftoverKnownResource(
            month: month,
            fileName: "different.jpg",
            fileSize: 43,
            contentHash: Data([2])
        )
        let unknownSize = LeftoverKnownResource(
            month: month,
            fileName: "unknown.jpg",
            fileSize: 0,
            contentHash: Data([3])
        )

        let result = LeftoverKnownResourceFilter.relevant(
            to: [file],
            resources: [sameSize, differentSize, unknownSize]
        )

        XCTAssertEqual(result, [sameSize, unknownSize])
    }

    func testKnownResourceFilterKeepsAllResourcesForUnknownTargetSize() {
        let resources = [
            LeftoverKnownResource(
                month: month,
                fileName: "one.jpg",
                fileSize: 42,
                contentHash: Data([1])
            ),
            LeftoverKnownResource(
                month: month,
                fileName: "two.jpg",
                fileSize: 43,
                contentHash: Data([2])
            )
        ]

        let result = LeftoverKnownResourceFilter.relevant(
            to: [target("leftover.jpg", size: 0)],
            resources: resources
        )

        XCTAssertEqual(result, resources)
    }

    func testKnownResourceFilterDropsAllResourcesWhenNothingIsLeftover() {
        let resource = LeftoverKnownResource(
            month: month,
            fileName: "one.jpg",
            fileSize: 42,
            contentHash: Data([1])
        )

        XCTAssertTrue(LeftoverKnownResourceFilter.relevant(to: [], resources: [resource]).isEmpty)
    }

    func testKnownResourceCatalogSupportsCrossMonthProbableAndHashMatches() throws {
        let catalog = try LeftoverKnownResourceCatalog()
        let hash = Data([1, 2, 3])
        let resource = LeftoverKnownResource(
            month: LibraryMonthKey(year: 2023, month: 12),
            fileName: "photo_2.jpg",
            fileSize: 42,
            contentHash: hash,
            creationDateMs: 1_002_000
        )
        try catalog.insert([resource])
        try catalog.finalize()
        let file = LeftoverFile(
            month: month,
            fileName: "photo_1.jpg",
            path: monthPath() + "/photo_1.jpg",
            size: 42,
            modificationDateMs: 1_000_000
        )

        let probable = try catalog.probableMatches(for: [file])[file.path]
        let hashMatches = try catalog.resources(matchingHash: hash)

        XCTAssertEqual(probable?.totalCount, 1)
        XCTAssertEqual(probable?.matches.first?.resource, resource)
        XCTAssertEqual(probable?.matches.first?.hasSimilarName, true)
        XCTAssertEqual(probable?.matches.first?.hasMatchingTime, true)
        XCTAssertEqual(hashMatches, [resource])
    }

    func testContentHashCheckerReportsManifestMatches() async throws {
        let client = InMemoryRemoteStorageClient()
        let bytes = Data("same content".utf8)
        let leftover = target("leftover.jpg", size: Int64(bytes.count))
        await client.seedFile(path: leftover.path, data: bytes)
        let hash = Data(SHA256.hash(data: bytes))
        let known = LeftoverKnownResource(
            month: LibraryMonthKey(year: 2023, month: 12),
            fileName: "original.jpg",
            fileSize: Int64(bytes.count),
            contentHash: hash
        )

        let status = try await LeftoverContentHashChecker(
            client: client,
            basePath: base,
            knownResourcesByHash: [hash: [known]]
        ).check(leftover)

        XCTAssertEqual(status, .matched(hashHex: hash.hexString, resources: [known]))
    }

    func testContentHashCheckerUsesTheScanCatalog() async throws {
        let client = InMemoryRemoteStorageClient()
        let bytes = Data("catalog content".utf8)
        let leftover = target("leftover.jpg", size: Int64(bytes.count))
        await client.seedFile(path: leftover.path, data: bytes)
        let hash = Data(SHA256.hash(data: bytes))
        let known = LeftoverKnownResource(
            month: LibraryMonthKey(year: 2023, month: 12),
            fileName: "original.jpg",
            fileSize: Int64(bytes.count),
            contentHash: hash
        )
        let catalog = try LeftoverKnownResourceCatalog()
        try catalog.insert([known])
        try catalog.finalize()

        let status = try await LeftoverContentHashChecker(
            client: client,
            basePath: base,
            knownResourcesByHash: [:],
            knownResourceCatalog: catalog
        ).check(leftover)

        XCTAssertEqual(status, .matched(hashHex: hash.hexString, resources: [known]))
    }

    func testContentHashCheckerReportsNoMatch() async throws {
        let client = InMemoryRemoteStorageClient()
        let bytes = Data("unique content".utf8)
        let leftover = target("leftover.jpg", size: Int64(bytes.count))
        await client.seedFile(path: leftover.path, data: bytes)
        let hash = Data(SHA256.hash(data: bytes))

        let status = try await LeftoverContentHashChecker(
            client: client,
            basePath: base,
            knownResourcesByHash: [:]
        ).check(leftover)

        XCTAssertEqual(status, .noMatch(hashHex: hash.hexString))
    }

    func testScanDoesNotOfferMissingManifestResourceAsAHashMatch() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: monthPath() + "/leftover.jpg", data: Data(count: 10))
        let missing = LeftoverKnownResource(
            month: month,
            fileName: "missing.jpg",
            fileSize: 10,
            contentHash: Data([9, 9, 9])
        )
        let scanner = LeftoverFileScanner(
            client: client,
            basePath: base,
            months: [month],
            manifestSnapshots: { _ in
                LeftoverManifestSnapshot(
                    fileNames: ["missing.jpg"],
                    assetFingerprintHexes: [],
                    knownResources: [missing]
                )
            }
        )

        let result = try await scanner.scanData()

        XCTAssertTrue(result.knownResources.isEmpty)
    }

    func testScanDoesNotOfferSizeMismatchedManifestResourceAsAHashMatch() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: monthPath() + "/original.jpg", data: Data(count: 99))
        let known = LeftoverKnownResource(
            month: month,
            fileName: "original.jpg",
            fileSize: 10,
            contentHash: Data([9, 9, 9])
        )
        let scanner = LeftoverFileScanner(
            client: client,
            basePath: base,
            months: [month],
            manifestSnapshots: { _ in
                LeftoverManifestSnapshot(
                    fileNames: ["original.jpg"],
                    assetFingerprintHexes: [],
                    knownResources: [known]
                )
            }
        )

        let result = try await scanner.scanData()

        XCTAssertTrue(result.knownResources.isEmpty)
    }

    func testScanDoesNotOfferCaseVariantAsAHashMatch() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: monthPath() + "/ORIGINAL.jpg", data: Data(count: 10))
        let known = LeftoverKnownResource(
            month: month,
            fileName: "original.jpg",
            fileSize: 10,
            contentHash: Data([9, 9, 9])
        )
        let scanner = LeftoverFileScanner(
            client: client,
            basePath: base,
            months: [month],
            manifestSnapshots: { _ in
                LeftoverManifestSnapshot(
                    fileNames: ["original.jpg"],
                    assetFingerprintHexes: [],
                    knownResources: [known]
                )
            }
        )

        let result = try await scanner.scanData()

        XCTAssertTrue(result.knownResources.isEmpty)
    }

    func testProbableMatchFindsCollisionSuffixWithSameSize() {
        let file = target("IMG_0001_1.JPG", size: 42)
        let resource = LeftoverKnownResource(
            month: month,
            fileName: "IMG_0001.JPG",
            fileSize: 42,
            contentHash: Data([1])
        )

        let summary = LeftoverProbableMatchFinder.find(
            files: [file],
            resources: [resource]
        )[file.path]

        XCTAssertEqual(summary?.totalCount, 1)
        XCTAssertEqual(summary?.matches.first?.resource, resource)
        XCTAssertEqual(summary?.matches.first?.hasSimilarName, true)
        XCTAssertEqual(summary?.matches.first?.hasMatchingTime, false)
    }

    func testProbableMatchFindsCollisionSuffixOnKnownResource() {
        let file = target("photo.jpg", size: 42)
        let resource = LeftoverKnownResource(
            month: month,
            fileName: "photo_1.jpg",
            fileSize: 42,
            contentHash: Data([1])
        )

        let summary = LeftoverProbableMatchFinder.find(
            files: [file],
            resources: [resource]
        )[file.path]

        XCTAssertEqual(summary?.matches.first?.resource, resource)
        XCTAssertEqual(summary?.matches.first?.hasSimilarName, true)
    }

    func testProbableMatchFindsSameSizeAndTimeWithDifferentName() {
        let file = LeftoverFile(
            month: month,
            fileName: "leftover.jpg",
            path: monthPath() + "/leftover.jpg",
            size: 42,
            modificationDateMs: 1_000_000
        )
        let resource = LeftoverKnownResource(
            month: month,
            fileName: "original.jpg",
            fileSize: 42,
            contentHash: Data([1]),
            creationDateMs: 1_002_000
        )

        let summary = LeftoverProbableMatchFinder.find(
            files: [file],
            resources: [resource]
        )[file.path]

        XCTAssertEqual(summary?.totalCount, 1)
        XCTAssertEqual(summary?.matches.first?.hasSimilarName, false)
        XCTAssertEqual(summary?.matches.first?.hasMatchingTime, true)
    }

    func testProbableMatchTimeToleranceIsInclusiveAtThreeSeconds() {
        let file = LeftoverFile(
            month: month,
            fileName: "leftover.jpg",
            path: monthPath() + "/leftover.jpg",
            size: 42,
            modificationDateMs: 1_000_000
        )
        let atBoundary = LeftoverKnownResource(
            month: month,
            fileName: "boundary.jpg",
            fileSize: 42,
            contentHash: Data([1]),
            creationDateMs: 1_003_000
        )
        let outsideBoundary = LeftoverKnownResource(
            month: month,
            fileName: "outside.jpg",
            fileSize: 42,
            contentHash: Data([2]),
            creationDateMs: 1_003_001
        )

        let summary = LeftoverProbableMatchFinder.find(
            files: [file],
            resources: [atBoundary, outsideBoundary]
        )[file.path]

        XCTAssertEqual(summary?.matches.map(\.resource), [atBoundary])
    }

    func testProbableMatchRejectsOverflowingTimestampDifference() {
        let file = LeftoverFile(
            month: month,
            fileName: "leftover.jpg",
            path: monthPath() + "/leftover.jpg",
            size: 42,
            modificationDateMs: Int64.min
        )
        let resource = LeftoverKnownResource(
            month: month,
            fileName: "original.jpg",
            fileSize: 42,
            contentHash: Data([1]),
            creationDateMs: Int64.max
        )

        let summary = LeftoverProbableMatchFinder.find(
            files: [file],
            resources: [resource]
        )[file.path]

        XCTAssertNil(summary)
    }

    func testProbableMatchRequiresKnownEqualSize() {
        let file = LeftoverFile(
            month: month,
            fileName: "IMG_0001_1.JPG",
            path: monthPath() + "/IMG_0001_1.JPG",
            size: 42,
            modificationDateMs: 1_000_000
        )
        let resource = LeftoverKnownResource(
            month: month,
            fileName: "IMG_0001.JPG",
            fileSize: 41,
            contentHash: Data([1]),
            creationDateMs: 1_000_000
        )

        let matches = LeftoverProbableMatchFinder.find(files: [file], resources: [resource])

        XCTAssertNil(matches[file.path])
    }

    func testProbableMatchCapsDisplayedCandidates() {
        let file = target("photo_9.jpg", size: 42)
        let resources = (1 ... 4).map { index in
            LeftoverKnownResource(
                month: month,
                fileName: "photo.jpg",
                fileSize: 42,
                contentHash: Data([UInt8(index)])
            )
        }

        let summary = LeftoverProbableMatchFinder.find(
            files: [file],
            resources: resources
        )[file.path]

        XCTAssertEqual(summary?.totalCount, 4)
        XCTAssertEqual(summary?.matches.count, 3)
    }

    func testProbableMatchDoesNotStripOriginalCameraSequenceFromBothNames() {
        let file = target("IMG_0001.JPG", size: 42)
        let resource = LeftoverKnownResource(
            month: month,
            fileName: "IMG_0002.JPG",
            fileSize: 42,
            contentHash: Data([1])
        )

        let matches = LeftoverProbableMatchFinder.find(files: [file], resources: [resource])

        XCTAssertNil(matches[file.path])
    }

    func testProbableMatchFindsDifferentMembersOfTheSameCollisionFamily() {
        let file = target("aaaabhe_1", size: 42)
        let resource = LeftoverKnownResource(
            month: month,
            fileName: "aaaabhe_2",
            fileSize: 42,
            contentHash: Data([1])
        )

        let summary = LeftoverProbableMatchFinder.find(files: [file], resources: [resource])[file.path]

        XCTAssertEqual(summary?.matches.first?.resource, resource)
        XCTAssertEqual(summary?.matches.first?.hasSimilarName, true)
    }

    func testScanCapturesRemoteModificationTimeForProbableMatching() async throws {
        let client = InMemoryRemoteStorageClient()
        let modifiedAt = Date(timeIntervalSince1970: 1_000)
        await client.seedFile(
            path: monthPath() + "/leftover.jpg",
            data: Data(count: 10),
            modificationDate: modifiedAt
        )
        let scanner = makeScanner(client: client, months: [month]) { _ in [] }

        let result = try await scanner.scan()

        XCTAssertEqual(
            result.allFiles.first?.modificationDateMs,
            modifiedAt.millisecondsSinceEpoch
        )
    }

    func testScanIgnoresUploadTimeWhenBackendDoesNotPreserveShotDate() async throws {
        let client = InMemoryRemoteStorageClient(supportsModificationDate: false)
        await client.seedFile(
            path: monthPath() + "/leftover.jpg",
            data: Data(count: 10),
            modificationDate: Date(timeIntervalSince1970: 1_000)
        )
        let scanner = makeScanner(client: client, months: [month]) { _ in [] }

        let result = try await scanner.scan()

        XCTAssertNil(result.allFiles.first?.modificationDateMs)
    }

    // A month whose manifest can't be established (nil) is skipped — its data files are never leftover files.
    func testScanSkipsMonthWithoutManifest() async throws {
        let client = InMemoryRemoteStorageClient()
        for name in ["a.jpg", "b.jpg"] {
            await client.seedFile(path: monthPath() + "/" + name, data: Data(count: 10))
        }
        let scanner = makeScanner(client: client, months: [month]) { _ in nil }

        let result = try await scanner.scan()

        XCTAssertEqual(result.totalCount, 0)
    }

    // Directory entries and the manifest sibling are never leftover candidates.
    func testScanExcludesDirectoriesAndManifestFile() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: monthPath() + "/a.jpg", data: Data(count: 10))
        await client.seedFile(path: monthPath() + "/b.jpg", data: Data(count: 10))
        await client.seedFile(path: monthPath() + "/" + MonthManifestStore.manifestFileName, data: Data(count: 5))
        await client.seedDirectory(monthPath() + "/nested")

        let scanner = makeScanner(client: client, months: [month]) { _ in ["a.jpg"] }

        let result = try await scanner.scan()

        XCTAssertEqual(result.allFiles.map(\.fileName), ["b.jpg"])
    }

    func testDeleteRemovesConfirmedLeftover() async throws {
        let client = InMemoryRemoteStorageClient()
        for name in ["a.jpg", "b.jpg", "c.jpg"] {
            await client.seedFile(path: monthPath() + "/" + name, data: Data(count: 10))
        }
        let scanner = makeScanner(client: client, months: [month]) { _ in ["a.jpg", "b.jpg"] }

        let result = try await scanner.delete([target("c.jpg")], assertOwnership: nil)

        XCTAssertEqual(result.deletedCount, 1)
        XCTAssertEqual(result.failedCount, 0)
        let deleted = await client.deletedPaths
        XCTAssertEqual(deleted, [monthPath() + "/c.jpg"])
    }

    // A file recorded by the manifest between scan and delete is no longer a leftover and must be kept.
    func testDeleteSkipsFileNowRecordedByManifest() async throws {
        let client = InMemoryRemoteStorageClient()
        for name in ["a.jpg", "b.jpg", "c.jpg"] {
            await client.seedFile(path: monthPath() + "/" + name, data: Data(count: 10))
        }
        // Manifest now records c.jpg too — it is no longer a leftover.
        let scanner = makeScanner(client: client, months: [month]) { _ in ["a.jpg", "b.jpg", "c.jpg"] }

        let result = try await scanner.delete([target("c.jpg")], assertOwnership: nil)

        XCTAssertEqual(result.deletedCount, 0)
        XCTAssertEqual(result.failedCount, 1)
        let deleted = await client.deletedPaths
        XCTAssertTrue(deleted.isEmpty)
    }

    // A same-named unrecorded file replaced (different size) between scan and confirm must not be deleted —
    // the user reviewed the old bytes.
    func testDeleteSkipsFileReplacedSinceScan() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: monthPath() + "/c.jpg", data: Data(count: 10))
        let scanner = makeScanner(client: client, months: [month]) { _ in [] }
        let reviewed = target("c.jpg", size: 10)
        // The file is swapped for different content (size 999) before the user confirms.
        await client.seedFile(path: monthPath() + "/c.jpg", data: Data(count: 999))

        let result = try await scanner.delete([reviewed], assertOwnership: nil)

        XCTAssertEqual(result.deletedCount, 0)
        XCTAssertEqual(result.failedCount, 1)
        let deleted = await client.deletedPaths
        XCTAssertTrue(deleted.isEmpty, "a file replaced since the scan must not be deleted")
    }

    // A backend that never reports a size lists 0 on both sides; 0 == 0 still deletes (name-only in effect).
    func testDeleteDeletesWhenBothSizesUnknown() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: monthPath() + "/c.jpg", data: Data(count: 0))
        let scanner = makeScanner(client: client, months: [month]) { _ in [] }

        let result = try await scanner.delete([target("c.jpg", size: 0)], assertOwnership: nil)

        XCTAssertEqual(result.deletedCount, 1)
        XCTAssertEqual(result.failedCount, 0)
    }

    // A size that disappears (reviewed 10 → listed 0) or appears (reviewed 0 → listed 999) signals a swapped
    // file and must fail closed, not delete.
    func testDeleteSkipsWhenSizeChangesToOrFromUnknown() async throws {
        let clientA = InMemoryRemoteStorageClient()
        await clientA.seedFile(path: monthPath() + "/c.jpg", data: Data(count: 10))
        let scannerA = makeScanner(client: clientA, months: [month]) { _ in [] }
        await clientA.seedFile(path: monthPath() + "/c.jpg", data: Data(count: 0))
        let resultA = try await scannerA.delete([target("c.jpg", size: 10)], assertOwnership: nil)
        XCTAssertEqual(resultA.deletedCount, 0)
        XCTAssertEqual(resultA.failedCount, 1)
        let deletedA = await clientA.deletedPaths
        XCTAssertTrue(deletedA.isEmpty)

        let clientB = InMemoryRemoteStorageClient()
        await clientB.seedFile(path: monthPath() + "/c.jpg", data: Data(count: 0))
        let scannerB = makeScanner(client: clientB, months: [month]) { _ in [] }
        await clientB.seedFile(path: monthPath() + "/c.jpg", data: Data(count: 999))
        let resultB = try await scannerB.delete([target("c.jpg", size: 0)], assertOwnership: nil)
        XCTAssertEqual(resultB.deletedCount, 0)
        XCTAssertEqual(resultB.failedCount, 1)
        let deletedB = await clientB.deletedPaths
        XCTAssertTrue(deletedB.isEmpty)
    }

    // The catastrophic-deletion guard: a nil manifest at delete time must delete nothing, not everything.
    func testDeleteSkipsEntireMonthWhenManifestUnavailable() async throws {
        let client = InMemoryRemoteStorageClient()
        for name in ["a.jpg", "b.jpg"] {
            await client.seedFile(path: monthPath() + "/" + name, data: Data(count: 10))
        }
        let scanner = makeScanner(client: client, months: [month]) { _ in nil }
        let progress = ProgressRecorder()

        let result = try await scanner.delete(
            [target("a.jpg"), target("b.jpg")],
            assertOwnership: nil
        ) { current, _ in
            await progress.append(current)
        }

        XCTAssertEqual(result.deletedCount, 0)
        XCTAssertEqual(result.failedCount, 2)
        let progressValues = await progress.values
        XCTAssertEqual(progressValues, [2])
        let deleted = await client.deletedPaths
        XCTAssertTrue(deleted.isEmpty)
    }

    @MainActor
    func testProgressReporterUsesAbsoluteBatchProgress() async {
        var snapshots: [RemoteSyncProgress] = []
        let reporter = LeftoverProgressReporter(
            total: 2,
            kind: .leftoverMaintenance(.deletingFiles)
        ) {
            snapshots.append($0)
        }

        await reporter.start()
        await reporter.update(current: 2)

        XCTAssertEqual(snapshots.map(\.current), [0, 2])
        XCTAssertEqual(snapshots.map(\.total), [2, 2])
    }

    func testDeleteStopsWhenOwnershipAssertionFails() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: monthPath() + "/c.jpg", data: Data(count: 10))
        let scanner = makeScanner(client: client, months: [month]) { _ in ["a.jpg"] }

        struct OwnershipLost: Error {}
        do {
            _ = try await scanner.delete([target("c.jpg")], assertOwnership: .uniform { throw OwnershipLost() })
            XCTFail("expected ownership assertion to abort the delete")
        } catch is OwnershipLost {
            // expected
        }

        let deleted = await client.deletedPaths
        XCTAssertTrue(deleted.isEmpty, "no file may be deleted once ownership cannot be proven")
    }

    // Ownership is re-proven before EVERY irreversible delete, not once per batch/month.
    func testDeleteAssertsOwnershipBeforeEachFile() async throws {
        let client = InMemoryRemoteStorageClient()
        for name in ["a.jpg", "b.jpg", "c.jpg"] {
            await client.seedFile(path: monthPath() + "/" + name, data: Data(count: 10))
        }
        let scanner = makeScanner(client: client, months: [month]) { _ in [] }
        let counter = CallCounter()

        let result = try await scanner.delete(
            [target("a.jpg"), target("b.jpg"), target("c.jpg")],
            assertOwnership: .uniform { await counter.bump() }
        )

        XCTAssertEqual(result.deletedCount, 3)
        let calls = await counter.count
        XCTAssertEqual(calls, 3, "ownership must be proven once per deleted file")
    }

    // A case-/Unicode-variant of a recorded file must never be deleted (case-insensitive backends).
    func testScanFoldsCaseAndDiacriticsAgainstManifest() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: monthPath() + "/photo.jpg", data: Data(count: 10))
        await client.seedFile(path: monthPath() + "/café.jpg", data: Data(count: 10))
        let scanner = makeScanner(client: client, months: [month]) { _ in ["Photo.JPG", "cafe.jpg"] }

        let result = try await scanner.scan()

        XCTAssertEqual(result.totalCount, 0)
    }

    func testScanAndDeleteSpanMultipleMonths() async throws {
        let m1 = LibraryMonthKey(year: 2024, month: 1)
        let m2 = LibraryMonthKey(year: 2024, month: 2)
        let client = InMemoryRemoteStorageClient()
        await seed(client, m1, "x.jpg"); await seed(client, m1, "keep1.jpg")
        await seed(client, m2, "y.jpg"); await seed(client, m2, "keep2.jpg")
        let manifest: [LibraryMonthKey: Set<String>] = [m1: ["keep1.jpg"], m2: ["keep2.jpg"]]
        let scanner = LeftoverFileScanner(client: client, basePath: base, months: [m1, m2]) { manifest[$0] }

        let scan = try await scanner.scan()
        XCTAssertEqual(Set(scan.allFiles.map(\.fileName)), ["x.jpg", "y.jpg"])

        let del = try await scanner.delete(scan.allFiles, assertOwnership: nil)
        XCTAssertEqual(del.deletedCount, 2)
        XCTAssertEqual(del.failedCount, 0)
        let deleted = await client.deletedPaths
        XCTAssertEqual(deleted.count, 2)
    }

    // A month whose manifest faults mid-delete is skipped (its targets fail) but other months proceed.
    func testDeleteContinuesPastAFaultingMonth() async throws {
        let m1 = LibraryMonthKey(year: 2024, month: 1)
        let m2 = LibraryMonthKey(year: 2024, month: 2)
        let client = InMemoryRemoteStorageClient()
        await seed(client, m1, "a.jpg")
        await seed(client, m2, "b.jpg")
        let scanner = LeftoverFileScanner(client: client, basePath: base, months: [m1, m2]) { month in
            if month == m1 { throw RemoteErrorFixtures.retryable }
            return []
        }

        let result = try await scanner.delete([target(m1, "a.jpg"), target(m2, "b.jpg")], assertOwnership: nil)

        XCTAssertEqual(result.deletedCount, 1)
        XCTAssertEqual(result.failedCount, 1)
        let deleted = await client.deletedPaths
        XCTAssertEqual(deleted, [LeftoverFileScanner.monthDataPath(basePath: base, month: m2) + "/b.jpg"])
    }

    // Mixed batch: some deleted, one no-longer-leftover; counts must sum to the target count.
    func testDeleteMixedOutcomeAccounting() async throws {
        let client = InMemoryRemoteStorageClient()
        for name in ["a.jpg", "b.jpg", "c.jpg"] {
            await client.seedFile(path: monthPath() + "/" + name, data: Data(count: 10))
        }
        let scanner = makeScanner(client: client, months: [month]) { _ in ["b.jpg"] }

        let result = try await scanner.delete(
            [target("a.jpg"), target("b.jpg"), target("c.jpg")],
            assertOwnership: nil
        )

        XCTAssertEqual(result.deletedCount, 2)
        XCTAssertEqual(result.failedCount, 1)
        XCTAssertEqual(result.deletedCount + result.failedCount, 3)
    }

    // A non-notFound fault from the manifest provider aborts the whole scan (fail closed).
    func testScanPropagatesManifestFault() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: monthPath() + "/a.jpg", data: Data(count: 10))
        let scanner = makeScanner(client: client, months: [month]) { _ in throw RemoteErrorFixtures.retryable }

        do {
            _ = try await scanner.scan()
            XCTFail("expected scan to fail closed on a transport fault")
        } catch {
            // expected
        }
    }
}
