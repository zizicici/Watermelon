import ImageIO
import Photos
import UniformTypeIdentifiers
import XCTest
@testable import Watermelon

final class PhotoLibraryBurstIntegrationTests: XCTestCase {
    func testAllBurstFramesSurviveLibraryAlbumFetchesAndIndexRebuild() async throws {
        #if targetEnvironment(simulator)
        let marker = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("RUN_BURST_INTEGRATION")
        guard FileManager.default.fileExists(atPath: marker.path) else {
            throw XCTSkip("Requires opt-in disposable simulator")
        }
        guard PHPhotoLibrary.authorizationStatus(for: .readWrite) == .authorized else {
            throw XCTSkip("Requires Photos permission on a test simulator")
        }
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let burstID = UUID().uuidString
        let date = Date(timeIntervalSince1970: 978_350_400)
        let files = try (0..<3).map { index in
            try makeBurstFrame(index: index, burstID: burstID, directory: directory)
        }
        var createdIDs = Set<String>()
        var albumID: String?
        try await PHPhotoLibrary.shared().performChanges {
            let placeholders = NSMutableArray()
            for file in files {
                let request = PHAssetCreationRequest.forAsset()
                request.creationDate = date
                request.addResource(with: .photo, fileURL: file, options: nil)
                if let placeholder = request.placeholderForCreatedAsset {
                    createdIDs.insert(placeholder.localIdentifier)
                    placeholders.add(placeholder)
                }
            }
            let album = PHAssetCollectionChangeRequest.creationRequestForAssetCollection(withTitle: "Burst test \(burstID)")
            album.addAssets(placeholders)
            albumID = album.placeholderForCreatedAssetCollection.localIdentifier
        }
        let ids = createdIDs
        let collectionID = try XCTUnwrap(albumID)
        XCTAssertEqual(ids.count, 3)
        let service = PhotoLibraryService()
        let frames = service.fetchAssets(localIdentifiers: ids)
        XCTAssertEqual(Set(frames.map(\.localIdentifier)), ids)
        XCTAssertEqual(Set(frames.compactMap(\.burstIdentifier)), [burstID])
        let defaultVisible = assetIDs(PHAsset.fetchAssets(with: PHFetchOptions())).intersection(ids)
        XCTAssertLessThan(defaultVisible.count, ids.count)

        XCTAssertTrue(ids.isSubset(of: service.collectAssetIDs(query: .library(.all))))
        XCTAssertTrue(ids.isSubset(of: service.collectAssetIDs(query: .library(.photos))))
        XCTAssertTrue(ids.isDisjoint(with: service.collectAssetIDs(query: .library(.videos))))
        try service.validateAlbumSelection([collectionID])
        let collection = try XCTUnwrap(PHAssetCollection.fetchAssetCollections(withLocalIdentifiers: [collectionID], options: nil).firstObject)
        XCTAssertEqual(service.collectAssetIDs(query: .albums([collectionID])), ids)
        XCTAssertEqual(service.fetchUserAlbums().first { $0.localIdentifier == collectionID }?.assetCount, 3)
        XCTAssertEqual(Set(service.fetchAssets(inAlbumIdentifiers: [collectionID]).map(\.localIdentifier)), ids)
        XCTAssertEqual(Set(service.fetchAssets(inAlbumIdentifiers: [collectionID], since: date).map(\.localIdentifier)), ids)
        XCTAssertTrue(service.fetchAssets(inAlbumIdentifiers: [collectionID], since: date.addingTimeInterval(1)).isEmpty)
        XCTAssertTrue(ids.isSubset(of: assetIDs(service.fetchAssetsResult(since: date))))
        XCTAssertTrue(ids.isDisjoint(with: assetIDs(service.fetchAssetsResult(since: date.addingTimeInterval(1)))))

        let repository = ContentHashIndexRepository(databaseManager: try DatabaseManager(databaseURL: directory.appendingPathComponent("index.sqlite")))
        let builder = LocalHashIndexBuildService(photoLibraryService: service, repository: repository)
        let expected = try Set(files.map { file in
            BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: [
                (role: 1, slot: 0, contentHash: try AssetProcessor.contentHash(of: file))
            ])
        })
        XCTAssertEqual(expected.count, 3)
        for _ in 0..<2 {
            try repository.clearLocalHashIndex()
            let scope = service.collectAssetIDs(query: .library(.all)).intersection(ids)
            let result = try await builder.buildIndex(for: scope, workerCount: 2, allowNetworkAccess: false)
            XCTAssertEqual(result.readyAssetIDs, ids)
            XCTAssertTrue(result.incompleteAssetIDs.isEmpty)
            let records = try repository.fetchAssetFingerprintRecords(assetIDs: ids)
            XCTAssertEqual(Set(records.values.map(\.fingerprint)), expected)
        }
        for id in ids {
            let snapshot = try await RestoreService.readImportedAsset(id, planned: [])
            XCTAssertEqual(snapshot.instances.count, 1)
            let fingerprint = BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: snapshot.instances.map {
                (role: $0.role, slot: $0.slot, contentHash: $0.resourceHash)
            })
            XCTAssertTrue(expected.contains(fingerprint))
        }

        let scope = HomeLocalLibraryScope.albums([collectionID])
        let month = LibraryMonthKey.from(date: date, calendar: LibraryMonthKey.currentPreferenceMonthCalendar())
        let worker = HomeDataProcessingWorker(photoLibraryService: service, contentHashIndexRepository: repository,
            remoteMonthSnapshot: { _ in nil })
        _ = await worker.reloadLocalIndex(scope: scope)
        XCTAssertEqual(worker.localAssetIDs(for: month, expectedScope: scope), ids)
        let members = PHAsset.fetchAssets(in: collection, options: nil)
        for shouldAdd in [false, true] {
            let expectedIDs = shouldAdd ? ids : []
            let changed = expectation(description: "Album burst membership changed: \(shouldAdd)")
            let observer = BurstChangeObserver { change in
                worker.handlePhotoLibraryChange(change) { _ in
                    if worker.localAssetIDs(for: month, expectedScope: scope) == expectedIDs {
                        changed.fulfill()
                    }
                }
            }
            PHPhotoLibrary.shared().register(observer)
            defer { PHPhotoLibrary.shared().unregisterChangeObserver(observer) }
            try await PHPhotoLibrary.shared().performChanges {
                let request = PHAssetCollectionChangeRequest(for: collection)
                if shouldAdd { request?.addAssets(members) } else { request?.removeAssets(members) }
            }
            await fulfillment(of: [changed], timeout: 10)
            XCTAssertEqual(worker.localAssetIDs(for: month, expectedScope: scope), expectedIDs)
            XCTAssertEqual(service.collectAssetIDs(query: .albums([collectionID])), expectedIDs)
        }
        #else
        throw XCTSkip("Simulator-only PhotoKit regression")
        #endif
    }

    private func assetIDs(_ result: PHFetchResult<PHAsset>) -> Set<String> {
        Set((0..<result.count).map { result.object(at: $0).localIdentifier })
    }

    private func makeBurstFrame(index: Int, burstID: String, directory: URL) throws -> URL {
        let context = try XCTUnwrap(CGContext(data: nil, width: 64, height: 64, bitsPerComponent: 8,
            bytesPerRow: 256, space: CGColorSpaceCreateDeviceRGB(), bitmapInfo: CGImageAlphaInfo.noneSkipLast.rawValue))
        context.setFillColor(CGColor(red: CGFloat(index) / 3, green: 0.5, blue: 1, alpha: 1))
        context.fill(CGRect(x: 0, y: 0, width: 64, height: 64))
        let image = try XCTUnwrap(context.makeImage())
        let url = directory.appendingPathComponent("burst-\(index).jpg")
        let destination = try XCTUnwrap(CGImageDestinationCreateWithURL(url as CFURL, UTType.jpeg.identifier as CFString, 1, nil))
        CGImageDestinationAddImage(destination, image, [
            kCGImagePropertyMakerAppleDictionary: ["11": burstID],
            kCGImagePropertyTIFFDictionary: [kCGImagePropertyTIFFMake: "Apple", kCGImagePropertyTIFFModel: "iPhone 5s"]
        ] as CFDictionary)
        XCTAssertTrue(CGImageDestinationFinalize(destination))
        return url
    }
}

private final class BurstChangeObserver: NSObject, PHPhotoLibraryChangeObserver {
    let handler: (PHChange) -> Void

    init(handler: @escaping (PHChange) -> Void) { self.handler = handler }

    func photoLibraryDidChange(_ changeInstance: PHChange) { handler(changeInstance) }
}
