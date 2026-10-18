import ImageIO
import Photos
import UniformTypeIdentifiers
import XCTest
@testable import Watermelon

final class PhotoLibraryAlbumIntegrationTests: XCTestCase {

    private func setup() throws -> (URL, PhotoLibraryService, ContentHashIndexRepository) {
        #if targetEnvironment(simulator)
        let marker = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0].appendingPathComponent("RUN_BURST_INTEGRATION")
        guard FileManager.default.fileExists(atPath: marker.path), PHPhotoLibrary.authorizationStatus(for: .readWrite) == .authorized else {
            throw XCTSkip("Requires opt-in disposable simulator")
        }
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let repository = ContentHashIndexRepository(databaseManager: try DatabaseManager(databaseURL: directory.appendingPathComponent("index.sqlite")))
        return (directory, PhotoLibraryService(), repository)
        #else
        throw XCTSkip("Simulator-only PhotoKit regression")
        #endif
    }

    private func create(files: [URL], dates: [Date], albumCount: Int) async throws -> ([String], [String]) {
        var ids: [String] = []
        var albums: [String] = []
        try await PHPhotoLibrary.shared().performChanges {
            let placeholders = NSMutableArray()
            for (index, file) in files.enumerated() {
                let request = PHAssetCreationRequest.forAsset()
                request.creationDate = dates[index]
                request.addResource(with: .photo, fileURL: file, options: nil)
                if let placeholder = request.placeholderForCreatedAsset {
                    ids.append(placeholder.localIdentifier)
                    placeholders.add(placeholder)
                }
            }
            for _ in 0..<albumCount {
                let album = PHAssetCollectionChangeRequest.creationRequestForAssetCollection(withTitle: "Album regression \(UUID())")
                album.addAssets(placeholders)
                albums.append(album.placeholderForCreatedAssetCollection.localIdentifier)
            }
        }
        return (ids, albums)
    }

    func testCommittedIndexReconciliationUsesPhotosBeforeChangeDelivery() async throws {
        let (directory, service, repository) = try setup()
        defer { try? FileManager.default.removeItem(at: directory) }
        let file = try makeBurstFrame(index: 0, burstID: nil, directory: directory)
        let (ids, _) = try await create(files: [file], dates: [Date()], albumCount: 0)
        let id = try XCTUnwrap(ids.first)
        let worker = HomeDataProcessingWorker(photoLibraryService: service,
            contentHashIndexRepository: repository, remoteMonthSnapshot: { _ in nil })
        _ = await worker.reloadLocalIndex(scope: .device(.all))
        let fingerprint = Data(repeating: 0x55, count: 32)
        try repository.upsertAssetFingerprint(assetLocalIdentifier: id, assetFingerprint: fingerprint,
            resourceCount: 1, totalFileSizeBytes: 100, modificationDateMs: nil)
        let stale = await worker.browserLocalSeed(expectedScope: .device(.all))
        XCTAssertNil(stale)
        let indexed = await worker.refreshLocalIndex(forAssetIDs: [], expectedScope: .device(.all))
        guard case .incremental(_, let membershipChangedMonths) = indexed else {
            return XCTFail("A retained delta must stay incremental")
        }
        XCTAssertTrue(membershipChangedMonths.isEmpty)
        let seed = await worker.browserLocalSeed(expectedScope: .device(.all))
        XCTAssertEqual(seed?.localIDByFingerprint[fingerprint], id)

        try repository.deleteIndexEntries(assetIDs: ids)
        _ = await worker.refreshLocalIndex(forAssetIDs: [], expectedScope: .device(.all))
        let unindexed = await worker.browserLocalSeed(expectedScope: .device(.all))
        XCTAssertTrue(unindexed?.assets.contains { $0.localIdentifier == id } == true)
        XCTAssertNil(unindexed?.localIDByFingerprint[fingerprint])


    }

    func testOrdinaryAlbumChangeKeepsUnrelatedMonthSize() async throws {
        let (directory, service, repository) = try setup()
        let dates = [Date(timeIntervalSince1970: 978_350_400), Date(timeIntervalSince1970: 982_065_600)]
        let files = try (0..<3).map { try makeBurstFrame(index: $0, burstID: nil, directory: directory) }
        let (ids, albums) = try await create(files: files, dates: dates + [dates[0]], albumCount: 1)
        XCTAssertEqual(ids.count, 3)
        // Let Photos finish post-import notifications before observing the favorite edit.
        try await Task.sleep(for: .seconds(1))
        let scope = HomeLocalLibraryScope.albums(Set(albums))
        let worker = HomeDataProcessingWorker(photoLibraryService: service, contentHashIndexRepository: repository, remoteMonthSnapshot: { _ in nil })
        _ = await worker.reloadLocalIndex(scope: scope)
        let months = dates.map { LibraryMonthKey.from(date: $0, calendar: LibraryMonthKey.currentPreferenceMonthCalendar()) }
        for month in months { _ = await worker.updateFileSize(for: month, sizeCache: [:]) }
        XCTAssertNotNil(worker.monthRow(for: months[1]).local?.totalSizeBytes)
        let asset = try XCTUnwrap(service.fetchAssets(localIdentifiers: [ids[0]]).first)
        XCTAssertNil(asset.burstIdentifier)
        XCTAssertFalse(asset.isFavorite)
        let otherAsset = try XCTUnwrap(service.fetchAssets(localIdentifiers: [ids[2]]).first)
        let unrelated = expectation(description: "Unrelated photo change processed")
        let changed = expectation(description: "Favorite ordinary photo")
        var delivered = false
        var unrelatedDelivered = false
        let observer = AlbumChangeObserver { change in
            let targetFavorited = change.changeDetails(for: asset)?.objectAfterChanges?.isFavorite == true
            let otherFavorited = change.changeDetails(for: otherAsset)?.objectAfterChanges?.isFavorite == true
            worker.handlePhotoLibraryChange(change) { result in
                if targetFavorited, !delivered {
                    delivered = true
                    print("ALBUM_REGRESSION ordinary favorite changedMonths=\(result.changedMonths) unrelatedSize=\(String(describing: worker.monthRow(for: months[1]).local?.totalSizeBytes))")
                    XCTAssertTrue(result.changedMonths.contains(months[0]))
                    XCTAssertFalse(result.changedMonths.contains(months[1]), "Unrelated month should not be invalidated")
                    XCTAssertNotNil(worker.monthRow(for: months[1]).local?.totalSizeBytes, "Unrelated size should survive one favorite change")
                    changed.fulfill()
                }
                if otherFavorited, !unrelatedDelivered {
                    unrelatedDelivered = true
                    XCTAssertFalse(delivered, "Another photo's change must not complete the favorite check")
                    unrelated.fulfill()
                }
            }
        }
        PHPhotoLibrary.shared().register(observer)
        defer { PHPhotoLibrary.shared().unregisterChangeObserver(observer) }
        try await PHPhotoLibrary.shared().performChanges { PHAssetChangeRequest(for: otherAsset).isFavorite = true }
        await fulfillment(of: [unrelated], timeout: 15)
        try await PHPhotoLibrary.shared().performChanges { PHAssetChangeRequest(for: asset).isFavorite = true }
        await fulfillment(of: [changed], timeout: 15)
    }

    func testBurstCrossMonthFilteringAndOverlappingAlbums() async throws {
        let (directory, service, repository) = try setup()
        let burstID = UUID().uuidString
        let dates = (0..<3).map { Date(timeIntervalSince1970: 978_350_400 + Double($0) * 40 * 86400) }
        let files = try (0..<3).map { try makeBurstFrame(index: $0, burstID: burstID, directory: directory) }
        let (ids, albums) = try await create(files: files, dates: dates, albumCount: 2)
        XCTAssertEqual(ids.count, 3)
        XCTAssertEqual(Set(service.fetchAssets(localIdentifiers: [ids[1]]).map(\.localIdentifier)), [ids[1]])
        XCTAssertEqual(service.collectAssetIDs(query: .albums(Set(albums))), Set(ids))
        XCTAssertEqual(Set(service.fetchAssets(inAlbumIdentifiers: Set(albums), since: dates[1]).map(\.localIdentifier)), Set(ids.dropFirst()))
        let counts = service.fetchUserAlbums()
        for album in albums { XCTAssertEqual(counts.first { $0.localIdentifier == album }?.assetCount, 3) }
        let scope = HomeLocalLibraryScope.albums(Set(albums))
        let worker = HomeDataProcessingWorker(photoLibraryService: service, contentHashIndexRepository: repository, remoteMonthSnapshot: { _ in nil })
        _ = await worker.reloadLocalIndex(scope: scope)
        let months = dates.map { LibraryMonthKey.from(date: $0, calendar: LibraryMonthKey.currentPreferenceMonthCalendar()) }
        for (i, month) in months.enumerated() {
            XCTAssertEqual(worker.localAssetIDs(for: month, expectedScope: scope), [ids[i]])
            XCTAssertEqual(worker.monthRow(for: month).local?.assetCount, 1)
        }
        let collection = try XCTUnwrap(PHAssetCollection.fetchAssetCollections(withLocalIdentifiers: [albums[0]], options: nil).firstObject)
        let members = PHAsset.fetchAssets(in: collection, options: nil)
        let representative = try XCTUnwrap(members.firstObject)
        XCTAssertFalse(representative.isFavorite)
        let unrelated = expectation(description: "Burst favorite change processed before removal")
        let changed = expectation(description: "Remove from first overlapping album")
        var delivered = false
        var unrelatedDelivered = false
        let observer = AlbumChangeObserver { change in
            let albumEmptied = change.changeDetails(for: members)?.fetchResultAfterChanges.count == 0
            let representativeFavorited = change.changeDetails(for: representative)?.objectAfterChanges?.isFavorite == true
            worker.handlePhotoLibraryChange(change) { _ in
                guard representativeFavorited, !unrelatedDelivered else { return }
                unrelatedDelivered = true
                XCTAssertFalse(delivered, "A favorite change must not complete the album removal check")
                unrelated.fulfill()
            }
            guard albumEmptied else { return }
            DispatchQueue.main.async {
                guard !delivered else { return }
                delivered = true
                // A membership-only change may emit no callback; these reads wait for the worker's queue.
                for (i, month) in months.enumerated() { XCTAssertEqual(worker.localAssetIDs(for: month, expectedScope: scope), [ids[i]]) }
                changed.fulfill()
            }
        }
        PHPhotoLibrary.shared().register(observer)
        defer { PHPhotoLibrary.shared().unregisterChangeObserver(observer) }
        try await PHPhotoLibrary.shared().performChanges { PHAssetChangeRequest(for: representative).isFavorite = true }
        await fulfillment(of: [unrelated], timeout: 15)
        try await PHPhotoLibrary.shared().performChanges { PHAssetCollectionChangeRequest(for: collection)?.removeAssets(members) }
        await fulfillment(of: [changed], timeout: 15)
        XCTAssertEqual(service.collectAssetIDs(query: .albums(Set(albums))), Set(ids))
        let remaining = service.fetchUserAlbums()
        XCTAssertEqual(remaining.first { $0.localIdentifier == albums[0] }?.assetCount, 0)
        XCTAssertEqual(remaining.first { $0.localIdentifier == albums[1] }?.assetCount, 3)
    }

    func testRealRestoreWritesEveryBurstFrameFingerprint() async throws {
        let (directory, library, repository) = try setup()
        let burstID = UUID().uuidString
        let files = try (0..<3).map { try makeBurstFrame(index: $0, burstID: burstID, directory: directory) }
        let profile = ServerProfileRecord(id: nil, name: "review", storageType: StorageType.webdav.rawValue,
            connectionParams: nil, sortOrder: 0, host: "fixture.local", port: 0, shareName: "fixture", basePath: "/p",
            username: "fixture", domain: nil, credentialRef: "fixture", backgroundBackupEnabled: false,
            createdAt: Date(), updatedAt: Date(), writerID: nil)
        var ids = Set<String>()
        var fingerprints = Set<Data>()
        for file in files {
            let data = try Data(contentsOf: file)
            let hash = try AssetProcessor.contentHash(of: file)
            let instance = RemoteAssetResourceInstance(role: 1, slot: 0, resourceHash: hash, fileName: file.lastPathComponent,
                fileSize: Int64(data.count), remoteRelativePath: "2001/01/" + file.lastPathComponent, creationDateMs: 978_350_400_000)
            let fp = BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: [(role: 1, slot: 0, contentHash: hash)])
            let client = InMemoryRemoteStorageClient()
            await client.enqueueDownloadData(data)
            let service = RestoreService(makeClient: { _, _ in client }, hashIndexRepository: repository)
            let result = try await service.restoreItems(items: [.init(instances: [instance], identity: fp, creationDate: Date(timeIntervalSince1970: 978_350_400))], profile: profile, password: "", onItemCompleted: { _, _, _ in })
            let asset = try XCTUnwrap(result.first?.asset)
            ids.insert(asset.localIdentifier)
            fingerprints.insert(fp)
            XCTAssertEqual(try repository.fetchAssetFingerprintRecords(assetIDs: [asset.localIdentifier])[asset.localIdentifier]?.fingerprint, fp)
        }
        XCTAssertEqual(ids.count, 3)
        XCTAssertTrue(ids.isSubset(of: library.collectAssetIDs(query: .library(.all))))
        XCTAssertEqual(Set(library.fetchAssets(localIdentifiers: ids).compactMap(\.burstIdentifier)), [burstID])
        try repository.clearLocalHashIndex()
        let result = try await LocalHashIndexBuildService(photoLibraryService: library, repository: repository).buildIndex(for: ids, workerCount: 2, allowNetworkAccess: false)
        XCTAssertEqual(result.readyAssetIDs, ids)
        XCTAssertEqual(Set(try repository.fetchAssetFingerprintRecords(assetIDs: ids).values.map(\.fingerprint)), fingerprints)
    }

    func testAlbumPickerCostWithoutBursts() async throws {
        let (directory, service, _) = try setup()
        let file = try makeBurstFrame(index: 0, burstID: nil, directory: directory)
        let date = Date(timeIntervalSince1970: 978_350_400)
        let (ids, albums) = try await create(files: Array(repeating: file, count: 2000), dates: Array(repeating: date, count: 2000), albumCount: 4)
        XCTAssertEqual(ids.count, 2000)
        func previousCounts() -> [String: Int] {
            let collections = PHAssetCollection.fetchAssetCollections(with: .album, subtype: .albumRegular, options: nil)
            let options = PHFetchOptions()
            options.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: false)]
            var counts: [String: Int] = [:]
            for i in 0..<collections.count {
                let album = collections.object(at: i)
                let assets = PHAsset.fetchAssets(in: album, options: options)
                _ = assets.firstObject?.localIdentifier
                counts[album.localIdentifier] = assets.count
            }
            return counts
        }
        for round in 0..<3 {
            let before = CFAbsoluteTimeGetCurrent()
            let old = previousCounts()
            let between = CFAbsoluteTimeGetCurrent()
            let current = service.fetchUserAlbums()
            let after = CFAbsoluteTimeGetCurrent()
            for album in albums {
                XCTAssertEqual(old[album], 2000)
                XCTAssertEqual(current.first(where: { $0.localIdentifier == album })?.assetCount, 2000)
            }
            print("ALBUM_REGRESSION picker round=\(round) oldSeconds=\(between-before) newSeconds=\(after-between) photos=2000 overlappingAlbums=4")
        }
    }

    private func assetIDs(_ result: PHFetchResult<PHAsset>) -> Set<String> {
        Set((0..<result.count).map { result.object(at: $0).localIdentifier })
    }

    private func makeBurstFrame(index: Int, burstID: String?, directory: URL) throws -> URL {
        let context = try XCTUnwrap(CGContext(data: nil, width: 64, height: 64, bitsPerComponent: 8,
            bytesPerRow: 256, space: CGColorSpaceCreateDeviceRGB(), bitmapInfo: CGImageAlphaInfo.noneSkipLast.rawValue))
        context.setFillColor(CGColor(red: CGFloat(index) / 3, green: 0.5, blue: 1, alpha: 1))
        context.fill(CGRect(x: 0, y: 0, width: 64, height: 64))
        let image = try XCTUnwrap(context.makeImage())
        let url = directory.appendingPathComponent("burst-\(index).jpg")
        let destination = try XCTUnwrap(CGImageDestinationCreateWithURL(url as CFURL, UTType.jpeg.identifier as CFString, 1, nil))
        CGImageDestinationAddImage(destination, image, [
            kCGImagePropertyMakerAppleDictionary: burstID.map { ["11": $0] } ?? [:],
            kCGImagePropertyTIFFDictionary: [kCGImagePropertyTIFFMake: "Apple", kCGImagePropertyTIFFModel: "iPhone 5s"]
        ] as CFDictionary)
        XCTAssertTrue(CGImageDestinationFinalize(destination))
        return url
    }
}

private final class AlbumChangeObserver: NSObject, PHPhotoLibraryChangeObserver {
    let handler: (PHChange) -> Void

    init(handler: @escaping (PHChange) -> Void) { self.handler = handler }

    func photoLibraryDidChange(_ changeInstance: PHChange) { handler(changeInstance) }
}
