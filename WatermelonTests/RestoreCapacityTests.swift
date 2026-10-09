import CryptoKit
import XCTest
@testable import Watermelon

final class RestoreCapacityTests: XCTestCase {
    private let reserve = RestoreDownloadPolicy().freeSpaceReserve

    private func profile(browserLink: Bool = false) -> ServerProfileRecord {
        ServerProfileRecord(id: nil, name: "capacity restore", storageType: StorageType.webdav.rawValue,
            connectionParams: nil, sortOrder: 0, host: "fixture.local", port: 0, shareName: "test", basePath: "/p",
            username: "test", domain: nil,
            credentialRef: browserLink ? ServerProfileRecord.browserLinkCredentialRef(sessionID: UUID().uuidString) : "test",
            backgroundBackupEnabled: false, createdAt: Date(), updatedAt: Date(), writerID: nil)
    }

    private func item(_ name: String, data: Data, recordedSize: Int64) -> RestoreService.RestoreItemDescriptor {
        .init(instances: [.init(role: ResourceTypeCode.photo, slot: 0,
            resourceHash: Data(SHA256.hash(data: data)), fileName: name, fileSize: recordedSize,
            remoteRelativePath: "2026/01/\(name)", creationDateMs: 1_000)], identity: Data(name.utf8))
    }

    func testSerialRestoreCorrectsLegacySizeAndReservesOnlyRemainingImportSpace() async throws {
        let probe = InMemoryRemoteStorageClient(), downloader = InMemoryRemoteStorageClient()
        let factory = RestoreTestClientFactory([probe, downloader])
        let data = Data(repeating: 1, count: 100)
        for client in [probe, downloader] { await client.seedFile(path: "/p/2026/01/first.jpg", data: data) }
        let downloaded = ExecutionTerminationControl()
        await downloader.setOnDownload { _ in downloaded.request(.stop) }
        let reserve = reserve
        let service = RestoreService(makeClient: { _, _ in factory.make() }, importAsset: { files, _ in
            XCTAssertEqual(try Data(contentsOf: XCTUnwrap(files.first?.1)), data)
            return "imported"
        }, availableCapacity: { reserve + (downloaded.shouldDrain ? 200 : 320) })
        let result = try await service.restoreItems(items: [item("first.jpg", data: data, recordedSize: 120)],
            profile: profile(), password: "", onItemCompleted: { index, total, _ in
                XCTAssertEqual(index, 1)
                XCTAssertEqual(total, 1)
            })
        XCTAssertEqual(result.first?.asset.localIdentifier, "imported")
        let metadata = await probe.metadataAttemptPaths
        XCTAssertEqual(metadata, ["/p/2026/01/first.jpg"])
        XCTAssertEqual(factory.createdCount, 2)
        for client in [probe, downloader] {
            let disconnects = await client.disconnectCount
            XCTAssertEqual(disconnects, 1)
        }
    }

    func testSerialBatchCorrectsEachLegacySizeAndKeepsImportOrder() async throws {
        let clients = [InMemoryRemoteStorageClient(), InMemoryRemoteStorageClient(), InMemoryRemoteStorageClient()]
        let factory = RestoreTestClientFactory(clients)
        let names = ["first.jpg", "second.jpg"]
        let data = Data(repeating: 1, count: 100)
        for client in clients {
            for name in names { await client.seedFile(path: "/p/2026/01/\(name)", data: data) }
        }
        let reserve = reserve
        let service = RestoreService(makeClient: { _, _ in factory.make() }, importAsset: { files, _ in
            files.first?.0.fileName
        }, availableCapacity: { reserve + 320 })
        let result = try await service.restoreItems(items: names.map { item($0, data: data, recordedSize: 120) },
            profile: profile(), password: "", onItemCompleted: { index, total, restored in
                XCTAssertEqual(total, 2)
                XCTAssertEqual(restored?.asset.localIdentifier, names[index - 1])
            })
        XCTAssertEqual(result.map(\.asset.localIdentifier), names)
        XCTAssertEqual(factory.createdCount, 3)
        for client in clients {
            for url in await client.downloadAttemptLocalURLs {
                XCTAssertFalse(FileManager.default.fileExists(atPath: url.deletingLastPathComponent().path))
            }
        }
    }

    func testUnknownMetadataFallsBackToOneAssetWithoutPrefetch() async throws {
        let probe = InMemoryRemoteStorageClient(), downloader = InMemoryRemoteStorageClient(), nextDownloader = InMemoryRemoteStorageClient()
        let factory = RestoreTestClientFactory([probe, downloader, nextDownloader])
        let first = Data(repeating: 1, count: 100), second = Data(repeating: 2, count: 10)
        await downloader.seedFile(path: "/p/2026/01/first.jpg", data: first)
        await nextDownloader.seedFile(path: "/p/2026/01/second.jpg", data: second)
        let imported = ExecutionTerminationControl()
        await nextDownloader.setOnDownloadAttempt { path in
            if path.hasSuffix("second.jpg") { XCTAssertTrue(imported.shouldDrain) }
        }
        let reserve = reserve
        let service = RestoreService(makeClient: { _, _ in factory.make() }, importAsset: { files, _ in
            let name = try XCTUnwrap(files.first?.0.fileName)
            if name == "first.jpg" { imported.request(.stop) }
            return name
        }, availableCapacity: { reserve + 320 })
        let result = try await service.restoreItems(items: [item("first.jpg", data: first, recordedSize: 120),
            item("second.jpg", data: second, recordedSize: 10)], profile: profile(), password: "",
            downloadPolicy: .init(), onItemCompleted: { _, _, _ in })
        XCTAssertEqual(result.map(\.asset.localIdentifier), ["first.jpg", "second.jpg"])
        let metadata = await probe.metadataAttemptPaths
        XCTAssertEqual(metadata.count, 1)
    }

    func testConfirmedOversizedFileFailsBeforeDownloading() async throws {
        let probe = InMemoryRemoteStorageClient()
        let factory = RestoreTestClientFactory([probe])
        let data = Data(repeating: 1, count: 120)
        await probe.seedFile(path: "/p/2026/01/first.jpg", data: data)
        let reserve = reserve
        let service = RestoreService(makeClient: { _, _ in factory.make() }, importAsset: { _, _ in
            XCTFail("insufficient space must not import")
            return nil
        }, availableCapacity: { reserve + 320 })
        do {
            _ = try await service.restoreItems(items: [item("first.jpg", data: data, recordedSize: 120)],
                profile: profile(), password: "", onItemCompleted: { _, _, _ in XCTFail("must not complete") })
            XCTFail("expected space error")
        } catch { XCTAssertEqual((error as NSError).code, CocoaError.fileWriteOutOfSpace.rawValue) }
        XCTAssertEqual(factory.createdCount, 1)
        let downloads = await probe.downloadAttemptPaths
        XCTAssertTrue(downloads.isEmpty)
        let disconnects = await probe.disconnectCount
        XCTAssertEqual(disconnects, 1)
    }

    func testUnknownSizeFailsBeforeImportAndCleansFilesWhenActualSpaceIsInsufficient() async throws {
        let probe = InMemoryRemoteStorageClient(), downloader = InMemoryRemoteStorageClient()
        let factory = RestoreTestClientFactory([probe, downloader])
        let data = Data(repeating: 1, count: 100)
        await downloader.seedFile(path: "/p/2026/01/first.jpg", data: data)
        let reserve = reserve
        let service = RestoreService(makeClient: { _, _ in factory.make() }, importAsset: { _, _ in
            XCTFail("actual space must be checked before importing")
            return nil
        }, availableCapacity: { reserve + 199 })
        do {
            _ = try await service.restoreItems(items: [item("first.jpg", data: data, recordedSize: 120)],
                profile: profile(), password: "", onItemCompleted: { _, _, _ in XCTFail("must not complete") })
            XCTFail("expected space error")
        } catch { XCTAssertEqual((error as NSError).code, CocoaError.fileWriteOutOfSpace.rawValue) }
        let urls = await downloader.downloadAttemptLocalURLs
        XCTAssertEqual(urls.count, 1)
        for url in urls { XCTAssertFalse(FileManager.default.fileExists(atPath: url.deletingLastPathComponent().path)) }
    }

    func testBrowserLinkFallsBackWithoutProbingOrAbortingSharedClient() async throws {
        let client = InMemoryRemoteStorageClient(onAbandon: { XCTFail("shared connection must not abort") })
        let data = Data(repeating: 1, count: 100)
        await client.seedFile(path: "/p/2026/01/first.jpg", data: data)
        let reserve = reserve
        let service = RestoreService(makeClient: { _, _ in client }, importAsset: { _, _ in "imported" },
            availableCapacity: { reserve + 320 })
        let result = try await service.restoreItems(items: [item("first.jpg", data: data, recordedSize: 120)],
            profile: profile(browserLink: true), password: "", onItemCompleted: { _, _, _ in })
        XCTAssertEqual(result.count, 1)
        let metadata = await client.metadataAttemptPaths
        XCTAssertTrue(metadata.isEmpty)
    }

    func testSizeProbeUsesTheSameHashedResourceDeduplicationAsDownload() async throws {
        let probe = InMemoryRemoteStorageClient(), downloader = InMemoryRemoteStorageClient()
        let factory = RestoreTestClientFactory([probe, downloader])
        let data = Data(repeating: 1, count: 100)
        for client in [probe, downloader] { await client.seedFile(path: "/p/2026/01/first.jpg", data: data) }
        let descriptor = item("first.jpg", data: data, recordedSize: 120)
        let duplicate = RestoreService.RestoreItemDescriptor(instances: descriptor.instances + descriptor.instances, identity: descriptor.identity)
        let reserve = reserve
        let service = RestoreService(makeClient: { _, _ in factory.make() }, importAsset: { _, _ in "imported" },
            availableCapacity: { reserve + 320 })
        let result = try await service.restoreItems(items: [duplicate], profile: profile(), password: "", onItemCompleted: { _, _, _ in })
        XCTAssertEqual(result.count, 1)
        let metadata = await probe.metadataAttemptPaths
        let downloads = await downloader.downloadAttemptPaths
        XCTAssertEqual(metadata.count, 1)
        XCTAssertEqual(downloads.count, 1)
    }

    func testPauseDuringSizeProbeAbortsProbeWithoutStartingDownload() async throws {
        let release = NetworkAbandonmentBarrier()
        let aborted = expectation(description: "probe transport aborted")
        let probe = InMemoryRemoteStorageClient(onAbandon: {
            aborted.fulfill()
            release.open()
        })
        let factory = RestoreTestClientFactory([probe])
        let started = expectation(description: "size probe started")
        await probe.setOnMetadata { _ in
            started.fulfill()
            await release.wait()
        }
        let reserve = reserve
        let service = RestoreService(makeClient: { _, _ in factory.make() }, importAsset: { _, _ in
            XCTFail("paused size probe must not import")
            return nil
        }, availableCapacity: { reserve + 320 })
        let drain = ExecutionTerminationControl()
        let finished = expectation(description: "paused restore settled")
        let items = [item("first.jpg", data: Data(repeating: 1, count: 100), recordedSize: 120)]
        let profile = profile()
        let task = Task {
            defer { finished.fulfill() }
            return try await service.restoreItems(items: items, profile: profile, password: "",
                shouldDrain: { drain.shouldDrain }, onItemCompleted: { _, _, _ in XCTFail("must not complete") })
        }
        await fulfillment(of: [started], timeout: 3)
        drain.request(.pause)
        await fulfillment(of: [aborted, finished], timeout: 3)
        release.open()
        do { _ = try await task.value; XCTFail("expected drain") } catch is CancellationError {}
        let downloads = await probe.downloadAttemptPaths
        XCTAssertTrue(downloads.isEmpty)
        XCTAssertEqual(factory.createdCount, 1)
    }
}
