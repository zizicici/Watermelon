import CryptoKit
import XCTest
@testable import Watermelon

final class RestoreParallelDownloadTests: XCTestCase {
    private func profile() -> ServerProfileRecord {
        ServerProfileRecord(id: nil, name: "parallel restore", storageType: StorageType.webdav.rawValue,
            connectionParams: nil, sortOrder: 0, host: "fixture.local", port: 0, shareName: "test", basePath: "/p",
            username: "test", domain: nil, credentialRef: "test", backgroundBackupEnabled: false,
            createdAt: Date(), updatedAt: Date(), writerID: nil)
    }

    private func item(_ name: String) -> RestoreService.RestoreItemDescriptor {
        let data = Data(name.utf8)
        let resource = RemoteAssetResourceInstance(role: ResourceTypeCode.photo, slot: 0,
            resourceHash: Data(SHA256.hash(data: data)), fileName: name, fileSize: Int64(data.count),
            remoteRelativePath: "2026/01/\(name)", creationDateMs: 1_000)
        return .init(instances: [resource], identity: Data(name.utf8))
    }

    private func seed(_ clients: [InMemoryRemoteStorageClient], names: [String]) async {
        for client in clients {
            for name in names { await client.seedFile(path: "/p/2026/01/\(name)", data: Data(name.utf8)) }
        }
    }

    func testParallelDownloadsImportInOrderReuseClientsAndCleanFiles() async throws {
        let clients = [InMemoryRemoteStorageClient(), InMemoryRemoteStorageClient()]
        let factory = RestoreTestClientFactory(clients)
        let names = ["first.jpg", "second.jpg", "third.jpg", "fourth.jpg"]
        await seed(clients, names: names)
        let head = RestoreTestLatch()
        let secondDownloaded = expectation(description: "second asset downloaded")
        for client in clients {
            await client.setOnDownloadAttempt { path in if path.hasSuffix("first.jpg") { await head.wait() } }
            await client.setOnDownload { path in if path.hasSuffix("second.jpg") { secondDownloaded.fulfill() } }
        }
        let recorder = RestoreParallelRecorder()
        let service = RestoreService(makeClient: { _, _ in factory.make() }, importAsset: { downloaded, _ in
            XCTAssertTrue(downloaded.allSatisfy { FileManager.default.fileExists(atPath: $0.1.path) })
            let name = try XCTUnwrap(downloaded.first?.0.fileName)
            await recorder.record(name)
            return name
        })
        let items = names.map(item)
        let profile = profile()
        let task = Task {
            try await service.restoreItems(items: items, profile: profile, password: "", downloadPolicy: .init(),
                onItemCompleted: { index, total, restored in
                    XCTAssertEqual(total, names.count)
                    XCTAssertEqual(restored?.asset.localIdentifier, names[index - 1])
                })
        }
        await fulfillment(of: [secondDownloaded], timeout: 3)
        let before = await recorder.names
        XCTAssertTrue(before.isEmpty)
        await head.open()
        let restored = try await task.value
        XCTAssertEqual(restored.map(\.asset.localIdentifier), names)
        XCTAssertEqual(factory.createdCount, 2)
        for client in clients {
            let urls = await client.downloadAttemptLocalURLs
            XCTAssertFalse(urls.isEmpty)
            for url in urls {
                XCTAssertFalse(FileManager.default.fileExists(atPath: url.path))
                XCTAssertFalse(FileManager.default.fileExists(atPath: url.deletingLastPathComponent().path))
            }
            let disconnects = await client.disconnectCount
            XCTAssertEqual(disconnects, 1)
        }
    }

    func testImportFailureCleansAlreadyPrefetchedAssetWithoutImportingIt() async throws {
        let clients = [InMemoryRemoteStorageClient(), InMemoryRemoteStorageClient()]
        let factory = RestoreTestClientFactory(clients)
        await seed(clients, names: ["first.jpg", "second.jpg"])
        let prefetched = RestoreTestLatch()
        for client in clients {
            await client.setOnDownload { path in if path.hasSuffix("second.jpg") { await prefetched.open() } }
        }
        let recorder = RestoreParallelRecorder()
        let service = RestoreService(makeClient: { _, _ in factory.make() }, importAsset: { downloaded, _ in
            await prefetched.wait()
            await recorder.record(try XCTUnwrap(downloaded.first?.0.fileName))
            throw NSError(domain: "ImportFixture", code: 1)
        })
        do {
            _ = try await service.restoreItems(items: [item("first.jpg"), item("second.jpg")], profile: profile(), password: "",
                downloadPolicy: .init(), onItemCompleted: { _, _, _ in XCTFail("no successful imports") })
            XCTFail("expected import failure")
        } catch {
            XCTAssertEqual((error as NSError).domain, "ImportFixture")
        }
        let imports = await recorder.names
        XCTAssertEqual(imports, ["first.jpg"])
        for client in clients {
            for url in await client.downloadAttemptLocalURLs {
                XCTAssertFalse(FileManager.default.fileExists(atPath: url.path))
            }
        }
    }

    func testDrainCompletesWholeHeadAssetAndCleansPrefetch() async throws {
        let clients = [InMemoryRemoteStorageClient(), InMemoryRemoteStorageClient()]
        let factory = RestoreTestClientFactory(clients)
        await seed(clients, names: ["first.jpg", "first.mov", "second.jpg"])
        let head = RestoreTestLatch()
        let secondDownloaded = expectation(description: "second asset downloaded")
        let drain = ExecutionTerminationControl()
        for client in clients {
            await client.setOnDownloadAttempt { path in if path.hasSuffix("first.jpg") { await head.wait() } }
            await client.setOnDownload { path in if path.hasSuffix("second.jpg") { secondDownloaded.fulfill() } }
        }
        let recorder = RestoreParallelRecorder()
        let service = RestoreService(makeClient: { _, _ in factory.make() }, importAsset: { downloaded, _ in
            XCTAssertEqual(downloaded.map { $0.0.fileName }, ["first.jpg", "first.mov"])
            XCTAssertTrue(downloaded.allSatisfy { FileManager.default.fileExists(atPath: $0.1.path) })
            await recorder.record("first")
            return "first"
        })
        var first = item("first.jpg")
        let videoData = Data("first.mov".utf8)
        let video = RemoteAssetResourceInstance(role: ResourceTypeCode.pairedVideo, slot: 0,
            resourceHash: Data(SHA256.hash(data: videoData)), fileName: "first.mov", fileSize: Int64(videoData.count),
            remoteRelativePath: "2026/01/first.mov", creationDateMs: 1_000)
        first = .init(instances: first.instances + [video], identity: first.identity)
        let items = [first, item("second.jpg")]
        let profile = profile()
        let task = Task {
            try await service.restoreItems(items: items, profile: profile, password: "", downloadPolicy: .init(),
                shouldDrain: { drain.shouldDrain }, onItemCompleted: { index, _, restored in
                    XCTAssertEqual(index, 1)
                    XCTAssertEqual(restored?.asset.localIdentifier, "first")
                })
        }
        await fulfillment(of: [secondDownloaded], timeout: 3)
        drain.request(.pause)
        await head.open()
        do { _ = try await task.value; XCTFail("expected drain") } catch is CancellationError {}
        let imports = await recorder.names
        XCTAssertEqual(imports, ["first"])
        for client in clients {
            for url in await client.downloadAttemptLocalURLs {
                XCTAssertFalse(FileManager.default.fileExists(atPath: url.path))
            }
        }
    }

    func testLegacyUnhashedResourcesAreAllIncludedInBudget() {
        let resources = ["photo.jpg", "video.mov"].map { name in
            RemoteAssetResourceInstance(role: 1, slot: 0, resourceHash: Data(), fileName: name,
                fileSize: 100, remoteRelativePath: "2026/01/\(name)", creationDateMs: nil)
        }
        XCTAssertEqual(RestoreDownloadPolicy.estimatedBytes(for: resources), 200)
        let hashed = item("first.jpg").instances
        XCTAssertEqual(RestoreDownloadPolicy.estimatedBytes(for: hashed + hashed), hashed[0].fileSize)
    }

    func testBrowserLinkSharedClientIsLimitedToTwoWorkers() async throws {
        let client = InMemoryRemoteStorageClient()
        let factory = RestoreTestClientFactory([client])
        let names = ["first.jpg", "second.jpg", "third.jpg", "fourth.jpg"]
        await seed([client], names: names)
        var profile = profile()
        profile.credentialRef = ServerProfileRecord.browserLinkCredentialRef(sessionID: UUID().uuidString)
        let service = RestoreService(makeClient: { _, _ in factory.make() }, importAsset: { downloaded, _ in
            downloaded.first?.0.fileName
        })
        let restored = try await service.restoreItems(items: names.map(item), profile: profile, password: "",
            downloadPolicy: .init(workerCount: 4), onItemCompleted: { _, _, _ in })
        XCTAssertEqual(factory.createdCount, 2)
        XCTAssertEqual(restored.map(\.asset.localIdentifier), names)
    }

    func testHardCancelWaitsForLateDownloadAndRemovesItsFile() async throws {
        let clients = [InMemoryRemoteStorageClient(), InMemoryRemoteStorageClient()]
        let factory = RestoreTestClientFactory(clients)
        await seed(clients, names: ["first.jpg", "second.jpg"])
        let downloadsStarted = expectation(description: "both downloads started")
        downloadsStarted.expectedFulfillmentCount = 2
        let cancellationObserved = expectation(description: "both cancellations observed")
        cancellationObserved.expectedFulfillmentCount = 2
        let release = RestoreTestLatch()
        for client in clients {
            await client.setOnDownloadAttempt { _ in
                downloadsStarted.fulfill()
                await withTaskCancellationHandler { await release.wait() } onCancel: { cancellationObserved.fulfill() }
            }
        }
        let finished = ExecutionTerminationControl()
        let service = RestoreService(makeClient: { _, _ in factory.make() }, importAsset: { _, _ in
            XCTFail("cancelled downloads must not import")
            return nil
        })
        let items = [item("first.jpg"), item("second.jpg")]
        let profile = profile()
        let task = Task {
            defer { finished.request(.stop) }
            return try await service.restoreItems(items: items, profile: profile, password: "", downloadPolicy: .init(),
                onItemCompleted: { _, _, _ in XCTFail("cancelled downloads must not complete") })
        }
        await fulfillment(of: [downloadsStarted], timeout: 3)
        task.cancel()
        await fulfillment(of: [cancellationObserved], timeout: 3)
        XCTAssertFalse(finished.shouldDrain)
        await release.open()
        do { _ = try await task.value; XCTFail("expected cancellation") } catch is CancellationError {}
        for client in clients {
            for url in await client.downloadAttemptLocalURLs {
                XCTAssertFalse(FileManager.default.fileExists(atPath: url.path))
                XCTAssertFalse(FileManager.default.fileExists(atPath: url.deletingLastPathComponent().path))
            }
            let disconnects = await client.disconnectCount
            XCTAssertEqual(disconnects, 1)
        }
    }

    func testDrainAbortsUnresponsivePrefetchAndFinishesHead() async throws {
        try await checkUnresponsivePrefetch(failImport: false)
    }

    func testImportFailureAbortsUnresponsivePrefetchBeforeCleanup() async throws {
        try await checkUnresponsivePrefetch(failImport: true)
    }

    private func checkUnresponsivePrefetch(failImport: Bool) async throws {
        let probes = [RestoreAbortProbe(), RestoreAbortProbe()]
        let clients = probes.map { probe in InMemoryRemoteStorageClient(onAbandon: { probe.abort() }) }
        let factory = RestoreTestClientFactory(clients)
        await seed(clients, names: ["first.jpg", "second.jpg"])
        let head = RestoreTestLatch()
        let prefetchStarted = expectation(description: "unresponsive prefetch started")
        for (client, probe) in zip(clients, probes) {
            await client.setOnDownloadAttempt { path in
                if path.hasSuffix("first.jpg") {
                    await head.wait()
                    XCTAssertEqual(probe.abortCount, 0)
                } else {
                    prefetchStarted.fulfill()
                    await probe.barrier.wait()
                }
            }
        }
        let recorder = RestoreParallelRecorder()
        let service = RestoreService(makeClient: { _, _ in factory.make() }, importAsset: { downloaded, _ in
            let name = try XCTUnwrap(downloaded.first?.0.fileName)
            await recorder.record(name)
            if failImport { throw NSError(domain: "ImportFixture", code: 1) }
            return name
        })
        let drain = ExecutionTerminationControl()
        let finished = expectation(description: "restore settled without manually releasing transport")
        let items = [item("first.jpg"), item("second.jpg")]
        let profile = profile()
        let task = Task {
            defer { finished.fulfill() }
            return try await service.restoreItems(items: items, profile: profile, password: "", downloadPolicy: .init(),
                shouldDrain: { drain.shouldDrain }, onItemCompleted: { index, _, _ in
                    XCTAssertFalse(failImport)
                    XCTAssertEqual(index, 1)
                })
        }
        await fulfillment(of: [prefetchStarted], timeout: 3)
        if !failImport { drain.request(.pause) }
        await head.open()
        await fulfillment(of: [finished], timeout: 3)
        probes.forEach { $0.barrier.open() }
        do {
            _ = try await task.value
            XCTFail("expected failure or drain")
        } catch {
            if failImport { XCTAssertEqual((error as NSError).domain, "ImportFixture") }
            else { XCTAssertTrue(error is CancellationError) }
        }
        XCTAssertEqual(probes.reduce(0) { $0 + $1.abortCount }, 1)
        let imports = await recorder.names
        XCTAssertEqual(imports, ["first.jpg"])
        for client in clients {
            for url in await client.downloadAttemptLocalURLs {
                XCTAssertFalse(FileManager.default.fileExists(atPath: url.deletingLastPathComponent().path))
            }
        }
    }

    func testHardCancellationAbortsReplacementClientAfterReconnect() async throws {
        let probes = [RestoreAbortProbe(), RestoreAbortProbe()]
        let clients = probes.map { probe in InMemoryRemoteStorageClient(onAbandon: { probe.abort() }) }
        let factory = RestoreTestClientFactory(clients)
        await seed(clients, names: ["first.jpg"])
        await clients[0].enqueueDownloadError(RemoteErrorFixtures.retryable)
        let started = expectation(description: "replacement client downloading")
        await clients[1].setOnDownloadAttempt { _ in
            started.fulfill()
            await probes[1].barrier.wait()
        }
        let service = RestoreService(makeClient: { _, _ in factory.make() }, importAsset: { _, _ in
            XCTFail("cancelled item must not import")
            return nil
        })
        let finished = expectation(description: "reconnected download cancelled")
        let items = [item("first.jpg")]
        let profile = profile()
        let task = Task {
            defer { finished.fulfill() }
            return try await service.restoreItems(items: items, profile: profile, password: "",
                onItemCompleted: { _, _, _ in XCTFail("cancelled item must not complete") })
        }
        await fulfillment(of: [started], timeout: 5)
        task.cancel()
        await fulfillment(of: [finished], timeout: 3)
        probes.forEach { $0.barrier.open() }
        do { _ = try await task.value; XCTFail("expected cancellation") } catch is CancellationError {}
        XCTAssertEqual(probes[0].abortCount, 0)
        XCTAssertEqual(probes[1].abortCount, 1)
        XCTAssertEqual(factory.createdCount, 2)
        for url in await clients[1].downloadAttemptLocalURLs {
            XCTAssertFalse(FileManager.default.fileExists(atPath: url.deletingLastPathComponent().path))
        }
    }

    func testBrowserLinkDrainCancelsOnlyPrefetchTransfer() async throws {
        let client = InMemoryRemoteStorageClient(onAbandon: { XCTFail("shared client must not be aborted") })
        await seed([client], names: ["first.jpg", "second.jpg"])
        let head = RestoreTestLatch()
        let started = expectation(description: "shared client prefetch started")
        let cancelled = expectation(description: "shared client prefetch cancelled")
        await client.setOnDownloadAttempt { path in
            if path.hasSuffix("first.jpg") {
                await head.wait()
                XCTAssertFalse(Task.isCancelled)
            } else {
                started.fulfill()
                do { try await Task.sleep(for: .seconds(5)); XCTFail("prefetch was not cancelled") }
                catch is CancellationError { cancelled.fulfill() }
                catch { XCTFail("unexpected error: \(error)") }
            }
        }
        let recorder = RestoreParallelRecorder()
        let service = RestoreService(makeClient: { _, _ in client }, importAsset: { downloaded, _ in
            let name = try XCTUnwrap(downloaded.first?.0.fileName)
            await recorder.record(name)
            return name
        })
        let drain = ExecutionTerminationControl()
        var profile = profile()
        profile.credentialRef = ServerProfileRecord.browserLinkCredentialRef(sessionID: UUID().uuidString)
        let items = [item("first.jpg"), item("second.jpg")]
        let task = Task {
            try await service.restoreItems(items: items, profile: profile, password: "", downloadPolicy: .init(),
                shouldDrain: { drain.shouldDrain }, onItemCompleted: { index, _, _ in XCTAssertEqual(index, 1) })
        }
        await fulfillment(of: [started], timeout: 3)
        drain.request(.pause)
        await fulfillment(of: [cancelled], timeout: 3)
        await head.open()
        do { _ = try await task.value; XCTFail("expected drain") } catch is CancellationError {}
        let imports = await recorder.names
        XCTAssertEqual(imports, ["first.jpg"])
    }

    func testStaleCleanupKeepsNewRunAndTypedImportStaysInItemDirectory() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString, isDirectory: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let stale = try RestoreStagingStore(rootDirectory: root)
        let cleanup = RestoreStagingStore.cleanupStaleSessions(in: root)
        let current = try RestoreStagingStore(rootDirectory: root)
        await cleanup.value
        XCTAssertFalse(FileManager.default.fileExists(atPath: stale.directory.path))
        XCTAssertTrue(FileManager.default.fileExists(atPath: current.directory.path))
        let itemDirectory = try current.itemDirectory(0)
        let source = itemDirectory.appendingPathComponent("source")
        try Data("dng".utf8).write(to: source)
        let importURL = try RestoreService.makePhotoKitImportURL(fileURL: source, contentTypeIdentifier: "com.adobe.raw-image")
        XCTAssertEqual(importURL.deletingLastPathComponent(), itemDirectory)
        current.removeAll()
        XCTAssertFalse(FileManager.default.fileExists(atPath: importURL.path))
    }
}

final class RestoreTestClientFactory: @unchecked Sendable {
    private let lock = NSLock()
    private let clients: [InMemoryRemoteStorageClient]
    private var count = 0
    init(_ clients: [InMemoryRemoteStorageClient]) { self.clients = clients }
    var createdCount: Int { lock.withLock { count } }
    func make() -> any RemoteStorageClientProtocol {
        lock.withLock {
            let client = clients[count % clients.count]
            count += 1
            return client
        }
    }
}

private actor RestoreParallelRecorder {
    private(set) var names: [String] = []
    func record(_ name: String) { names.append(name) }
}

private final class RestoreAbortProbe: @unchecked Sendable {
    let barrier = NetworkAbandonmentBarrier()
    private let lock = NSLock()
    private var count = 0
    var abortCount: Int { lock.withLock { count } }
    func abort() {
        lock.withLock { count += 1 }
        barrier.open()
    }
}
