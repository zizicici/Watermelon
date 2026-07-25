import Photos
import XCTest
@testable import Watermelon

final class RemoteLivePhotoResourcePairTests: XCTestCase {
    func testOrdersSameGenerationCandidatesFromCurrentToOriginal() {
        let candidates = RemoteThumbnailService.localLivePhotoResourcePairCandidates([
            .photo,
            .pairedVideo,
            .adjustmentBasePhoto,
            .adjustmentBasePairedVideo,
            .fullSizePhoto,
            .fullSizePairedVideo
        ])

        XCTAssertEqual(candidates.count, 3)
        XCTAssertEqual(candidates[0].photo, 4)
        XCTAssertEqual(candidates[0].video, 5)
        XCTAssertEqual(candidates[1].photo, 2)
        XCTAssertEqual(candidates[1].video, 3)
        XCTAssertEqual(candidates[2].photo, 0)
        XCTAssertEqual(candidates[2].video, 1)
    }

    func testReturnsOnlyOriginalPairWhenItIsTheOnlyCompleteGeneration() {
        let candidates = RemoteThumbnailService.localLivePhotoResourcePairCandidates([
            .photo,
            .adjustmentData,
            .pairedVideo
        ])

        XCTAssertEqual(candidates.count, 1)
        XCTAssertEqual(candidates[0].photo, 0)
        XCTAssertEqual(candidates[0].video, 2)
    }

    func testDoesNotMixResourceGenerations() {
        XCTAssertTrue(RemoteThumbnailService.localLivePhotoResourcePairCandidates([
            .fullSizePhoto,
            .pairedVideo
        ]).isEmpty)
    }

    func testExportFailureFallsBackToNextCompleteGeneration() async throws {
        let resourceTypes: [PHAssetResourceType] = [
            .photo,
            .pairedVideo,
            .fullSizePhoto,
            .fullSizePairedVideo
        ]
        var attemptedTypes: [PHAssetResourceType] = []
        var returnedURLs: [URL] = []

        let pair = await RemoteThumbnailService.firstUsableLocalLivePhotoPair(
            resourceTypes: resourceTypes,
            export: { index in
                let type = resourceTypes[index]
                attemptedTypes.append(type)
                if type == .fullSizePhoto { return nil }
                let materialized = Self.makeTemporaryMaterializedOriginal(label: "\(type.rawValue)")
                returnedURLs.append(materialized.url)
                return materialized
            },
            validate: { _ in true }
        )
        defer {
            for url in returnedURLs {
                try? FileManager.default.removeItem(at: url)
            }
        }

        let result = try XCTUnwrap(pair)
        XCTAssertEqual(attemptedTypes, [.fullSizePhoto, .photo, .pairedVideo])
        XCTAssertTrue(FileManager.default.fileExists(atPath: result.photo.url.path))
        XCTAssertTrue(FileManager.default.fileExists(atPath: result.video.url.path))
    }

    func testValidationFailureCleansPreferredPairAndFallsBackToOriginal() async throws {
        let resourceTypes: [PHAssetResourceType] = [
            .photo,
            .pairedVideo,
            .fullSizePhoto,
            .fullSizePairedVideo
        ]
        var createdURLsByType: [PHAssetResourceType: URL] = [:]
        var validationCount = 0

        let pair = await RemoteThumbnailService.firstUsableLocalLivePhotoPair(
            resourceTypes: resourceTypes,
            export: { index in
                let type = resourceTypes[index]
                let materialized = Self.makeTemporaryMaterializedOriginal(label: "\(type.rawValue)")
                createdURLsByType[type] = materialized.url
                return materialized
            },
            validate: { _ in
                validationCount += 1
                return validationCount == 2
            }
        )
        let result = try XCTUnwrap(pair)
        defer {
            try? FileManager.default.removeItem(at: result.photo.url)
            try? FileManager.default.removeItem(at: result.video.url)
        }

        XCTAssertEqual(validationCount, 2)
        XCTAssertFalse(FileManager.default.fileExists(
            atPath: try XCTUnwrap(createdURLsByType[.fullSizePhoto]).path
        ))
        XCTAssertFalse(FileManager.default.fileExists(
            atPath: try XCTUnwrap(createdURLsByType[.fullSizePairedVideo]).path
        ))
        XCTAssertTrue(FileManager.default.fileExists(atPath: result.photo.url.path))
        XCTAssertTrue(FileManager.default.fileExists(atPath: result.video.url.path))
    }

    func testExtensionlessLocalResourceUsesResourceTypeFallback() {
        let photo = RemoteThumbnailService.localResourceFilenameExtension(
            filename: "extensionless",
            uniformTypeIdentifier: nil,
            type: .fullSizePhoto
        )
        let video = RemoteThumbnailService.localResourceFilenameExtension(
            filename: "",
            uniformTypeIdentifier: nil,
            type: .fullSizePairedVideo
        )

        XCTAssertEqual(photo, "jpg")
        XCTAssertEqual(video, "mov")
    }

    func testLocalResourceExtensionPrefersOriginalFilenameAndUTI() {
        XCTAssertEqual(
            RemoteThumbnailService.localResourceFilenameExtension(
                filename: "IMG_0001.custom",
                uniformTypeIdentifier: "public.heic",
                type: .fullSizePhoto
            ),
            "custom"
        )
        XCTAssertEqual(
            RemoteThumbnailService.localResourceFilenameExtension(
                filename: "IMG_0001",
                uniformTypeIdentifier: "public.heic",
                type: .fullSizePhoto
            ),
            "heic"
        )
    }

    func testCancellingStreamedExportCancelsRequestAndIgnoresLateCallbacks() async {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("live_export_cancel_\(UUID().uuidString)")
        let request = ResourceRequestHarness()
        defer { try? FileManager.default.removeItem(at: url) }

        let task = Task {
            await RemoteThumbnailService.streamLocalResource(
                to: url,
                startRequest: request.start,
                cancelRequest: request.cancel
            )
        }
        await request.waitUntilStarted()
        request.send(Data("partial".utf8))
        task.cancel()

        let succeeded = await task.value
        XCTAssertFalse(succeeded)
        XCTAssertEqual(request.cancelledRequestIDs, [42])
        XCTAssertFalse(FileManager.default.fileExists(atPath: url.path))

        request.send(Data("late".utf8))
        request.complete(error: nil)
        XCTAssertFalse(FileManager.default.fileExists(atPath: url.path))
    }

    func testFailedStreamedExportRemovesPartialFile() async {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("live_export_failure_\(UUID().uuidString)")
        let request = ResourceRequestHarness()
        defer { try? FileManager.default.removeItem(at: url) }

        let task = Task {
            await RemoteThumbnailService.streamLocalResource(
                to: url,
                startRequest: request.start,
                cancelRequest: request.cancel
            )
        }
        await request.waitUntilStarted()
        request.send(Data("partial".utf8))
        request.complete(error: NSError(domain: "test", code: 1))

        let succeeded = await task.value
        XCTAssertFalse(succeeded)
        XCTAssertTrue(request.cancelledRequestIDs.isEmpty)
        XCTAssertFalse(FileManager.default.fileExists(atPath: url.path))
    }

    func testCancellationDoesNotWaitForBlockedFileWrite() async {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("live_export_blocked_write_\(UUID().uuidString)")
        let request = ResourceRequestHarness()
        let writer = BlockingResourceWriter()
        defer {
            writer.release()
            try? FileManager.default.removeItem(at: url)
        }

        let task = Task {
            await RemoteThumbnailService.streamLocalResource(
                to: url,
                startRequest: request.start,
                cancelRequest: request.cancel,
                writeData: { fileHandle, data in
                    try writer.write(fileHandle: fileHandle, data: data)
                }
            )
        }
        await request.waitUntilStarted()
        let sendTask = Task.detached {
            request.send(Data("partial".utf8))
        }
        await writer.waitUntilStarted()

        task.cancel()
        await request.waitUntilCancelled()
        XCTAssertEqual(request.cancelledRequestIDs, [42])

        writer.release()
        _ = await sendTask.value
        let succeeded = await task.value
        XCTAssertFalse(succeeded)
        XCTAssertFalse(FileManager.default.fileExists(atPath: url.path))
    }

    private static func makeTemporaryMaterializedOriginal(
        label: String
    ) -> RemoteThumbnailService.MaterializedOriginal {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("live_pair_\(label)_\(UUID().uuidString)")
        FileManager.default.createFile(atPath: url.path, contents: Data())
        return RemoteThumbnailService.MaterializedOriginal(url: url, isTemporary: true)
    }

    private final class ResourceRequestHarness: @unchecked Sendable {
        private let lock = NSLock()
        private var dataHandler: ((Data) -> Void)?
        private var completionHandler: ((Error?) -> Void)?
        private var startedWaiters: [CheckedContinuation<Void, Never>] = []
        private var cancelledWaiters: [CheckedContinuation<Void, Never>] = []
        private var cancelledIDs: [PHAssetResourceDataRequestID] = []

        var cancelledRequestIDs: [PHAssetResourceDataRequestID] {
            lock.withLock { cancelledIDs }
        }

        func start(
            dataHandler: @escaping (Data) -> Void,
            completionHandler: @escaping (Error?) -> Void
        ) -> PHAssetResourceDataRequestID {
            let waiters = lock.withLock {
                self.dataHandler = dataHandler
                self.completionHandler = completionHandler
                let waiters = startedWaiters
                startedWaiters.removeAll()
                return waiters
            }
            for waiter in waiters {
                waiter.resume()
            }
            return 42
        }

        func cancel(_ requestID: PHAssetResourceDataRequestID) {
            let waiters = lock.withLock {
                cancelledIDs.append(requestID)
                let waiters = cancelledWaiters
                cancelledWaiters.removeAll()
                return waiters
            }
            for waiter in waiters {
                waiter.resume()
            }
        }

        func waitUntilStarted() async {
            await withCheckedContinuation { continuation in
                let shouldResume = lock.withLock {
                    guard dataHandler == nil else { return true }
                    startedWaiters.append(continuation)
                    return false
                }
                if shouldResume {
                    continuation.resume()
                }
            }
        }

        func waitUntilCancelled() async {
            await withCheckedContinuation { continuation in
                let shouldResume = lock.withLock {
                    guard cancelledIDs.isEmpty else { return true }
                    cancelledWaiters.append(continuation)
                    return false
                }
                if shouldResume {
                    continuation.resume()
                }
            }
        }

        func send(_ data: Data) {
            let handler = lock.withLock { dataHandler }
            handler?(data)
        }

        func complete(error: Error?) {
            let handler = lock.withLock { completionHandler }
            handler?(error)
        }
    }

    private final class BlockingResourceWriter: @unchecked Sendable {
        private let lock = NSLock()
        private let releaseSemaphore = DispatchSemaphore(value: 0)
        private var started = false
        private var released = false
        private var startedWaiters: [CheckedContinuation<Void, Never>] = []

        func write(fileHandle: FileHandle, data: Data) throws {
            let waiters = lock.withLock {
                started = true
                let waiters = startedWaiters
                startedWaiters.removeAll()
                return waiters
            }
            for waiter in waiters {
                waiter.resume()
            }
            releaseSemaphore.wait()
            try fileHandle.write(contentsOf: data)
        }

        func waitUntilStarted() async {
            await withCheckedContinuation { continuation in
                let shouldResume = lock.withLock {
                    guard !started else { return true }
                    startedWaiters.append(continuation)
                    return false
                }
                if shouldResume {
                    continuation.resume()
                }
            }
        }

        func release() {
            let shouldSignal = lock.withLock {
                guard !released else { return false }
                released = true
                return true
            }
            if shouldSignal {
                releaseSemaphore.signal()
            }
        }
    }
}
