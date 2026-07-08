import XCTest
@testable import Watermelon

// Pins the writeback cancellation shield: a drain/backfill cancel landing mid-write must not abort the
// sidecar transfer — an aborted `.createIfAbsent` upload can leave a torn partial at the canonical path
// (SMB keeps it), which later exists/collision probes trust as a valid sidecar, wedging the shared
// thumbnail until a full purge.
final class RemoteSidecarWritebackCancellationTests: XCTestCase {
    private final class TaskBox: @unchecked Sendable {
        private let lock = NSLock()
        private var task: Task<Bool, Error>?

        func set(_ task: Task<Bool, Error>) {
            lock.withLock { self.task = task }
        }

        // Waits for the handle so the cancel is guaranteed to land while writeSidecar is still running.
        func cancelWhenAvailable() async {
            while true {
                let cancelled = lock.withLock { () -> Bool in
                    guard let task else { return false }
                    task.cancel()
                    return true
                }
                if cancelled { return }
                try? await Task.sleep(nanoseconds: 1_000_000)
            }
        }
    }

    func testCancelledWritebackStillLandsCompleteSidecar() async throws {
        let client = InMemoryRemoteStorageClient()
        // Model URLSession-backed backends whose in-flight requests abort when their task is cancelled.
        await client.setRespectTaskCancellation(true)
        let thumbPath = "/base/.watermelon/thumbs/de/deadbeef.jpg"
        let shardDir = "/base/.watermelon/thumbs/de"
        let data = Data("complete-sidecar-jpeg".utf8)

        let box = TaskBox()
        await client.enqueueExistsPostAction(forPathSuffix: "deadbeef.jpg") { await box.cancelWhenAvailable() }
        let task = Task {
            try await RemoteThumbnailService.writeSidecar(
                data,
                fingerprintHex: "deadbeef",
                thumbPath: thumbPath,
                shardDir: shardDir,
                client: client
            )
        }
        box.set(task)

        let written = try await task.value
        XCTAssertTrue(written, "the cancelled writeback must finish the upload, not abort it")
        let landed = await client.fileData(path: thumbPath)
        XCTAssertEqual(landed, data, "the sidecar must land complete at the canonical path")
    }

    // Same shield for the backup-path writer: a stop cancelling the run mid-transfer must not abort the
    // inline sidecar upload (WebDAV excludes bare cancels from partial cleanup; SMB cleanup fails on a
    // dead session), or the torn partial passes every later exists/collision probe.
    func testCancelledBackupSidecarUploadStillLandsComplete() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setRespectTaskCancellation(true)
        let data = Data("complete-backup-sidecar-jpeg".utf8)
        let localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("backup_thumb_\(UUID().uuidString).jpg")
        try data.write(to: localURL)
        addTeardownBlock { try? FileManager.default.removeItem(at: localURL) }
        let thumbPath = "/base/.watermelon/thumbs/de/deadbeef.jpg"

        let task = Task {
            while !Task.isCancelled { try? await Task.sleep(nanoseconds: 1_000_000) }
            try await AssetProcessor.uploadSidecarReplacing(localURL: localURL, thumbPath: thumbPath, client: client)
        }
        task.cancel()
        try await task.value
        let landed = await client.fileData(path: thumbPath)
        XCTAssertEqual(landed, data, "the cancelled backup sidecar upload must finish the transfer, not abort it")
    }
}
