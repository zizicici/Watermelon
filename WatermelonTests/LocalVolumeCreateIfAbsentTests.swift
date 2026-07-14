import XCTest
@testable import Watermelon

private final class OneShotDirectorySyncFailure: @unchecked Sendable {
    private let lock = NSLock()
    private var armed = true
    private var count = 0

    func call() throws {
        try lock.withLock {
            count += 1
            guard armed else { return }
            armed = false
            throw POSIXError(.EIO)
        }
    }

    var callCount: Int { lock.withLock { count } }
}

final class LocalVolumeCreateIfAbsentTests: XCTestCase {

    private func makeTempDir() throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("lv-cia-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }

    private func writeSource(_ contents: Data) throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("lv-src-\(UUID().uuidString).bin")
        try contents.write(to: url)
        return url
    }

    // Mirrors how WriteLockService.isNameCollision unwraps the client error: a LocalVolume collision
    // surfaces as RemoteStorageClientError.underlying(NSPOSIXErrorDomain/EEXIST).
    private func assertIsNameCollision(_ error: Error, file: StaticString = #filePath, line: UInt = #line) {
        var ns = error as NSError
        if let storage = error as? RemoteStorageClientError, case .underlying(let inner) = storage {
            ns = inner as NSError
        }
        XCTAssertEqual(ns.domain, NSPOSIXErrorDomain, file: file, line: line)
        XCTAssertEqual(ns.code, Int(EEXIST), file: file, line: line)
    }

    func testCreateIfAbsentToFreshPathWritesBody() async throws {
        let root = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: root) }
        let payload = Data("lock-body".utf8)
        let source = try writeSource(payload)
        defer { try? FileManager.default.removeItem(at: source) }

        let client = try LocalVolumeClient(connectedRootURL: root)
        try await client.upload(
            localURL: source,
            remotePath: "claim.lock",
            mode: .createIfAbsent,
            respectTaskCancellation: false,
            onProgress: nil
        )

        let dest = root.appendingPathComponent("claim.lock")
        XCTAssertEqual(try Data(contentsOf: dest), payload)
    }

    // A create-if-absent onto a pre-existing path must surface a collision and leave the existing
    // owner's file untouched — the failed claim must never delete or overwrite it.
    func testCreateIfAbsentCollisionPreservesExistingFile() async throws {
        let root = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: root) }
        let dest = root.appendingPathComponent("claim.lock")
        let existing = Data("existing-owner".utf8)
        try existing.write(to: dest)
        let source = try writeSource(Data("new-body".utf8))
        defer { try? FileManager.default.removeItem(at: source) }

        let client = try LocalVolumeClient(connectedRootURL: root)
        do {
            try await client.upload(
                localURL: source,
                remotePath: "claim.lock",
                mode: .createIfAbsent,
                respectTaskCancellation: false,
                onProgress: nil
            )
            XCTFail("expected a collision error")
        } catch {
            assertIsNameCollision(error)
        }

        XCTAssertTrue(FileManager.default.fileExists(atPath: dest.path))
        XCTAssertEqual(try Data(contentsOf: dest), existing)
    }

    // Two same-writer sessions (separate client instances over the same root) racing a create-if-absent
    // claim onto a fresh path: the loser must surface a collision and must never delete the winner's
    // just-published file. The old pre-check + existedBeforeCopy flag let a loser whose check ran before
    // the winner's copy delete the winner's lock.
    func testCreateIfAbsentConcurrentClaimsPreserveTheWinner() async throws {
        for _ in 0..<8 {
            let root = try makeTempDir()
            defer { try? FileManager.default.removeItem(at: root) }
            let dest = root.appendingPathComponent("claim.lock")

            let claimants = 6
            var sources: [URL] = []
            var bodies: [Data] = []
            for i in 0..<claimants {
                let body = Data("claimant-\(i)-body".utf8)
                bodies.append(body)
                sources.append(try writeSource(body))
            }
            defer { sources.forEach { try? FileManager.default.removeItem(at: $0) } }

            let winners = await withTaskGroup(of: Bool.self) { group -> Int in
                for source in sources {
                    group.addTask {
                        do {
                            let client = try LocalVolumeClient(connectedRootURL: root)
                            try await client.upload(
                                localURL: source,
                                remotePath: "claim.lock",
                                mode: .createIfAbsent,
                                respectTaskCancellation: false,
                                onProgress: nil
                            )
                            return true
                        } catch {
                            return false
                        }
                    }
                }
                var count = 0
                for await ok in group where ok { count += 1 }
                return count
            }

            XCTAssertGreaterThanOrEqual(winners, 1, "at least one concurrent claim must win")
            XCTAssertTrue(FileManager.default.fileExists(atPath: dest.path), "a losing claim must never delete the winner's lock")
            let survivingBody = try Data(contentsOf: dest)
            XCTAssertTrue(bodies.contains(survivingBody), "the surviving lock must be one claimant's full body, never a deleted/partial file")
        }
    }

    func testStaticIntermediateSymlinkCannotEscapeSelectedRoot() async throws {
        let root = try makeTempDir()
        let outside = try makeTempDir()
        defer {
            try? FileManager.default.removeItem(at: root)
            try? FileManager.default.removeItem(at: outside)
        }
        let victim = outside.appendingPathComponent("victim.txt")
        let victimBody = Data("outside-victim".utf8)
        try victimBody.write(to: victim)
        let originalModificationDate = try XCTUnwrap(
            FileManager.default.attributesOfItem(atPath: victim.path)[.modificationDate] as? Date
        )
        try FileManager.default.createSymbolicLink(
            at: root.appendingPathComponent("escape"),
            withDestinationURL: outside
        )
        let safeSource = root.appendingPathComponent("safe.txt")
        try Data("safe".utf8).write(to: safeSource)
        let uploadSource = try writeSource(Data("new".utf8))
        let downloadTarget = FileManager.default.temporaryDirectory
            .appendingPathComponent("lv-download-\(UUID().uuidString).bin")
        defer {
            try? FileManager.default.removeItem(at: uploadSource)
            try? FileManager.default.removeItem(at: downloadTarget)
        }

        let client = try LocalVolumeClient(connectedRootURL: root)
        let directReadURL = await client.directReadURL(forRemotePath: "/escape/victim.txt")
        XCTAssertNil(directReadURL)
        await XCTAssertThrowsErrorAsync {
            try await client.download(remotePath: "/escape/victim.txt", localURL: downloadTarget)
        }
        await XCTAssertThrowsErrorAsync {
            try await client.upload(
                localURL: uploadSource,
                remotePath: "/escape/victim.txt",
                respectTaskCancellation: false,
                onProgress: nil
            )
        }
        await XCTAssertThrowsErrorAsync {
            try await client.delete(path: "/escape/victim.txt")
        }
        await XCTAssertThrowsErrorAsync {
            try await client.createDirectory(path: "/escape/new-directory")
        }
        await XCTAssertThrowsErrorAsync {
            try await client.copy(from: "/safe.txt", to: "/escape/copied.txt")
        }
        await XCTAssertThrowsErrorAsync {
            try await client.move(from: "/safe.txt", to: "/escape/moved.txt")
        }
        await XCTAssertThrowsErrorAsync {
            _ = try await client.list(path: "/escape")
        }
        await XCTAssertThrowsErrorAsync {
            _ = try await client.exists(path: "/escape/victim.txt")
        }
        await XCTAssertThrowsErrorAsync {
            _ = try await client.metadata(path: "/escape/victim.txt")
        }
        await XCTAssertThrowsErrorAsync {
            try await client.setModificationDate(Date(timeIntervalSince1970: 1), forPath: "/escape/victim.txt")
        }

        XCTAssertEqual(try Data(contentsOf: victim), victimBody)
        XCTAssertEqual(
            try FileManager.default.attributesOfItem(atPath: victim.path)[.modificationDate] as? Date,
            originalModificationDate
        )
        XCTAssertFalse(FileManager.default.fileExists(atPath: outside.appendingPathComponent("new-directory").path))
        XCTAssertFalse(FileManager.default.fileExists(atPath: outside.appendingPathComponent("copied.txt").path))
        XCTAssertFalse(FileManager.default.fileExists(atPath: outside.appendingPathComponent("moved.txt").path))
        XCTAssertTrue(FileManager.default.fileExists(atPath: safeSource.path))
    }

    func testListingSkipsUnrelatedSymbolicLinks() async throws {
        let root = try makeTempDir()
        let outside = try makeTempDir()
        defer {
            try? FileManager.default.removeItem(at: root)
            try? FileManager.default.removeItem(at: outside)
        }
        try Data("visible".utf8).write(to: root.appendingPathComponent("visible.txt"))
        try FileManager.default.createSymbolicLink(
            at: root.appendingPathComponent("unrelated-link"),
            withDestinationURL: outside
        )

        let client = try LocalVolumeClient(connectedRootURL: root)
        let names = try await client.list(path: "/").map(\.name)

        XCTAssertEqual(names, ["visible.txt"])
        await XCTAssertThrowsErrorAsync {
            _ = try await client.list(path: "/unrelated-link")
        }
    }

    func testDirectoryHierarchySyncRetriesAfterCreationFailure() async throws {
        let root = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: root) }
        let failure = OneShotDirectorySyncFailure()
        let client = try LocalVolumeClient(
            connectedRootURL: root,
            directorySynchronizer: { _ in try failure.call() }
        )

        await XCTAssertThrowsErrorAsync {
            try await client.createDirectory(path: "/photos")
        }
        XCTAssertTrue(FileManager.default.fileExists(atPath: root.appendingPathComponent("photos").path))

        try await client.createDirectory(path: "/photos")
        XCTAssertGreaterThan(failure.callCount, 1)
    }

    func testConnectedRootReplacementWithOutsideSymlinkFailsClosedForAllOperations() async throws {
        let root = try makeTempDir()
        let outside = try makeTempDir()
        defer {
            try? FileManager.default.removeItem(at: root)
            try? FileManager.default.removeItem(at: outside)
        }
        let victim = outside.appendingPathComponent("victim.txt")
        let victimBody = Data("outside-victim".utf8)
        try victimBody.write(to: victim)
        let originalModificationDate = try XCTUnwrap(
            FileManager.default.attributesOfItem(atPath: victim.path)[.modificationDate] as? Date
        )
        let uploadSource = try writeSource(Data("replacement".utf8))
        let downloadTarget = FileManager.default.temporaryDirectory
            .appendingPathComponent("lv-root-replacement-\(UUID().uuidString).bin")
        defer {
            try? FileManager.default.removeItem(at: uploadSource)
            try? FileManager.default.removeItem(at: downloadTarget)
        }
        let client = try LocalVolumeClient(connectedRootURL: root)

        try FileManager.default.removeItem(at: root)
        try FileManager.default.createSymbolicLink(at: root, withDestinationURL: outside)

        let directReadURL = await client.directReadURL(forRemotePath: "/victim.txt")
        XCTAssertNil(directReadURL)
        await XCTAssertThrowsErrorAsync {
            try await client.download(remotePath: "/victim.txt", localURL: downloadTarget)
        }
        await XCTAssertThrowsErrorAsync {
            try await client.upload(
                localURL: uploadSource,
                remotePath: "/victim.txt",
                respectTaskCancellation: false,
                onProgress: nil
            )
        }
        await XCTAssertThrowsErrorAsync {
            try await client.delete(path: "/victim.txt")
        }
        await XCTAssertThrowsErrorAsync {
            try await client.createDirectory(path: "/new-directory")
        }
        await XCTAssertThrowsErrorAsync {
            try await client.copy(from: "/victim.txt", to: "/copied.txt")
        }
        await XCTAssertThrowsErrorAsync {
            try await client.move(from: "/victim.txt", to: "/moved.txt")
        }
        await XCTAssertThrowsErrorAsync {
            _ = try await client.list(path: "/")
        }
        await XCTAssertThrowsErrorAsync {
            _ = try await client.metadata(path: "/victim.txt")
        }
        await XCTAssertThrowsErrorAsync {
            _ = try await client.exists(path: "/victim.txt")
        }
        await XCTAssertThrowsErrorAsync {
            try await client.setModificationDate(Date(timeIntervalSince1970: 1), forPath: "/victim.txt")
        }

        XCTAssertEqual(try Data(contentsOf: victim), victimBody)
        XCTAssertEqual(
            try FileManager.default.attributesOfItem(atPath: victim.path)[.modificationDate] as? Date,
            originalModificationDate
        )
        XCTAssertFalse(FileManager.default.fileExists(atPath: outside.appendingPathComponent("new-directory").path))
        XCTAssertFalse(FileManager.default.fileExists(atPath: outside.appendingPathComponent("copied.txt").path))
        XCTAssertFalse(FileManager.default.fileExists(atPath: outside.appendingPathComponent("moved.txt").path))
    }

    func testConnectedRootDeleteAndRecreateAtSamePathRemainsUsable() async throws {
        let root = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: root) }
        let source = try writeSource(Data("new-root".utf8))
        defer { try? FileManager.default.removeItem(at: source) }
        let client = try LocalVolumeClient(connectedRootURL: root)

        try FileManager.default.removeItem(at: root)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)

        try await client.upload(
            localURL: source,
            remotePath: "/file.bin",
            respectTaskCancellation: false,
            onProgress: nil
        )
        XCTAssertEqual(
            try Data(contentsOf: root.appendingPathComponent("file.bin")),
            Data("new-root".utf8)
        )
    }

    func testCancelledChunkedUploadDoesNotCleanThroughReplacedParentSymlink() async throws {
        let root = try makeTempDir()
        let outside = try makeTempDir()
        defer {
            try? FileManager.default.removeItem(at: root)
            try? FileManager.default.removeItem(at: outside)
        }
        let subdirectory = root.appendingPathComponent("sub", isDirectory: true)
        let movedSubdirectory = root.appendingPathComponent("sub-original", isDirectory: true)
        try FileManager.default.createDirectory(at: subdirectory, withIntermediateDirectories: true)
        let victim = outside.appendingPathComponent("victim.bin")
        let victimBody = Data("outside-victim".utf8)
        try victimBody.write(to: victim)
        let originalModificationDate = try XCTUnwrap(
            FileManager.default.attributesOfItem(atPath: victim.path)[.modificationDate] as? Date
        )
        let source = try writeLargeSource()
        defer { try? FileManager.default.removeItem(at: source) }
        let client = try LocalVolumeClient(connectedRootURL: root)
        var didMutate = false
        var mutationError: Error?

        do {
            try await client.upload(
                localURL: source,
                remotePath: "/sub/victim.bin",
                respectTaskCancellation: true,
                onProgress: { _ in
                    guard !didMutate else { return }
                    didMutate = true
                    do {
                        try FileManager.default.moveItem(at: subdirectory, to: movedSubdirectory)
                        try FileManager.default.createSymbolicLink(at: subdirectory, withDestinationURL: outside)
                    } catch {
                        mutationError = error
                    }
                    withUnsafeCurrentTask { $0?.cancel() }
                }
            )
            XCTFail("Expected cancellation")
        } catch is CancellationError {}

        XCTAssertTrue(didMutate)
        XCTAssertNil(mutationError)
        XCTAssertEqual(try Data(contentsOf: victim), victimBody)
        XCTAssertEqual(
            try FileManager.default.attributesOfItem(atPath: victim.path)[.modificationDate] as? Date,
            originalModificationDate
        )
    }

    func testCancelledChunkedUploadCleansItsRandomStagingPath() async throws {
        let root = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: root) }
        let subdirectory = root.appendingPathComponent("sub", isDirectory: true)
        try FileManager.default.createDirectory(at: subdirectory, withIntermediateDirectories: true)
        let replacementBody = Data("replacement-file".utf8)
        let source = try writeLargeSource()
        defer { try? FileManager.default.removeItem(at: source) }
        let client = try LocalVolumeClient(connectedRootURL: root)
        var didReplace = false
        var replacementError: Error?
        var replacementURL: URL?

        do {
            try await client.upload(
                localURL: source,
                remotePath: "/sub/victim.bin",
                respectTaskCancellation: true,
                onProgress: { _ in
                    guard !didReplace else { return }
                    didReplace = true
                    do {
                        let stagingURL = try XCTUnwrap(self.temporaryUploadFiles(in: subdirectory).first)
                        try FileManager.default.removeItem(at: stagingURL)
                        try replacementBody.write(to: stagingURL)
                        replacementURL = stagingURL
                    } catch {
                        replacementError = error
                    }
                    withUnsafeCurrentTask { $0?.cancel() }
                }
            )
            XCTFail("Expected cancellation")
        } catch is CancellationError {}

        XCTAssertTrue(didReplace)
        XCTAssertNil(replacementError)
        XCTAssertFalse(FileManager.default.fileExists(atPath: try XCTUnwrap(replacementURL).path))
    }

    func testCancelledChunkedUploadCleansMatchingPartialFile() async throws {
        let root = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: root) }
        let subdirectory = root.appendingPathComponent("sub", isDirectory: true)
        try FileManager.default.createDirectory(at: subdirectory, withIntermediateDirectories: true)
        let destination = subdirectory.appendingPathComponent("partial.bin")
        let source = try writeLargeSource()
        defer { try? FileManager.default.removeItem(at: source) }
        let client = try LocalVolumeClient(connectedRootURL: root)
        var didCancel = false

        do {
            try await client.upload(
                localURL: source,
                remotePath: "/sub/partial.bin",
                respectTaskCancellation: true,
                onProgress: { _ in
                    guard !didCancel else { return }
                    didCancel = true
                    withUnsafeCurrentTask { $0?.cancel() }
                }
            )
            XCTFail("Expected cancellation")
        } catch is CancellationError {}

        XCTAssertTrue(didCancel)
        XCTAssertFalse(FileManager.default.fileExists(atPath: destination.path))
        XCTAssertTrue(try temporaryUploadFiles(in: subdirectory).isEmpty)
    }

    func testReplaceMissingSourcePreservesExistingFinalForFastAndCancellationAwarePaths() async throws {
        for respectCancellation in [false, true] {
            let root = try makeTempDir()
            defer { try? FileManager.default.removeItem(at: root) }
            let final = root.appendingPathComponent("target.bin")
            let oldBody = Data("old-final".utf8)
            try oldBody.write(to: final)
            let oldModificationDate = try XCTUnwrap(
                FileManager.default.attributesOfItem(atPath: final.path)[.modificationDate] as? Date
            )
            let missingSource = root.appendingPathComponent("missing-source.bin")
            let client = try LocalVolumeClient(connectedRootURL: root)

            await XCTAssertThrowsErrorAsync {
                try await client.upload(
                    localURL: missingSource,
                    remotePath: "/target.bin",
                    respectTaskCancellation: respectCancellation,
                    onProgress: nil
                )
            }

            XCTAssertEqual(try Data(contentsOf: final), oldBody)
            XCTAssertEqual(
                try FileManager.default.attributesOfItem(atPath: final.path)[.modificationDate] as? Date,
                oldModificationDate
            )
            XCTAssertTrue(try temporaryUploadFiles(in: root).isEmpty)
        }
    }

    func testReplaceMidChunkCancellationPreservesExistingFinalAndCleansStagingFile() async throws {
        let root = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: root) }
        let final = root.appendingPathComponent("target.bin")
        let oldBody = Data("old-final".utf8)
        try oldBody.write(to: final)
        let oldModificationDate = try XCTUnwrap(
            FileManager.default.attributesOfItem(atPath: final.path)[.modificationDate] as? Date
        )
        let source = try writeLargeSource()
        defer { try? FileManager.default.removeItem(at: source) }
        let client = try LocalVolumeClient(connectedRootURL: root)
        var didCancel = false

        do {
            try await client.upload(
                localURL: source,
                remotePath: "/target.bin",
                respectTaskCancellation: true,
                onProgress: { _ in
                    guard !didCancel else { return }
                    didCancel = true
                    withUnsafeCurrentTask { $0?.cancel() }
                }
            )
            XCTFail("Expected cancellation")
        } catch is CancellationError {}

        XCTAssertTrue(didCancel)
        XCTAssertEqual(try Data(contentsOf: final), oldBody)
        XCTAssertEqual(
            try FileManager.default.attributesOfItem(atPath: final.path)[.modificationDate] as? Date,
            oldModificationDate
        )
        XCTAssertTrue(try temporaryUploadFiles(in: root).isEmpty)
    }

    func testReplacePublishesCompleteNewFileAndLeavesNoStagingFile() async throws {
        let root = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: root) }
        let final = root.appendingPathComponent("target.bin")
        try Data("old-final".utf8).write(to: final)
        let newBody = Data("new-final".utf8)
        let source = try writeSource(newBody)
        defer { try? FileManager.default.removeItem(at: source) }
        let client = try LocalVolumeClient(connectedRootURL: root)

        try await client.upload(
            localURL: source,
            remotePath: "/target.bin",
            respectTaskCancellation: false,
            onProgress: nil
        )

        XCTAssertEqual(try Data(contentsOf: final), newBody)
        XCTAssertTrue(try temporaryUploadFiles(in: root).isEmpty)
    }

    func testReplacePublishesCompleteFileWhenFinalDoesNotExist() async throws {
        let root = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: root) }
        let newBody = Data("new-final".utf8)
        let source = try writeSource(newBody)
        defer { try? FileManager.default.removeItem(at: source) }
        let client = try LocalVolumeClient(connectedRootURL: root)

        try await client.upload(
            localURL: source,
            remotePath: "/target.bin",
            respectTaskCancellation: false,
            onProgress: nil
        )

        XCTAssertEqual(try Data(contentsOf: root.appendingPathComponent("target.bin")), newBody)
        XCTAssertTrue(try temporaryUploadFiles(in: root).isEmpty)
    }

    func testReplacePublishFailurePreservesExistingFinalAndCleansOwnedStagingFile() async throws {
        let root = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: root) }
        let final = root.appendingPathComponent("target.bin")
        let oldBody = Data("old-final".utf8)
        try oldBody.write(to: final)
        let oldModificationDate = try XCTUnwrap(
            FileManager.default.attributesOfItem(atPath: final.path)[.modificationDate] as? Date
        )
        let source = try writeSource(Data("new-final".utf8))
        defer { try? FileManager.default.removeItem(at: source) }
        let client = try LocalVolumeClient(
            connectedRootURL: root,
            stagedUploadPublisher: { _, _ in throw POSIXError(.EIO) }
        )

        await XCTAssertThrowsErrorAsync {
            try await client.upload(
                localURL: source,
                remotePath: "/target.bin",
                respectTaskCancellation: false,
                onProgress: nil
            )
        }

        XCTAssertEqual(try Data(contentsOf: final), oldBody)
        XCTAssertEqual(
            try FileManager.default.attributesOfItem(atPath: final.path)[.modificationDate] as? Date,
            oldModificationDate
        )
        XCTAssertTrue(try temporaryUploadFiles(in: root).isEmpty)
    }

    func testReplaceRejectsStagingLeafSwappedToSymlinkWithoutPublishingOrDeletingArtifact() async throws {
        let root = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: root) }
        let final = root.appendingPathComponent("target.bin")
        let oldBody = Data("old-final".utf8)
        try oldBody.write(to: final)
        let source = try writeLargeSource()
        defer { try? FileManager.default.removeItem(at: source) }
        let bypass = root.appendingPathComponent("bypass.bin")
        let client = try LocalVolumeClient(connectedRootURL: root)
        var didSwap = false
        var swapError: Error?

        do {
            try await client.upload(
                localURL: source,
                remotePath: "/target.bin",
                respectTaskCancellation: true,
                onProgress: { progress in
                    guard progress >= 1, !didSwap else { return }
                    didSwap = true
                    do {
                        let stagingURL = try XCTUnwrap(self.temporaryUploadFiles(in: root).first)
                        try FileManager.default.moveItem(at: stagingURL, to: bypass)
                        try FileManager.default.createSymbolicLink(at: stagingURL, withDestinationURL: bypass)
                    } catch {
                        swapError = error
                    }
                }
            )
            XCTFail("Expected staging leaf validation failure")
        } catch {}

        XCTAssertTrue(didSwap)
        XCTAssertNil(swapError)
        XCTAssertEqual(try Data(contentsOf: final), oldBody)
        XCTAssertFalse(try final.resourceValues(forKeys: [.isSymbolicLinkKey]).isSymbolicLink ?? false)
        XCTAssertTrue(FileManager.default.fileExists(atPath: bypass.path))
        XCTAssertEqual(
            (try FileManager.default.attributesOfItem(atPath: bypass.path)[.size] as? NSNumber)?.intValue,
            20 * 1024 * 1024
        )
    }

    func testStagedPublishDeviceLossPOSIXErrorsClassifyExternalStorageUnavailable() async throws {
        for code in [POSIXErrorCode.ENODEV, .ESTALE] {
            let root = try makeTempDir()
            defer { try? FileManager.default.removeItem(at: root) }
            let final = root.appendingPathComponent("target.bin")
            let oldBody = Data("old-final".utf8)
            try oldBody.write(to: final)
            let source = try writeSource(Data("new-final".utf8))
            defer { try? FileManager.default.removeItem(at: source) }
            let client = try LocalVolumeClient(
                connectedRootURL: root,
                stagedUploadPublisher: { _, _ in throw POSIXError(code) }
            )
            var capturedError: Error?

            do {
                try await client.upload(
                    localURL: source,
                    remotePath: "/target.bin",
                    respectTaskCancellation: false,
                    onProgress: nil
                )
                XCTFail("Expected \(code)")
            } catch {
                capturedError = error
            }

            let error = try XCTUnwrap(capturedError)
            XCTAssertTrue(RemoteStorageClientError.isLikelyExternalStorageUnavailable(error), "\(code)")
            XCTAssertTrue(makeExternalProfile().isConnectionUnavailableError(error), "\(code)")
            XCTAssertEqual(try Data(contentsOf: final), oldBody)
            XCTAssertTrue(try temporaryUploadFiles(in: root).isEmpty)
        }
    }

    func testStagedPublishStableRootPOSIXErrorsRemainUnderlying() async throws {
        for code in [
            POSIXErrorCode.EIO,
            POSIXErrorCode.ENOENT,
            .EACCES,
            .EPERM,
            .ENOSPC,
            .EROFS,
            .EXDEV
        ] {
            let root = try makeTempDir()
            defer { try? FileManager.default.removeItem(at: root) }
            let final = root.appendingPathComponent("target.bin")
            let oldBody = Data("old-final".utf8)
            try oldBody.write(to: final)
            let source = try writeSource(Data("new-final".utf8))
            defer { try? FileManager.default.removeItem(at: source) }
            let client = try LocalVolumeClient(
                connectedRootURL: root,
                stagedUploadPublisher: { _, _ in throw POSIXError(code) }
            )
            var capturedError: Error?

            do {
                try await client.upload(
                    localURL: source,
                    remotePath: "/target.bin",
                    respectTaskCancellation: false,
                    onProgress: nil
                )
                XCTFail("Expected \(code)")
            } catch {
                capturedError = error
            }

            let error = try XCTUnwrap(capturedError)
            XCTAssertFalse(RemoteStorageClientError.isLikelyExternalStorageUnavailable(error), "\(code)")
            XCTAssertFalse(makeExternalProfile().isConnectionUnavailableError(error), "\(code)")
            guard let storageError = error as? RemoteStorageClientError,
                  case .underlying(let underlying) = storageError else {
                XCTFail("Expected underlying POSIX error for \(code)")
                continue
            }
            XCTAssertEqual((underlying as NSError).domain, NSPOSIXErrorDomain)
            XCTAssertEqual((underlying as NSError).code, Int(code.rawValue))
            XCTAssertEqual(try Data(contentsOf: final), oldBody)
            XCTAssertTrue(try temporaryUploadFiles(in: root).isEmpty)
        }
    }

    func testStagedPublishENOENTClassifiesUnavailableWhenAnchoredRootIsLost() async throws {
        let root = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: root) }
        try Data("old-final".utf8).write(to: root.appendingPathComponent("target.bin"))
        let source = try writeSource(Data("new-final".utf8))
        defer { try? FileManager.default.removeItem(at: source) }
        let client = try LocalVolumeClient(
            connectedRootURL: root,
            stagedUploadPublisher: { _, _ in
                try FileManager.default.removeItem(at: root)
                throw POSIXError(.ENOENT)
            }
        )
        var capturedError: Error?

        do {
            try await client.upload(
                localURL: source,
                remotePath: "/target.bin",
                respectTaskCancellation: false,
                onProgress: nil
            )
            XCTFail("Expected ENOENT")
        } catch {
            capturedError = error
        }

        let error = try XCTUnwrap(capturedError)
        XCTAssertTrue(RemoteStorageClientError.isLikelyExternalStorageUnavailable(error))
        XCTAssertTrue(makeExternalProfile().isConnectionUnavailableError(error))
    }

    func testContainedResolverAllowsNormalNewNestedFilePath() async throws {
        let root = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: root) }
        let body = Data("nested-body".utf8)
        let source = try writeSource(body)
        defer { try? FileManager.default.removeItem(at: source) }
        let client = try LocalVolumeClient(connectedRootURL: root)

        try await client.upload(
            localURL: source,
            remotePath: "/new/child/file.bin",
            respectTaskCancellation: false,
            onProgress: nil
        )

        XCTAssertEqual(
            try Data(contentsOf: root.appendingPathComponent("new/child/file.bin")),
            body
        )
    }

    private func writeLargeSource() throws -> URL {
        try writeSource(Data(repeating: 0xA5, count: 20 * 1024 * 1024))
    }

    private func temporaryUploadFiles(in directory: URL) throws -> [URL] {
        try FileManager.default.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: nil
        ).filter { $0.lastPathComponent.hasPrefix(".watermelon-upload-") }
    }

    private func makeExternalProfile() -> ServerProfileRecord {
        ServerProfileRecord(
            name: "External",
            storageType: StorageType.externalVolume.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: "external",
            port: 0,
            shareName: "external-location",
            basePath: "/",
            username: "local",
            domain: nil,
            credentialRef: "external-ref",
            backgroundBackupEnabled: false,
            backgroundBackupMinIntervalMinutes: 720,
            backgroundBackupRequiresWiFi: true,
            generateRemoteThumbnails: false,
            createdAt: Date(),
            updatedAt: Date()
        )
    }
}

private func XCTAssertThrowsErrorAsync(
    _ expression: () async throws -> Void,
    file: StaticString = #filePath,
    line: UInt = #line
) async {
    do {
        try await expression()
        XCTFail("Expected error", file: file, line: line)
    } catch {}
}
