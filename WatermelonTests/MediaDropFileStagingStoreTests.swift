import XCTest
@testable import Watermelon

final class MediaDropFileStagingStoreTests: XCTestCase {
    func testStageCopiesBytesAndKeepsOriginalFilename() async throws {
        let sourceDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(
            at: sourceDirectory,
            withIntermediateDirectories: true
        )
        defer { try? FileManager.default.removeItem(at: sourceDirectory) }

        let sourceURL = sourceDirectory.appendingPathComponent("report.final.pdf")
        let bytes = Data("media-drop".utf8)
        try bytes.write(to: sourceURL)
        let store = MediaDropFileStagingStore()

        let files = try await store.stage([sourceURL])

        XCTAssertEqual(files.count, 1)
        XCTAssertEqual(files[0].preferredName, "report.final.pdf")
        XCTAssertEqual(try Data(contentsOf: files[0].localURL), bytes)
        XCTAssertEqual(files[0].fileSize, Int64(bytes.count))
        XCTAssertTrue(FileManager.default.fileExists(atPath: sourceURL.path))
        await store.removeAll()
        XCTAssertFalse(FileManager.default.fileExists(atPath: files[0].localURL.path))
    }

    func testStageRejectsDirectory() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        do {
            _ = try await MediaDropFileStagingStore().stage([directory])
            XCTFail("Expected directory rejection")
        } catch let error as MediaDropFileStagingStore.StagingError {
            guard case .directoryNotSupported = error else {
                return XCTFail("Unexpected staging error: \(error)")
            }
        }
    }

    func testStageIgnoresFileAlreadySelected() async throws {
        let sourceDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: sourceDirectory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: sourceDirectory) }

        let sourceURL = sourceDirectory.appendingPathComponent("report.pdf")
        try Data("same-file".utf8).write(to: sourceURL)
        let store = MediaDropFileStagingStore()
        let initial = try await store.stage([sourceURL])

        let repeated = try await store.stage([sourceURL], excluding: initial)

        XCTAssertEqual(initial.count, 1)
        XCTAssertTrue(repeated.isEmpty)
    }

    func testStageKeepsSameNamedFileWhenContentsDiffer() async throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let firstDirectory = root.appendingPathComponent("first", isDirectory: true)
        let secondDirectory = root.appendingPathComponent("second", isDirectory: true)
        try FileManager.default.createDirectory(at: firstDirectory, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: secondDirectory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let firstURL = firstDirectory.appendingPathComponent("report.pdf")
        let secondURL = secondDirectory.appendingPathComponent("report.pdf")
        try Data("first".utf8).write(to: firstURL)
        try Data("other".utf8).write(to: secondURL)
        let store = MediaDropFileStagingStore()
        let initial = try await store.stage([firstURL])

        let additional = try await store.stage([secondURL], excluding: initial)

        XCTAssertEqual(additional.count, 1)
        XCTAssertEqual(try Data(contentsOf: additional[0].localURL), Data("other".utf8))
    }

    func testStageCollapsesDuplicatesWithinOnePickerResult() async throws {
        let sourceDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: sourceDirectory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: sourceDirectory) }

        let sourceURL = sourceDirectory.appendingPathComponent("report.pdf")
        try Data("same-file".utf8).write(to: sourceURL)

        let files = try await MediaDropFileStagingStore().stage([sourceURL, sourceURL])

        XCTAssertEqual(files.count, 1)
    }

    func testCleanupStaleSessionsOnlyRemovesSessionsPresentAtStart() async throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let stale = root.appendingPathComponent("stale", isDirectory: true)
        try FileManager.default.createDirectory(at: stale, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let cleanup = MediaDropFileStagingStore.cleanupStaleSessions(in: root)
        let current = root.appendingPathComponent("current", isDirectory: true)
        try FileManager.default.createDirectory(at: current, withIntermediateDirectories: true)
        await cleanup?.value

        XCTAssertFalse(FileManager.default.fileExists(atPath: stale.path))
        XCTAssertTrue(FileManager.default.fileExists(atPath: current.path))
    }
}
