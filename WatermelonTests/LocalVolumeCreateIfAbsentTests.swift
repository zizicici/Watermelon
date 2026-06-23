import XCTest
@testable import Watermelon

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

        let client = LocalVolumeClient(connectedRootURL: root)
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

        let client = LocalVolumeClient(connectedRootURL: root)
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
}
