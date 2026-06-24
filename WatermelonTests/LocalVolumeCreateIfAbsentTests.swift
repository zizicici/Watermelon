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
                        let client = LocalVolumeClient(connectedRootURL: root)
                        do {
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
}
