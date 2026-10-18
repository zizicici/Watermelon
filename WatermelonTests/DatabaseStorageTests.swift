import GRDB
import XCTest
@testable import Watermelon

final class DatabaseStorageTests: XCTestCase {
    private func temporaryDirectory() throws -> URL {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        addTeardownBlock { try? FileManager.default.removeItem(at: directory) }
        return directory
    }

    func testManagersForSameFileShareQueueAndSourceButDifferentFilesDoNot() throws {
        let directory = try temporaryDirectory()
        let first = try DatabaseManager(databaseURL: directory.appendingPathComponent("index.sqlite"))
        let second = try DatabaseManager(databaseURL: directory.appendingPathComponent("./index.sqlite"))
        let other = try DatabaseManager(databaseURL: directory.appendingPathComponent("other.sqlite"))
        XCTAssertTrue(first.dbQueue === second.dbQueue)
        XCTAssertTrue(first.localIndexChanges === second.localIndexChanges)
        XCTAssertFalse(first.dbQueue === other.dbQueue)
        XCTAssertFalse(first.localIndexChanges === other.localIndexChanges)
        let alias = directory.appendingPathComponent("alias.sqlite")
        try FileManager.default.createSymbolicLink(at: alias, withDestinationURL: directory.appendingPathComponent("index.sqlite"))
        let throughAlias = try DatabaseManager(databaseURL: alias)
        XCTAssertTrue(first.dbQueue === throughAlias.dbQueue)
    }

    func testConcurrentInitializationSharesOneWriter() throws {
        let url = try temporaryDirectory().appendingPathComponent("index.sqlite")
        let results = InitializationResults()
        DispatchQueue.concurrentPerform(iterations: 8) { _ in
            results.append(Result { try DatabaseManager(databaseURL: url) })
        }
        let managers = try results.values.map { try $0.get() }
        let first = try XCTUnwrap(managers.first)
        XCTAssertEqual(managers.count, 8)
        XCTAssertTrue(managers.allSatisfy { $0.dbQueue === first.dbQueue })
        XCTAssertTrue(managers.allSatisfy { $0.localIndexChanges === first.localIndexChanges })
    }

    func testStorageIsReleasedAfterLastManagerAndReopenObservesPersistedDeletes() throws {
        let url = try temporaryDirectory().appendingPathComponent("index.sqlite")
        weak var releasedQueue: DatabaseQueue?
        try autoreleasepool {
            let database = try DatabaseManager(databaseURL: url)
            releasedQueue = database.dbQueue
            try database.write { db in
                try db.execute(sql: "INSERT INTO local_assets (assetLocalIdentifier, assetFingerprint, updatedAt) VALUES (?, ?, ?)",
                               arguments: ["persisted", Data([1]), Date()])
            }
        }
        XCTAssertNil(releasedQueue)
        let reopened = try DatabaseManager(databaseURL: url)
        let revision = reopened.localIndexChanges.currentRevision
        try reopened.write { try $0.execute(sql: "DELETE FROM local_assets") }
        guard case .touched(let ids) = reopened.localIndexChanges.delta(since: revision).change else {
            return XCTFail("The reopened observer must know persisted identities")
        }
        XCTAssertEqual(ids, ["persisted"])
    }

    @MainActor
    func testUIObserverCoalescesCommitsWithoutLosingIdentifiers() async throws {
        let database = try DatabaseManager(databaseURL: temporaryDirectory().appendingPathComponent("index.sqlite"))
        let source = database.localIndexChanges
        let baseRevision = source.currentRevision
        let delivered = expectation(description: "Coalesced committed changes")
        var deliveries = 0
        let token = source.addCoalescedMainActorObserver {
            deliveries += 1
            XCTAssertEqual(try? database.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM local_assets") }, 20)
            delivered.fulfill()
        }
        defer { source.removeObserver(token) }
        for index in 0..<20 {
            try database.write { db in
                try db.execute(sql: "INSERT INTO local_assets (assetLocalIdentifier, assetFingerprint, updatedAt) VALUES (?, ?, ?)",
                    arguments: ["asset-\(index)", Data([1]), Date()])
            }
        }
        XCTAssertEqual(deliveries, 0)
        await fulfillment(of: [delivered], timeout: 3)
        XCTAssertEqual(deliveries, 1)
        guard case .touched(let ids) = source.delta(since: baseRevision).change else { return XCTFail("Expected incremental changes") }
        XCTAssertEqual(ids, Set((0..<20).map { "asset-\($0)" }))
    }

    private final class InitializationResults: @unchecked Sendable {
        private let lock = NSLock()
        private var stored: [Result<DatabaseManager, Error>] = []
        func append(_ value: Result<DatabaseManager, Error>) { lock.withLock { stored.append(value) } }
        var values: [Result<DatabaseManager, Error>] { lock.withLock { stored } }
    }
}
