import XCTest
import UIKit
@testable import Watermelon

@MainActor
final class HomeFileSizeScanCoordinatorTests: XCTestCase {
    private let recent = LibraryMonthKey(year: 2025, month: 2)
    private let older = LibraryMonthKey(year: 2024, month: 1)

    @MainActor
    private final class Recorder {
        var months: [LibraryMonthKey] = []
        var scanned: [LibraryMonthKey] = []
        var persisted: [[AssetSizeUpdate]] = []
        var notifications: [Set<LibraryMonthKey>] = []
        var loads = 0
        var load: () async -> Void = {}
        var scan: (LibraryMonthKey) async -> [AssetSizeUpdate] = { _ in [] }
        var persist: () async -> Void = {}

        func make(active: Bool = true) -> HomeFileSizeScanCoordinator {
            let coordinator = HomeFileSizeScanCoordinator(initiallyActive: active, hooks: .init(
                months: { self.months },
                loadSizes: { self.loads += 1; await self.load(); return [:] },
                scan: { month, _ in self.scanned.append(month); return await self.scan(month) },
                persist: { self.persisted.append($0); await self.persist() }
            ))
            coordinator.onFileSizesUpdated = { self.notifications.append($0) }
            return coordinator
        }
    }

    func testBackgroundFullAndPartialRequestsCoalesceIntoOneForegroundScan() async {
        let recorder = Recorder()
        defer { recorder.scan = { _ in [] }; recorder.persist = {}; recorder.load = {} }
        recorder.months = [older, recent]
        let coordinator = recorder.make(active: false)
        coordinator.enqueueRescan(for: [recent])
        coordinator.startFullScan()
        coordinator.enqueueRescan(for: [older])
        await coordinator._testWaitUntilIdle()
        XCTAssertTrue(recorder.scanned.isEmpty)
        coordinator.setActive(true)
        await coordinator._testWaitUntilIdle()
        XCTAssertEqual(recorder.scanned, [recent, older])
        XCTAssertEqual(recorder.loads, 1)
        XCTAssertEqual(recorder.notifications, [[recent, older]])
    }

    func testFullReloadDuringScanDiscardsOldResultsAndWaitsForOldScan() async {
        let recorder = Recorder()
        defer { recorder.scan = { _ in [] }; recorder.persist = {}; recorder.load = {} }
        recorder.months = [older]
        let coordinator = recorder.make()
        let gate = RestoreTestLatch()
        let started = expectation(description: "old scan started")
        recorder.scan = { _ in
            if recorder.scanned.count == 1 {
                started.fulfill()
                await gate.wait()
                return [self.update("old")]
            }
            return [self.update("new")]
        }
        coordinator.startFullScan()
        await fulfillment(of: [started], timeout: 3)
        recorder.months = [recent]
        coordinator.startFullScan()
        XCTAssertEqual(recorder.scanned, [older])
        await gate.open()
        await coordinator._testWaitUntilIdle()
        XCTAssertEqual(recorder.scanned, [older, recent])
        XCTAssertEqual(recorder.loads, 2)
        XCTAssertEqual(recorder.persisted.flatMap { $0 }.map(\.assetLocalIdentifier), ["new"])
        XCTAssertEqual(recorder.notifications, [[recent]])
    }

    func testBackgroundFinishesCurrentMonthAndForegroundResumesRemainingMonths() async {
        let recorder = Recorder()
        defer { recorder.scan = { _ in [] }; recorder.persist = {}; recorder.load = {} }
        recorder.months = [recent, older]
        let coordinator = recorder.make()
        recorder.scan = { _ in
            if recorder.scanned.count == 1 { coordinator.setActive(false) }
            return [self.update("asset")]
        }
        coordinator.startFullScan()
        await coordinator._testWaitUntilIdle()
        XCTAssertEqual(recorder.scanned, [recent])
        XCTAssertTrue(recorder.notifications.isEmpty)
        XCTAssertEqual(recorder.persisted.count, 1)
        coordinator.setActive(true)
        await coordinator._testWaitUntilIdle()
        XCTAssertEqual(recorder.scanned, [recent, older])
        XCTAssertEqual(recorder.notifications, [[recent], [older]])
        XCTAssertEqual(recorder.loads, 2)
    }

    func testContinuousRecentMonthChangesDoNotStarveOlderMonthsOrNotifications() async throws {
        let recorder = Recorder()
        defer { recorder.scan = { _ in [] }; recorder.persist = {}; recorder.load = {} }
        recorder.months = [recent, older]
        let coordinator = recorder.make()
        recorder.scan = { month in
            if recorder.scanned.count < 4 { coordinator.enqueueRescan(for: [self.recent]) }
            try? await Task.sleep(for: .milliseconds(270))
            if recorder.scanned.count > 1 { XCTAssertFalse(recorder.notifications.isEmpty) }
            return []
        }
        coordinator.startFullScan()
        await coordinator._testWaitUntilIdle()
        XCTAssertEqual(Array(recorder.scanned.prefix(2)), [recent, older])
        XCTAssertEqual(recorder.scanned.count, 4)
        XCTAssertEqual(recorder.notifications.count, 4)
    }

    func testNewScanWaitsForPreviousWriteBack() async {
        let recorder = Recorder()
        defer { recorder.scan = { _ in [] }; recorder.persist = {}; recorder.load = {} }
        let coordinator = recorder.make()
        let gate = RestoreTestLatch()
        let started = expectation(description: "write started")
        recorder.scan = { _ in [self.update("asset")] }
        recorder.persist = {
            if recorder.persisted.count == 1 {
                started.fulfill()
                await gate.wait()
            }
        }
        coordinator.enqueueRescan(for: [recent])
        await fulfillment(of: [started], timeout: 3)
        coordinator.enqueueRescan(for: [older])
        XCTAssertEqual(recorder.scanned, [recent])
        await gate.open()
        await coordinator._testWaitUntilIdle()
        XCTAssertEqual(recorder.scanned, [recent, older])
        XCTAssertEqual(recorder.persisted.count, 2)
    }

    func testResetWhileLoadingDoesNotScanOrPublish() async {
        let recorder = Recorder()
        defer { recorder.scan = { _ in [] }; recorder.persist = {}; recorder.load = {} }
        let coordinator = recorder.make()
        let gate = RestoreTestLatch()
        let started = expectation(description: "cache load started")
        recorder.load = { started.fulfill(); await gate.wait() }
        coordinator.enqueueRescan(for: [recent])
        await fulfillment(of: [started], timeout: 3)
        coordinator.reset()
        await gate.open()
        await coordinator._testWaitUntilIdle()
        XCTAssertTrue(recorder.scanned.isEmpty)
        XCTAssertTrue(recorder.notifications.isEmpty)
    }

    func testMemoryWarningDefersFullRecoveryUntilNextForeground() async {
        let recorder = Recorder()
        defer { recorder.scan = { _ in [] }; recorder.persist = {}; recorder.load = {} }
        recorder.months = [recent, older]
        let coordinator = recorder.make()
        recorder.scan = { _ in
            if recorder.scanned.count == 1 {
                NotificationCenter.default.post(name: UIApplication.didReceiveMemoryWarningNotification, object: nil)
            }
            return []
        }
        coordinator.startFullScan()
        await coordinator._testWaitUntilIdle()
        XCTAssertEqual(recorder.scanned, [recent])
        coordinator.enqueueRescan(for: [older])
        await coordinator._testWaitUntilIdle()
        XCTAssertEqual(recorder.scanned, [recent])
        coordinator.setActive(false)
        coordinator.setActive(true)
        await coordinator._testWaitUntilIdle()
        XCTAssertEqual(recorder.scanned, [recent, recent, older])
        XCTAssertEqual(recorder.notifications, [[recent, older]])
    }

    private func update(_ id: String) -> AssetSizeUpdate {
        AssetSizeUpdate(assetLocalIdentifier: id, totalFileSizeBytes: 100, modificationDateMs: 1)
    }
}
