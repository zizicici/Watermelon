import MoreKit
import XCTest
@testable import Watermelon

final class ShortcutsAccessTests: XCTestCase {
    private var suiteName: String!
    private var defaults: UserDefaults!
    private var store: ShortcutsAccessStore!

    override func setUp() {
        super.setUp()
        suiteName = "ShortcutsAccessTests.\(UUID().uuidString)"
        defaults = UserDefaults(suiteName: suiteName)
        store = ShortcutsAccessStore(defaults: defaults)
    }

    override func tearDown() {
        defaults.removePersistentDomain(forName: suiteName)
        store = nil
        defaults = nil
        super.tearDown()
    }

    func testFreeUsersCanCompleteFiveBackupsAndSixthNeverStarts() async throws {
        XCTAssertNoThrow(try store.checkEnabled())
        for remaining in stride(from: 4, through: 0, by: -1) {
            let result = try await store.run(isPro: false) { 42 }
            XCTAssertEqual(result, 42)
            XCTAssertEqual(store.remainingTrialRuns, remaining)
        }
        do {
            try await store.run(isPro: false) { XCTFail("Exhausted trials must not start the backup") }
            XCTFail("Expected the trial limit")
        } catch {
            XCTAssertEqual(error as? ShortcutsAccessError, .trialEnded)
        }
    }

    func testFailuresAndCancellationDoNotConsumeTrials() async throws {
        for error: Error in [TestFailure.failed, CancellationError()] {
            do {
                try await store.run(isPro: false) { throw error }
                XCTFail("Expected the operation error")
            } catch {}
            XCTAssertEqual(store.remainingTrialRuns, 5)
        }
        try await store.run(isPro: false) {}
        XCTAssertEqual(store.remainingTrialRuns, 4)
    }

    func testCancellationBeforeExecutionDoesNotRunOrConsumeTrial() async {
        let store = store!
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try await store.run(isPro: false) {
                XCTFail("Cancelled backup must not start")
            }
        }
        do {
            try await task.value
            XCTFail("Expected cancellation")
        } catch {
            XCTAssertTrue(error is CancellationError)
        }
        XCTAssertEqual(store.remainingTrialRuns, 5)
    }

    func testCancellationAfterOperationReturnsDoesNotConsumeTrial() async {
        let store = store!
        let task = Task {
            try await store.run(isPro: false) {
                withUnsafeCurrentTask { $0?.cancel() }
            }
        }
        do {
            try await task.value
            XCTFail("Expected cancellation")
        } catch {
            XCTAssertTrue(error is CancellationError)
        }
        XCTAssertEqual(store.remainingTrialRuns, 5)
    }

    func testDisabledSettingBlocksFreeAndProWithoutUsingTrials() async {
        defaults.set(ShortcutsSetting.disable.rawValue, forKey: ShortcutsSetting.getKey())
        for isPro in [false, true] {
            do {
                try await store.run(isPro: isPro) { XCTFail("Disabled shortcut must not execute") }
                XCTFail("Expected disabled error")
            } catch {
                XCTAssertEqual(error as? ShortcutsAccessError, .disabled)
            }
        }
        XCTAssertEqual(store.remainingTrialRuns, 5)
    }

    @MainActor
    func testDisabledBackupIntentStopsBeforeReadingParametersOrStartingBackgroundWork() async throws {
        guard #available(iOS 27.0, *) else { throw XCTSkip("Requires iOS 27 App Intents") }
        let sharedDefaults = ShortcutsSetting.userDefaults
        let key = ShortcutsSetting.getKey()
        let previous = sharedDefaults.object(forKey: key)
        defer {
            if let previous { sharedDefaults.set(previous, forKey: key) }
            else { sharedDefaults.removeObject(forKey: key) }
        }
        sharedDefaults.set(ShortcutsSetting.disable.rawValue, forKey: key)
        do {
            _ = try await RunBackupIntent().perform()
            XCTFail("The disabled intent must stop before using unset parameters")
        } catch {
            XCTAssertEqual(error as? ShortcutsAccessError, .disabled)
        }
    }

    func testProUsersRunWithoutConsumingOrRequiringTrials() async throws {
        for _ in 0..<8 { try await store.run(isPro: true) {} }
        XCTAssertEqual(store.remainingTrialRuns, 5)
        for _ in 0..<5 { try await store.run(isPro: false) {} }
        for _ in 0..<8 { try await store.run(isPro: true) {} }
        XCTAssertEqual(store.remainingTrialRuns, 0)
    }

    func testCountSurvivesStoreReloadAndTogglingButResetsWithFreshAppData() async throws {
        try await store.run(isPro: false) {}
        defaults.set(ShortcutsSetting.disable.rawValue, forKey: ShortcutsSetting.getKey())
        defaults.set(ShortcutsSetting.enable.rawValue, forKey: ShortcutsSetting.getKey())
        let reloaded = ShortcutsAccessStore(defaults: defaults)
        XCTAssertEqual(reloaded.remainingTrialRuns, 4)
        defaults.removePersistentDomain(forName: suiteName)
        XCTAssertEqual(ShortcutsAccessStore(defaults: defaults).remainingTrialRuns, 5)
    }

    func testConcurrentAttemptsCannotSpendTheLastTrialTwice() async throws {
        let store = store!
        for _ in 0..<4 { try await store.run(isPro: false) {} }
        let entered = expectation(description: "Last trial is in use")
        let gate = ShortcutsTrialGate()
        let task = Task {
            try await store.run(isPro: false) {
                entered.fulfill()
                await gate.wait()
            }
        }
        await fulfillment(of: [entered], timeout: 3)
        do {
            try await store.run(isPro: false) { XCTFail("The last trial is already reserved") }
            XCTFail("Expected in-progress error")
        } catch {
            XCTAssertTrue(error is BackgroundBackupRunError)
        }
        XCTAssertEqual(store.remainingTrialRuns, 1)
        await gate.open()
        try await task.value
        XCTAssertEqual(store.remainingTrialRuns, 0)
    }

    func testFailedLastTrialReleasesReservationForRetry() async throws {
        for _ in 0..<4 { try await store.run(isPro: false) {} }
        do {
            try await store.run(isPro: false) { throw TestFailure.failed }
            XCTFail("Expected failure")
        } catch {}
        try await store.run(isPro: false) {}
        XCTAssertEqual(store.remainingTrialRuns, 0)
    }

    private enum TestFailure: Error { case failed }
}

private actor ShortcutsTrialGate {
    private var continuation: CheckedContinuation<Void, Never>?
    private var isOpen = false

    func wait() async {
        if isOpen { return }
        await withCheckedContinuation { continuation = $0 }
    }

    func open() {
        isOpen = true
        continuation?.resume()
        continuation = nil
    }
}
