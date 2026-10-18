import UIKit
import XCTest
@testable import Watermelon

@MainActor
final class HomeRemoteReconciliationTests: XCTestCase {
    private final class ApplicationState {
        var value: UIApplication.State = .background
    }

    @MainActor
    private final class Fixture {
        let directory: URL
        let dependencies: DependencyContainer
        let controller: HomeConnectionController
        let profileID: Int64

        init(applicationState: @escaping @MainActor @Sendable () -> UIApplication.State = { .active },
             reload: @escaping @MainActor @Sendable (ServerProfileRecord, String) async throws -> Void) throws {
            directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
            dependencies = DependencyContainer(databaseManager: database, reconcileOneDriveAccounts: false)
            var profile = ServerProfileRecord(id: nil, name: "test", storageType: StorageType.webdav.rawValue,
                connectionParams: nil, sortOrder: 0, host: "test", port: 443, shareName: "", basePath: "/photos",
                username: "u", domain: nil, credentialRef: "test", backgroundBackupEnabled: false,
                createdAt: Date(), updatedAt: Date())
            try database.saveServerProfile(&profile)
            profileID = try XCTUnwrap(profile.id)
            dependencies.appSession.activate(profile: profile, password: "secret")
            controller = HomeConnectionController(dependencies: dependencies, applicationState: applicationState,
                reloadRemoteIndex: reload)
        }

        func marker(_ seconds: TimeInterval) throws {
            try dependencies.databaseManager.setBackgroundBackupLastRanAt(Date(timeIntervalSince1970: seconds),
                profileID: profileID)
        }

        func close() {
            controller.disconnect()
            try? dependencies.databaseManager.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }
    }

    private func waitUntil(_ condition: () -> Bool) async throws {
        let deadline = Date().addingTimeInterval(2)
        while !condition(), Date() < deadline { try await Task.sleep(for: .milliseconds(5)) }
        XCTAssertTrue(condition())
    }

    func testBackgroundMarkerWaitsForForegroundWithoutDependingOnHomeVisibility() async throws {
        let state = ApplicationState()
        var calls = 0
        let fixture = try Fixture(applicationState: { state.value }) { _, _ in calls += 1 }
        defer { fixture.close() }
        try fixture.marker(1)
        fixture.controller.refreshRemoteIndexIfNeeded()
        XCTAssertEqual(calls, 0)
        state.value = .active
        NotificationCenter.default.post(name: UIApplication.didBecomeActiveNotification, object: nil)
        try await waitUntil { calls == 1 }
        fixture.controller.refreshRemoteIndexIfNeeded()
        await Task.yield()
        XCTAssertEqual(calls, 1)
    }

    func testLateBackgroundMarkerRefreshesWithoutHomeForwarding() async throws {
        var calls = 0
        let fixture = try Fixture { _, _ in calls += 1 }
        defer { fixture.close() }
        NotificationCenter.default.post(name: UIApplication.didBecomeActiveNotification, object: nil)
        await Task.yield()
        XCTAssertEqual(calls, 0)
        try fixture.marker(1)
        NotificationCenter.default.post(name: .BackgroundBackupRunMarkerDidChange, object: nil)
        try await waitUntil { calls == 1 }
    }

    func testInFlightPresentationScanYieldsToBackgroundAndExecutionThenCatchesUp() async throws {
        let state = ApplicationState()
        state.value = .active
        let started = RestoreTestLatch()
        let cancelled = expectation(description: "background cancels presentation scan")
        let executionCancelled = expectation(description: "execution cancels presentation scan")
        var calls = 0
        let fixture = try Fixture(applicationState: { state.value }) { _, _ in
            calls += 1
            if calls <= 2 {
                let cancellation = calls == 1 ? cancelled : executionCancelled
                await started.open()
                do { try await Task.sleep(for: .seconds(60)) }
                catch { cancellation.fulfill(); throw error }
            }
        }
        defer { fixture.close() }
        try fixture.marker(1)
        fixture.controller.refreshRemoteIndexIfNeeded()
        await started.wait()
        state.value = .background
        NotificationCenter.default.post(name: UIApplication.willResignActiveNotification, object: nil)
        await fulfillment(of: [cancelled], timeout: 0.5)
        XCTAssertEqual(calls, 1)
        state.value = .active
        NotificationCenter.default.post(name: UIApplication.didBecomeActiveNotification, object: nil)
        try await waitUntil { calls == 2 }
        let claim = try XCTUnwrap(fixture.dependencies.appRuntimeFlags.tryEnterExecution())
        await fulfillment(of: [executionCancelled], timeout: 0.5)
        XCTAssertEqual(calls, 2)
        fixture.dependencies.appRuntimeFlags.exitExecution(claim)
        try await waitUntil { calls == 3 }
    }

    func testSuccessPicksUpChangeArrivingDuringReloadCleanup() async throws {
        let started = RestoreTestLatch()
        let finish = RestoreTestLatch()
        var calls = 0
        let fixture = try Fixture { _, _ in
            calls += 1
            if calls == 1 {
                await started.open()
                await finish.wait()
            }
        }
        defer { fixture.close() }
        try fixture.marker(1)
        fixture.controller.refreshRemoteIndexIfNeeded()
        await started.wait()
        try fixture.marker(2)
        fixture.controller.refreshRemoteIndexIfNeeded()
        XCTAssertEqual(calls, 1)
        await finish.open()
        try await waitUntil { calls == 2 }
        fixture.controller.refreshRemoteIndexIfNeeded()
        await Task.yield()
        XCTAssertEqual(calls, 2)
    }

    func testFailureKeepsMarkerPendingWithoutImmediateRetryLoop() async throws {
        var calls = 0
        let fixture = try Fixture { _, _ in
            calls += 1
            if calls == 1 { throw RemoteErrorFixtures.retryable }
        }
        defer { fixture.close() }
        try fixture.marker(1)
        fixture.controller.refreshRemoteIndexIfNeeded()
        try await waitUntil { calls == 1 }
        await Task.yield()
        XCTAssertEqual(calls, 1)
        NotificationCenter.default.post(name: UIApplication.didBecomeActiveNotification, object: nil)
        try await waitUntil { calls == 2 }
        fixture.controller.refreshRemoteIndexIfNeeded()
        await Task.yield()
        XCTAssertEqual(calls, 2)
    }

    func testProfileSwitchDropsLateCompletionAndUsesNewProfilesMarker() async throws {
        let started = RestoreTestLatch()
        let finish = RestoreTestLatch()
        let oldFinished = RestoreTestLatch()
        var profiles: [Int64?] = []
        let fixture = try Fixture { profile, _ in
            profiles.append(profile.id)
            if profiles.count == 1 {
                await started.open()
                await finish.wait()
                await oldFinished.open()
            }
        }
        defer { fixture.close() }
        try fixture.marker(100)
        fixture.controller.refreshRemoteIndexIfNeeded()
        await started.wait()
        var other = try XCTUnwrap(fixture.dependencies.appSession.activeProfile)
        other.id = nil
        other.host = "other"
        other.credentialRef = "other"
        try fixture.dependencies.databaseManager.saveServerProfile(&other)
        let otherID = try XCTUnwrap(other.id)
        try fixture.dependencies.databaseManager.setBackgroundBackupLastRanAt(Date(timeIntervalSince1970: 1), profileID: otherID)
        fixture.controller.refreshRemoteIndexIfNeeded()
        XCTAssertEqual(profiles, [fixture.profileID])

        fixture.controller.disconnect()
        fixture.dependencies.appSession.activate(profile: other, password: "secret")
        try await waitUntil { profiles.count == 2 }
        await finish.open()
        await oldFinished.wait()
        await Task.yield()
        fixture.controller.refreshRemoteIndexIfNeeded()
        await Task.yield()
        XCTAssertEqual(profiles, [fixture.profileID, otherID])
        XCTAssertEqual(fixture.dependencies.appSession.activeProfile?.id, otherID)
    }
}
