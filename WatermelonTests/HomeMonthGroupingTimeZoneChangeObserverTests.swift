import XCTest
@testable import Watermelon

@MainActor
final class HomeMonthGroupingTimeZoneChangeObserverTests: XCTestCase {
    private var savedMonthGroupingTimeZoneRaw: String?

    override func setUp() {
        super.setUp()
        savedMonthGroupingTimeZoneRaw = UserDefaults.standard.string(forKey: MonthGroupingTimeZonePreference.storageKey)
        UserDefaults.standard.removeObject(forKey: MonthGroupingTimeZonePreference.storageKey)
    }

    override func tearDown() {
        if let savedMonthGroupingTimeZoneRaw {
            UserDefaults.standard.set(savedMonthGroupingTimeZoneRaw, forKey: MonthGroupingTimeZonePreference.storageKey)
        } else {
            UserDefaults.standard.removeObject(forKey: MonthGroupingTimeZonePreference.storageKey)
        }
        savedMonthGroupingTimeZoneRaw = nil
        super.tearDown()
    }

    func testPreferenceChangeRequestsLocalIndexReload() async {
        let notificationCenter = NotificationCenter()
        var reloadCount = 0
        let reloadExpectation = expectation(description: "preference change requests reload")
        let observer = HomeMonthGroupingTimeZoneChangeObserver(notificationCenter: notificationCenter, hooks: .init(
            requestLocalIndexReload: {
                reloadCount += 1
                reloadExpectation.fulfill()
            }
        ))

        notificationCenter.post(name: .MonthGroupingTimeZonePreferenceDidChange, object: nil)
        await fulfillment(of: [reloadExpectation], timeout: 1)

        XCTAssertEqual(reloadCount, 1)
        _ = observer
    }

    func testSystemTimeZoneChangeOnlyRequestsReloadInSystemMode() async {
        let fixedModeNotificationCenter = NotificationCenter()
        let fixedModeReload = expectation(description: "fixed mode should ignore system time zone changes")
        fixedModeReload.isInverted = true
        let fixedModeObserver = HomeMonthGroupingTimeZoneChangeObserver(notificationCenter: fixedModeNotificationCenter, hooks: .init(
            requestLocalIndexReload: {
                fixedModeReload.fulfill()
            }
        ))

        MonthGroupingTimeZonePreference.setCurrent(.fixedUTC())
        fixedModeNotificationCenter.post(name: Notification.Name.NSSystemTimeZoneDidChange, object: nil)
        await fulfillment(of: [fixedModeReload], timeout: 0.1)
        _ = fixedModeObserver

        let systemModeNotificationCenter = NotificationCenter()
        var reloadCount = 0
        let systemModeReload = expectation(description: "system mode reloads on system time zone changes")
        let systemModeObserver = HomeMonthGroupingTimeZoneChangeObserver(notificationCenter: systemModeNotificationCenter, hooks: .init(
            requestLocalIndexReload: {
                reloadCount += 1
                systemModeReload.fulfill()
            }
        ))

        MonthGroupingTimeZonePreference.setCurrent(.defaultPreference)
        systemModeNotificationCenter.post(name: Notification.Name.NSSystemTimeZoneDidChange, object: nil)
        await fulfillment(of: [systemModeReload], timeout: 1)

        XCTAssertEqual(reloadCount, 1)
        _ = systemModeObserver
    }
}
