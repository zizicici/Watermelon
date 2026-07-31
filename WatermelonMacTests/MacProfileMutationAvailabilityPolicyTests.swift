import XCTest
@testable import WatermelonMac

final class MacProfileMutationAvailabilityPolicyTests:
    XCTestCase
{
    func testMutationRequiresEveryActivityGateToBeClear() {
        XCTAssertTrue(
            MacProfileMutationAvailabilityPolicy.canMutate(
                executionActive: false,
                maintenanceActive: false,
                connectionActive: false
            )
        )
        for blocked in [
            (true, false, false),
            (false, true, false),
            (false, false, true),
        ] {
            XCTAssertFalse(
                MacProfileMutationAvailabilityPolicy.canMutate(
                    executionActive: blocked.0,
                    maintenanceActive: blocked.1,
                    connectionActive: blocked.2
                )
            )
        }
    }

    @MainActor
    func testActivityObserverTracksEveryMutationGate() {
        let notificationCenter = NotificationCenter()
        var changeCount = 0
        var observer: MacProfileMutationActivityObserver? =
            MacProfileMutationActivityObserver(
                notificationCenter: notificationCenter
            ) {
                changeCount += 1
            }
        XCTAssertNotNil(observer)

        for name in [
            Notification.Name.ExecutionLifecycleDidChange,
            .RemoteMaintenanceDidChange,
            .ConnectionLifecycleDidChange
        ] {
            notificationCenter.post(name: name, object: nil)
        }
        XCTAssertEqual(changeCount, 3)

        observer = nil
        notificationCenter.post(
            name: .ExecutionLifecycleDidChange,
            object: nil
        )
        XCTAssertEqual(changeCount, 3)
    }

    func testAsyncLeaseCoversVerificationAndNestedSave() async {
        AppRuntimeFlags._testReset()
        defer { AppRuntimeFlags._testReset() }
        let flags = AppRuntimeFlags()
        let competingExecution = AppRuntimeFlags()

        let saved = await flags.withAsyncProfileMutationLease(
            profileID: 7
        ) {
            XCTAssertNil(competingExecution.tryEnterExecution())
            await Task.yield()
            let nested = flags.withProfileMutationLease(
                profileID: 7
            ) {
                true
            }
            XCTAssertEqual(nested, true)
            return nested == true
        }

        XCTAssertEqual(saved, true)
        let claim = try! XCTUnwrap(
            competingExecution.tryEnterExecution()
        )
        competingExecution.exitExecution(claim)
    }
}
