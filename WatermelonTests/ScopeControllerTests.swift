import XCTest
@testable import Watermelon

@MainActor
final class ScopeControllerTests: XCTestCase {
    func testSetActive_userActionWhenIdle_appliesAndMarksReloading() {
        let controller = HomeScopeController()
        let target = HomeLocalLibraryScope.albums(["albumA"])

        let result = controller.setActive(target, isExecuting: false)
        XCTAssertEqual(result, .applied)
        XCTAssertEqual(controller.activeScope, target)
        XCTAssertTrue(controller.isReloading)
    }

    func testSetActive_reloadingToReloading_coalesces() {
        // A second user-driven setActive arriving before the first reload completes
        // must flip activeScope and keep the reload gate up — so the surrounding
        // refresh loop will run a fresh reload after the first one drains.
        let controller = HomeScopeController()
        _ = controller.setActive(.albums(["a"]), isExecuting: false)
        XCTAssertTrue(controller.isReloading)

        let result = controller.setActive(.albums(["b"]), isExecuting: false)
        XCTAssertEqual(result, .applied)
        XCTAssertEqual(controller.activeScope, .albums(["b"]))
        XCTAssertTrue(controller.isReloading, "gate stays up across overlapping setActive calls")

        // After the first reload lands `hasMoreReloadPending=true` keeps the gate;
        // the surrounding loop reloads against the new active and clears at the end.
        controller.completeReload(loaded: .albums(["a"]), hasMoreReloadPending: true)
        XCTAssertTrue(controller.isReloading)
        controller.completeReload(loaded: .albums(["b"]), hasMoreReloadPending: false)
        XCTAssertFalse(controller.isReloading)
    }

    func testSetActive_sameScope_noChange() {
        let controller = HomeScopeController()
        _ = controller.setActive(.albums(["albumA"]), isExecuting: false)
        controller.completeReload(loaded: .albums(["albumA"]), hasMoreReloadPending: false)

        let result = controller.setActive(.albums(["albumA"]), isExecuting: false)
        XCTAssertEqual(result, .noChange)
        XCTAssertFalse(controller.isReloading)
    }

    func testSetActive_duringExecution_deferred() {
        let controller = HomeScopeController()
        let target = HomeLocalLibraryScope.albums(["albumA"])

        let result = controller.setActive(target, isExecuting: true)
        XCTAssertEqual(result, .deferred)
        XCTAssertEqual(controller.activeScope, .allPhotos, "active should NOT change while executing")
        XCTAssertEqual(controller.pendingScope, target)
    }

    func testCompleteReload_clearsReloading_whenNoMorePending() {
        let controller = HomeScopeController()
        _ = controller.setActive(.albums(["albumA"]), isExecuting: false)
        XCTAssertTrue(controller.isReloading)

        controller.completeReload(loaded: .albums(["albumA"]), hasMoreReloadPending: false)
        XCTAssertFalse(controller.isReloading)
        XCTAssertEqual(controller.loadedScope, .albums(["albumA"]))
    }

    func testCompleteReload_keepsReloading_whenMorePending() {
        let controller = HomeScopeController()
        _ = controller.setActive(.albums(["albumA"]), isExecuting: false)
        controller.completeReload(loaded: .albums(["albumA"]), hasMoreReloadPending: true)
        XCTAssertTrue(controller.isReloading, "another reload is queued — gate stays up")
    }

    func testResumeFromDeferred_appliesPending_marksReloading() {
        let controller = HomeScopeController()
        let pending = HomeLocalLibraryScope.albums(["albumA"])
        _ = controller.setActive(pending, isExecuting: true)
        XCTAssertEqual(controller.activeScope, .allPhotos)

        let resumed = controller.resumeFromDeferred()
        XCTAssertEqual(resumed, pending)
        XCTAssertEqual(controller.activeScope, pending)
        XCTAssertTrue(controller.isReloading)
        XCTAssertNil(controller.pendingScope)
    }

    func testResumeFromDeferred_pendingMatchesActive_doesNotMarkReloading() {
        // requestPostExecutionRenormalization stashes the current active scope as pending.
        // Resuming such a stash should NOT trigger a fresh reload (active didn't change).
        let controller = HomeScopeController()
        _ = controller.setActive(.albums(["x"]), isExecuting: false)
        controller.completeReload(loaded: .albums(["x"]), hasMoreReloadPending: false)
        controller.requestPostExecutionRenormalization()
        XCTAssertEqual(controller.pendingScope, .albums(["x"]))

        let resumed = controller.resumeFromDeferred()
        XCTAssertEqual(resumed, .albums(["x"]))
        XCTAssertFalse(controller.isReloading)
    }

    func testSetActiveFromNormalize_silentNoOp_whenIdentitySame() {
        let controller = HomeScopeController()
        _ = controller.setActive(.albums(["a"]), isExecuting: false)
        controller.completeReload(loaded: .albums(["a"]), hasMoreReloadPending: false)

        let changed = controller.setActiveFromNormalize(.albums(["a"]))
        XCTAssertFalse(changed)
        XCTAssertFalse(controller.isReloading, "normalize must not flip the reload gate")
    }

    func testSetActiveFromNormalize_appliesNewScope_doesNotMarkReloading() {
        let controller = HomeScopeController()
        _ = controller.setActive(.albums(["a", "b"]), isExecuting: false)
        controller.completeReload(loaded: .albums(["a", "b"]), hasMoreReloadPending: false)

        let changed = controller.setActiveFromNormalize(.allPhotos)
        XCTAssertTrue(changed)
        XCTAssertEqual(controller.activeScope, .allPhotos)
        XCTAssertFalse(controller.isReloading, "normalize must not flip the reload gate; surrounding flow drives it")
    }

    func testDeferActiveScopeForReevaluation_onlyForAlbumScope() {
        // .allPhotos doesn't need normalization downgrade; deferral is a no-op.
        let controller = HomeScopeController()
        controller.requestPostExecutionRenormalization()
        XCTAssertNil(controller.pendingScope)

        _ = controller.setActive(.albums(["a"]), isExecuting: false)
        controller.completeReload(loaded: .albums(["a"]), hasMoreReloadPending: false)
        controller.requestPostExecutionRenormalization()
        XCTAssertEqual(controller.pendingScope, .albums(["a"]))
    }

    func testDeferActiveScopeForReevaluation_idempotent() {
        let controller = HomeScopeController()
        _ = controller.setActive(.albums(["a"]), isExecuting: false)
        controller.completeReload(loaded: .albums(["a"]), hasMoreReloadPending: false)

        controller.requestPostExecutionRenormalization()
        let first = controller.pendingScope
        controller.requestPostExecutionRenormalization()
        XCTAssertEqual(controller.pendingScope, first, "second call shouldn't overwrite a different pending value")
    }

    func testOnChange_firesOnEveryTransition() {
        let controller = HomeScopeController()
        var fireCount = 0
        controller.onChange = { fireCount += 1 }

        _ = controller.setActive(.albums(["a"]), isExecuting: false)
        controller.completeReload(loaded: .albums(["a"]), hasMoreReloadPending: false)
        _ = controller.setActive(.allPhotos, isExecuting: false)
        controller.completeReload(loaded: .allPhotos, hasMoreReloadPending: false)

        XCTAssertEqual(fireCount, 4)
    }
}
