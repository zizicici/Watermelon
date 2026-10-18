import UIKit
import XCTest
import StoreKit
import StoreKitTest
@testable import MoreKit
@testable import Watermelon

@MainActor
final class MembershipPurchaseTests: XCTestCase {
    private var session: SKTestSession!
    private var savedLifetime = false
    private var savedSubscriptions: [SubscriptionMembership] = []

    override func setUpWithError() throws {
        guard ProcessInfo.processInfo.environment["WATERMELON_STOREKIT_TESTS"] == "1" else {
            throw XCTSkip("Use the Watermelon StoreKit scheme for the local product catalog.")
        }
        savedLifetime = Store.shared.proTier() == .lifetime
        savedSubscriptions = Store.shared.activeSubscriptions()
        session = try SKTestSession(contentsOf: XCTUnwrap(Bundle(for: Self.self).url(forResource: "Memberships", withExtension: "storekit")))
        session.resetToDefaultState()
        session.clearTransactions()
        session.disableDialogs = true
        session.askToBuyEnabled = false
        Store.shared.applyMembership(false)
        clearSubscriptions()
    }

    override func tearDownWithError() throws {
        guard session != nil else { return }
        session.clearTransactions()
        session.resetToDefaultState()
        clearSubscriptions()
        Store.shared.applyMembership(savedLifetime)
        Store.shared.applySubscriptionOutcomes(Dictionary(uniqueKeysWithValues: savedSubscriptions.map { ($0.productID, .active($0)) }))
    }

    func testMonthlyAndYearlyPurchaseAndRestore() async throws {
        try await loadProducts()
        XCTAssertEqual(Set(Store.shared.memberships.map(\.id)), Set(ProStatus.displayProductIDs))
        for id in ProStatus.subscriptionProductIDs {
            guard case .success = try await Store.shared.purchase(productID: id) else { return XCTFail("Purchase failed: \(id)") }
            XCTAssertTrue(ProStatus.isPro)
            XCTAssertEqual(Store.shared.proTier(), .subscription)
            let verified = await ProStatus.verifyEntitlement()
            XCTAssertTrue(verified)
            let transaction = try XCTUnwrap(session.allTransactions().first { $0.productIdentifier == id })
            try session.disableAutoRenewForTransaction(identifier: transaction.identifier)
            for _ in 0..<50 {
                await Store.shared.updateCustomerProductStatus()
                if Store.shared.activeSubscriptions().first(where: { $0.productID == id })?.willAutoRenew == false { break }
                try await Task.sleep(for: .milliseconds(50))
            }
            XCTAssertTrue(ProStatus.isPro)
            XCTAssertEqual(Store.shared.activeSubscriptions().first(where: { $0.productID == id })?.willAutoRenew, false)
            clearSubscriptions()
            let restored = try await Store.shared.syncMembershipStatus()
            XCTAssertTrue(restored)
            XCTAssertTrue(ProStatus.isPro)
            session.clearTransactions()
            clearSubscriptions()
        }
    }

    func testExpiryRemovesProAndDoesNotLatchLifetime() async throws {
        session.timeRate = .oneRenewalEveryTwoSeconds
        try await loadProducts()
        _ = try await Store.shared.purchase(productID: ProStatus.monthlyProductID)
        let transaction = try XCTUnwrap(session.allTransactions().first)
        try session.disableAutoRenewForTransaction(identifier: transaction.identifier)
        for _ in 0..<80 {
            await Store.shared.updateCustomerProductStatus()
            if !ProStatus.isPro { break }
            try await Task.sleep(for: .milliseconds(100))
        }
        XCTAssertFalse(ProStatus.isPro)
        XCTAssertFalse(UserDefaults.standard.bool(forKey: ProStatus.membershipKey))
    }

    func testLifetimeUpgradeSurvivesSubscriptionRefund() async throws {
        try await loadProducts()
        _ = try await Store.shared.purchase(productID: ProStatus.monthlyProductID)
        let transaction = try XCTUnwrap(session.allTransactions().first)
        _ = try await Store.shared.purchaseLifetimeMembership()
        try session.refundTransaction(identifier: transaction.identifier)
        await Store.shared.updateCustomerProductStatus()
        XCTAssertTrue(ProStatus.isPro)
        XCTAssertEqual(Store.shared.proTier(), .lifetime)
        guard case .alreadyOwned = try await Store.shared.purchase(productID: ProStatus.yearlyProductID) else {
            return XCTFail("Lifetime member was offered another purchase")
        }
    }

    func testPendingPurchaseDoesNotUnlockPro() async throws {
        try await loadProducts()
        session.askToBuyEnabled = true
        guard case .pending = try await Store.shared.purchase(productID: ProStatus.monthlyProductID) else {
            return XCTFail("Expected pending purchase")
        }
        XCTAssertFalse(ProStatus.isPro)
        let transaction = try XCTUnwrap(session.allTransactions().first)
        try session.approveAskToBuyTransaction(identifier: transaction.identifier)
        for _ in 0..<50 {
            await Store.shared.updateCustomerProductStatus()
            if ProStatus.isPro { break }
            try await Task.sleep(for: .milliseconds(50))
        }
        XCTAssertTrue(ProStatus.isPro)
    }

    func testExistingLifetimeRestoresAndIsNeverOfferedAnotherPurchase() async throws {
        try await loadProducts()
        let transaction = try await session.buyProduct(identifier: ProStatus.productID, options: [
            .purchaseDate(Date().addingTimeInterval(-90 * 24 * 60 * 60))
        ])
        XCTAssertEqual(transaction.productType, .nonConsumable)
        Store.shared.applyMembership(false)
        let restored = try await Store.shared.syncMembershipStatus()
        XCTAssertTrue(restored)
        XCTAssertEqual(Store.shared.proTier(), .lifetime)
        XCTAssertTrue(ProStatus.isPro)
        XCTAssertTrue(UserDefaults.standard.bool(forKey: ProStatus.membershipKey))
        XCTAssertTrue(InboxTransferAccessPolicy(isPro: ProStatus.isPro).allows(itemCount: 1000))
        XCTAssertEqual(BrowserLinkRateLimitSetting.unlimited.effectiveValue(isPro: ProStatus.isPro), .unlimited)
        XCTAssertEqual(BrowserLinkRateLimitSetting.standard.effectiveValue(isPro: ProStatus.isPro), .standard)

        let transactionCount = session.allTransactions().count
        for id in ProStatus.displayProductIDs {
            guard case .alreadyOwned = try await Store.shared.purchase(productID: id) else {
                return XCTFail("Existing lifetime owner was offered another purchase: \(id)")
            }
        }
        guard case .alreadyOwned = try await Store.shared.purchaseLifetimeMembership() else {
            return XCTFail("Legacy purchase entry must also recognize existing ownership")
        }
        let page = WatermelonPaywallViewController(configuration: .init(
            title: WatermelonProBenefits.title, features: WatermelonProBenefits.features,
            productIDs: ProStatus.displayProductIDs,
            privacyPolicyURL: URL(string: "https://watermelonbackup.com/privacy.html")!
        ))
        let scene = try XCTUnwrap(UIApplication.shared.connectedScenes.compactMap { $0 as? UIWindowScene }.first)
        let window = UIWindow(windowScene: scene)
        window.rootViewController = page
        window.makeKeyAndVisible()
        defer { window.isHidden = true; window.rootViewController = nil }
        page.view.layoutIfNeeded()
        try await Task.sleep(for: .milliseconds(100))
        page.view.layoutIfNeeded()
        XCTAssertNil(find("paywall.plans", in: page.view))
        for id in ProStatus.displayProductIDs { XCTAssertNil(find("paywall.product." + id, in: page.view)) }
        let button = try XCTUnwrap(find("paywall.purchase", in: page.view) as? UIButton)
        XCTAssertEqual(button.configuration?.title, String(localized: "common.done"))
        XCTAssertEqual(session.allTransactions().count, transactionCount)
    }

    func testExistingLifetimeSurvivesMissingEntitlementsAndFailedRestore() async throws {
        try await loadProducts()
        Store.shared.applyMembership(true)
        XCTAssertEqual(Store.shared.proTier(), .lifetime)
        let previousScan = Store.shared.scanOverrideForTesting
        let previousSubscriptionScan = Store.shared.subscriptionScanOverrideForTesting
        defer {
            Store.shared.scanOverrideForTesting = previousScan
            Store.shared.subscriptionScanOverrideForTesting = previousSubscriptionScan
            session.resetToDefaultState()
        }
        Store.shared.scanOverrideForTesting = { .missing }
        Store.shared.subscriptionScanOverrideForTesting = {
            Dictionary(uniqueKeysWithValues: ProStatus.subscriptionProductIDs.map { ($0, .inactive) })
        }
        Store.shared.applySubscriptionOutcomes([ProStatus.monthlyProductID: .active(.init(
            productID: ProStatus.monthlyProductID, expirationDate: .distantPast
        ))])
        let verified = await ProStatus.verifyEntitlement()
        XCTAssertTrue(verified)
        try await session.setSimulatedError(.generic(.networkError(URLError(.notConnectedToInternet))), forAPI: .appStoreSync)
        let restored = try await Store.shared.syncMembershipStatus()
        XCTAssertTrue(restored)
        XCTAssertEqual(Store.shared.proTier(), .lifetime)
        XCTAssertTrue(ProStatus.isPro)
        XCTAssertTrue(UserDefaults.standard.bool(forKey: ProStatus.membershipKey))
        XCTAssertTrue(Store.shared.activeSubscriptions().isEmpty)
    }

    func testPaywallDisplaysAllPlansAndStorePrices() async throws {
        try await loadProducts()
        let page = WatermelonPaywallViewController(configuration: .init(
            title: WatermelonProBenefits.title, features: WatermelonProBenefits.features,
            productIDs: ProStatus.displayProductIDs,
            privacyPolicyURL: URL(string: "https://watermelonbackup.com/privacy.html")!
        ))
        let scene = try XCTUnwrap(UIApplication.shared.connectedScenes.compactMap { $0 as? UIWindowScene }.first)
        let window = UIWindow(windowScene: scene)
        window.rootViewController = page
        window.makeKeyAndVisible()
        defer { window.isHidden = true; window.rootViewController = nil }
        page.loadViewIfNeeded()
        for _ in 0..<100 where Store.shared.isLoadingProducts {
            try await Task.sleep(for: .milliseconds(50))
        }
        await Task.yield()
        for _ in 0..<50 {
            if (find("paywall.purchase", in: page.view) as? UIButton)?.configuration?.showsActivityIndicator == false { break }
            try await Task.sleep(for: .milliseconds(50))
        }
        for id in ProStatus.displayProductIDs {
            XCTAssertNotNil(find("paywall.product." + id, in: page.view))
            let product = try XCTUnwrap(Store.shared.memberships.first { $0.id == id })
            XCTAssertTrue(find("paywall.product." + id, in: page.view)?.accessibilityLabel?.contains(product.displayPrice) == true)
            XCTAssertNil(product.subscription?.introductoryOffer)
        }
        XCTAssertNotNil((find("paywall.appIcon", in: page.view) as? UIImageView)?.image)
        let crown = try XCTUnwrap(find("paywall.crown", in: page.view) as? UIImageView)
        XCTAssertNotNil(crown.image)
        XCTAssertEqual(atan2(crown.transform.b, crown.transform.a), 25 * .pi / 180, accuracy: 0.001)
        page.view.layoutIfNeeded()
        let sparkles = try (0..<3).map { try XCTUnwrap(find("paywall.sparkle.\($0)", in: page.view)) }
        if #available(iOS 26.0, *) {
            for sparkle in sparkles {
                let shape = try XCTUnwrap(sparkle.layer as? CAShapeLayer)
                XCTAssertEqual(shape.preferredDynamicRange, .high)
                XCTAssertGreaterThan(UIColor(cgColor: try XCTUnwrap(shape.fillColor)).linearExposure, 1)
            }
        }
        if !UIAccessibility.isReduceMotionEnabled, UIView.areAnimationsEnabled {
            try await Task.sleep(for: .milliseconds(200))
            sparkles.forEach { $0.superview?.layoutIfNeeded() }
            let positions = sparkles.map(\.center)
            var moved = Set<Int>()
            for _ in 0..<50 {
                try await Task.sleep(for: .milliseconds(160))
                for (index, sparkle) in sparkles.enumerated() where sparkle.center != positions[index] {
                    moved.insert(index)
                }
                if moved.count == sparkles.count { break }
            }
            XCTAssertEqual(moved.count, sparkles.count)
        }
        for feature in page.configuration.features {
            let info = try XCTUnwrap(find("paywall.info." + feature.id, in: page.view) as? UIButton)
            let infoScroll = try XCTUnwrap(find("paywall.scroll", in: page.view) as? UIScrollView)
            infoScroll.scrollRectToVisible(info.convert(info.bounds, to: infoScroll), animated: false)
            page.view.layoutIfNeeded()
            info.sendActions(for: .touchUpInside)
            let popover = try XCTUnwrap(page.presentedViewController as? WatermelonFeatureInfoViewController)
            XCTAssertEqual(popover.title, feature.title)
            XCTAssertEqual(popover.message, feature.explanation)
            XCTAssertEqual(popover.modalPresentationStyle, .popover)
            XCTAssertTrue(popover.popoverPresentationController?.sourceView === info)
            XCTAssertGreaterThanOrEqual(info.bounds.height, 44)
            for _ in 0..<30 where popover.isBeingPresented {
                try await Task.sleep(for: .milliseconds(50))
            }
            if feature.id == "backgroundBackup" {
                try await Task.sleep(for: .milliseconds(150))
                let preview = UIGraphicsImageRenderer(bounds: window.bounds).image { _ in
                    window.drawHierarchy(in: window.bounds, afterScreenUpdates: true)
                }
                try preview.pngData()?.write(to: URL(fileURLWithPath: "/tmp/watermelon-feature-popover.png"))
            }
            await withCheckedContinuation { continuation in
                popover.dismiss(animated: false) { continuation.resume() }
            }
        }
        page.view.layoutIfNeeded()
        let planRow = try XCTUnwrap(find("paywall.plans", in: page.view) as? UIStackView)
        XCTAssertEqual(planRow.arrangedSubviews.count, 3)
        let first = try XCTUnwrap(planRow.arrangedSubviews.first)
        let firstLabels = first.subviews.compactMap { $0 as? UILabel }
        for card in planRow.arrangedSubviews {
            XCTAssertEqual(card.frame.minY, first.frame.minY, accuracy: 0.5)
            XCTAssertEqual(card.frame.width, first.frame.width, accuracy: 0.5)
            XCTAssertEqual(card.frame.height, first.frame.height, accuracy: 0.5)
            let labels = card.subviews.compactMap { $0 as? UILabel }
            XCTAssertEqual(labels.count, 3)
            for (label, firstLabel) in zip(labels, firstLabels) {
                XCTAssertEqual(label.frame.minY, firstLabel.frame.minY, accuracy: 0.5)
                XCTAssertEqual(label.font.pointSize, firstLabel.font.pointSize)
            }
        }
        let lifetime = try XCTUnwrap(find("paywall.product." + ProStatus.productID, in: page.view) as? UIControl)
        XCTAssertTrue(try XCTUnwrap(Store.shared.memberships.first { $0.id == ProStatus.productID }).isFamilyShareable)
        XCTAssertTrue(lifetime.accessibilityLabel?.contains(String(localized: "paywall.oneTimeFamilyShort")) == true)
        let scroll = try XCTUnwrap(find("paywall.scroll", in: page.view) as? UIScrollView)
        scroll.setContentOffset(CGPoint(x: 0, y: max(0, scroll.contentSize.height - scroll.bounds.height + scroll.adjustedContentInset.bottom)), animated: false)
        page.view.layoutIfNeeded()
        XCTAssertFalse(scroll.automaticallyAdjustsScrollIndicatorInsets)
        let panel = try XCTUnwrap(find("paywall.dock", in: page.view))
        let indicatorBottom = scroll.convert(CGPoint(x: 0, y: scroll.bounds.maxY - scroll.verticalScrollIndicatorInsets.bottom), to: page.view).y
        XCTAssertEqual(indicatorBottom, panel.convert(panel.bounds, to: page.view).minY - 8, accuracy: 0.5)
        let offset = scroll.contentOffset.y
        let position = planRow.convert(planRow.bounds, to: page.view).minY
        lifetime.sendActions(for: .touchUpInside)
        page.view.layoutIfNeeded()
        XCTAssertTrue(find("paywall.plans", in: page.view) === planRow)
        XCTAssertEqual(scroll.contentOffset.y, offset, accuracy: 0.5)
        XCTAssertEqual(planRow.convert(planRow.bounds, to: page.view).minY, position, accuracy: 0.5)
        XCTAssertEqual((find("paywall.purchase", in: page.view) as? UIButton)?.configuration?.title, String(localized: "paywall.unlockLifetime"))
        XCTAssertTrue(find("paywall.product." + ProStatus.productID, in: page.view)?.accessibilityTraits.contains(.selected) == true)
        try await capture(page, name: "watermelon-paywall-lifetime")
        let yearly = try XCTUnwrap(Store.shared.memberships.first { $0.id == ProStatus.yearlyProductID })
        let card = try XCTUnwrap(find("paywall.product." + yearly.id, in: page.view) as? UIControl)
        XCTAssertTrue(card.accessibilityLabel?.contains(yearly.displayPrice) == true)
        card.sendActions(for: .touchUpInside)
        XCTAssertTrue(find("paywall.product." + yearly.id, in: page.view)?.accessibilityTraits.contains(.selected) == true)
        XCTAssertNotNil(find("paywall.restore", in: page.view))
        let footer = try XCTUnwrap(find("paywall.links", in: page.view) as? UIStackView)
        XCTAssertEqual(footer.arrangedSubviews.compactMap { ($0 as? UIButton)?.title(for: .normal) }, [
            String(localized: "paywall.privacy"), String(localized: "store.restore"), String(localized: "paywall.terms")
        ])
        page.view.layoutIfNeeded()
        for button in footer.arrangedSubviews.compactMap({ $0 as? UIButton }) {
            XCTAssertEqual(button.frame.width, footer.bounds.width / 3, accuracy: 0.5)
            XCTAssertEqual(button.contentHorizontalAlignment, .center)
        }
        for feature in page.configuration.features {
            XCTAssertFalse(feature.freeValue.hasPrefix("paywall."))
            XCTAssertFalse(feature.proValue.hasPrefix("paywall."))
        }
        for style in [UIUserInterfaceStyle.light, .dark] {
            window.overrideUserInterfaceStyle = style
            page.view.layoutIfNeeded()
            let scroll = try XCTUnwrap(find("paywall.scroll", in: page.view) as? UIScrollView)
            scroll.setContentOffset(.zero, animated: false)
            try await capture(page, name: "watermelon-paywall-\(style.rawValue)-top")
            scroll.setContentOffset(CGPoint(x: 0, y: max(0, scroll.contentSize.height - scroll.bounds.height + scroll.adjustedContentInset.bottom)), animated: false)
            scroll.flashScrollIndicators()
            try await capture(page, name: "watermelon-paywall-\(style.rawValue)-plans")
        }
        let purchase = try XCTUnwrap(find("paywall.purchase", in: page.view) as? UIButton)
        XCTAssertTrue(purchase.isEnabled)
        let activated = expectation(description: "Paywall activates Pro after a verified purchase")
        page.onMembershipActivated = { activated.fulfill() }
        purchase.sendActions(for: .touchUpInside)
        await fulfillment(of: [activated], timeout: 10)
        XCTAssertTrue(ProStatus.isPro)
    }

    func testPaywallKeepsPopoverAnchorDuringRefreshAndSupportsResizing() async throws {
        try await loadProducts()
        let page = WatermelonPaywallViewController(configuration: .init(
            title: WatermelonProBenefits.title, features: WatermelonProBenefits.features,
            productIDs: ProStatus.displayProductIDs,
            privacyPolicyURL: URL(string: "https://watermelonbackup.com/privacy.html")!
        ))
        let scene = try XCTUnwrap(UIApplication.shared.connectedScenes.compactMap { $0 as? UIWindowScene }.first)
        let window = UIWindow(windowScene: scene)
        let host = UIViewController()
        window.rootViewController = host
        window.makeKeyAndVisible()
        defer { window.isHidden = true; window.rootViewController = nil }
        host.addChild(page)
        host.view.addSubview(page.view)
        page.view.frame = host.view.bounds
        page.didMove(toParent: host)
        page.view.layoutIfNeeded()
        let comparison = try XCTUnwrap(find("paywall.comparison", in: page.view))
        let source = try XCTUnwrap(find("paywall.info.pip", in: page.view) as? UIButton)
        let scroll = try XCTUnwrap(find("paywall.scroll", in: page.view) as? UIScrollView)
        scroll.scrollRectToVisible(source.convert(source.bounds, to: scroll), animated: false)
        source.sendActions(for: .touchUpInside)
        let popover = try XCTUnwrap(page.presentedViewController as? WatermelonFeatureInfoViewController)
        for _ in 0..<30 where popover.isBeingPresented {
            try await Task.sleep(for: .milliseconds(50))
        }
        for style in [UIUserInterfaceStyle.dark, .light] {
            window.overrideUserInterfaceStyle = style
            for name in [Notification.Name.StoreInfoLoaded, .StoreProductsLoaded, .StoreInfoLoaded] {
                NotificationCenter.default.post(name: name, object: nil)
                try await Task.sleep(for: .milliseconds(30))
                page.view.layoutIfNeeded()
                XCTAssertTrue(find("paywall.comparison", in: page.view) === comparison)
                XCTAssertTrue(find("paywall.info.pip", in: page.view) === source)
                XCTAssertTrue(source.window === window)
                XCTAssertTrue(popover.popoverPresentationController?.sourceView === source)
                XCTAssertTrue(page.presentedViewController === popover)
            }
        }
        await withCheckedContinuation { continuation in
            popover.dismiss(animated: false) { continuation.resume() }
        }
        for size in [CGSize(width: 320, height: 568), CGSize(width: 568, height: 320),
                     CGSize(width: 1024, height: 768), CGSize(width: 768, height: 1024), CGSize(width: 320, height: 568)] {
            page.view.frame = CGRect(origin: .zero, size: size)
            page.view.setNeedsLayout()
            page.view.layoutIfNeeded()
            try await Task.sleep(for: .milliseconds(30))
            page.view.layoutIfNeeded()
            XCTAssertEqual(scroll.bounds.width, size.width, accuracy: 0.5)
            XCTAssertTrue(scroll.contentSize.height.isFinite)
            XCTAssertGreaterThan(scroll.contentSize.height, 0)
            for identifier in ["paywall.scroll", "paywall.comparison", "paywall.plans", "paywall.dock", "paywall.links"] {
                let view = try XCTUnwrap(find(identifier, in: page.view))
                XCTAssertFalse(view.hasAmbiguousLayout, identifier)
                XCTAssertTrue(view.bounds.width.isFinite && view.bounds.height.isFinite, identifier)
                XCTAssertGreaterThan(view.bounds.width, 0, identifier)
                XCTAssertGreaterThan(view.bounds.height, 0, identifier)
            }
        }
    }

    private func capture(_ page: UIViewController, name: String) async throws {
        try await Task.sleep(for: .milliseconds(150))
        page.view.layoutIfNeeded()
        let image = UIGraphicsImageRenderer(bounds: page.view.bounds).image { _ in
            page.view.drawHierarchy(in: page.view.bounds, afterScreenUpdates: true)
        }
        let attachment = XCTAttachment(image: image)
        attachment.name = name
        attachment.lifetime = .keepAlways
        add(attachment)
        try image.pngData()?.write(to: URL(fileURLWithPath: "/tmp/\(name).png"))
    }

    private func loadProducts() async throws {
        for _ in 0..<3 {
            for _ in 0..<100 where Store.shared.isLoadingProducts {
                try await Task.sleep(for: .milliseconds(50))
            }
            await Store.shared.requestProducts()
            if Store.shared.memberships.count == ProStatus.displayProductIDs.count { return }
            try await Task.sleep(for: .milliseconds(100))
        }
        XCTFail("Local StoreKit products unavailable: \(String(describing: Store.shared.productsError))")
    }

    private func clearSubscriptions() {
        Store.shared.applySubscriptionOutcomes(Dictionary(uniqueKeysWithValues: ProStatus.subscriptionProductIDs.map { ($0, .inactive) }))
    }

    private func find(_ identifier: String, in view: UIView) -> UIView? {
        if view.accessibilityIdentifier == identifier { return view }
        return view.subviews.lazy.compactMap { self.find(identifier, in: $0) }.first
    }
}
