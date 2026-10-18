import UIKit
import XCTest
@testable import MoreKit
@testable import Watermelon

@MainActor
final class WatermelonProPresentationTests: XCTestCase {
    private var lifetime = false
    private var subscriptions: [SubscriptionMembership] = []
    private var scanOverride: (@MainActor () async -> EntitlementScanOutcome)?
    private var subscriptionScanOverride: (@MainActor () async -> [String: SubscriptionScanOutcome])?

    override func setUp() {
        super.setUp()
        lifetime = Store.shared.proTier() == .lifetime
        subscriptions = Store.shared.activeSubscriptions()
        scanOverride = Store.shared.scanOverrideForTesting
        subscriptionScanOverride = Store.shared.subscriptionScanOverrideForTesting
        Store.shared.scanOverrideForTesting = { .missing }
        Store.shared.subscriptionScanOverrideForTesting = { [:] }
        Store.shared.applyMembership(false)
        clearSubscriptions()
    }

    override func tearDown() {
        clearSubscriptions()
        Store.shared.applyMembership(lifetime)
        Store.shared.applySubscriptionOutcomes(Dictionary(uniqueKeysWithValues: subscriptions.map { ($0.productID, .active($0)) }))
        Store.shared.scanOverrideForTesting = scanOverride
        Store.shared.subscriptionScanOverrideForTesting = subscriptionScanOverride
        super.tearDown()
    }

    func testBothSubscriptionsUnlockExistingProGatesAndExpirationRemovesAccess() throws {
        for id in ProStatus.subscriptionProductIDs {
            Store.shared.applySubscriptionOutcomes([id: .active(.init(productID: id, expirationDate: .distantFuture))])
            XCTAssertTrue(ProStatus.isPro)
            XCTAssertTrue(InboxTransferAccessPolicy(isPro: ProStatus.isPro).allows(itemCount: 100))
            XCTAssertEqual(BrowserLinkRateLimitSetting.unlimited.effectiveValue(isPro: ProStatus.isPro), .unlimited)
            XCTAssertNotEqual(Store.shared.proTier(), .lifetime)
            clearSubscriptions()
        }
        Store.shared.applySubscriptionOutcomes([ProStatus.monthlyProductID: .active(.init(
            productID: ProStatus.monthlyProductID, expirationDate: .distantPast
        ))])
        XCTAssertFalse(ProStatus.isPro)
        XCTAssertEqual(BrowserLinkRateLimitSetting.unlimited.effectiveValue(isPro: ProStatus.isPro), .standard)
    }

    func testLifetimeSurvivesSubscriptionLoss() {
        Store.shared.applyMembership(true)
        clearSubscriptions()
        XCTAssertTrue(ProStatus.isPro)
        XCTAssertEqual(Store.shared.proTier(), .lifetime)
    }

    func testLegacyPaidCacheMigrationPreservesLifetimeAndExistingSettings() {
        XCTAssertEqual(ProStatus.productID, "com.zizicici.watermelon.pro")
        XCTAssertEqual(ProStatus.membershipKey, "com.zizicici.watermelon.membership.lifetime")
        let defaults = UserDefaults.standard
        let legacyKey = "com.zizicici.watermelon.pro.entitled"
        let settings = [
            (BrowserLinkRateLimitSetting.getKey(), BrowserLinkRateLimitSetting.unlimited.rawValue),
            (BackgroundBackupSetting.getKey(), BackgroundBackupSetting.enable.rawValue),
            (PiPProgressSetting.getKey(), PiPProgressSetting.enable.rawValue),
            (ShortcutsSetting.getKey(), ShortcutsSetting.enable.rawValue)
        ]
        let keys = [legacyKey, ProStatus.membershipKey] + settings.map(\.0)
        let saved = keys.map { ($0, defaults.object(forKey: $0)) }
        defer {
            for (key, value) in saved {
                if let value { defaults.set(value, forKey: key) }
                else { defaults.removeObject(forKey: key) }
            }
        }
        let cases: [(Bool?, Bool?)] = [(true, nil), (true, false), (true, true), (nil, true), (false, true), (false, false), (nil, nil)]
        for (legacy, current) in cases {
            for (key, value) in [(legacyKey, legacy), (ProStatus.membershipKey, current)] {
                if let value { defaults.set(value, forKey: key) }
                else { defaults.removeObject(forKey: key) }
            }
            for (key, value) in settings { defaults.set(value, forKey: key) }
            for _ in 0..<2 {
                ProStatus.migrateLegacyCacheIfNeeded()
                let ownsLifetime = legacy == true || current == true
                XCTAssertEqual(defaults.bool(forKey: ProStatus.membershipKey), ownsLifetime)
                XCTAssertEqual(MembershipCache(defaults: defaults, key: ProStatus.membershipKey).tier(), ownsLifetime ? .lifetime : .none)
                XCTAssertNil(defaults.object(forKey: legacyKey))
                for (key, value) in settings { XCTAssertEqual(defaults.integer(forKey: key), value, key) }
            }
        }
    }

    func testOnlyMembershipErrorsRouteToPaywall() {
        XCTAssertEqual(WatermelonProFeature.requiringUpgrade(for: BrowserLinkRateLimitSettingError.requiresPro), .computerSpeed)
        XCTAssertEqual(WatermelonProFeature.requiringUpgrade(for: BackgroundBackupSettingError.requiresPro), .backgroundBackup)
        XCTAssertEqual(WatermelonProFeature.requiringUpgrade(for: PiPProgressSettingError.requiresPro), .pip)
        XCTAssertEqual(WatermelonProFeature.requiringUpgrade(for: ShortcutsAccessError.trialEnded), .shortcuts)
        XCTAssertNil(WatermelonProFeature.requiringUpgrade(for: ShortcutsAccessError.disabled))
        XCTAssertNil(WatermelonProFeature.requiringUpgrade(for: URLError(.notConnectedToInternet)))
    }

    func testMoreProEntryOpensFullScreenPaywallWithContextAndAllPlans() async throws {
        let dataSource = WatermelonMoreDataSource(dependencies: nil, onProfilesChanged: nil)
        let controller = MoreViewController(configuration: .init(
            title: String(localized: "controller.more.title"), email: "watermelon@zi.ci", appStoreId: "6762260596",
            specificationsConfig: .init(summaryItems: [], thirdPartyLibraries: [])
        ), dataSource: dataSource)
        let scene = try XCTUnwrap(UIApplication.shared.connectedScenes.compactMap { $0 as? UIWindowScene }.first)
        let window = UIWindow(windowScene: scene)
        let navigation = AppNavigationController(rootViewController: controller)
        window.rootViewController = navigation
        window.makeKeyAndVisible()
        defer { window.isHidden = true; window.rootViewController = nil }
        controller.loadViewIfNeeded()
        controller.view.layoutIfNeeded()
        let table = try XCTUnwrap(controller.view.subviews.first { $0 is UITableView } as? UITableView)
        let entry = try XCTUnwrap(table.cellForRow(at: IndexPath(row: 0, section: 0)))
        XCTAssertTrue(entry is WatermelonProEntryCell)
        XCTAssertTrue(entry.accessibilityLabel?.contains(WatermelonProBenefits.title) == true)
        XCTAssertEqual(entry.accessoryType, .none)
        let preview = UIGraphicsImageRenderer(bounds: navigation.view.bounds).image { _ in
            navigation.view.drawHierarchy(in: navigation.view.bounds, afterScreenUpdates: true)
        }
        try preview.pngData()?.write(to: URL(fileURLWithPath: "/tmp/watermelon-more-entry.png"))
        for isPro in [false, true] {
            Store.shared.applyMembership(isPro)
            for (category, size) in [(UIContentSizeCategory.large, "normal"), (.extraExtraExtraLarge, "larger"), (.accessibilityExtraExtraExtraLarge, "largest")] {
                window.traitOverrides.preferredContentSizeCategory = category
                table.reloadData()
                let scaledEntry = try await visibleProEntry(in: table)
                XCTAssertEqual(scaledEntry.traitCollection.preferredContentSizeCategory, category)
                let button = try XCTUnwrap(find("settings.proEntry.open", in: scaledEntry) as? UIButton)
                XCTAssertEqual(button.configuration?.title, isPro ? String(localized: "pro.entry.viewBenefits") : String(localized: "paywall.choosePlan"))
                let row = try XCTUnwrap(scaledEntry.contentView.subviews.first as? UIStackView)
                let labels = try XCTUnwrap(row.arrangedSubviews.first)
                let labelRect = labels.convert(labels.bounds, to: scaledEntry.contentView)
                let buttonRect = button.convert(button.bounds, to: scaledEntry.contentView)
                XCTAssertFalse(labelRect.intersects(buttonRect))
                XCTAssertLessThanOrEqual(buttonRect.maxY, scaledEntry.contentView.bounds.height)
                XCTAssertLessThanOrEqual(buttonRect.maxX, scaledEntry.contentView.bounds.width)
                if category.isAccessibilityCategory {
                    XCTAssertGreaterThan(buttonRect.minY, labelRect.maxY)
                    XCTAssertGreaterThan(button.titleLabel?.font.pointSize ?? 0, 17)
                }
                let height = scaledEntry.bounds.height
                for _ in 0..<3 {
                    table.setContentOffset(CGPoint(x: 0, y: max(0, table.contentSize.height - table.bounds.height)), animated: false)
                    table.layoutIfNeeded()
                    try await Task.sleep(for: .milliseconds(50))
                    _ = try await visibleProEntry(in: table)
                    table.reloadData()
                    let refreshedEntry = try await visibleProEntry(in: table)
                    let refreshedButton = try XCTUnwrap(find("settings.proEntry.open", in: refreshedEntry) as? UIButton)
                    let refreshedRect = refreshedButton.convert(refreshedButton.bounds, to: refreshedEntry.contentView)
                    XCTAssertEqual(refreshedEntry.bounds.height, height, accuracy: 1)
                    XCTAssertEqual(refreshedRect.minY, buttonRect.minY, accuracy: 1)
                    XCTAssertEqual(refreshedRect.width, buttonRect.width, accuracy: 1)
                }
                let image = UIGraphicsImageRenderer(bounds: navigation.view.bounds).image { _ in
                    navigation.view.drawHierarchy(in: navigation.view.bounds, afterScreenUpdates: true)
                }
                let state = isPro ? "member" : "free"
                try image.pngData()?.write(to: URL(fileURLWithPath: "/tmp/watermelon-more-entry-\(state)-\(size).png"))
            }
        }
        Store.shared.applyMembership(false)
        window.traitOverrides.preferredContentSizeCategory = .large
        table.reloadData()
        let currentEntry = try await visibleProEntry(in: table)
        let open = try XCTUnwrap(find("settings.proEntry.open", in: currentEntry) as? UIButton)
        open.sendActions(for: .touchUpInside)
        try await Task.sleep(for: .milliseconds(400))
        let page = try XCTUnwrap(controller.presentedViewController as? WatermelonPaywallViewController)
        XCTAssertEqual(page.modalPresentationStyle, .fullScreen)
        XCTAssertEqual(page.configuration.resolvedProductIDs, ProStatus.displayProductIDs)
        XCTAssertEqual(page.configuration.features.count, WatermelonProBenefits.features.count)
        for _ in 0..<30 where page.isBeingPresented {
            try await Task.sleep(for: .milliseconds(50))
        }
        await withCheckedContinuation { continuation in
            page.dismiss(animated: false) { continuation.resume() }
        }
        XCTAssertTrue(controller.presentProUpgradeIfNeeded(for: PiPProgressSettingError.requiresPro))
        let contextual = try XCTUnwrap(controller.presentedViewController as? WatermelonPaywallViewController)
        XCTAssertEqual(contextual.configuration.highlightedFeature, .pip)
        contextual.dismiss(animated: false)
    }

    func testFeatureInfoScrollsLongTitlesAndBodyAtAccessibilitySizes() throws {
        let scene = try XCTUnwrap(UIApplication.shared.connectedScenes.compactMap { $0 as? UIWindowScene }.first)
        let window = UIWindow(windowScene: scene)
        window.traitOverrides.preferredContentSizeCategory = .accessibilityExtraExtraExtraLarge
        let host = UIViewController()
        window.rootViewController = host
        window.makeKeyAndVisible()
        defer { window.isHidden = true; window.rootViewController = nil }
        let feature = WatermelonProFeature.backgroundBackup
        let page = WatermelonFeatureInfoViewController(
            title: Array(repeating: feature.title, count: 8).joined(separator: " "),
            message: Array(repeating: feature.explanation, count: 6).joined(separator: "\n"), width: 320
        )
        host.addChild(page)
        host.view.addSubview(page.view)
        page.didMove(toParent: host)
        let scroll = try XCTUnwrap(page.view.subviews.first as? UIScrollView)
        let content = try XCTUnwrap(scroll.subviews.first { $0 is UIStackView } as? UIStackView)
        for size in [CGSize(width: 320, height: 420), CGSize(width: 280, height: 180), CGSize(width: 600, height: 240)] {
            page.view.frame = CGRect(origin: .zero, size: size)
            page.view.setNeedsLayout()
            page.view.layoutIfNeeded()
            XCTAssertFalse(scroll.hasAmbiguousLayout)
            XCTAssertFalse(content.hasAmbiguousLayout)
            XCTAssertEqual(scroll.bounds.height, size.height, accuracy: 0.5)
            XCTAssertGreaterThan(scroll.contentSize.height, scroll.bounds.height)
            for label in content.arrangedSubviews {
                XCTAssertGreaterThan(label.bounds.height, 0)
                XCTAssertGreaterThanOrEqual(label.frame.minY, 0)
                XCTAssertLessThanOrEqual(label.frame.maxY, content.bounds.height + 0.5)
            }
            scroll.setContentOffset(CGPoint(x: 0, y: scroll.contentSize.height - scroll.bounds.height), animated: false)
            XCTAssertGreaterThan(scroll.contentOffset.y, 0)
        }
    }

    func testTutorialsScrollSafelyAfterRapidReveals() async throws {
        let scene = try XCTUnwrap(UIApplication.shared.connectedScenes.compactMap { $0 as? UIWindowScene }.first)
        let window = UIWindow(windowScene: scene)
        window.traitOverrides.preferredContentSizeCategory = .accessibilityExtraExtraExtraLarge
        let host = UIViewController()
        window.rootViewController = host
        window.makeKeyAndVisible()
        defer { window.isHidden = true; window.rootViewController = nil }
        let tutorial = ProgressiveTutorialViewController(
            tutorialTitle: "Tutorial",
            items: (0..<20).map { .init(title: "Step \($0)", subtitle: String(repeating: "Backup photos and videos. ", count: 8), symbolName: "photo") },
            completionButtonTitle: "Done", allowsDismissal: true
        )
        let pages: [(UIViewController, Int)] = [(tutorial, 20), (OnboardingViewController(), 4)]
        for (page, count) in pages {
            host.addChild(page)
            host.view.addSubview(page.view)
            page.view.frame = CGRect(x: 0, y: 0, width: 320, height: 460)
            page.didMove(toParent: host)
            page.view.layoutIfNeeded()
            let table = try XCTUnwrap(page.view.subviews.first { $0 is UITableView } as? UITableView)
            let next = try XCTUnwrap(page.view.subviews.first { $0 is UIButton } as? UIButton)
            for _ in 1..<count { next.sendActions(for: .touchUpInside) }
            let last = IndexPath(row: 0, section: count - 1)
            for _ in 0..<100 {
                try await Task.sleep(for: .milliseconds(30))
                page.view.layoutIfNeeded()
                if table.numberOfSections == count, table.indexPathsForVisibleRows?.contains(last) == true,
                   table.rectForRow(at: last).maxY <= table.bounds.maxY - table.adjustedContentInset.bottom + 1 { break }
            }
            XCTAssertEqual(table.numberOfSections, count)
            XCTAssertEqual(table.numberOfRows(inSection: count - 1), 1)
            XCTAssertTrue(table.indexPathsForVisibleRows?.contains(last) == true)
            XCTAssertGreaterThan(table.contentOffset.y, 0)
            XCTAssertLessThanOrEqual(table.rectForRow(at: last).maxY, table.bounds.maxY - table.adjustedContentInset.bottom + 1)
            page.willMove(toParent: nil)
            page.view.removeFromSuperview()
            page.removeFromParent()
        }
    }

    private func visibleProEntry(in table: UITableView) async throws -> UITableViewCell {
        for _ in 0..<10 {
            table.setContentOffset(CGPoint(x: 0, y: -table.adjustedContentInset.top), animated: false)
            table.layoutIfNeeded()
            try await Task.sleep(for: .milliseconds(50))
            table.layoutIfNeeded()
            if let cell = table.cellForRow(at: IndexPath(row: 0, section: 0)) {
                cell.contentView.layoutIfNeeded()
                return cell
            }
        }
        return try XCTUnwrap(table.cellForRow(at: IndexPath(row: 0, section: 0)))
    }

    private func clearSubscriptions() {
        Store.shared.applySubscriptionOutcomes(Dictionary(uniqueKeysWithValues: ProStatus.subscriptionProductIDs.map { ($0, .inactive) }))
    }

    private func find(_ identifier: String, in view: UIView) -> UIView? {
        if view.accessibilityIdentifier == identifier { return view }
        return view.subviews.lazy.compactMap { self.find(identifier, in: $0) }.first
    }
}
