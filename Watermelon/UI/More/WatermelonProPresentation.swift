import UIKit
import MoreKit

@MainActor
extension UIViewController {
    func presentWatermelonPaywall(highlighting feature: WatermelonProFeature? = nil) {
        guard presentedViewController == nil else { return }
        view.endEditing(true)
        let paywall = WatermelonPaywallViewController(configuration: .init(
            title: WatermelonProBenefits.title,
            features: WatermelonProBenefits.features,
            productIDs: ProStatus.displayProductIDs,
            privacyPolicyURL: URL(string: "https://watermelonbackup.com/privacy.html")!,
            highlightedFeature: feature
        ))
        paywall.modalPresentationStyle = .fullScreen
        paywall.onMembershipActivated = { [weak paywall] in
            paywall?.dismiss(animated: ConsideringUser.animated)
        }
        present(paywall, animated: ConsideringUser.animated)
    }

    @discardableResult
    func presentProUpgradeIfNeeded(for error: Error) -> Bool {
        guard let feature = WatermelonProFeature.requiringUpgrade(for: error),
              presentedViewController == nil else { return false }
        presentWatermelonPaywall(highlighting: feature)
        return true
    }
}
