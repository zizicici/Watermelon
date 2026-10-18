import UIKit
import MoreKit
import StoreKit
import SafariServices

struct WatermelonPaywallConfiguration {
    let title: String
    struct Feature {
        let title: String
        let freeValue: String
        let proValue: String
        let id: String
        let explanation: String
        let isSubfeature: Bool
    }
    let features: [Feature]
    let productIDs: [String]
    let privacyPolicyURL: URL
    let termsURL: URL
    let highlightedFeature: WatermelonProFeature?

    init(title: String, features: [Feature],
                productIDs: [String] = [], privacyPolicyURL: URL,
                highlightedFeature: WatermelonProFeature? = nil,
                termsURL: URL = URL(string: "https://www.apple.com/legal/internet-services/itunes/dev/stdeula/")!) {
        self.title = title
        self.features = features
        self.productIDs = productIDs
        self.privacyPolicyURL = privacyPolicyURL
        self.termsURL = termsURL
        self.highlightedFeature = highlightedFeature
    }

    var resolvedProductIDs: [String] {
        let requested = productIDs.isEmpty ? MoreKit.membershipProductIDs : productIDs
        var seen = Set<String>()
        return requested.filter { MoreKit.membershipProductIDs.contains($0) && seen.insert($0).inserted }
    }
}

@MainActor
final class WatermelonPaywallViewController: UIViewController, UIPopoverPresentationControllerDelegate {
    let configuration: WatermelonPaywallConfiguration
    var onMembershipActivated: (() -> Void)?
    private let store: Store
    private var selectedProductID: String?
    private var productCards: [String: UIControl] = [:]
    private var featureButtons: [UIButton] = []
    private var purchaseSections: [UIView] = []
    private var disclosureHeight: NSLayoutConstraint?
    private var isWorking = false
    private let scrollView = UIScrollView()
    private let backdrop = WatermelonPaywallBackground()
    private let dockBackground: UIVisualEffectView = {
        if #available(iOS 26.0, *) {
            return UIVisualEffectView(effect: UIGlassEffect(style: .regular))
        }
        return UIVisualEffectView(effect: UIBlurEffect(style: .systemUltraThinMaterial))
    }()
    private let content = UIStackView()
    private let header = WatermelonPaywallHeader()
    private let plans = UIStackView()
    private let dock = UIStackView()
    private let disclosure = UILabel()
    private let purchaseButton = UIButton(type: .system)
    private let links = UIStackView()
    private let closeButton = UIButton(type: .system)
    private var previousNavigationBarHidden: Bool?

    convenience init(configuration: WatermelonPaywallConfiguration) {
        self.init(configuration: configuration, store: .shared)
    }

    init(configuration: WatermelonPaywallConfiguration, store: Store) {
        self.configuration = configuration
        self.store = store
        super.init(nibName: nil, bundle: nil)
        title = configuration.title
        navigationItem.title = ""
        navigationItem.largeTitleDisplayMode = .never
    }

    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }
    deinit {
        NotificationCenter.default.removeObserver(self)
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        view.tintColor = .appTint
        view.accessibilityIdentifier = "watermelon.paywall"
        let navigation = UINavigationBarAppearance()
        navigation.configureWithTransparentBackground()
        navigationItem.standardAppearance = navigation
        navigationItem.scrollEdgeAppearance = navigation
        scrollView.contentInsetAdjustmentBehavior = .never
        scrollView.automaticallyAdjustsScrollIndicatorInsets = false
        scrollView.accessibilityIdentifier = "paywall.scroll"
        scrollView.alwaysBounceVertical = true
        content.axis = .vertical
        content.spacing = 28
        plans.axis = .vertical
        plans.spacing = 10
        dock.axis = .vertical
        dock.spacing = 12
        dock.backgroundColor = .clear
        if #available(iOS 26.0, *) {
            dockBackground.cornerConfiguration = .uniformEdges(topRadius: .fixed(28), bottomRadius: .fixed(0))
        } else {
            dockBackground.layer.cornerRadius = 28
            dockBackground.layer.maskedCorners = [.layerMinXMinYCorner, .layerMaxXMinYCorner]
        }
        dockBackground.clipsToBounds = true
        dockBackground.accessibilityIdentifier = "paywall.dock"
        disclosure.accessibilityIdentifier = "paywall.disclosure"
        disclosure.font = .systemFont(ofSize: 12)
        disclosure.textColor = .label
        disclosure.textAlignment = .center
        disclosure.numberOfLines = 0
        let disclosureHeight = disclosure.heightAnchor.constraint(equalToConstant: 0)
        disclosureHeight.isActive = true
        self.disclosureHeight = disclosureHeight
        links.axis = .horizontal
        links.accessibilityIdentifier = "paywall.links"
        links.distribution = .fillEqually
        purchaseButton.accessibilityIdentifier = "paywall.purchase"
        purchaseButton.addTarget(self, action: #selector(purchaseSelected), for: .touchUpInside)
        dock.addArrangedSubview(disclosure)
        dock.addArrangedSubview(purchaseButton)
        dock.addArrangedSubview(links)
        view.addSubview(backdrop)
        view.addSubview(scrollView)
        view.addSubview(dockBackground)
        dockBackground.contentView.addSubview(dock)
        closeButton.setImage(UIImage(systemName: "xmark", withConfiguration: UIImage.SymbolConfiguration(pointSize: 14, weight: .semibold)), for: .normal)
        closeButton.tintColor = .label
        closeButton.backgroundColor = WatermelonPaywallPalette.card
        closeButton.layer.cornerRadius = 22
        closeButton.accessibilityLabel = String(localized: "common.close")
        closeButton.accessibilityIdentifier = "paywall.close"
        closeButton.addTarget(self, action: #selector(close), for: .touchUpInside)
        closeButton.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(closeButton)
        scrollView.addSubview(content)
        for child in [backdrop, scrollView, content, dockBackground, dock] { child.translatesAutoresizingMaskIntoConstraints = false }
        NSLayoutConstraint.activate([
            backdrop.topAnchor.constraint(equalTo: view.topAnchor),
            backdrop.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            backdrop.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            backdrop.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            dockBackground.topAnchor.constraint(equalTo: dock.topAnchor, constant: -14),
            dockBackground.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            dockBackground.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            dockBackground.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            closeButton.topAnchor.constraint(equalTo: view.safeAreaLayoutGuide.topAnchor, constant: 8),
            closeButton.trailingAnchor.constraint(equalTo: view.safeAreaLayoutGuide.trailingAnchor, constant: -20),
            closeButton.widthAnchor.constraint(equalToConstant: 44),
            closeButton.heightAnchor.constraint(equalToConstant: 44),
            scrollView.topAnchor.constraint(equalTo: view.topAnchor),
            scrollView.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            scrollView.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            scrollView.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            content.topAnchor.constraint(equalTo: scrollView.contentLayoutGuide.topAnchor),
            content.bottomAnchor.constraint(equalTo: scrollView.contentLayoutGuide.bottomAnchor, constant: -16),
            content.leadingAnchor.constraint(equalTo: scrollView.contentLayoutGuide.leadingAnchor),
            content.trailingAnchor.constraint(equalTo: scrollView.contentLayoutGuide.trailingAnchor),
            content.widthAnchor.constraint(equalTo: scrollView.frameLayoutGuide.widthAnchor),
            dock.centerXAnchor.constraint(equalTo: view.centerXAnchor),
            dock.widthAnchor.constraint(equalTo: view.widthAnchor, constant: -48).withPriority(.init(999)),
            dock.widthAnchor.constraint(lessThanOrEqualTo: view.widthAnchor, constant: -48),
            dock.widthAnchor.constraint(lessThanOrEqualToConstant: 520),
            dock.bottomAnchor.constraint(equalTo: view.safeAreaLayoutGuide.bottomAnchor, constant: -12),
            purchaseButton.heightAnchor.constraint(equalToConstant: 52),
            links.heightAnchor.constraint(greaterThanOrEqualToConstant: 44)
        ])
        content.addArrangedSubview(header)
        appendInset(comparisonTable())
        registerForTraitChanges([UITraitUserInterfaceStyle.self]) { (controller: WatermelonPaywallViewController, _) in
            controller.updateSelection()
        }
        for name in [Notification.Name.StoreInfoLoaded, .StoreProductsLoaded] {
            NotificationCenter.default.addObserver(self, selector: #selector(observedChange), name: name, object: nil)
        }
        render()
    }

    override func viewDidLayoutSubviews() {
        super.viewDidLayoutSubviews()
        updateDisclosureHeight()
        let overlap = max(0, scrollView.frame.maxY - dockBackground.frame.minY)
        let bottomInset = overlap + 12
        if abs(scrollView.contentInset.bottom - bottomInset) > 0.5 {
            scrollView.contentInset.bottom = bottomInset
        }
        scrollView.verticalScrollIndicatorInsets = UIEdgeInsets(top: view.safeAreaInsets.top + 8, left: 0, bottom: overlap + 8, right: 0)
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        if previousNavigationBarHidden == nil {
            previousNavigationBarHidden = navigationController?.isNavigationBarHidden
        }
        navigationController?.setNavigationBarHidden(true, animated: animated)
        Task { [weak self] in
            guard let self else { return }
            await store.updateCustomerProductStatus()
            await store.requestProducts()
        }
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        if let previousNavigationBarHidden {
            navigationController?.setNavigationBarHidden(previousNavigationBarHidden, animated: animated)
        }
    }

    @objc private func close() {
        if let navigationController, navigationController.viewControllers.first !== self {
            navigationController.popViewController(animated: true)
        } else { dismiss(animated: true) }
    }

    @objc nonisolated private func observedChange() {
        Task { @MainActor [weak self] in self?.render() }
    }

    private var products: [Product] {
        configuration.resolvedProductIDs.compactMap { id in store.memberships.first { $0.id == id } }
    }
    private var selectedProduct: Product? { products.first { $0.id == selectedProductID } }

    private func render() {
        guard isViewLoaded else { return }
        if !products.contains(where: { $0.id == selectedProductID }) {
            selectedProductID = products.first(where: { product in
                store.activeSubscriptions().contains { $0.productID == product.id }
            })?.id ?? products.first?.id
        }
        closeButton.isEnabled = !isWorking
        isModalInPresentation = isWorking
        purchaseSections.forEach { $0.removeFromSuperview() }
        purchaseSections.removeAll()
        productCards.removeAll()

        header.configure(title: configuration.title, subtitle: store.hasValidMembership() ? String(localized: "paywall.thanks") : (configuration.highlightedFeature?.upgradeMessage ?? String(localized: "paywall.subtitle")))
        plans.arrangedSubviews.forEach { $0.removeFromSuperview() }
        if store.proTier() != .lifetime {
            if !products.isEmpty {
                let row = UIStackView()
                row.axis = .horizontal
                row.distribution = .fillEqually
                row.spacing = 8
                row.accessibilityIdentifier = "paywall.plans"
                for product in products { row.addArrangedSubview(planCard(product)) }
                plans.addArrangedSubview(row)
            }
            if products.count < configuration.resolvedProductIDs.count || store.productsError != nil {
                let status = UIStackView()
                status.axis = .vertical
                status.alignment = .center
                status.spacing = 8
                let message = label(store.isLoadingProducts ? String(localized: "paywall.loading") : String(localized: "store.error.productsUnavailable"), size: 13, color: .secondaryLabel)
                message.numberOfLines = 0
                message.textAlignment = .center
                status.addArrangedSubview(message)
                if !store.isLoadingProducts { status.addArrangedSubview(link(String(localized: "paywall.retry")) { [weak self] in self?.run { await self?.store.requestProducts() } }) }
                plans.addArrangedSubview(status)
            }
            let choices = UIStackView()
            choices.axis = .vertical
            choices.spacing = 14
            choices.addArrangedSubview(label(String(localized: "paywall.choosePlan"), size: 20, weight: .semibold))
            let explanation = label(String(localized: "paywall.sameBenefits"), size: 13, color: .secondaryLabel)
            explanation.numberOfLines = 0
            choices.addArrangedSubview(explanation)
            choices.addArrangedSubview(plans)
            purchaseSections.append(appendInset(choices))
        }

        updatePurchaseControls()
        if MoreKit.supportsSubscriptions && !store.activeSubscriptions().isEmpty {
            purchaseSections.append(appendInset(link(String(localized: "paywall.manage"), identifier: "paywall.manage") { [weak self] in self?.run { try await self?.manageSubscriptions() } }))
        }
    }

    private func updatePurchaseControls() {
        featureButtons.forEach { $0.isEnabled = !isWorking }
        disclosure.text = disclosureText(for: selectedProduct)
        updateDisclosureHeight()
        var button = UIButton.Configuration.filled()
        button.title = purchaseTitle()
        button.titleAlignment = .center
        button.baseBackgroundColor = .appTint
        button.baseForegroundColor = .materialOnPrimary(dark: .Material.Green._800)
        button.cornerStyle = .fixed
        button.background.cornerRadius = 16
        button.titleTextAttributesTransformer = .init { var attributes = $0; attributes.font = .systemFont(ofSize: 17, weight: .semibold); return attributes }
        button.showsActivityIndicator = isWorking
        purchaseButton.configuration = button
        purchaseButton.isEnabled = !isWorking && (store.proTier() == .lifetime || selectedProduct != nil)
        if let product = selectedProduct, store.activeSubscriptions().contains(where: { $0.productID == product.id }), store.proTier() != .lifetime {
            purchaseButton.isEnabled = false
        }
        links.arrangedSubviews.forEach { $0.removeFromSuperview() }
        links.addArrangedSubview(link(String(localized: "paywall.privacy")) { [weak self] in self?.openURL(self?.configuration.privacyPolicyURL) })
        links.addArrangedSubview(link(String(localized: "store.restore"), identifier: "paywall.restore") { [weak self] in self?.restore() })
        links.addArrangedSubview(link(String(localized: "paywall.terms")) { [weak self] in self?.openURL(self?.configuration.termsURL) })
    }

    private func updateDisclosureHeight() {
        let width = min(view.bounds.width - 48, 520)
        guard width > 0 else { return }
        let heights = products.compactMap { disclosureText(for: $0) }.map { text in
            (text as NSString).boundingRect(
                with: CGSize(width: width, height: .greatestFiniteMagnitude),
                options: [.usesLineFragmentOrigin, .usesFontLeading],
                attributes: [.font: disclosure.font!], context: nil
            ).height
        }
        let height = ceil(heights.max() ?? 0)
        if let disclosureHeight, abs(disclosureHeight.constant - height) > 0.5 {
            disclosureHeight.constant = height
        }
    }

    private func updateSelection() {
        for (id, card) in productCards {
            let selected = id == selectedProductID
            card.backgroundColor = selected ? WatermelonPaywallPalette.selectedCard : WatermelonPaywallPalette.card
            card.layer.borderWidth = selected ? 2 : 0.5
            card.layer.borderColor = (selected ? UIColor.appTint : UIColor.white.withAlphaComponent(0.3)).resolvedColor(with: traitCollection).cgColor
            card.accessibilityTraits = selected ? [.button, .selected] : .button
        }
        updatePurchaseControls()
    }

    private func presentFeatureInfo(_ feature: WatermelonPaywallConfiguration.Feature, from source: UIView) {
        guard !isWorking, presentedViewController == nil, let window = view.window,
              source.window === window, source.isDescendant(of: view) else { return }
        let controller = WatermelonFeatureInfoViewController(
            title: feature.title, message: feature.explanation,
            width: min(320, view.bounds.width - 40)
        )
        controller.modalPresentationStyle = .popover
        if let popover = controller.popoverPresentationController {
            popover.sourceView = source
            popover.sourceRect = source.bounds
            popover.permittedArrowDirections = [.up, .down]
            popover.delegate = self
        }
        present(controller, animated: true)
    }

    func adaptivePresentationStyle(for controller: UIPresentationController) -> UIModalPresentationStyle { .none }

    func adaptivePresentationStyle(for controller: UIPresentationController, traitCollection: UITraitCollection) -> UIModalPresentationStyle { .none }

    @discardableResult
    private func appendInset(_ child: UIView) -> UIView {
        let wrapper = UIView()
        child.translatesAutoresizingMaskIntoConstraints = false
        wrapper.addSubview(child)
        NSLayoutConstraint.activate([
            child.topAnchor.constraint(equalTo: wrapper.topAnchor),
            child.bottomAnchor.constraint(equalTo: wrapper.bottomAnchor),
            child.centerXAnchor.constraint(equalTo: wrapper.centerXAnchor),
            child.widthAnchor.constraint(equalTo: wrapper.widthAnchor, constant: -48).withPriority(.init(999)),
            child.widthAnchor.constraint(lessThanOrEqualTo: wrapper.widthAnchor, constant: -48),
            child.widthAnchor.constraint(lessThanOrEqualToConstant: 520)
        ])
        content.addArrangedSubview(wrapper)
        return wrapper
    }

    private func comparisonTable() -> UIView {
        let section = UIStackView()
        section.axis = .vertical
        section.spacing = 16
        section.addArrangedSubview(label(String(localized: "paywall.benefits"), size: 20, weight: .semibold))
        let table = UIStackView()
        table.axis = .vertical
        table.layer.cornerRadius = 16
        table.clipsToBounds = true
        table.accessibilityIdentifier = "paywall.comparison"
        table.addArrangedSubview(comparisonRow(title: "", free: String(localized: "paywall.free"), pro: "Pro", isHeader: true))
        for feature in configuration.features {
            table.addArrangedSubview(comparisonRow(title: feature.title, free: feature.freeValue, pro: feature.proValue, feature: feature))
        }
        section.addArrangedSubview(table)
        let note = label(String(localized: "paywall.benefitsNote"), size: 12, color: .secondaryLabel)
        note.numberOfLines = 0
        section.addArrangedSubview(note)
        return section
    }

    private func comparisonRow(title: String, free: String, pro: String, feature: WatermelonPaywallConfiguration.Feature? = nil, isHeader: Bool = false) -> UIView {
        let row = UIView()
        row.backgroundColor = WatermelonPaywallPalette.tableRow
        let freeBackground = UIView()
        freeBackground.backgroundColor = WatermelonPaywallPalette.freeColumn
        let proBackground = UIView()
        proBackground.backgroundColor = WatermelonPaywallPalette.proColumn
        let isSubfeature = feature?.isSubfeature == true
        let font = UIFont.systemFont(ofSize: isSubfeature ? 12 : 13)
        let name = UILabel()
        name.font = font
        name.numberOfLines = 0
        let color: UIColor = isSubfeature ? .secondaryLabel : .label
        let nameText = NSMutableAttributedString(string: title, attributes: [.font: font, .foregroundColor: color])
        let nameArea: UIView
        if let feature {
            let attachment = NSTextAttachment()
            attachment.image = UIImage(systemName: "info.circle", withConfiguration: UIImage.SymbolConfiguration(pointSize: 12))?.withTintColor(.secondaryLabel, renderingMode: .alwaysOriginal)
            attachment.bounds = CGRect(x: 0, y: -2, width: 12, height: 12)
            nameText.append(NSAttributedString(string: "\u{00A0}"))
            nameText.append(NSAttributedString(attachment: attachment))
            let button = UIButton(type: .custom)
            featureButtons.append(button)
            button.accessibilityIdentifier = "paywall.info." + feature.id
            button.accessibilityLabel = feature.title
            button.accessibilityHint = String(format: String(localized: "paywall.featureInfo"), feature.title)
            button.isEnabled = !isWorking
            button.addAction(UIAction { [weak self, weak button] _ in
                guard let button else { return }
                self?.presentFeatureInfo(feature, from: button)
            }, for: .touchUpInside)
            name.isUserInteractionEnabled = false
            name.isAccessibilityElement = false
            name.translatesAutoresizingMaskIntoConstraints = false
            button.addSubview(name)
            NSLayoutConstraint.activate([
                name.leadingAnchor.constraint(equalTo: button.leadingAnchor),
                name.trailingAnchor.constraint(equalTo: button.trailingAnchor),
                name.topAnchor.constraint(equalTo: button.topAnchor, constant: 8),
                name.bottomAnchor.constraint(equalTo: button.bottomAnchor, constant: -8),
                button.heightAnchor.constraint(greaterThanOrEqualToConstant: 44)
            ])
            nameArea = button
        } else {
            nameArea = name
        }
        name.attributedText = nameText
        let freeLabel = label(free, size: 13, color: .secondaryLabel)
        let proLabel = label(pro, size: 13, weight: .medium, color: .appTint)
        for label in [name, freeLabel, proLabel] {
            label.numberOfLines = 0
            label.lineBreakMode = .byWordWrapping
            label.setContentCompressionResistancePriority(.required, for: .vertical)
        }
        freeLabel.textAlignment = .center
        proLabel.textAlignment = .center
        for child in [freeBackground, proBackground, nameArea, freeLabel, proLabel] {
            row.addSubview(child)
            child.translatesAutoresizingMaskIntoConstraints = false
        }
        NSLayoutConstraint.activate([
            nameArea.leadingAnchor.constraint(equalTo: row.leadingAnchor, constant: isSubfeature ? 26 : 14),
            nameArea.widthAnchor.constraint(equalTo: row.widthAnchor, multiplier: 0.44, constant: isSubfeature ? -34 : -22),
            nameArea.centerYAnchor.constraint(equalTo: row.centerYAnchor),
            nameArea.topAnchor.constraint(greaterThanOrEqualTo: row.topAnchor, constant: 8),
            nameArea.bottomAnchor.constraint(lessThanOrEqualTo: row.bottomAnchor, constant: -8),
            freeLabel.leadingAnchor.constraint(equalTo: nameArea.trailingAnchor, constant: 8),
            freeLabel.widthAnchor.constraint(equalTo: row.widthAnchor, multiplier: 0.28),
            freeLabel.centerYAnchor.constraint(equalTo: row.centerYAnchor),
            freeLabel.topAnchor.constraint(greaterThanOrEqualTo: row.topAnchor, constant: 12),
            freeLabel.bottomAnchor.constraint(lessThanOrEqualTo: row.bottomAnchor, constant: -12),
            freeBackground.leadingAnchor.constraint(equalTo: freeLabel.leadingAnchor),
            freeBackground.trailingAnchor.constraint(equalTo: freeLabel.trailingAnchor),
            freeBackground.topAnchor.constraint(equalTo: row.topAnchor),
            freeBackground.bottomAnchor.constraint(equalTo: row.bottomAnchor),
            proBackground.leadingAnchor.constraint(equalTo: freeLabel.trailingAnchor),
            proBackground.trailingAnchor.constraint(equalTo: row.trailingAnchor),
            proBackground.topAnchor.constraint(equalTo: row.topAnchor),
            proBackground.bottomAnchor.constraint(equalTo: row.bottomAnchor),
            proLabel.leadingAnchor.constraint(equalTo: proBackground.leadingAnchor, constant: 5),
            proLabel.trailingAnchor.constraint(equalTo: proBackground.trailingAnchor, constant: -5),
            proLabel.centerYAnchor.constraint(equalTo: row.centerYAnchor),
            proLabel.topAnchor.constraint(greaterThanOrEqualTo: row.topAnchor, constant: 12),
            proLabel.bottomAnchor.constraint(lessThanOrEqualTo: row.bottomAnchor, constant: -12),
            row.heightAnchor.constraint(greaterThanOrEqualToConstant: isHeader ? 38 : 60),
            row.heightAnchor.constraint(equalToConstant: isHeader ? 38 : 60).withPriority(.defaultLow)
        ])
        if !isHeader {
            let divider = UIView()
            divider.backgroundColor = .separator
            divider.alpha = 0.22
            divider.translatesAutoresizingMaskIntoConstraints = false
            row.addSubview(divider)
            NSLayoutConstraint.activate([
                divider.leadingAnchor.constraint(equalTo: row.leadingAnchor, constant: 14),
                divider.trailingAnchor.constraint(equalTo: row.trailingAnchor, constant: -14),
                divider.bottomAnchor.constraint(equalTo: row.bottomAnchor),
                divider.heightAnchor.constraint(equalToConstant: 0.5)
            ])
            freeLabel.accessibilityLabel = String(localized: "paywall.free") + ": " + free
            proLabel.accessibilityLabel = "Pro: " + pro
            var elements: [Any] = [nameArea]
            elements.append(contentsOf: [freeLabel, proLabel])
            row.accessibilityElements = elements
        }
        return row
    }

    private func planCard(_ product: Product) -> UIControl {
        let card = selectableSurface(product)
        let title = label(planName(product), size: 14, weight: .semibold)
        title.adjustsFontSizeToFitWidth = true
        title.minimumScaleFactor = 0.8
        let price = label(product.displayPrice, size: 22, weight: .semibold)
        price.font = .monospacedDigitSystemFont(ofSize: 22, weight: .semibold)
        price.adjustsFontSizeToFitWidth = true
        price.minimumScaleFactor = 0.7
        price.baselineAdjustment = .alignCenters
        let period = product.type == .nonConsumable
            ? (product.isFamilyShareable ? String(localized: "paywall.oneTimeFamilyShort") : String(localized: "paywall.oneTimeShort"))
            : billingPeriod(product)
        let detail = label(period, size: 11, color: .secondaryLabel)
        detail.numberOfLines = 2
        for label in [title, price, detail] {
            label.textAlignment = .center
            label.isUserInteractionEnabled = false
            label.translatesAutoresizingMaskIntoConstraints = false
            card.addSubview(label)
            NSLayoutConstraint.activate([
                label.leadingAnchor.constraint(equalTo: card.leadingAnchor, constant: 10),
                label.trailingAnchor.constraint(equalTo: card.trailingAnchor, constant: -10)
            ])
        }
        NSLayoutConstraint.activate([
            title.topAnchor.constraint(equalTo: card.topAnchor, constant: 18),
            title.heightAnchor.constraint(equalToConstant: 20),
            price.topAnchor.constraint(equalTo: title.bottomAnchor, constant: 10),
            price.heightAnchor.constraint(equalToConstant: 28),
            detail.topAnchor.constraint(equalTo: price.bottomAnchor, constant: 6),
            detail.heightAnchor.constraint(equalToConstant: 32),
            detail.bottomAnchor.constraint(equalTo: card.bottomAnchor, constant: -16)
        ])
        card.accessibilityLabel = [title.text, price.text, detail.text].compactMap { $0 }.joined(separator: ", ")
        return card
    }

    private func selectableSurface(_ product: Product) -> UIControl {
        let selected = product.id == selectedProductID
        let card = UIControl()
        card.isEnabled = !isWorking
        card.backgroundColor = selected ? WatermelonPaywallPalette.selectedCard : WatermelonPaywallPalette.card
        card.layer.cornerRadius = 16
        card.layer.cornerCurve = .continuous
        card.layer.borderWidth = selected ? 2 : 0.5
        card.layer.borderColor = (selected ? UIColor.appTint : UIColor.white.withAlphaComponent(0.3)).resolvedColor(with: traitCollection).cgColor
        card.accessibilityIdentifier = "paywall.product.\(product.id)"
        card.isAccessibilityElement = true
        card.accessibilityTraits = selected ? [.button, .selected] : .button
        productCards[product.id] = card
        card.addAction(UIAction { [weak self] _ in
            guard let self, !isWorking, selectedProductID != product.id else { return }
            selectedProductID = product.id
            updateSelection()
        }, for: .touchUpInside)
        return card
    }

    private func planName(_ product: Product) -> String {
        switch product.id {
        case ProStatus.monthlyProductID: return String(localized: "paywall.monthly")
        case ProStatus.yearlyProductID: return String(localized: "paywall.yearly")
        case ProStatus.productID: return String(localized: "paywall.lifetime")
        default: return product.displayName
        }
    }

    private func billingPeriod(_ product: Product) -> String {
        guard let period = product.subscription?.subscriptionPeriod else { return String(localized: "paywall.oneTime") }
        return String(format: String(localized: "paywall.everyPeriod"), duration(period))
    }

    private func duration(_ period: Product.SubscriptionPeriod, count: Int = 1) -> String {
        var components = DateComponents()
        switch period.unit {
        case .day: components.day = period.value * count
        case .week: components.weekOfMonth = period.value * count
        case .month: components.month = period.value * count
        case .year: components.year = period.value * count
        @unknown default: break
        }
        let formatter = DateComponentsFormatter()
        formatter.unitsStyle = .full
        return formatter.string(from: components) ?? ""
    }

    private func disclosureText(for product: Product?) -> String? {
        if store.proTier() == .lifetime { return nil }
        guard let product else { return nil }
        guard product.type == .autoRenewable else {
            var lines = [store.proTier() == .subscription ? String(localized: "paywall.lifetimeWhileSubscribed") : String(localized: "paywall.oneTime")]
            if product.isFamilyShareable { lines.append(String(localized: "paywall.familySharing")) }
            return lines.joined(separator: "\n")
        }
        if let active = store.activeSubscriptions().first(where: { $0.productID == product.id }),
           active.willAutoRenew == false {
            return [String(localized: "paywall.renewalOff"), String(format: String(localized: "paywall.accessUntil"),
                active.accessExpirationDate.formatted(date: .abbreviated, time: .omitted))].joined(separator: "\n")
        }
        guard let period = product.subscription?.subscriptionPeriod else { return nil }
        let recurring = String(format: String(localized: "paywall.recurringPrice"), product.displayPrice, duration(period))
        var lines = [recurring]
        lines.append(String(localized: "paywall.autoRenewShort"))
        if let active = store.activeSubscriptions().first {
            lines.append(String(format: String(localized: "paywall.accessUntil"), active.accessExpirationDate.formatted(date: .abbreviated, time: .omitted)))
        }
        return lines.joined(separator: "\n")
    }

    private func purchaseTitle() -> String {
        if store.proTier() == .lifetime { return String(localized: "common.done") }
        guard let product = selectedProduct else { return String(localized: "paywall.continue") }
        if store.activeSubscriptions().contains(where: { $0.productID == product.id }) { return String(localized: "paywall.currentPlan") }
        if product.type == .autoRenewable, store.proTier() == .subscription { return String(localized: "paywall.manage") }
        return product.type == .autoRenewable ? String(localized: "paywall.subscribe") : String(localized: "paywall.unlockLifetime")
    }

    @objc private func purchaseSelected() {
        guard !isWorking else { return }
        if store.proTier() == .lifetime { close(); return }
        guard let product = selectedProduct else { return }
        run { [self] in
            if product.type == .autoRenewable, store.proTier() == .subscription {
                try await manageSubscriptions()
                return
            }
            switch try await store.purchase(productID: product.id) {
            case .alreadyOwned: onMembershipActivated?()
            case .success:
                if !store.hasValidMembership() { await store.updateCustomerProductStatus() }
                if store.hasValidMembership() { onMembershipActivated?() }
                else { showMessage(String(localized: "paywall.activationPending.title"), message: String(localized: "paywall.activationPending.message")) }
            case .pending: showMessage(String(localized: "store.purchase.pending.title"), message: String(localized: "store.purchase.pending.message"))
            case .cancelled: break
            }
        }
    }

    private func restore() {
        run { [self] in
            let restored = try await store.syncMembershipStatus()
            if restored, let onMembershipActivated { onMembershipActivated() }
            else { showMessage(restored ? String(localized: "store.restore.success") : String(localized: "store.restore.noPurchases")) }
        }
    }

    private func run(_ operation: @escaping @MainActor () async throws -> Void) {
        guard !isWorking else { return }
        isWorking = true
        render()
        Task {
            defer { isWorking = false; render() }
            do { try await operation() }
            catch StoreKitError.userCancelled {}
            catch { showMessage(String(localized: "store.orderFailure"), message: error.localizedDescription) }
        }
    }

    private func manageSubscriptions() async throws {
        guard let scene = view.window?.windowScene else { return }
        try await AppStore.showManageSubscriptions(in: scene)
        await store.updateCustomerProductStatus()
    }

    private func openURL(_ url: URL?) {
        guard let url, !isWorking else { return }
        present(SFSafariViewController(url: url), animated: true)
    }

    private func label(_ text: String, size: CGFloat, weight: UIFont.Weight = .regular, color: UIColor = .label) -> UILabel {
        let label = UILabel()
        label.text = text
        label.font = .systemFont(ofSize: size, weight: weight)
        label.textColor = color
        return label
    }

    private func link(_ title: String, identifier: String? = nil, action: @escaping () -> Void) -> UIButton {
        let button = UIButton(type: .system, primaryAction: UIAction { _ in action() })
        button.setTitle(title, for: .normal)
        button.titleLabel?.font = .systemFont(ofSize: 12)
        button.titleLabel?.textAlignment = .center
        button.titleLabel?.adjustsFontSizeToFitWidth = true
        button.titleLabel?.minimumScaleFactor = 0.8
        button.contentHorizontalAlignment = .center
        button.tintColor = .label
        button.accessibilityIdentifier = identifier
        button.isEnabled = !isWorking
        return button
    }

    private func showMessage(_ title: String, message: String? = nil) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.done"), style: .default))
        present(alert, animated: true)
    }
}

private extension NSLayoutConstraint {
    func withPriority(_ priority: UILayoutPriority) -> NSLayoutConstraint { self.priority = priority; return self }
}
