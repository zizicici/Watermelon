import UIKit

final class WatermelonProEntryCell: UITableViewCell {
    private let gradient = CAGradientLayer()
    private let titleLabel = UILabel()
    private let subtitleLabel = UILabel()
    private let button = UIButton(type: .system)
    private let row = UIStackView()

    init(isPro: Bool, onOpen: @escaping () -> Void) {
        super.init(style: .default, reuseIdentifier: nil)
        selectionStyle = .none
        accessibilityIdentifier = "settings.proEntry"
        let backdrop = UIView()
        backdrop.layer.addSublayer(gradient)
        backgroundView = backdrop
        gradient.startPoint = .zero
        gradient.endPoint = CGPoint(x: 1, y: 1)
        let heading = NSMutableAttributedString(string: WatermelonProBenefits.title, attributes: [.foregroundColor: UIColor.white])
        let range = (WatermelonProBenefits.title as NSString).range(of: "Pro")
        if range.location != NSNotFound { heading.addAttribute(.foregroundColor, value: UIColor.Material.Amber._200, range: range) }
        titleLabel.attributedText = heading
        titleLabel.numberOfLines = 0
        titleLabel.adjustsFontForContentSizeCategory = true
        subtitleLabel.text = isPro ? String(localized: "pro.entry.activeStatus") : String(localized: "pro.entry.upgradeSubtitle")
        subtitleLabel.textColor = .white.withAlphaComponent(0.85)
        subtitleLabel.numberOfLines = 0
        subtitleLabel.adjustsFontForContentSizeCategory = true
        let labels = UIStackView(arrangedSubviews: [titleLabel, subtitleLabel])
        labels.axis = .vertical
        labels.spacing = 6
        var configuration = UIButton.Configuration.filled()
        configuration.title = isPro ? String(localized: "pro.entry.viewBenefits") : String(localized: "paywall.choosePlan")
        configuration.titleAlignment = .center
        configuration.titleLineBreakMode = .byWordWrapping
        configuration.baseBackgroundColor = .white
        configuration.baseForegroundColor = .Material.Green._800
        configuration.cornerStyle = .fixed
        configuration.background.cornerRadius = 14
        configuration.contentInsets = .init(top: 12, leading: 14, bottom: 12, trailing: 14)
        button.configuration = configuration
        button.titleLabel?.adjustsFontForContentSizeCategory = true
        button.addAction(UIAction { _ in onOpen() }, for: .touchUpInside)
        button.accessibilityIdentifier = "settings.proEntry.open"
        button.setContentHuggingPriority(.required, for: .horizontal)
        button.setContentCompressionResistancePriority(.required, for: .horizontal)
        row.addArrangedSubview(labels)
        row.addArrangedSubview(button)
        row.alignment = .center
        row.spacing = 16
        row.translatesAutoresizingMaskIntoConstraints = false
        contentView.addSubview(row)
        NSLayoutConstraint.activate([
            row.leadingAnchor.constraint(equalTo: contentView.leadingAnchor, constant: 20),
            row.trailingAnchor.constraint(equalTo: contentView.trailingAnchor, constant: -20),
            row.topAnchor.constraint(equalTo: contentView.topAnchor, constant: 24),
            row.bottomAnchor.constraint(equalTo: contentView.bottomAnchor, constant: -24),
            button.heightAnchor.constraint(greaterThanOrEqualToConstant: 44)
        ])
        isAccessibilityElement = true
        accessibilityTraits = .button
        accessibilityLabel = WatermelonProBenefits.title + ", " + (subtitleLabel.text ?? "")
        registerForTraitChanges([UITraitUserInterfaceStyle.self]) { (cell: WatermelonProEntryCell, _) in
            cell.updateGradient()
        }
        registerForTraitChanges([UITraitPreferredContentSizeCategory.self]) { (cell: WatermelonProEntryCell, _) in
            cell.updateTypography()
        }
        updateTypography()
        updateGradient()
    }

    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }

    override func layoutSubviews() {
        updateLayout(for: contentView.bounds.width)
        super.layoutSubviews()
        gradient.frame = backgroundView?.bounds ?? bounds
    }

    override func systemLayoutSizeFitting(_ targetSize: CGSize, withHorizontalFittingPriority horizontalFittingPriority: UILayoutPriority, verticalFittingPriority: UILayoutPriority) -> CGSize {
        updateLayout(for: targetSize.width)
        return super.systemLayoutSizeFitting(targetSize, withHorizontalFittingPriority: horizontalFittingPriority, verticalFittingPriority: verticalFittingPriority)
    }

    private func updateTypography() {
        titleLabel.font = .preferredFont(forTextStyle: .title3, compatibleWith: traitCollection).withWeight(.bold)
        subtitleLabel.font = .preferredFont(forTextStyle: .footnote, compatibleWith: traitCollection)
        let font = UIFont.preferredFont(forTextStyle: .headline, compatibleWith: traitCollection)
        button.configuration?.titleTextAttributesTransformer = .init { attributes in
            var attributes = attributes
            attributes.font = font
            return attributes
        }
        updateLayout(for: contentView.bounds.width)
        setNeedsLayout()
    }

    private func updateLayout(for width: CGFloat) {
        guard width > 40 else { return }
        let titleWidth = ((titleLabel.text ?? "") as NSString).size(withAttributes: [.font: titleLabel.font!]).width
        let subtitleWidth = ((subtitleLabel.text ?? "") as NSString).size(withAttributes: [.font: subtitleLabel.font!]).width
        let buttonFont = UIFont.preferredFont(forTextStyle: .headline, compatibleWith: traitCollection)
        let buttonWidth = ((button.configuration?.title ?? "") as NSString).size(withAttributes: [.font: buttonFont]).width + 28
        let vertical = traitCollection.preferredContentSizeCategory.isAccessibilityCategory
            || ceil(max(titleWidth, subtitleWidth) + buttonWidth + 16) > width - 40
        let axis: NSLayoutConstraint.Axis = vertical ? .vertical : .horizontal
        guard row.axis != axis else { return }
        row.axis = axis
        row.alignment = vertical ? .fill : .center
        row.spacing = vertical ? 14 : 16
        button.setContentHuggingPriority(vertical ? .defaultLow : .required, for: .horizontal)
    }

    private func updateGradient() {
        gradient.colors = (traitCollection.userInterfaceStyle == .dark
            ? [UIColor.Material.Green._900, UIColor.Material.Green._800]
            : [UIColor.Material.Green._700, UIColor.Material.Green._600]).map(\.cgColor)
    }
}
