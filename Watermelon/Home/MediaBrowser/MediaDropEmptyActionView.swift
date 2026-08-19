import UIKit

@MainActor
final class MediaDropEmptyActionView: UIView {
    init(title: String, actionTitle: String, action: UIAction) {
        super.init(frame: .zero)

        let titleLabel = UILabel()
        titleLabel.text = title
        titleLabel.font = .systemFont(ofSize: 15, weight: .medium)
        titleLabel.textColor = .secondaryLabel
        titleLabel.textAlignment = .center
        titleLabel.numberOfLines = 0

        var configuration = UIButton.Configuration.plain()
        configuration.title = actionTitle
        configuration.baseForegroundColor = .materialPrimary(
            light: .Material.Green._600,
            dark: .Material.Green._200
        )
        let actionButton = UIButton(configuration: configuration, primaryAction: action)

        let stack = UIStackView(arrangedSubviews: [titleLabel, actionButton])
        stack.axis = .vertical
        stack.alignment = .center
        stack.spacing = 12
        addSubview(stack)
        stack.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            stack.centerXAnchor.constraint(equalTo: centerXAnchor),
            stack.centerYAnchor.constraint(equalTo: centerYAnchor),
            stack.leadingAnchor.constraint(greaterThanOrEqualTo: leadingAnchor, constant: 32),
            stack.trailingAnchor.constraint(lessThanOrEqualTo: trailingAnchor, constant: -32),
        ])
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }
}
