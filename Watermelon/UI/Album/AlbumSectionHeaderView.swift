import SnapKit
import UIKit

final class AlbumSectionHeaderView: UICollectionReusableView {
    static let reuseID = "AlbumSectionHeaderView"

    private let titleButton = UIButton(type: .system)
    var onTap: (() -> Void)?
    var onLongPress: (() -> Void)?

    private var leadingConstraint: Constraint?
    private var trailingConstraint: Constraint?
    private var heightConstraint: Constraint?
    private var isRoundedStyleEnabled = false
    private var currentTitle: String?

    override init(frame: CGRect) {
        super.init(frame: frame)

        titleButton.isUserInteractionEnabled = true
        titleButton.contentHorizontalAlignment = .leading
        titleButton.setContentCompressionResistancePriority(.required, for: .horizontal)
        titleButton.setContentHuggingPriority(.required, for: .horizontal)
        titleButton.addTarget(self, action: #selector(titleButtonTapped), for: .touchUpInside)
        let longPress = UILongPressGestureRecognizer(target: self, action: #selector(titleButtonLongPressed(_:)))
        longPress.minimumPressDuration = 0.45
        longPress.cancelsTouchesInView = true
        titleButton.addGestureRecognizer(longPress)

        addSubview(titleButton)
        titleButton.snp.makeConstraints { make in
            leadingConstraint = make.leading.equalToSuperview().inset(4).constraint
            trailingConstraint = make.trailing.lessThanOrEqualToSuperview().inset(4).constraint
            make.centerY.equalToSuperview()
            heightConstraint = make.height.equalTo(24).constraint
        }

        applyStyle()
    }

    override func prepareForReuse() {
        super.prepareForReuse()
        onTap = nil
        onLongPress = nil
        currentTitle = nil
        setTitle(nil)
        setRoundedRectStyle(false)
    }

    func setHorizontalInset(_ inset: CGFloat) {
        leadingConstraint?.update(inset: inset)
        trailingConstraint?.update(inset: inset)
    }

    func setTitle(_ title: String?) {
        currentTitle = title
        var config = titleButton.configuration ?? .plain()
        config.title = title
        titleButton.configuration = config
    }

    func setRoundedRectStyle(_ enabled: Bool) {
        isRoundedStyleEnabled = enabled
        applyStyle()
    }

    private func applyStyle() {
        var config: UIButton.Configuration = isRoundedStyleEnabled ? .filled() : .plain()
        config.title = currentTitle
        config.titleAlignment = .leading

        if isRoundedStyleEnabled {
            config.baseBackgroundColor = .secondarySystemBackground
            config.baseForegroundColor = .label
            config.cornerStyle = .large
            config.contentInsets = NSDirectionalEdgeInsets(top: 8, leading: 14, bottom: 8, trailing: 14)
            config.titleTextAttributesTransformer = UIConfigurationTextAttributesTransformer { incoming in
                var attrs = incoming
                attrs.font = .systemFont(ofSize: 18, weight: .semibold)
                return attrs
            }
            heightConstraint?.update(offset: 40)
        } else {
            config.baseForegroundColor = .label
            config.contentInsets = NSDirectionalEdgeInsets(top: 2, leading: 2, bottom: 2, trailing: 2)
            config.titleTextAttributesTransformer = UIConfigurationTextAttributesTransformer { incoming in
                var attrs = incoming
                attrs.font = .systemFont(ofSize: 16, weight: .semibold)
                return attrs
            }
            heightConstraint?.update(offset: 24)
        }

        titleButton.configuration = config
    }

    @objc
    private func titleButtonTapped() {
        onTap?()
    }

    @objc
    private func titleButtonLongPressed(_ gesture: UILongPressGestureRecognizer) {
        guard gesture.state == .began else { return }
        onLongPress?()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }
}
