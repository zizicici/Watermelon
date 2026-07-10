import SnapKit
import UIKit

final class GradientView: UIView {
    override class var layerClass: AnyClass {
        CAGradientLayer.self
    }

    private var gradientLayer: CAGradientLayer {
        layer as! CAGradientLayer
    }

    init(colors: [UIColor], startPoint: CGPoint, endPoint: CGPoint, locations: [NSNumber]) {
        super.init(frame: .zero)
        isUserInteractionEnabled = false
        gradientLayer.colors = colors.map(\.cgColor)
        gradientLayer.startPoint = startPoint
        gradientLayer.endPoint = endPoint
        gradientLayer.locations = locations
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }
}

func withCancellableDetachedValue<Value: Sendable>(
    priority: TaskPriority? = nil,
    operation: @escaping @Sendable () -> Value
) async -> Value {
    let task = Task.detached(priority: priority, operation: operation)
    return await withTaskCancellationHandler {
        await task.value
    } onCancel: {
        task.cancel()
    }
}

extension UIFont {
    func withWeight(_ weight: UIFont.Weight) -> UIFont {
        let descriptor = fontDescriptor.addingAttributes([
            .traits: [UIFontDescriptor.TraitKey.weight: weight.rawValue]
        ])
        return UIFont(descriptor: descriptor, size: pointSize)
    }
}

func makeAlbumEmptyStateView(title: String, message: String) -> UIView {
    let view = UIView()

    let titleLabel = UILabel()
    titleLabel.text = title
    titleLabel.textColor = .secondaryLabel
    titleLabel.font = .preferredFont(forTextStyle: .headline)
    titleLabel.textAlignment = .center
    titleLabel.adjustsFontForContentSizeCategory = true

    let messageLabel = UILabel()
    messageLabel.text = message
    messageLabel.textColor = .tertiaryLabel
    messageLabel.font = .preferredFont(forTextStyle: .subheadline)
    messageLabel.textAlignment = .center
    messageLabel.numberOfLines = 0
    messageLabel.adjustsFontForContentSizeCategory = true

    let stackView = UIStackView(arrangedSubviews: [titleLabel, messageLabel])
    stackView.axis = .vertical
    stackView.alignment = .fill
    stackView.spacing = 8

    view.addSubview(stackView)
    stackView.snp.makeConstraints { make in
        make.center.equalToSuperview()
        make.leading.greaterThanOrEqualToSuperview().offset(32)
        make.trailing.lessThanOrEqualToSuperview().offset(-32)
    }
    return view
}
