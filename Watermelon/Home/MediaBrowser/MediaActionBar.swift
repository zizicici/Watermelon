import UIKit

// A horizontal row of vertical (SF-symbol-over-label) action buttons. Centers when the buttons fit the
// width, scrolls when they overflow. Generic over a caller tag so a screen can mix real action kinds with
// synthetic entries (e.g. a single "Delete" that opens its own menu). Shared by the viewer chrome and the
// grid's multi-select toolbar.
final class MediaActionBar: UIView {
    struct Entry {
        let id: AnyHashable
        let symbolName: String
        let title: String
        let isDestructive: Bool

        init(id: AnyHashable, symbolName: String, title: String, isDestructive: Bool = false) {
            self.id = id
            self.symbolName = symbolName
            self.title = title
            self.isDestructive = isDestructive
        }
    }

    // Non-destructive foreground; destructive entries always render red. Set to .white over the dark viewer.
    // Set before `configure`.
    var foregroundColor: UIColor = .label

    private let scrollView = UIScrollView()
    private let stack = UIStackView()
    private var onTap: ((AnyHashable) -> Void)?
    private var buttonsByID: [AnyHashable: UIButton] = [:]

    // The button view for an entry — lets a caller anchor a popover (iPad) to the specific action, not the bar.
    func buttonView(for id: AnyHashable) -> UIView? { buttonsByID[id] }

    override init(frame: CGRect) {
        super.init(frame: frame)
        scrollView.showsHorizontalScrollIndicator = false
        scrollView.alwaysBounceHorizontal = false
        scrollView.contentInsetAdjustmentBehavior = .never
        stack.axis = .horizontal
        stack.alignment = .center
        stack.spacing = 8
        addSubview(scrollView)
        scrollView.addSubview(stack)
        scrollView.translatesAutoresizingMaskIntoConstraints = false
        stack.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            scrollView.topAnchor.constraint(equalTo: topAnchor),
            scrollView.bottomAnchor.constraint(equalTo: bottomAnchor),
            scrollView.leadingAnchor.constraint(equalTo: leadingAnchor),
            scrollView.trailingAnchor.constraint(equalTo: trailingAnchor),
            stack.topAnchor.constraint(equalTo: scrollView.contentLayoutGuide.topAnchor),
            stack.bottomAnchor.constraint(equalTo: scrollView.contentLayoutGuide.bottomAnchor),
            stack.leadingAnchor.constraint(equalTo: scrollView.contentLayoutGuide.leadingAnchor),
            stack.trailingAnchor.constraint(equalTo: scrollView.contentLayoutGuide.trailingAnchor),
            stack.heightAnchor.constraint(equalTo: scrollView.frameLayoutGuide.heightAnchor),
        ])
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func configure(entries: [Entry], onTap: @escaping (AnyHashable) -> Void) {
        self.onTap = onTap
        stack.arrangedSubviews.forEach { $0.removeFromSuperview() }
        buttonsByID.removeAll()
        for entry in entries {
            let button = makeButton(for: entry)
            buttonsByID[entry.id] = button
            stack.addArrangedSubview(button)
        }
        setNeedsLayout()
    }

    private func makeButton(for entry: Entry) -> UIButton {
        var config = UIButton.Configuration.plain()
        config.image = UIImage(systemName: entry.symbolName, withConfiguration: UIImage.SymbolConfiguration(pointSize: 16, weight: .regular))
        config.title = entry.title
        config.imagePlacement = .top
        config.imagePadding = 7
        config.titleAlignment = .center
        config.contentInsets = NSDirectionalEdgeInsets(top: 4, leading: 12, bottom: 4, trailing: 12)
        config.titleTextAttributesTransformer = UIConfigurationTextAttributesTransformer { incoming in
            var out = incoming
            out.font = .preferredFont(forTextStyle: .caption2)
            return out
        }
        config.baseForegroundColor = entry.isDestructive ? .systemRed : foregroundColor
        let button = UIButton(configuration: config)
        // The bar has a fixed height; clamp Dynamic Type so a symbol-over-label button can't grow past it and clip.
        button.maximumContentSizeCategory = .extraExtraLarge
        let id = entry.id
        button.addAction(UIAction { [weak self] _ in self?.onTap?(id) }, for: .touchUpInside)
        return button
    }

    // Center the row when it's narrower than the bar; let it scroll (inset 0) when it overflows.
    override func layoutSubviews() {
        super.layoutSubviews()
        scrollView.layoutIfNeeded()
        let inset = max(0, (scrollView.bounds.width - scrollView.contentSize.width) / 2)
        if abs(scrollView.contentInset.left - inset) > 0.5 {
            scrollView.contentInset = UIEdgeInsets(top: 0, left: inset, bottom: 0, right: inset)
        }
    }
}
