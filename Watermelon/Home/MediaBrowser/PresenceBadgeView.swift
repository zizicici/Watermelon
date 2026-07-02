import UIKit

enum MediaPresenceStyle {
    // `remoteSymbol` is the connected storage type's glyph (smb→server.rack, s3→cloud, …), matching the
    // node menu; a remote-only item is badged with it.
    static func symbolName(for presence: MediaPresence, remoteSymbol: String) -> String {
        switch presence {
        case .localOnly: return "iphone"
        case .remoteOnly: return remoteSymbol
        case .both: return "arrow.trianglehead.2.clockwise.rotate.90"
        }
    }

    static func title(for presence: MediaPresence) -> String {
        switch presence {
        case .localOnly: return String(localized: "mediaBrowser.presence.localOnly")
        case .remoteOnly: return String(localized: "mediaBrowser.presence.remoteOnly")
        case .both: return String(localized: "mediaBrowser.presence.both")
        }
    }
}

// Small pill showing an item's presence (local / remote / both) with an SF Symbol; used both as a grid
// corner glyph (symbol only) and in the viewer chrome (symbol + label).
final class PresenceBadgeView: UIView {
    private let imageView = UIImageView()
    private let label = UILabel()
    private let stack = UIStackView()

    override init(frame: CGRect) {
        super.init(frame: frame)
        stack.axis = .horizontal
        stack.alignment = .center
        stack.spacing = 4
        stack.isLayoutMarginsRelativeArrangement = true
        stack.layoutMargins = UIEdgeInsets(top: 3, left: 7, bottom: 3, right: 7)

        imageView.contentMode = .scaleAspectFit
        imageView.setContentHuggingPriority(.required, for: .horizontal)
        label.font = .preferredFont(forTextStyle: .caption1)
        label.adjustsFontForContentSizeCategory = true

        stack.addArrangedSubview(imageView)
        stack.addArrangedSubview(label)
        addSubview(stack)
        stack.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            stack.topAnchor.constraint(equalTo: topAnchor),
            stack.bottomAnchor.constraint(equalTo: bottomAnchor),
            stack.leadingAnchor.constraint(equalTo: leadingAnchor),
            stack.trailingAnchor.constraint(equalTo: trailingAnchor),
        ])
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func configure(presence: MediaPresence, remoteSymbol: String, showsLabel: Bool, tint: UIColor = .white) {
        imageView.image = UIImage(systemName: MediaPresenceStyle.symbolName(for: presence, remoteSymbol: remoteSymbol))
        imageView.tintColor = tint
        label.text = showsLabel ? MediaPresenceStyle.title(for: presence) : nil
        label.textColor = tint
        label.isHidden = !showsLabel
    }
}
