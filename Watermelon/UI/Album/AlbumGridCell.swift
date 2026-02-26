import SnapKit
import UIKit
import Kingfisher

final class AlbumGridCell: UICollectionViewCell {
    static let reuseID = "AlbumGridCell"

    let imageView = UIImageView()
    let titleLabel = UILabel()
    var representedID: String?

    private let topLeftBadgeLabel = UILabel()
    private let topRightBadgeLabel = UILabel()
    private let bottomRightBadgeLabel = UILabel()

    override init(frame: CGRect) {
        super.init(frame: frame)

        contentView.layer.cornerRadius = 0
        contentView.layer.masksToBounds = false
        contentView.backgroundColor = .secondarySystemBackground

        imageView.contentMode = .scaleAspectFill
        imageView.clipsToBounds = true
        imageView.backgroundColor = .tertiarySystemBackground

        titleLabel.font = .systemFont(ofSize: 11, weight: .medium)
        titleLabel.textColor = .white
        titleLabel.numberOfLines = 1
        titleLabel.setContentCompressionResistancePriority(.required, for: .vertical)
        titleLabel.layer.shadowColor = UIColor.black.cgColor
        titleLabel.layer.shadowOpacity = 0.8
        titleLabel.layer.shadowRadius = 2
        titleLabel.layer.shadowOffset = CGSize(width: 0, height: 1)

        configurePillBadgeLabel(topLeftBadgeLabel)
        topLeftBadgeLabel.isHidden = true

        configurePillBadgeLabel(topRightBadgeLabel)
        topRightBadgeLabel.isHidden = true

        configureBottomMetaLabel(bottomRightBadgeLabel)
        bottomRightBadgeLabel.isHidden = true

        contentView.addSubview(imageView)
        imageView.addSubview(topLeftBadgeLabel)
        imageView.addSubview(topRightBadgeLabel)
        imageView.addSubview(bottomRightBadgeLabel)
        imageView.addSubview(titleLabel)

        imageView.snp.makeConstraints { make in
            make.edges.equalToSuperview()
        }

        topLeftBadgeLabel.snp.makeConstraints { make in
            make.top.leading.equalToSuperview().inset(6)
        }

        topRightBadgeLabel.snp.makeConstraints { make in
            make.top.trailing.equalToSuperview().inset(6)
        }

        bottomRightBadgeLabel.snp.makeConstraints { make in
            make.bottom.trailing.equalToSuperview().inset(6)
        }

        titleLabel.snp.makeConstraints { make in
            make.leading.bottom.equalToSuperview().inset(6)
            make.trailing.lessThanOrEqualTo(bottomRightBadgeLabel.snp.leading).offset(-6)
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func prepareForReuse() {
        super.prepareForReuse()
        imageView.kf.cancelDownloadTask()
        representedID = nil
        imageView.image = nil
        titleLabel.text = nil
        clearBadges()
        clearBottomBadges()
        setTopRightBadge(nil)
        layer.borderWidth = 0
        layer.borderColor = nil
    }

    func setBadges(_ badges: [(String, UIColor)]) {
        clearBadges()
        guard let first = badges.first else { return }
        applyPillBadge(text: first.0, color: first.1, to: topLeftBadgeLabel)
    }

    func setUnbacked(_ isUnbacked: Bool) {
        // Intentionally no-op: "未备份" badge has been removed from UI.
    }

    func setTopRightBadge(_ badge: (String, UIColor)?) {
        guard let badge else {
            topRightBadgeLabel.isHidden = true
            return
        }
        topRightBadgeLabel.text = " \(badge.0) "
        topRightBadgeLabel.backgroundColor = badge.1
        topRightBadgeLabel.isHidden = false
    }

    func setBottomBadges(_ badges: [(String, UIColor)]) {
        clearBottomBadges()
        let text = badges.map(\.0).joined(separator: " · ")
        guard !text.isEmpty else { return }
        bottomRightBadgeLabel.text = text
        bottomRightBadgeLabel.isHidden = false
    }

    private func clearBadges() {
        topLeftBadgeLabel.text = nil
        topLeftBadgeLabel.isHidden = true
    }

    private func clearBottomBadges() {
        bottomRightBadgeLabel.text = nil
        bottomRightBadgeLabel.isHidden = true
    }

    private func configurePillBadgeLabel(_ label: UILabel) {
        label.font = .systemFont(ofSize: 9, weight: .bold)
        label.textColor = .white
        label.layer.cornerRadius = 4
        label.layer.masksToBounds = true
    }

    private func configureBottomMetaLabel(_ label: UILabel) {
        label.font = .systemFont(ofSize: 11, weight: .medium)
        label.textColor = .white
        label.numberOfLines = 1
        label.layer.shadowColor = UIColor.black.cgColor
        label.layer.shadowOpacity = 0.8
        label.layer.shadowRadius = 2
        label.layer.shadowOffset = CGSize(width: 0, height: 1)
    }

    private func applyPillBadge(text: String, color: UIColor, to label: UILabel) {
        label.text = " \(text) "
        label.backgroundColor = color
        label.isHidden = false
    }
}
