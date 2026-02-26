import SnapKit
import UIKit
import Kingfisher

final class AlbumGridCell: UICollectionViewCell {
    static let reuseID = "AlbumGridCell"

    let imageView = UIImageView()
    let titleLabel = UILabel()
    var representedID: String?

    private let badgeStack = UIStackView()
    private let unbackedLabel = UILabel()

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

        badgeStack.axis = .vertical
        badgeStack.spacing = 4
        badgeStack.alignment = .leading

        unbackedLabel.font = .systemFont(ofSize: 9, weight: .bold)
        unbackedLabel.textColor = .white
        unbackedLabel.backgroundColor = .systemRed
        unbackedLabel.text = " 未备份 "
        unbackedLabel.layer.cornerRadius = 4
        unbackedLabel.layer.masksToBounds = true
        unbackedLabel.isHidden = true

        contentView.addSubview(imageView)
        imageView.addSubview(badgeStack)
        imageView.addSubview(unbackedLabel)
        imageView.addSubview(titleLabel)

        imageView.snp.makeConstraints { make in
            make.edges.equalToSuperview()
        }

        badgeStack.snp.makeConstraints { make in
            make.top.leading.equalToSuperview().inset(6)
        }

        unbackedLabel.snp.makeConstraints { make in
            make.top.trailing.equalToSuperview().inset(6)
        }

        titleLabel.snp.makeConstraints { make in
            make.leading.bottom.equalToSuperview().inset(6)
            make.trailing.lessThanOrEqualToSuperview().inset(6)
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
        setUnbacked(false)
        layer.borderWidth = 0
        layer.borderColor = nil
    }

    func setBadges(_ badges: [(String, UIColor)]) {
        clearBadges()

        for (title, color) in badges {
            let label = UILabel()
            label.font = .systemFont(ofSize: 9, weight: .bold)
            label.textColor = .white
            label.backgroundColor = color
            label.text = " \(title) "
            label.layer.cornerRadius = 4
            label.layer.masksToBounds = true
            badgeStack.addArrangedSubview(label)
        }
    }

    func setUnbacked(_ isUnbacked: Bool) {
        unbackedLabel.isHidden = !isUnbacked
    }

    private func clearBadges() {
        badgeStack.arrangedSubviews.forEach {
            badgeStack.removeArrangedSubview($0)
            $0.removeFromSuperview()
        }
    }
}
