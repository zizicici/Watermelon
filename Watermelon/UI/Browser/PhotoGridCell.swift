import SnapKit
import UIKit

final class PhotoGridCell: UICollectionViewCell {
    static let reuseID = "PhotoGridCell"

    let imageView = UIImageView()
    let titleLabel = UILabel()
    var representedID: String?

    override init(frame: CGRect) {
        super.init(frame: frame)

        contentView.layer.cornerRadius = 10
        contentView.layer.masksToBounds = true
        contentView.backgroundColor = .secondarySystemBackground

        imageView.contentMode = .scaleAspectFill
        imageView.clipsToBounds = true
        imageView.backgroundColor = .tertiarySystemFill

        titleLabel.font = .systemFont(ofSize: 11, weight: .medium)
        titleLabel.numberOfLines = 2
        titleLabel.textColor = .label

        contentView.addSubview(imageView)
        contentView.addSubview(titleLabel)

        imageView.snp.makeConstraints { make in
            make.leading.trailing.top.equalToSuperview()
            make.height.equalTo(contentView.snp.width)
        }

        titleLabel.snp.makeConstraints { make in
            make.top.equalTo(imageView.snp.bottom).offset(4)
            make.leading.trailing.equalToSuperview().inset(6)
            make.bottom.lessThanOrEqualToSuperview().inset(6)
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }
}
