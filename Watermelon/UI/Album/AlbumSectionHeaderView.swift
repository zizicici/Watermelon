import SnapKit
import UIKit

final class AlbumSectionHeaderView: UICollectionReusableView {
    static let reuseID = "AlbumSectionHeaderView"

    let titleLabel = UILabel()
    private var leadingConstraint: Constraint?
    private var trailingConstraint: Constraint?

    override init(frame: CGRect) {
        super.init(frame: frame)

        titleLabel.font = .systemFont(ofSize: 16, weight: .semibold)
        titleLabel.textColor = .label

        addSubview(titleLabel)
        titleLabel.snp.makeConstraints { make in
            leadingConstraint = make.leading.equalToSuperview().inset(4).constraint
            trailingConstraint = make.trailing.equalToSuperview().inset(4).constraint
            make.centerY.equalToSuperview()
        }
    }

    func setHorizontalInset(_ inset: CGFloat) {
        leadingConstraint?.update(inset: inset)
        trailingConstraint?.update(inset: inset)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }
}
