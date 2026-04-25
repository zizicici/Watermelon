import SnapKit
import UIKit

let directionArrowElementKind = "direction-arrow"

// MARK: - Merged Section Header View

final class MergedSectionHeaderView: UICollectionReusableView {
    private let leftHalf = HalfHeaderView()
    private let rightHalf = HalfHeaderView()
    private let divider = UIView()

    private var sectionIndex: Int = 0

    var onLeftTap: ((Int) -> Void)?
    var onRightTap: ((Int) -> Void)?

    override init(frame: CGRect) {
        super.init(frame: frame)
        divider.backgroundColor = .clear

        addSubview(leftHalf)
        addSubview(divider)
        addSubview(rightHalf)

        leftHalf.snp.makeConstraints { make in
            make.top.bottom.leading.equalToSuperview()
            make.trailing.equalTo(divider.snp.leading)
        }
        divider.snp.makeConstraints { make in
            make.centerX.equalToSuperview()
            make.top.bottom.equalToSuperview()
            make.width.equalTo(2)
        }
        rightHalf.snp.makeConstraints { make in
            make.top.bottom.trailing.equalToSuperview()
            make.leading.equalTo(divider.snp.trailing)
        }

        leftHalf.addGestureRecognizer(UITapGestureRecognizer(target: self, action: #selector(leftTapped)))
        rightHalf.addGestureRecognizer(UITapGestureRecognizer(target: self, action: #selector(rightTapped)))
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError() }

    @objc private func leftTapped() { onLeftTap?(sectionIndex) }
    @objc private func rightTapped() { onRightTap?(sectionIndex) }

    func configure(section: HomeMergedYearSection,
                   sectionIndex: Int,
                   leftState: HomeSelectionState,
                   rightState: HomeSelectionState,
                   leftSelectionEnabled: Bool = true,
                   rightSelectionEnabled: Bool = true,
                   selectedColor: UIColor, deselectedColor: UIColor) {
        self.sectionIndex = sectionIndex
        let headerFont = UIFont.monospacedDigitSystemFont(ofSize: 12, weight: .regular)
        let headerColor = UIColor.tertiaryLabel
        leftHalf.configure(
            title: section.title,
            countText: Self.mediaCountAttributedString(photoCount: section.localPhotoCount, videoCount: section.localVideoCount, font: headerFont, color: headerColor),
            sizeText: section.localSizeBytes.map { ByteCountFormatter.string(fromByteCount: $0, countStyle: .file) },
            selectionState: leftState,
            selectionEnabled: leftSelectionEnabled,
            selectedColor: selectedColor,
            deselectedColor: deselectedColor
        )
        rightHalf.configure(
            title: section.title,
            countText: Self.mediaCountAttributedString(photoCount: section.remotePhotoCount, videoCount: section.remoteVideoCount, font: headerFont, color: headerColor),
            sizeText: section.remoteSizeBytes.map { ByteCountFormatter.string(fromByteCount: $0, countStyle: .file) },
            selectionState: rightState,
            selectionEnabled: rightSelectionEnabled,
            selectedColor: selectedColor,
            deselectedColor: deselectedColor
        )
    }

    private static func mediaCountAttributedString(photoCount: Int, videoCount: Int, font: UIFont, color: UIColor) -> NSAttributedString {
        let symbolConfig = UIImage.SymbolConfiguration(pointSize: 9, weight: .bold)
        let result = NSMutableAttributedString()
        if let img = UIImage(systemName: "photo", withConfiguration: symbolConfig)?.withTintColor(color, renderingMode: .alwaysOriginal) {
            result.append(NSAttributedString(attachment: NSTextAttachment(image: img)))
        }
        result.append(NSAttributedString(string: " \(photoCount)  ", attributes: [.font: font, .foregroundColor: color]))
        if let img = UIImage(systemName: "video", withConfiguration: symbolConfig)?.withTintColor(color, renderingMode: .alwaysOriginal) {
            result.append(NSAttributedString(attachment: NSTextAttachment(image: img)))
        }
        result.append(NSAttributedString(string: " \(videoCount)", attributes: [.font: font, .foregroundColor: color]))
        return result
    }
}

// MARK: - Half Header View

final class HalfHeaderView: UIView {
    private let checkmark = UIImageView()
    private let titleLabel = UILabel()
    private let countLabel = UILabel()
    private let sizeLabel = UILabel()

    override init(frame: CGRect) {
        super.init(frame: frame)

        checkmark.contentMode = .scaleAspectFit
        checkmark.tintColor = .tertiaryLabel
        checkmark.isHidden = true

        titleLabel.font = .systemFont(ofSize: 14, weight: .semibold)
        titleLabel.textColor = .secondaryLabel

        countLabel.font = .monospacedDigitSystemFont(ofSize: 12, weight: .regular)
        countLabel.textColor = .tertiaryLabel

        sizeLabel.font = .monospacedDigitSystemFont(ofSize: 12, weight: .regular)
        sizeLabel.textColor = .tertiaryLabel

        addSubview(checkmark)
        addSubview(titleLabel)
        addSubview(sizeLabel)
        addSubview(countLabel)

        titleLabel.setContentHuggingPriority(.required, for: .horizontal)
        titleLabel.setContentCompressionResistancePriority(.required, for: .horizontal)

        checkmark.snp.makeConstraints { make in
            make.leading.equalToSuperview().inset(18)
            make.centerY.equalToSuperview()
            make.size.equalTo(18)
        }
        titleLabel.snp.makeConstraints { make in
            make.leading.equalToSuperview().inset(16)
            make.bottom.equalTo(snp.centerY).offset(-3)
        }
        sizeLabel.snp.makeConstraints { make in
            make.leading.equalTo(titleLabel.snp.trailing).offset(6)
            make.centerY.equalTo(titleLabel)
            make.trailing.lessThanOrEqualToSuperview().inset(16)
        }
        countLabel.snp.makeConstraints { make in
            make.top.equalTo(snp.centerY).offset(3)
            make.leading.equalToSuperview().inset(16)
            make.trailing.lessThanOrEqualToSuperview().inset(16)
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError() }

    func configure(title: String?, countText: NSAttributedString?, sizeText: String?,
                   selectionState: HomeSelectionState,
                   selectionEnabled: Bool = true,
                   selectedColor: UIColor?, deselectedColor: UIColor?) {
        titleLabel.text = title
        countLabel.attributedText = countText
        sizeLabel.text = sizeText
        sizeLabel.isHidden = sizeText == nil

        checkmark.isHidden = false
        switch selectionState {
        case .all:
            checkmark.image = UIImage(systemName: "checkmark.circle.fill")
            checkmark.tintColor = selectionEnabled ? (selectedColor ?? .secondaryLabel) : .quaternaryLabel
        case .partial:
            checkmark.image = UIImage(systemName: "minus.circle.fill")
            checkmark.tintColor = selectionEnabled ? (selectedColor ?? .secondaryLabel) : .quaternaryLabel
        case .none:
            checkmark.image = UIImage(systemName: "circle")
            checkmark.tintColor = selectionEnabled ? (deselectedColor ?? .tertiaryLabel) : .quaternaryLabel
        }
        titleLabel.snp.updateConstraints { make in
            make.leading.equalToSuperview().inset(50)
        }
        countLabel.snp.updateConstraints { make in
            make.leading.equalToSuperview().inset(50)
        }
    }
}

// MARK: - Direction Arrow Badge

final class DirectionArrowView: UICollectionReusableView {
    private static let percentFont: UIFont = .monospacedDigitSystemFont(ofSize: 9, weight: .light)

    private let imageView = UIImageView()
    private let percentLabel: UILabel = {
        let label = UILabel()
        label.font = DirectionArrowView.percentFont
        label.textAlignment = .center
        label.adjustsFontSizeToFitWidth = true
        label.minimumScaleFactor = 0.7
        return label
    }()

    override init(frame: CGRect) {
        super.init(frame: frame)
        isUserInteractionEnabled = false
        backgroundColor = .clear
        imageView.contentMode = .scaleAspectFit
        addSubview(imageView)
        addSubview(percentLabel)
        imageView.snp.makeConstraints { make in
            make.top.leading.trailing.equalToSuperview()
            make.height.equalTo(20)
        }
        percentLabel.snp.makeConstraints { make in
            make.top.equalTo(imageView.snp.bottom)
            make.leading.trailing.equalToSuperview()
            make.bottom.lessThanOrEqualToSuperview()
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError() }

    override func prepareForReuse() {
        super.prepareForReuse()
        imageView.image = nil
        percentLabel.attributedText = nil
        isHidden = true
    }

    func configure(intent: MonthIntent?, percent: Double? = nil) {
        guard let intent else {
            imageView.image = nil
            percentLabel.attributedText = nil
            isHidden = true
            return
        }

        let iconColor = intent.tintColor
        let config = UIImage.SymbolConfiguration(pointSize: 14, weight: .bold)
        imageView.image = UIImage(systemName: intent.iconSymbolName, withConfiguration: config)
        imageView.tintColor = iconColor

        if let percent {
            let text = String(format: "%.1f%%", percent)
            let attrStr = NSAttributedString(string: text, attributes: [
                .kern: -0.5,
                .font: Self.percentFont,
                .foregroundColor: iconColor
            ])
            percentLabel.attributedText = attrStr
            percentLabel.isHidden = false
        } else {
            percentLabel.attributedText = nil
            percentLabel.isHidden = true
        }

        isHidden = false
    }
}

// MARK: - Month Cell

final class MonthCell: UICollectionViewCell {
    private let colorView = UIView()
    private let checkmark = UIImageView()
    private let activityIndicator = UIActivityIndicatorView(style: .medium)
    private let monthLabel = UILabel()
    private let countLabel = UILabel()
    private let sizeLabel = UILabel()
    private var leftStackLeading: Constraint?
    private var currentTitleColor: UIColor = .label
    private var currentDetailColor: UIColor = .secondaryLabel

    override init(frame: CGRect) {
        super.init(frame: frame)
        backgroundColor = .clear
        contentView.backgroundColor = .clear

        contentView.addSubview(colorView)
        colorView.snp.makeConstraints { make in
            make.edges.equalToSuperview()
        }

        checkmark.contentMode = .scaleAspectFit
        checkmark.tintColor = .tertiaryLabel
        checkmark.isHidden = true
        colorView.addSubview(checkmark)
        checkmark.snp.makeConstraints { make in
            make.leading.equalToSuperview().inset(18)
            make.centerY.equalToSuperview()
            make.size.equalTo(18)
        }

        activityIndicator.hidesWhenStopped = true
        colorView.addSubview(activityIndicator)
        activityIndicator.snp.makeConstraints { make in
            make.center.equalTo(checkmark)
        }

        monthLabel.font = .systemFont(ofSize: 15, weight: .medium)
        countLabel.font = .monospacedDigitSystemFont(ofSize: 13, weight: .regular)
        sizeLabel.font = .monospacedDigitSystemFont(ofSize: 13, weight: .regular)

        colorView.addSubview(monthLabel)
        colorView.addSubview(sizeLabel)
        colorView.addSubview(countLabel)

        monthLabel.setContentHuggingPriority(.required, for: .horizontal)
        monthLabel.setContentCompressionResistancePriority(.required, for: .horizontal)

        monthLabel.snp.makeConstraints { make in
            self.leftStackLeading = make.leading.equalToSuperview().inset(16).constraint
            make.bottom.equalTo(colorView.snp.centerY).offset(-3)
        }
        sizeLabel.snp.makeConstraints { make in
            make.leading.equalTo(monthLabel.snp.trailing).offset(6)
            make.centerY.equalTo(monthLabel)
            make.trailing.lessThanOrEqualToSuperview().inset(16)
        }
        countLabel.snp.makeConstraints { make in
            make.leading.equalTo(monthLabel)
            make.top.equalTo(colorView.snp.centerY).offset(3)
            make.trailing.lessThanOrEqualToSuperview().inset(16)
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError() }

    func configure(monthTitle: String, countText: NSAttributedString, sizeText: String?,
                   bgColor: UIColor, titleColor: UIColor, detailColor: UIColor,
                   isSelected: Bool, selectionEnabled: Bool = true) {
        activityIndicator.stopAnimating()
        monthLabel.text = monthTitle
        monthLabel.isHidden = false
        countLabel.attributedText = countText
        countLabel.isHidden = false
        sizeLabel.text = sizeText
        sizeLabel.isHidden = sizeText == nil
        colorView.backgroundColor = bgColor
        monthLabel.textColor = titleColor
        countLabel.textColor = detailColor
        sizeLabel.textColor = detailColor
        currentTitleColor = titleColor
        currentDetailColor = detailColor

        checkmark.isHidden = false
        checkmark.image = UIImage(systemName: isSelected ? "checkmark.circle.fill" : "circle")
        checkmark.tintColor = selectionEnabled ? (isSelected ? titleColor : detailColor) : .quaternaryLabel
        leftStackLeading?.update(inset: 50)
    }

    func configureEmpty(bgColor: UIColor) {
        activityIndicator.stopAnimating()
        monthLabel.text = nil
        monthLabel.isHidden = true
        countLabel.text = nil
        countLabel.isHidden = true
        sizeLabel.text = nil
        sizeLabel.isHidden = true
        checkmark.isHidden = true
        colorView.backgroundColor = bgColor
        leftStackLeading?.update(inset: 50)
    }

    func setSelected(_ selected: Bool) {
        checkmark.isHidden = false
        checkmark.image = UIImage(systemName: selected ? "checkmark.circle.fill" : "circle")
        checkmark.tintColor = selected ? currentTitleColor : currentDetailColor
        leftStackLeading?.update(inset: 50)
    }

    func configureRunning(monthTitle: String, countText: NSAttributedString, sizeText: String?,
                          bgColor: UIColor, titleColor: UIColor, detailColor: UIColor,
                          showSpinner: Bool = true) {
        monthLabel.text = monthTitle
        monthLabel.isHidden = false
        countLabel.attributedText = countText
        countLabel.isHidden = false
        sizeLabel.text = sizeText
        sizeLabel.isHidden = sizeText == nil
        colorView.backgroundColor = bgColor
        monthLabel.textColor = titleColor
        countLabel.textColor = detailColor
        sizeLabel.textColor = detailColor
        currentTitleColor = titleColor
        currentDetailColor = detailColor

        checkmark.isHidden = true
        activityIndicator.color = titleColor
        if showSpinner {
            if !activityIndicator.isAnimating {
                activityIndicator.startAnimating()
            }
        } else {
            activityIndicator.stopAnimating()
        }
        leftStackLeading?.update(inset: 50)
    }

    func showWarningIndicator() {
        checkmark.isHidden = false
        checkmark.image = UIImage(systemName: "exclamationmark.triangle.fill")
        checkmark.tintColor = .systemOrange
        activityIndicator.stopAnimating()
    }

    func showPauseIndicator() {
        checkmark.isHidden = false
        checkmark.image = UIImage(systemName: "pause.circle.fill")
        checkmark.tintColor = currentTitleColor
        activityIndicator.stopAnimating()
    }

    func configureCompleted(monthTitle: String, countText: NSAttributedString, sizeText: String?) {
        monthLabel.text = monthTitle
        monthLabel.isHidden = false
        countLabel.attributedText = countText
        countLabel.isHidden = false
        sizeLabel.text = sizeText
        sizeLabel.isHidden = sizeText == nil

        let grayBg = UIColor.systemGray5
        let grayTitle = UIColor.secondaryLabel
        let grayDetail = UIColor.tertiaryLabel
        colorView.backgroundColor = grayBg
        monthLabel.textColor = grayTitle
        countLabel.textColor = grayDetail
        sizeLabel.textColor = grayDetail
        currentTitleColor = grayTitle
        currentDetailColor = grayDetail

        checkmark.isHidden = false
        checkmark.image = UIImage(systemName: "checkmark.circle.fill")
        checkmark.tintColor = .appTint
        activityIndicator.stopAnimating()
        leftStackLeading?.update(inset: 50)
    }

    func configureFailed(monthTitle: String, countText: NSAttributedString, sizeText: String?) {
        monthLabel.text = monthTitle
        monthLabel.isHidden = false
        countLabel.attributedText = countText
        countLabel.isHidden = false
        sizeLabel.text = sizeText
        sizeLabel.isHidden = sizeText == nil

        let grayBg = UIColor.systemGray5
        let grayTitle = UIColor.secondaryLabel
        let grayDetail = UIColor.tertiaryLabel
        colorView.backgroundColor = grayBg
        monthLabel.textColor = grayTitle
        countLabel.textColor = grayDetail
        sizeLabel.textColor = grayDetail
        currentTitleColor = grayTitle
        currentDetailColor = grayDetail

        checkmark.isHidden = false
        checkmark.image = UIImage(systemName: "exclamationmark.circle.fill")
        checkmark.tintColor = .systemRed
        activityIndicator.stopAnimating()
        leftStackLeading?.update(inset: 50)
    }

    override func prepareForReuse() {
        super.prepareForReuse()
        activityIndicator.stopAnimating()
    }
}
