import SnapKit
import UIKit

extension ExecutionLogPalette {
    static func color(for level: ExecutionLogLevel) -> UIColor {
        switch level {
        case .debug:
            return .materialOnSurfaceVariant(light: .Material.BlueGrey._700, dark: .Material.BlueGrey._200)
        case .info:
            return .materialPrimary(light: .Material.Blue._600, dark: .Material.Blue._200)
        case .warning:
            return .materialPrimary(light: .Material.Orange._700, dark: .Material.Orange._200)
        case .error:
            return .materialPrimary(light: .Material.Red._700, dark: .Material.Red._200)
        }
    }

    static let secondary: UIColor = .materialOnSurfaceVariant(
        light: .Material.BlueGrey._700,
        dark: .Material.BlueGrey._200
    )
}

final class ExecutionLogEntryCell: UITableViewCell {
    static let reuseIdentifier = "ExecutionLogEntryCell"

    private let timestampLabel = UILabel()
    private let levelLabel = UILabel()
    private let messageLabel = UILabel()

    override init(style: UITableViewCell.CellStyle, reuseIdentifier: String?) {
        super.init(style: style, reuseIdentifier: reuseIdentifier)

        selectionStyle = .none
        backgroundColor = .clear

        timestampLabel.font = .monospacedDigitSystemFont(ofSize: 12, weight: .semibold)
        timestampLabel.textColor = ExecutionLogPalette.secondary
        timestampLabel.setContentHuggingPriority(.required, for: .horizontal)
        timestampLabel.setContentCompressionResistancePriority(.required, for: .horizontal)

        levelLabel.font = .monospacedDigitSystemFont(ofSize: 12, weight: .semibold)
        levelLabel.setContentHuggingPriority(.required, for: .horizontal)
        levelLabel.setContentCompressionResistancePriority(.required, for: .horizontal)

        messageLabel.font = .monospacedSystemFont(ofSize: 12, weight: .regular)
        messageLabel.numberOfLines = 0

        contentView.addSubview(timestampLabel)
        contentView.addSubview(levelLabel)
        contentView.addSubview(messageLabel)

        timestampLabel.snp.makeConstraints { make in
            make.leading.equalToSuperview().inset(12)
            make.top.equalToSuperview().inset(6)
        }

        levelLabel.snp.makeConstraints { make in
            make.leading.equalTo(timestampLabel.snp.trailing).offset(6)
            make.top.equalTo(timestampLabel)
        }

        messageLabel.snp.makeConstraints { make in
            make.leading.equalTo(levelLabel.snp.trailing).offset(6)
            make.trailing.equalToSuperview().inset(12)
            make.top.equalTo(timestampLabel)
            make.bottom.equalToSuperview().inset(6)
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func configure(with entry: ExecutionLogEntry) {
        timestampLabel.text = "[\(ExecutionLogPalette.timestampFormatter.string(from: entry.timestamp))]"
        let color = ExecutionLogPalette.color(for: entry.level)
        levelLabel.text = "[\(ExecutionLogPalette.tag(for: entry.level))]"
        levelLabel.textColor = color
        messageLabel.text = entry.message
        messageLabel.textColor = color
    }
}
