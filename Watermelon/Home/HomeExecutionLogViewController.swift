import MoreKit
import SnapKit
import UIKit

@MainActor
final class HomeExecutionLogViewController: UIViewController {
    private static let timestampFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm:ss"
        return formatter
    }()

    private let coordinator: HomeExecutionCoordinator

    private let statusCardView = UIView()
    private let statusTitleLabel = UILabel()
    private let statusLabel = UILabel()
    private let logTextView = UITextView()
    private let copyButton = UIButton(type: .system)

    private lazy var filterBarButtonItem = UIBarButtonItem(
        title: "筛选",
        image: UIImage(systemName: "line.3.horizontal.decrease.circle"),
        primaryAction: nil,
        menu: makeFilterMenu()
    )

    private var logObserverID: UUID?
    private var snapshot = HomeExecutionLogSnapshot(statusText: "未开始", entries: [])
    private var selectedLevels = ExecutionLogFilterPreference.getValue().enabledLevels

    init(coordinator: HomeExecutionCoordinator) {
        self.coordinator = coordinator
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        guard let logObserverID else { return }
        let coordinator = coordinator
        Task { @MainActor in
            coordinator.removeLogObserver(logObserverID)
        }
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        title = "执行日志"
        navigationItem.rightBarButtonItem = filterBarButtonItem

        buildUI()

        logObserverID = coordinator.addLogObserver { [weak self] snapshot in
            self?.apply(snapshot)
        }
    }

    private func buildUI() {
        statusCardView.backgroundColor = .secondarySystemBackground
        statusCardView.layer.cornerRadius = 12
        statusCardView.layer.masksToBounds = true

        statusTitleLabel.font = .systemFont(ofSize: 13, weight: .semibold)
        statusTitleLabel.textColor = .secondaryLabel
        statusTitleLabel.text = "当前状态"

        statusLabel.font = .systemFont(ofSize: 18, weight: .semibold)
        statusLabel.textColor = .label
        statusLabel.numberOfLines = 0

        logTextView.font = .monospacedSystemFont(ofSize: 12, weight: .regular)
        logTextView.backgroundColor = .secondarySystemBackground
        logTextView.textColor = .label
        logTextView.layer.cornerRadius = 12
        logTextView.isEditable = false
        logTextView.isSelectable = true
        logTextView.alwaysBounceVertical = true
        logTextView.textContainerInset = UIEdgeInsets(top: 10, left: 10, bottom: 10, right: 10)

        var copyConfig = UIButton.Configuration.filled()
        copyConfig.title = "复制当前日志"
        copyConfig.cornerStyle = .medium
        copyConfig.baseBackgroundColor = .materialPrimary(light: .Material.Green._600, dark: .Material.Green._200)
        copyConfig.baseForegroundColor = .materialOnPrimary(dark: .Material.Green._800)
        copyButton.configuration = copyConfig
        copyButton.addTarget(self, action: #selector(copyTapped), for: .touchUpInside)

        view.addSubview(statusCardView)
        statusCardView.addSubview(statusTitleLabel)
        statusCardView.addSubview(statusLabel)
        view.addSubview(logTextView)
        view.addSubview(copyButton)

        statusCardView.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).offset(12)
            make.leading.trailing.equalToSuperview().inset(12)
        }
        statusTitleLabel.snp.makeConstraints { make in
            make.top.leading.trailing.equalToSuperview().inset(12)
        }
        statusLabel.snp.makeConstraints { make in
            make.top.equalTo(statusTitleLabel.snp.bottom).offset(8)
            make.leading.trailing.bottom.equalToSuperview().inset(12)
        }
        logTextView.snp.makeConstraints { make in
            make.top.equalTo(statusCardView.snp.bottom).offset(12)
            make.leading.trailing.equalToSuperview().inset(12)
            make.bottom.equalTo(copyButton.snp.top).offset(-12)
        }
        copyButton.snp.makeConstraints { make in
            make.leading.trailing.equalToSuperview().inset(12)
            make.bottom.equalTo(view.safeAreaLayoutGuide).inset(12)
            make.height.greaterThanOrEqualTo(44)
        }
    }

    private func apply(_ snapshot: HomeExecutionLogSnapshot) {
        let shouldStickToBottom = self.snapshot.entries.isEmpty || isNearBottom()
        self.snapshot = snapshot
        statusLabel.text = snapshot.statusText
        let filteredEntries = filteredEntries(from: snapshot)
        logTextView.attributedText = formattedLogAttributedText(for: filteredEntries)
        copyButton.isEnabled = !filteredEntries.isEmpty

        guard shouldStickToBottom else { return }
        scrollToBottom()
    }

    private func filteredEntries(from snapshot: HomeExecutionLogSnapshot) -> [HomeExecutionLogEntry] {
        snapshot.entries.filter { selectedLevels.contains($0.level) }
    }

    private func formattedLogText(for snapshot: HomeExecutionLogSnapshot) -> String {
        filteredEntries(from: snapshot)
            .map { entry in
                "[\(Self.timestampFormatter.string(from: entry.timestamp))] [\(tag(for: entry.level))] \(entry.message)"
            }
            .joined(separator: "\n")
    }

    private func formattedLogAttributedText(for entries: [HomeExecutionLogEntry]) -> NSAttributedString {
        let result = NSMutableAttributedString()
        let timestampFont = UIFont.monospacedDigitSystemFont(ofSize: 12, weight: .semibold)
        let messageFont = UIFont.monospacedSystemFont(ofSize: 12, weight: .regular)

        guard !entries.isEmpty else {
            return NSAttributedString(
                string: "当前筛选下没有日志。",
                attributes: [
                    .font: messageFont,
                    .foregroundColor: secondaryLogColor()
                ]
            )
        }

        for (index, entry) in entries.enumerated() {
            let timestamp = "[\(Self.timestampFormatter.string(from: entry.timestamp))] "
            let levelTag = "[\(tag(for: entry.level))] "
            result.append(NSAttributedString(
                string: timestamp,
                attributes: [
                    .font: timestampFont,
                    .foregroundColor: secondaryLogColor()
                ]
            ))
            result.append(NSAttributedString(
                string: levelTag,
                attributes: [
                    .font: timestampFont,
                    .foregroundColor: color(for: entry.level)
                ]
            ))
            result.append(NSAttributedString(
                string: entry.message,
                attributes: [
                    .font: messageFont,
                    .foregroundColor: color(for: entry.level)
                ]
            ))
            if index < entries.count - 1 {
                result.append(NSAttributedString(string: "\n"))
            }
        }

        return result
    }

    private func isNearBottom() -> Bool {
        let visibleBottom = logTextView.contentOffset.y + logTextView.bounds.height - logTextView.adjustedContentInset.bottom
        let contentBottom = logTextView.contentSize.height
        return visibleBottom >= contentBottom - 80
    }

    private func scrollToBottom() {
        let bottomOffset = logTextView.contentSize.height - logTextView.bounds.height + logTextView.adjustedContentInset.bottom
        guard bottomOffset > 0 else { return }
        logTextView.setContentOffset(CGPoint(x: 0, y: bottomOffset), animated: false)
    }

    private func makeFilterMenu() -> UIMenu {
        let allAction = UIAction(
            title: "显示全部",
            image: UIImage(systemName: "line.3.horizontal.decrease.circle"),
            state: selectedLevels.count == ExecutionLogLevel.allCases.count ? .on : .off
        ) { [weak self] _ in
            guard let self else { return }
            self.selectedLevels = Set(ExecutionLogLevel.allCases)
            self.persistSelectedLevels()
            self.refreshFilteredDisplay()
        }

        let levelActions = ExecutionLogLevel.allCases.map { level in
            UIAction(
                title: title(for: level),
                state: selectedLevels.contains(level) ? .on : .off
            ) { [weak self] _ in
                guard let self else { return }
                if self.selectedLevels.contains(level) {
                    self.selectedLevels.remove(level)
                } else {
                    self.selectedLevels.insert(level)
                }
                self.persistSelectedLevels()
                self.refreshFilteredDisplay()
            }
        }

        return UIMenu(
            title: "筛选日志级别",
            children: [
                allAction,
                UIMenu(title: "日志级别", options: .displayInline, children: levelActions)
            ]
        )
    }

    private func refreshFilterMenu() {
        filterBarButtonItem.menu = makeFilterMenu()
    }

    private func refreshFilteredDisplay() {
        refreshFilterMenu()
        apply(snapshot)
    }

    private func title(for level: ExecutionLogLevel) -> String {
        switch level {
        case .debug:
            return "调试"
        case .info:
            return "信息"
        case .warning:
            return "警告"
        case .error:
            return "错误"
        }
    }

    private func color(for level: ExecutionLogLevel) -> UIColor {
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

    private func secondaryLogColor() -> UIColor {
        .materialOnSurfaceVariant(light: .Material.BlueGrey._700, dark: .Material.BlueGrey._200)
    }

    private func tag(for level: ExecutionLogLevel) -> String {
        switch level {
        case .debug:
            return "DEBUG"
        case .info:
            return "INFO"
        case .warning:
            return "WARN"
        case .error:
            return "ERROR"
        }
    }

    private func persistSelectedLevels() {
        var preference = ExecutionLogFilterPreference(rawValue: 0)
        for level in ExecutionLogLevel.allCases {
            preference = preference.updating(level, isEnabled: selectedLevels.contains(level))
        }
        ExecutionLogFilterPreference.setValue(preference)
    }

    @objc
    private func copyTapped() {
        UIPasteboard.general.string = formattedLogText(for: snapshot)

        let alert = UIAlertController(title: nil, message: "已复制执行日志", preferredStyle: .alert)
        present(alert, animated: true)
        Task { @MainActor [weak alert] in
            try? await Task.sleep(nanoseconds: 800_000_000)
            alert?.dismiss(animated: true)
        }
    }
}
