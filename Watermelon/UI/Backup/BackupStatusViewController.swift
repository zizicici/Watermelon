import Photos
import SnapKit
import UIKit

@MainActor
final class BackupStatusViewController: UIViewController {
    private enum FilterMode: Int, CaseIterable {
        case all
        case success
        case failed
        case skipped
        case log

        var baseTitle: String {
            switch self {
            case .all: return "全部"
            case .success: return "成功"
            case .failed: return "失败"
            case .skipped: return "跳过"
            case .log: return "日志"
            }
        }
    }

    private let sessionController: BackupSessionController

    private let statusLabel = UILabel()
    private let filterControl = TwoLineSegmentedControl(items: FilterMode.allCases.map { $0.baseTitle })
    private let tableView = UITableView(frame: .zero, style: .plain)
    private let logTextView = UITextView()

    private lazy var startBarButtonItem = UIBarButtonItem(
        image: UIImage(systemName: "play.fill"),
        style: .plain,
        target: self,
        action: #selector(startTapped)
    )
    private lazy var pauseBarButtonItem = UIBarButtonItem(
        image: UIImage(systemName: "pause.fill"),
        style: .plain,
        target: self,
        action: #selector(pauseTapped)
    )
    private lazy var stopBarButtonItem = UIBarButtonItem(
        image: UIImage(systemName: "stop.fill"),
        style: .plain,
        target: self,
        action: #selector(stopTapped)
    )

    private let imageManager = PHCachingImageManager()
    private let thumbnailCache = NSCache<NSString, UIImage>()

    private var observerID: UUID?
    private var renderedLogCount: Int = 0
    private var latestSnapshot: BackupSessionController.Snapshot?
    private var filteredItems: [BackupSessionController.ProcessedItem] = []

    init(sessionController: BackupSessionController) {
        self.sessionController = sessionController
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        title = "备份状态"
        navigationItem.largeTitleDisplayMode = .never

        startBarButtonItem.accessibilityLabel = "开始备份"
        pauseBarButtonItem.accessibilityLabel = "暂停备份"
        stopBarButtonItem.accessibilityLabel = "中止备份"
        stopBarButtonItem.tintColor = .systemRed
        navigationItem.rightBarButtonItems = [stopBarButtonItem, pauseBarButtonItem, startBarButtonItem]

        buildUI()

        observerID = sessionController.addObserver { [weak self] snapshot in
            self?.render(snapshot: snapshot)
        }
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        if (isBeingDismissed || isMovingFromParent), let observerID {
            sessionController.removeObserver(observerID)
            self.observerID = nil
        }
    }

    private func buildUI() {
        statusLabel.font = .systemFont(ofSize: 13, weight: .medium)
        statusLabel.textColor = .secondaryLabel
        statusLabel.numberOfLines = 2
        statusLabel.text = "未开始"

        filterControl.selectedIndex = FilterMode.all.rawValue
        filterControl.addTarget(self, action: #selector(filterChanged), for: .valueChanged)

        tableView.register(BackupStatusItemCell.self, forCellReuseIdentifier: BackupStatusItemCell.reuseID)
        tableView.dataSource = self
        tableView.delegate = self
        tableView.rowHeight = 88

        logTextView.font = .monospacedSystemFont(ofSize: 12, weight: .regular)
        logTextView.isEditable = false
        logTextView.backgroundColor = .secondarySystemBackground
        logTextView.layer.cornerRadius = 10
        logTextView.textContainerInset = UIEdgeInsets(top: 8, left: 8, bottom: 8, right: 8)
        logTextView.isHidden = true

        view.addSubview(statusLabel)
        view.addSubview(filterControl)
        view.addSubview(tableView)
        view.addSubview(logTextView)

        statusLabel.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).offset(8)
            make.leading.trailing.equalToSuperview().inset(12)
        }

        filterControl.snp.makeConstraints { make in
            make.top.equalTo(statusLabel.snp.bottom).offset(8)
            make.leading.trailing.equalToSuperview().inset(12)
            make.height.greaterThanOrEqualTo(58)
        }

        tableView.snp.makeConstraints { make in
            make.top.equalTo(filterControl.snp.bottom).offset(10)
            make.leading.trailing.equalToSuperview().inset(12)
            make.bottom.equalTo(view.safeAreaLayoutGuide).inset(12)
        }

        logTextView.snp.makeConstraints { make in
            make.top.equalTo(filterControl.snp.bottom).offset(10)
            make.leading.trailing.equalToSuperview().inset(12)
            make.bottom.equalTo(view.safeAreaLayoutGuide).inset(12)
        }
    }

    private func render(snapshot: BackupSessionController.Snapshot) {
        latestSnapshot = snapshot
        statusLabel.text = snapshot.statusText
        updateFilterTitles(using: snapshot)
        updateLogContent(snapshot.logs)
        applyFilter()

        switch snapshot.state {
        case .running:
            startBarButtonItem.isEnabled = false
            pauseBarButtonItem.isEnabled = true
            stopBarButtonItem.isEnabled = true
        case .paused:
            startBarButtonItem.isEnabled = true
            pauseBarButtonItem.isEnabled = false
            stopBarButtonItem.isEnabled = true
        case .idle, .stopped, .failed, .completed:
            startBarButtonItem.isEnabled = true
            pauseBarButtonItem.isEnabled = false
            stopBarButtonItem.isEnabled = false
        }
    }

    private func updateFilterTitles(using snapshot: BackupSessionController.Snapshot) {
        let all = snapshot.processedItems.count
        let success = snapshot.processedItems.filter { $0.status == .success }.count
        let failed = snapshot.processedItems.filter { $0.status == .failed }.count
        let skipped = snapshot.processedItems.filter { $0.status == .skipped }.count
        let logs = snapshot.logs.count

        filterControl.setSubtitle("\(all) 项", forItemAt: FilterMode.all.rawValue)
        filterControl.setSubtitle("\(success) 项", forItemAt: FilterMode.success.rawValue)
        filterControl.setSubtitle("\(failed) 项", forItemAt: FilterMode.failed.rawValue)
        filterControl.setSubtitle("\(skipped) 项", forItemAt: FilterMode.skipped.rawValue)
        filterControl.setSubtitle("\(logs) 条", forItemAt: FilterMode.log.rawValue)
    }

    private func updateLogContent(_ logs: [String]) {
        if logs.count < renderedLogCount {
            logTextView.text = logs.joined(separator: "\n")
            renderedLogCount = logs.count
        } else if logs.count > renderedLogCount {
            let newLines = logs[renderedLogCount...].joined(separator: "\n")
            if logTextView.text.isEmpty {
                logTextView.text = newLines
            } else {
                logTextView.text.append("\n" + newLines)
            }
            renderedLogCount = logs.count
        }

        if renderedLogCount > 0 {
            let range = NSRange(location: max(logTextView.text.count - 1, 0), length: 1)
            logTextView.scrollRangeToVisible(range)
        }
    }

    private func applyFilter() {
        guard let snapshot = latestSnapshot else { return }
        let mode = FilterMode(rawValue: filterControl.selectedIndex) ?? .all

        if mode == .log {
            tableView.isHidden = true
            logTextView.isHidden = false
            return
        }

        tableView.isHidden = false
        logTextView.isHidden = true

        switch mode {
        case .all:
            filteredItems = snapshot.processedItems
        case .success:
            filteredItems = snapshot.processedItems.filter { $0.status == .success }
        case .failed:
            filteredItems = snapshot.processedItems.filter { $0.status == .failed }
        case .skipped:
            filteredItems = snapshot.processedItems.filter { $0.status == .skipped }
        case .log:
            filteredItems = []
        }

        tableView.reloadData()
    }

    @objc
    private func filterChanged() {
        applyFilter()
    }

    @objc
    private func startTapped() {
        sessionController.startBackup()
    }

    @objc
    private func pauseTapped() {
        sessionController.pauseBackup()
    }

    @objc
    private func stopTapped() {
        sessionController.stopBackup()
    }

    private func statusText(for status: BackupItemStatus) -> String {
        switch status {
        case .success: return "成功"
        case .failed: return "失败"
        case .skipped: return "跳过"
        }
    }

    private func thumbnail(for item: BackupSessionController.ProcessedItem) -> UIImage? {
        thumbnailCache.object(forKey: item.assetLocalIdentifier as NSString)
    }

    private func requestThumbnailIfNeeded(for item: BackupSessionController.ProcessedItem) {
        let key = item.assetLocalIdentifier as NSString
        if thumbnailCache.object(forKey: key) != nil { return }

        let auth = PHPhotoLibrary.authorizationStatus(for: .readWrite)
        guard auth == .authorized || auth == .limited else { return }

        let fetched = PHAsset.fetchAssets(withLocalIdentifiers: [item.assetLocalIdentifier], options: nil)
        guard let asset = fetched.firstObject else { return }

        let options = PHImageRequestOptions()
        options.deliveryMode = .fastFormat
        options.resizeMode = .fast
        options.isNetworkAccessAllowed = true

        imageManager.requestImage(
            for: asset,
            targetSize: CGSize(width: 140, height: 140),
            contentMode: .aspectFit,
            options: options
        ) { [weak self] image, _ in
            guard let self, let image else { return }
            self.thumbnailCache.setObject(image, forKey: key)

            guard let row = self.filteredItems.firstIndex(where: { $0.id == item.id }) else { return }
            let indexPath = IndexPath(row: row, section: 0)
            guard let cell = self.tableView.cellForRow(at: indexPath) else { return }
            self.configure(cell, with: item)
        }
    }

    private func configure(_ cell: UITableViewCell, with item: BackupSessionController.ProcessedItem) {
        guard let cell = cell as? BackupStatusItemCell else { return }
        let summary = item.resourceSummary?.trimmingCharacters(in: .whitespacesAndNewlines)
        let reason = item.reason?.trimmingCharacters(in: .whitespacesAndNewlines)
        let detailText: String?
        if let summary, !summary.isEmpty, let reason, !reason.isEmpty {
            detailText = summary + " | " + reason
        } else if let summary, !summary.isEmpty {
            detailText = summary
        } else {
            detailText = reason
        }
        cell.apply(
            title: item.displayName,
            detail: detailText,
            statusText: statusText(for: item.status),
            status: item.status,
            thumbnail: thumbnail(for: item)
        )
    }
}

extension BackupStatusViewController: UITableViewDataSource, UITableViewDelegate {
    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        filteredItems.count
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: BackupStatusItemCell.reuseID, for: indexPath)
        guard indexPath.row < filteredItems.count else { return cell }

        let item = filteredItems[indexPath.row]
        configure(cell, with: item)
        requestThumbnailIfNeeded(for: item)
        return cell
    }
}

private final class BackupStatusItemCell: UITableViewCell {
    static let reuseID = "backup_item"

    private let thumbnailContainerView = UIView()
    private let thumbnailImageView = UIImageView()
    private let titleLabel = UILabel()
    private let detailLabel = UILabel()
    private let statusLabel = UILabel()

    override init(style: UITableViewCell.CellStyle, reuseIdentifier: String?) {
        super.init(style: style, reuseIdentifier: reuseIdentifier)
        buildUI()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func prepareForReuse() {
        super.prepareForReuse()
        thumbnailImageView.image = UIImage(systemName: "photo")
        titleLabel.text = nil
        detailLabel.text = " "
        statusLabel.text = nil
        statusLabel.textColor = .secondaryLabel
    }

    func apply(
        title: String,
        detail: String?,
        statusText: String,
        status: BackupItemStatus,
        thumbnail: UIImage?
    ) {
        titleLabel.text = title
        detailLabel.text = (detail?.isEmpty == false) ? detail : " "
        statusLabel.text = statusText
        thumbnailImageView.image = thumbnail ?? UIImage(systemName: "photo")

        switch status {
        case .success:
            statusLabel.textColor = .systemGreen
        case .failed:
            statusLabel.textColor = .systemRed
        case .skipped:
            statusLabel.textColor = .systemOrange
        }
    }

    private func buildUI() {
        selectionStyle = .none
        preservesSuperviewLayoutMargins = true
        contentView.directionalLayoutMargins = NSDirectionalEdgeInsets(top: 8, leading: 0, bottom: 8, trailing: 0)

        thumbnailContainerView.backgroundColor = .secondarySystemBackground
        thumbnailContainerView.clipsToBounds = true
        thumbnailContainerView.layer.cornerRadius = 6

        thumbnailImageView.contentMode = .scaleAspectFit
        thumbnailImageView.image = UIImage(systemName: "photo")
        thumbnailImageView.tintColor = .secondaryLabel

        titleLabel.font = .systemFont(ofSize: 15, weight: .semibold)
        titleLabel.textColor = .label
        titleLabel.numberOfLines = 1
        titleLabel.lineBreakMode = .byTruncatingMiddle

        detailLabel.font = .systemFont(ofSize: 12, weight: .regular)
        detailLabel.textColor = .secondaryLabel
        detailLabel.numberOfLines = 1
        detailLabel.lineBreakMode = .byTruncatingTail

        statusLabel.font = .systemFont(ofSize: 12, weight: .semibold)
        statusLabel.numberOfLines = 1
        statusLabel.textColor = .secondaryLabel

        contentView.addSubview(thumbnailContainerView)
        thumbnailContainerView.addSubview(thumbnailImageView)
        contentView.addSubview(titleLabel)
        contentView.addSubview(detailLabel)
        contentView.addSubview(statusLabel)

        thumbnailContainerView.snp.makeConstraints { make in
            make.leading.equalToSuperview()
            make.centerY.equalToSuperview()
            make.top.greaterThanOrEqualTo(contentView.layoutMarginsGuide.snp.top)
            make.bottom.lessThanOrEqualTo(contentView.layoutMarginsGuide.snp.bottom)
            make.height.equalTo(contentView.snp.height).offset(-16)
            make.width.equalTo(thumbnailContainerView.snp.height)
        }

        thumbnailImageView.snp.makeConstraints { make in
            make.edges.equalToSuperview().inset(4)
        }

        titleLabel.snp.makeConstraints { make in
            make.top.equalTo(contentView.layoutMarginsGuide.snp.top)
            make.leading.equalTo(thumbnailContainerView.snp.trailing).offset(12)
            make.trailing.equalToSuperview()
        }

        detailLabel.snp.makeConstraints { make in
            make.top.equalTo(titleLabel.snp.bottom).offset(2)
            make.leading.trailing.equalTo(titleLabel)
        }

        statusLabel.snp.makeConstraints { make in
            make.leading.trailing.equalTo(titleLabel)
            make.bottom.equalTo(contentView.layoutMarginsGuide.snp.bottom)
        }
    }
}

private final class TwoLineSegmentedControl: UIControl {
    private var titles: [String]
    private var subtitles: [String]
    private var buttons: [UIButton] = []
    private let stackView = UIStackView()

    var selectedIndex: Int = 0 {
        didSet {
            guard selectedIndex != oldValue else { return }
            updateButtonStyles()
        }
    }

    init(items: [String]) {
        self.titles = items
        self.subtitles = Array(repeating: "", count: items.count)
        super.init(frame: .zero)
        buildUI()
        updateButtonStyles()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func setSubtitle(_ subtitle: String, forItemAt index: Int) {
        guard index >= 0, index < subtitles.count else { return }
        subtitles[index] = subtitle
        applyConfiguration(to: buttons[index], index: index, isSelected: index == selectedIndex)
    }

    private func buildUI() {
        stackView.axis = .horizontal
        stackView.spacing = 8
        stackView.distribution = .fillEqually
        addSubview(stackView)
        stackView.snp.makeConstraints { make in
            make.edges.equalToSuperview()
        }

        for index in titles.indices {
            let button = UIButton(type: .system)
            button.tag = index
            button.addTarget(self, action: #selector(itemTapped(_:)), for: .touchUpInside)
            button.layer.cornerRadius = 10
            button.clipsToBounds = true
            buttons.append(button)
            stackView.addArrangedSubview(button)
        }
    }

    @objc
    private func itemTapped(_ sender: UIButton) {
        let index = sender.tag
        guard index != selectedIndex else { return }
        selectedIndex = index
        sendActions(for: .valueChanged)
    }

    private func updateButtonStyles() {
        for (index, button) in buttons.enumerated() {
            applyConfiguration(to: button, index: index, isSelected: index == selectedIndex)
        }
    }

    private func applyConfiguration(to button: UIButton, index: Int, isSelected: Bool) {
        var config = UIButton.Configuration.filled()
        config.title = titles[index]
        config.subtitle = subtitles[index]
        config.titleAlignment = .center
        config.titleTextAttributesTransformer = UIConfigurationTextAttributesTransformer { incoming in
            var attrs = incoming
            attrs.font = .systemFont(ofSize: 13, weight: .semibold)
            return attrs
        }
        config.subtitleTextAttributesTransformer = UIConfigurationTextAttributesTransformer { incoming in
            var attrs = incoming
            attrs.font = .systemFont(ofSize: 11, weight: .regular)
            return attrs
        }
        config.contentInsets = NSDirectionalEdgeInsets(top: 6, leading: 8, bottom: 6, trailing: 8)

        if isSelected {
            config.baseBackgroundColor = .systemBlue
            config.baseForegroundColor = .white
        } else {
            config.baseBackgroundColor = .secondarySystemBackground
            config.baseForegroundColor = .label
        }

        button.configuration = config
    }
}
