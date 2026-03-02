import Photos
import SnapKit
import UIKit

@MainActor
final class BackupViewController: UIViewController {
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
    private let dependencies: DependencyContainer

    private let scopeCardView = UIView()
    private let scopeTitleLabel = UILabel()
    private let scopeSummaryLabel = UILabel()
    private let scopeDetailLabel = UILabel()
    private let scopeAdjustButton = UIButton(type: .system)
    private let statusCardView = UIView()
    private let statusThumbnailContainer = UIView()
    private let statusThumbnailImageView = UIImageView()
    private let statusTitleLabel = UILabel()
    private let statusRightStack = UIStackView()
    private let statusTopRowStack = UIStackView()
    private let statusDateLabel = UILabel()
    private let resourcePercentLabel = UILabel()
    private let overallProgressLeadingLabel = UILabel()
    private let overallProgressPercentLabel = UILabel()
    private let overallProgressView = UIProgressView(progressViewStyle: .default)
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
    private var deferredTableReload = false
    private var statusThumbnailAssetID: String?
    private var statusThumbnailRequestID: PHImageRequestID?
    private var thumbnailRequestIDs: [String: PHImageRequestID] = [:]
    private var pendingOpenScopeSelectorAfterStop = false

    private static let resourceDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter
    }()

    private static let byteCountFormatter: ByteCountFormatter = {
        let formatter = ByteCountFormatter()
        formatter.allowedUnits = [.useKB, .useMB, .useGB, .useTB]
        formatter.countStyle = .file
        formatter.includesUnit = true
        formatter.isAdaptive = true
        return formatter
    }()

    init(
        sessionController: BackupSessionController,
        dependencies: DependencyContainer
    ) {
        self.sessionController = sessionController
        self.dependencies = dependencies
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        title = "备份"
        navigationItem.largeTitleDisplayMode = .never

        startBarButtonItem.accessibilityLabel = "开始备份"
        pauseBarButtonItem.accessibilityLabel = "暂停备份"
        stopBarButtonItem.accessibilityLabel = "中止备份"
        stopBarButtonItem.tintColor = .systemRed
        navigationItem.rightBarButtonItems = [stopBarButtonItem, pauseBarButtonItem, startBarButtonItem]

        // Backup list keeps changing; cap cache to avoid unbounded growth during long runs.
        thumbnailCache.countLimit = 240
        thumbnailCache.totalCostLimit = 40 * 1024 * 1024

        buildUI()

        Task { [weak self] in
            guard let self else { return }
            await self.sessionController.ensureDefaultScopeSummaryLoaded()
        }

        observerID = sessionController.addObserver { [weak self] snapshot in
            self?.render(snapshot: snapshot)
        }
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        if (isBeingDismissed || isMovingFromParent), let observerID {
            sessionController.removeObserver(observerID)
            self.observerID = nil
            cancelAllImageRequests()
            thumbnailCache.removeAllObjects()
        }
    }

    private func buildUI() {
        scopeCardView.backgroundColor = .secondarySystemBackground
        scopeCardView.layer.cornerRadius = 12
        scopeCardView.layer.masksToBounds = true

        scopeTitleLabel.font = .systemFont(ofSize: 14, weight: .semibold)
        scopeTitleLabel.textColor = .label
        scopeTitleLabel.text = "备份范围"

        scopeSummaryLabel.font = .systemFont(ofSize: 13, weight: .medium)
        scopeSummaryLabel.textColor = .secondaryLabel
        scopeSummaryLabel.text = "全选"

        scopeDetailLabel.font = .systemFont(ofSize: 12, weight: .regular)
        scopeDetailLabel.textColor = .secondaryLabel
        scopeDetailLabel.numberOfLines = 2
        scopeDetailLabel.text = "总计 0 张 · 0 KB"

        scopeAdjustButton.setTitle("调整", for: .normal)
        scopeAdjustButton.titleLabel?.font = .systemFont(ofSize: 13, weight: .semibold)
        scopeAdjustButton.addTarget(self, action: #selector(scopeAdjustTapped), for: .touchUpInside)

        statusCardView.backgroundColor = .secondarySystemBackground
        statusCardView.layer.cornerRadius = 12
        statusCardView.layer.masksToBounds = true

        statusThumbnailContainer.backgroundColor = .tertiarySystemBackground
        statusThumbnailContainer.layer.cornerRadius = 8
        statusThumbnailContainer.layer.masksToBounds = true

        statusThumbnailImageView.image = UIImage(systemName: "photo")
        statusThumbnailImageView.tintColor = .secondaryLabel
        statusThumbnailImageView.contentMode = .scaleAspectFit

        statusTitleLabel.font = .systemFont(ofSize: 14, weight: .semibold)
        statusTitleLabel.textColor = .label
        statusTitleLabel.numberOfLines = 1
        statusTitleLabel.lineBreakMode = .byTruncatingMiddle
        statusTitleLabel.text = "未开始"
        statusTitleLabel.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        statusTitleLabel.setContentHuggingPriority(.required, for: .vertical)

        statusRightStack.axis = .vertical
        statusRightStack.alignment = .fill
        statusRightStack.distribution = .fill
        statusRightStack.spacing = 4

        statusTopRowStack.axis = .horizontal
        statusTopRowStack.alignment = .firstBaseline
        statusTopRowStack.distribution = .fill
        statusTopRowStack.spacing = 8

        statusDateLabel.font = .systemFont(ofSize: 12, weight: .regular)
        statusDateLabel.textColor = .secondaryLabel
        statusDateLabel.numberOfLines = 1
        statusDateLabel.textAlignment = .left
        statusDateLabel.lineBreakMode = .byTruncatingHead
        statusDateLabel.text = "--"
        statusDateLabel.setContentCompressionResistancePriority(.required, for: .horizontal)
        statusDateLabel.setContentHuggingPriority(.required, for: .horizontal)
        statusDateLabel.setContentHuggingPriority(.required, for: .vertical)

        resourcePercentLabel.font = .systemFont(ofSize: 12, weight: .medium)
        resourcePercentLabel.textColor = .secondaryLabel
        resourcePercentLabel.numberOfLines = 1
        resourcePercentLabel.textAlignment = .left
        resourcePercentLabel.lineBreakMode = .byTruncatingMiddle
        resourcePercentLabel.text = "--"

        overallProgressLeadingLabel.font = .systemFont(ofSize: 12, weight: .medium)
        overallProgressLeadingLabel.textColor = .secondaryLabel
        overallProgressLeadingLabel.numberOfLines = 1
        overallProgressLeadingLabel.lineBreakMode = .byTruncatingTail
        overallProgressLeadingLabel.text = "0/0（未开始）"

        overallProgressPercentLabel.font = .monospacedDigitSystemFont(ofSize: 12, weight: .semibold)
        overallProgressPercentLabel.textColor = .label
        overallProgressPercentLabel.numberOfLines = 1
        overallProgressPercentLabel.textAlignment = .right
        overallProgressPercentLabel.text = "0%"

        overallProgressView.trackTintColor = .tertiarySystemFill
        overallProgressView.progressTintColor = .systemGreen
        overallProgressView.progress = 0

        filterControl.selectedIndex = FilterMode.all.rawValue
        filterControl.addTarget(self, action: #selector(filterChanged), for: .valueChanged)

        tableView.register(BackupItemCell.self, forCellReuseIdentifier: BackupItemCell.reuseID)
        tableView.dataSource = self
        tableView.delegate = self
        tableView.rowHeight = 88

        logTextView.font = .monospacedSystemFont(ofSize: 12, weight: .regular)
        logTextView.isEditable = false
        logTextView.backgroundColor = .secondarySystemBackground
        logTextView.layer.cornerRadius = 10
        logTextView.textContainerInset = UIEdgeInsets(top: 8, left: 8, bottom: 8, right: 8)
        logTextView.isHidden = true

        view.addSubview(scopeCardView)
        scopeCardView.addSubview(scopeTitleLabel)
        scopeCardView.addSubview(scopeSummaryLabel)
        scopeCardView.addSubview(scopeDetailLabel)
        scopeCardView.addSubview(scopeAdjustButton)

        view.addSubview(statusCardView)
        statusCardView.addSubview(statusThumbnailContainer)
        statusThumbnailContainer.addSubview(statusThumbnailImageView)
        statusCardView.addSubview(statusRightStack)
        statusRightStack.addArrangedSubview(statusTopRowStack)
        statusTopRowStack.addArrangedSubview(statusTitleLabel)
        statusTopRowStack.addArrangedSubview(statusDateLabel)
        statusRightStack.addArrangedSubview(resourcePercentLabel)
        statusCardView.addSubview(overallProgressLeadingLabel)
        statusCardView.addSubview(overallProgressPercentLabel)
        statusCardView.addSubview(overallProgressView)

        view.addSubview(filterControl)
        view.addSubview(tableView)
        view.addSubview(logTextView)

        scopeCardView.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).offset(8)
            make.leading.trailing.equalToSuperview().inset(12)
            make.height.greaterThanOrEqualTo(78)
        }

        scopeTitleLabel.snp.makeConstraints { make in
            make.leading.top.equalToSuperview().inset(12)
            make.trailing.lessThanOrEqualTo(scopeAdjustButton.snp.leading).offset(-8)
        }

        scopeAdjustButton.snp.makeConstraints { make in
            make.trailing.equalToSuperview().inset(10)
            make.centerY.equalTo(scopeTitleLabel)
        }

        scopeSummaryLabel.snp.makeConstraints { make in
            make.leading.equalTo(scopeTitleLabel)
            make.top.equalTo(scopeTitleLabel.snp.bottom).offset(2)
            make.trailing.equalToSuperview().inset(12)
        }

        scopeDetailLabel.snp.makeConstraints { make in
            make.leading.equalTo(scopeTitleLabel)
            make.top.equalTo(scopeSummaryLabel.snp.bottom).offset(2)
            make.trailing.equalToSuperview().inset(12)
            make.bottom.equalToSuperview().inset(10)
        }

        statusCardView.snp.makeConstraints { make in
            make.top.equalTo(scopeCardView.snp.bottom).offset(8)
            make.leading.trailing.equalToSuperview().inset(12)
            make.height.greaterThanOrEqualTo(120)
        }

        statusThumbnailContainer.snp.makeConstraints { make in
            make.leading.top.equalToSuperview().inset(12)
            make.size.equalTo(CGSize(width: 56, height: 56))
        }

        statusThumbnailImageView.snp.makeConstraints { make in
            make.edges.equalToSuperview().inset(4)
        }

        statusRightStack.snp.makeConstraints { make in
            make.leading.equalTo(statusThumbnailContainer.snp.trailing).offset(10)
            make.trailing.equalToSuperview().inset(12)
            make.centerY.equalTo(statusThumbnailContainer.snp.centerY)
            make.bottom.lessThanOrEqualTo(overallProgressLeadingLabel.snp.top).offset(-8)
        }

        statusDateLabel.setContentCompressionResistancePriority(.required, for: .horizontal)
        statusDateLabel.setContentHuggingPriority(.required, for: .horizontal)

        overallProgressLeadingLabel.snp.makeConstraints { make in
            make.top.equalTo(statusThumbnailContainer.snp.bottom).offset(10)
            make.leading.equalToSuperview().inset(12)
            make.trailing.lessThanOrEqualTo(overallProgressPercentLabel.snp.leading).offset(-8)
        }

        overallProgressPercentLabel.snp.makeConstraints { make in
            make.centerY.equalTo(overallProgressLeadingLabel)
            make.trailing.equalToSuperview().inset(12)
        }

        overallProgressView.snp.makeConstraints { make in
            make.top.equalTo(overallProgressLeadingLabel.snp.bottom).offset(6)
            make.leading.trailing.equalToSuperview().inset(12)
            make.height.equalTo(4)
            make.bottom.equalToSuperview().inset(12)
        }

        filterControl.snp.makeConstraints { make in
            make.leading.trailing.equalToSuperview().inset(12)
            make.bottom.equalTo(view.safeAreaLayoutGuide).inset(12)
            make.height.greaterThanOrEqualTo(58)
        }

        tableView.snp.makeConstraints { make in
            make.top.equalTo(statusCardView.snp.bottom).offset(10)
            make.leading.trailing.equalToSuperview().inset(12)
            make.bottom.equalTo(filterControl.snp.top).offset(-10)
        }

        logTextView.snp.makeConstraints { make in
            make.top.equalTo(statusCardView.snp.bottom).offset(10)
            make.leading.trailing.equalToSuperview().inset(12)
            make.bottom.equalTo(filterControl.snp.top).offset(-10)
        }
    }

    private func render(snapshot: BackupSessionController.Snapshot) {
        let previousSnapshot = latestSnapshot
        latestSnapshot = snapshot
        updateScopeCard(using: snapshot)
        updateStatusCard(using: snapshot)
        updateFilterTitles(using: snapshot)
        updateLogContent(snapshot.logs)
        if shouldRefreshProcessedItems(previous: previousSnapshot, next: snapshot) {
            applyFilter(using: snapshot)
        }

        if snapshot.controlsLocked {
            startBarButtonItem.isEnabled = false
            pauseBarButtonItem.isEnabled = false
            stopBarButtonItem.isEnabled = false
            return
        }

        if snapshot.state == .running,
           (snapshot.statusText.contains("正在暂停") || snapshot.statusText.contains("正在停止")) {
            startBarButtonItem.isEnabled = false
            pauseBarButtonItem.isEnabled = false
            stopBarButtonItem.isEnabled = false
            return
        }

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

        if pendingOpenScopeSelectorAfterStop,
           snapshot.canAdjustScope,
           !snapshot.controlsLocked {
            pendingOpenScopeSelectorAfterStop = false
            presentScopeSelector(readOnly: false)
        }
    }

    private func shouldRefreshProcessedItems(
        previous: BackupSessionController.Snapshot?,
        next: BackupSessionController.Snapshot
    ) -> Bool {
        guard let previous else { return true }
        let previousEventTime = previous.latestItemEvent?.updatedAt
        let nextEventTime = next.latestItemEvent?.updatedAt
        return previousEventTime != nextEventTime || previous.processedItems.count != next.processedItems.count
    }

    private func updateScopeCard(using snapshot: BackupSessionController.Snapshot) {
        let summary = snapshot.scopeSummary
        let modeText: String
        switch summary.mode {
        case .all:
            modeText = "全选"
        case .partial:
            modeText = "部分选择"
        case .empty:
            modeText = "未选择"
        }
        scopeSummaryLabel.text = modeText

        if summary.mode == .all {
            if let totalBytes = summary.totalEstimatedBytes {
                let totalText = Self.byteCountFormatter.string(fromByteCount: totalBytes)
                scopeDetailLabel.text = "总计 \(summary.totalAssetCount) 张 · \(totalText)"
            } else {
                scopeDetailLabel.text = "总计 \(summary.totalAssetCount) 张 · 容量待统计"
            }
        } else if let selectedBytes = summary.selectedEstimatedBytes,
                  let totalBytes = summary.totalEstimatedBytes {
            let selectedText = Self.byteCountFormatter.string(fromByteCount: selectedBytes)
            let totalText = Self.byteCountFormatter.string(fromByteCount: totalBytes)
            scopeDetailLabel.text = "已选 \(summary.selectedAssetCount)/\(summary.totalAssetCount) 张 · \(selectedText) / \(totalText)"
        } else {
            scopeDetailLabel.text = "已选 \(summary.selectedAssetCount)/\(summary.totalAssetCount) 张 · 容量待统计"
        }

        scopeAdjustButton.isEnabled = !snapshot.controlsLocked
        scopeAdjustButton.alpha = snapshot.controlsLocked ? 0.5 : 1
    }

    private func updateStatusCard(using snapshot: BackupSessionController.Snapshot) {
        let completed = snapshot.succeeded + snapshot.failed + snapshot.skipped
        let total = snapshot.total
        let overallFraction: Float = total > 0 ? Float(completed) / Float(total) : 0
        let overallPercent = Int((overallFraction * 100).rounded())
        overallProgressLeadingLabel.text = "\(completed)/\(total)（\(taskStateText(for: snapshot))）"
        overallProgressPercentLabel.text = "\(overallPercent)%"
        overallProgressView.progress = overallFraction

        guard let transfer = snapshot.transferState else {
            statusTitleLabel.text = snapshot.latestItemEvent?.displayName ?? "暂无上传项目"
            if let date = snapshot.latestItemEvent?.resourceDate {
                statusDateLabel.text = Self.resourceDateFormatter.string(from: date)
            } else {
                statusDateLabel.text = "--"
            }
            resourcePercentLabel.text = "--"
            applyStatusThumbnail(assetLocalIdentifier: snapshot.latestItemEvent?.assetLocalIdentifier)
            return
        }

        let clamped = transfer.clampedResourceFraction
        let resourcePercent = clamped >= 1 ? 100 : Int(floor(Double(clamped) * 100))
        statusTitleLabel.text = transfer.assetDisplayName
        if let resourceDate = transfer.resourceDate {
            statusDateLabel.text = Self.resourceDateFormatter.string(from: resourceDate)
        } else {
            statusDateLabel.text = "--"
        }
        resourcePercentLabel.text = "\(resourcePercent)%"
        applyStatusThumbnail(assetLocalIdentifier: transfer.assetLocalIdentifier)
    }

    private func taskStateText(for snapshot: BackupSessionController.Snapshot) -> String {
        if snapshot.statusText.contains("正在停止") {
            return "正在停止"
        }
        switch snapshot.state {
        case .running:
            return "进行中"
        case .paused:
            return "已暂停"
        case .stopped:
            return "已停止"
        case .failed:
            return "失败"
        case .completed:
            return "已完成"
        case .idle:
            return "未开始"
        }
    }

    private func applyStatusThumbnail(assetLocalIdentifier: String?) {
        guard statusThumbnailAssetID != assetLocalIdentifier else { return }
        statusThumbnailAssetID = assetLocalIdentifier
        if let requestID = statusThumbnailRequestID {
            imageManager.cancelImageRequest(requestID)
            statusThumbnailRequestID = nil
        }
        statusThumbnailImageView.image = UIImage(systemName: "photo")

        guard let assetLocalIdentifier else { return }
        let key = assetLocalIdentifier as NSString
        if let cached = thumbnailCache.object(forKey: key) {
            statusThumbnailImageView.image = cached
            return
        }
        let auth = PHPhotoLibrary.authorizationStatus(for: .readWrite)
        guard auth == .authorized || auth == .limited else { return }

        let fetched = PHAsset.fetchAssets(withLocalIdentifiers: [assetLocalIdentifier], options: nil)
        guard let asset = fetched.firstObject else { return }

        let options = PHImageRequestOptions()
        options.deliveryMode = .fastFormat
        options.resizeMode = .fast
        options.isNetworkAccessAllowed = true

        let requestID = imageManager.requestImage(
            for: asset,
            targetSize: CGSize(width: 120, height: 120),
            contentMode: .aspectFill,
            options: options
        ) { [weak self] image, info in
            guard let self else { return }
            if (info?[PHImageCancelledKey] as? Bool) == true { return }
            if (info?[PHImageResultIsDegradedKey] as? Bool) == true { return }
            guard self.statusThumbnailAssetID == assetLocalIdentifier else { return }
            guard let image else {
                self.statusThumbnailImageView.image = UIImage(systemName: "photo")
                return
            }
            self.thumbnailCache.setObject(image, forKey: key, cost: Self.imageCost(image))
            self.statusThumbnailImageView.image = image
        }
        statusThumbnailRequestID = requestID
    }

    private func updateFilterTitles(using snapshot: BackupSessionController.Snapshot) {
        let success = snapshot.succeeded
        let failed = snapshot.failed
        let skipped = snapshot.skipped
        let all = success + failed + skipped
        let logs = snapshot.logs.count

        filterControl.setSubtitle("\(all) 项", forItemAt: FilterMode.all.rawValue)
        filterControl.setSubtitle("\(success) 项", forItemAt: FilterMode.success.rawValue)
        filterControl.setSubtitle("\(failed) 项", forItemAt: FilterMode.failed.rawValue)
        filterControl.setSubtitle("\(skipped) 项", forItemAt: FilterMode.skipped.rawValue)
        filterControl.setSubtitle("\(logs) 条", forItemAt: FilterMode.log.rawValue)
    }

    private func updateLogContent(_ logs: [String]) {
        let shouldAutoScroll = shouldKeepLogPinnedToBottom()
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

        if renderedLogCount > 0, shouldAutoScroll {
            let range = NSRange(location: max(logTextView.text.count - 1, 0), length: 1)
            logTextView.scrollRangeToVisible(range)
        }
    }

    private func shouldKeepLogPinnedToBottom() -> Bool {
        guard !logTextView.isDragging, !logTextView.isDecelerating else {
            return false
        }
        let visibleBottom = logTextView.contentOffset.y + logTextView.bounds.height
        return visibleBottom >= logTextView.contentSize.height - 24
    }

    private func applyFilter(using snapshot: BackupSessionController.Snapshot, forceReload: Bool = false) {
        let mode = currentFilterMode()

        if mode == .log {
            tableView.isHidden = true
            logTextView.isHidden = false
            return
        }

        tableView.isHidden = false
        logTextView.isHidden = true

        let displayOrderedItems = Array(snapshot.processedItems.reversed())
        let nextItems: [BackupSessionController.ProcessedItem]
        switch mode {
        case .all:
            nextItems = displayOrderedItems
        case .success:
            nextItems = displayOrderedItems.filter { $0.status == .success }
        case .failed:
            nextItems = displayOrderedItems.filter { $0.status == .failed }
        case .skipped:
            nextItems = displayOrderedItems.filter { $0.status == .skipped }
        case .log:
            nextItems = []
        }

        let changed = nextItems != filteredItems
        filteredItems = nextItems

        if !forceReload, (tableView.isDragging || tableView.isDecelerating) {
            if changed {
                deferredTableReload = true
            }
            return
        }

        if changed || forceReload || deferredTableReload {
            deferredTableReload = false
            tableView.reloadData()
        }
    }

    private func currentFilterMode() -> FilterMode {
        FilterMode(rawValue: filterControl.selectedIndex) ?? .all
    }

    private func flushDeferredTableReloadIfNeeded() {
        guard deferredTableReload else { return }
        guard !tableView.isHidden else { return }
        deferredTableReload = false
        tableView.reloadData()
    }

    @objc
    private func filterChanged() {
        guard let snapshot = latestSnapshot else { return }
        applyFilter(using: snapshot, forceReload: true)
    }

    @objc
    private func scopeAdjustTapped() {
        guard let snapshot = latestSnapshot else { return }
        if snapshot.canAdjustScope {
            presentScopeSelector(readOnly: false)
            return
        }

        if snapshot.state == .running || snapshot.state == .paused {
            let alert = UIAlertController(
                title: "当前任务进行中",
                message: "可先仅查看当前范围；如需修改范围，需要先停止当前任务。",
                preferredStyle: .alert
            )
            alert.addAction(UIAlertAction(title: "仅查看", style: .default, handler: { [weak self] _ in
                self?.presentScopeSelector(readOnly: true)
            }))
            alert.addAction(UIAlertAction(title: "停止并调整", style: .destructive, handler: { [weak self] _ in
                guard let self else { return }
                self.pendingOpenScopeSelectorAfterStop = true
                self.sessionController.stopBackup()
            }))
            alert.addAction(UIAlertAction(title: "取消", style: .cancel))
            present(alert, animated: true)
            return
        }

        presentScopeSelector(readOnly: true)
    }

    private func presentScopeSelector(readOnly: Bool) {
        let selector = BackupRangeSelectorViewController(
            dependencies: dependencies,
            initialSelection: sessionController.currentScopeSelection(),
            readOnly: readOnly
        ) { [weak self] selection in
            guard let self else { return }
            _ = self.sessionController.updateScopeSelection(selection)
        }
        let nav = UINavigationController(rootViewController: selector)
        nav.modalPresentationStyle = .pageSheet
        present(nav, animated: true)
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
        if thumbnailRequestIDs[item.assetLocalIdentifier] != nil { return }

        let auth = PHPhotoLibrary.authorizationStatus(for: .readWrite)
        guard auth == .authorized || auth == .limited else { return }

        let fetched = PHAsset.fetchAssets(withLocalIdentifiers: [item.assetLocalIdentifier], options: nil)
        guard let asset = fetched.firstObject else { return }

        let options = PHImageRequestOptions()
        options.deliveryMode = .fastFormat
        options.resizeMode = .fast
        options.isNetworkAccessAllowed = true

        let requestID = imageManager.requestImage(
            for: asset,
            targetSize: CGSize(width: 140, height: 140),
            contentMode: .aspectFit,
            options: options
        ) { [weak self] image, info in
            guard let self else { return }
            if (info?[PHImageCancelledKey] as? Bool) == true {
                self.thumbnailRequestIDs[item.assetLocalIdentifier] = nil
                return
            }
            if (info?[PHImageResultIsDegradedKey] as? Bool) == true {
                return
            }
            self.thumbnailRequestIDs[item.assetLocalIdentifier] = nil
            guard let image else { return }
            self.thumbnailCache.setObject(image, forKey: key, cost: Self.imageCost(image))

            guard let row = self.filteredItems.firstIndex(where: { $0.id == item.id }) else { return }
            let indexPath = IndexPath(row: row, section: 0)
            guard let cell = self.tableView.cellForRow(at: indexPath) else { return }
            self.configure(cell, with: item)
        }
        thumbnailRequestIDs[item.assetLocalIdentifier] = requestID
    }

    private func cancelAllImageRequests() {
        if let requestID = statusThumbnailRequestID {
            imageManager.cancelImageRequest(requestID)
            statusThumbnailRequestID = nil
        }
        for requestID in thumbnailRequestIDs.values {
            imageManager.cancelImageRequest(requestID)
        }
        thumbnailRequestIDs.removeAll()
    }

    private static func imageCost(_ image: UIImage) -> Int {
        let width = Int(image.size.width * image.scale)
        let height = Int(image.size.height * image.scale)
        return max(width * height * 4, 1)
    }

    private func configure(_ cell: UITableViewCell, with item: BackupSessionController.ProcessedItem) {
        guard let cell = cell as? BackupItemCell else { return }
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

extension BackupViewController: UITableViewDataSource, UITableViewDelegate {
    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        filteredItems.count
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: BackupItemCell.reuseID, for: indexPath)
        guard indexPath.row < filteredItems.count else { return cell }

        let item = filteredItems[indexPath.row]
        configure(cell, with: item)
        requestThumbnailIfNeeded(for: item)
        return cell
    }

    func scrollViewDidEndDragging(_ scrollView: UIScrollView, willDecelerate decelerate: Bool) {
        if scrollView === tableView, !decelerate {
            flushDeferredTableReloadIfNeeded()
        }
    }

    func scrollViewDidEndDecelerating(_ scrollView: UIScrollView) {
        if scrollView === tableView {
            flushDeferredTableReloadIfNeeded()
        }
    }
}

private final class BackupItemCell: UITableViewCell {
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
