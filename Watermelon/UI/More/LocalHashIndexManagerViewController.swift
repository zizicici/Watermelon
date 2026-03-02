import Photos
import SnapKit
import UIKit

@MainActor
final class LocalHashIndexManagerViewController: UIViewController {
    private enum BuildMode: Int {
        case create
        case update

        var text: String {
            switch self {
            case .create: return "创建"
            case .update: return "更新"
            }
        }
    }

    private enum RunState {
        case idle
        case running
        case paused
        case stopped
        case completed
        case failed
    }

    private enum TerminationIntent {
        case none
        case pause
        case stop
    }

    private let dependencies: DependencyContainer
    private let repository: ContentHashIndexRepository

    private let statsCardView = UIView()
    private let statsTitleLabel = UILabel()
    private let statsSummaryLabel = UILabel()
    private let statsCoverageLabel = UILabel()

    private let modeSegmentedControl = UISegmentedControl(items: [BuildMode.create.text, BuildMode.update.text])
    private let removeStaleSwitch = UISwitch()
    private let removeStaleLabel = UILabel()

    private let progressCardView = UIView()
    private let progressTitleLabel = UILabel()
    private let progressDetailLabel = UILabel()
    private let progressView = UIProgressView(progressViewStyle: .default)

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
    private lazy var resetBarButtonItem = UIBarButtonItem(
        title: "重置",
        style: .plain,
        target: self,
        action: #selector(resetTapped)
    )

    private var runTask: Task<Void, Never>?
    private var cancellationController: BackupCancellationController?
    private var terminationIntent: TerminationIntent = .none
    private var runState: RunState = .idle {
        didSet {
            updateControlState()
        }
    }

    private var pendingAssetIDs: [String] = []
    private var processedAssetIDs: Set<String> = []
    private var processedCount = 0
    private var totalCount = 0
    private var logs: [String] = []

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies
        self.repository = ContentHashIndexRepository(databaseManager: dependencies.databaseManager)
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        title = "本地 Hash 索引"

        navigationItem.rightBarButtonItem = resetBarButtonItem
        navigationItem.rightBarButtonItems = [resetBarButtonItem, stopBarButtonItem, pauseBarButtonItem, startBarButtonItem]
        stopBarButtonItem.tintColor = .systemRed

        buildUI()
        refreshStats()
        updateControlState()
    }

    deinit {
        runTask?.cancel()
    }

    private func buildUI() {
        statsCardView.backgroundColor = .secondarySystemBackground
        statsCardView.layer.cornerRadius = 12
        statsCardView.layer.masksToBounds = true

        statsTitleLabel.font = .systemFont(ofSize: 14, weight: .semibold)
        statsTitleLabel.text = "索引状态"
        statsSummaryLabel.font = .systemFont(ofSize: 13, weight: .regular)
        statsSummaryLabel.textColor = .secondaryLabel
        statsCoverageLabel.font = .systemFont(ofSize: 12, weight: .regular)
        statsCoverageLabel.textColor = .secondaryLabel

        modeSegmentedControl.selectedSegmentIndex = BuildMode.update.rawValue

        removeStaleLabel.font = .systemFont(ofSize: 13, weight: .regular)
        removeStaleLabel.textColor = .label
        removeStaleLabel.text = "移除本地已不存在条目"
        removeStaleSwitch.isOn = true

        progressCardView.backgroundColor = .secondarySystemBackground
        progressCardView.layer.cornerRadius = 12
        progressCardView.layer.masksToBounds = true
        progressTitleLabel.font = .systemFont(ofSize: 14, weight: .semibold)
        progressTitleLabel.text = "未开始"
        progressDetailLabel.font = .systemFont(ofSize: 12, weight: .regular)
        progressDetailLabel.textColor = .secondaryLabel
        progressDetailLabel.text = "0 / 0"
        progressView.progress = 0
        progressView.trackTintColor = .tertiarySystemFill
        progressView.progressTintColor = .systemBlue

        logTextView.font = .monospacedSystemFont(ofSize: 12, weight: .regular)
        logTextView.backgroundColor = .secondarySystemBackground
        logTextView.layer.cornerRadius = 12
        logTextView.isEditable = false
        logTextView.textContainerInset = UIEdgeInsets(top: 8, left: 8, bottom: 8, right: 8)

        view.addSubview(statsCardView)
        statsCardView.addSubview(statsTitleLabel)
        statsCardView.addSubview(statsSummaryLabel)
        statsCardView.addSubview(statsCoverageLabel)

        view.addSubview(modeSegmentedControl)
        view.addSubview(removeStaleLabel)
        view.addSubview(removeStaleSwitch)

        view.addSubview(progressCardView)
        progressCardView.addSubview(progressTitleLabel)
        progressCardView.addSubview(progressDetailLabel)
        progressCardView.addSubview(progressView)

        view.addSubview(logTextView)

        statsCardView.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).offset(10)
            make.leading.trailing.equalToSuperview().inset(12)
        }
        statsTitleLabel.snp.makeConstraints { make in
            make.leading.top.equalToSuperview().inset(12)
            make.trailing.equalToSuperview().inset(12)
        }
        statsSummaryLabel.snp.makeConstraints { make in
            make.leading.trailing.equalTo(statsTitleLabel)
            make.top.equalTo(statsTitleLabel.snp.bottom).offset(4)
        }
        statsCoverageLabel.snp.makeConstraints { make in
            make.leading.trailing.equalTo(statsTitleLabel)
            make.top.equalTo(statsSummaryLabel.snp.bottom).offset(2)
            make.bottom.equalToSuperview().inset(12)
        }

        modeSegmentedControl.snp.makeConstraints { make in
            make.top.equalTo(statsCardView.snp.bottom).offset(10)
            make.leading.trailing.equalToSuperview().inset(12)
        }
        removeStaleLabel.snp.makeConstraints { make in
            make.top.equalTo(modeSegmentedControl.snp.bottom).offset(10)
            make.leading.equalToSuperview().inset(12)
        }
        removeStaleSwitch.snp.makeConstraints { make in
            make.centerY.equalTo(removeStaleLabel)
            make.trailing.equalToSuperview().inset(12)
        }

        progressCardView.snp.makeConstraints { make in
            make.top.equalTo(removeStaleLabel.snp.bottom).offset(10)
            make.leading.trailing.equalToSuperview().inset(12)
        }
        progressTitleLabel.snp.makeConstraints { make in
            make.leading.top.equalToSuperview().inset(12)
            make.trailing.equalToSuperview().inset(12)
        }
        progressDetailLabel.snp.makeConstraints { make in
            make.leading.trailing.equalTo(progressTitleLabel)
            make.top.equalTo(progressTitleLabel.snp.bottom).offset(4)
        }
        progressView.snp.makeConstraints { make in
            make.leading.trailing.equalTo(progressTitleLabel)
            make.top.equalTo(progressDetailLabel.snp.bottom).offset(8)
            make.height.equalTo(4)
            make.bottom.equalToSuperview().inset(12)
        }

        logTextView.snp.makeConstraints { make in
            make.top.equalTo(progressCardView.snp.bottom).offset(10)
            make.leading.trailing.bottom.equalToSuperview().inset(12)
        }
    }

    private func updateControlState() {
        switch runState {
        case .running:
            startBarButtonItem.isEnabled = false
            pauseBarButtonItem.isEnabled = true
            stopBarButtonItem.isEnabled = true
            resetBarButtonItem.isEnabled = false
            modeSegmentedControl.isEnabled = false
            removeStaleSwitch.isEnabled = false
        case .paused:
            startBarButtonItem.isEnabled = true
            pauseBarButtonItem.isEnabled = false
            stopBarButtonItem.isEnabled = true
            resetBarButtonItem.isEnabled = false
            modeSegmentedControl.isEnabled = false
            removeStaleSwitch.isEnabled = false
        default:
            startBarButtonItem.isEnabled = true
            pauseBarButtonItem.isEnabled = false
            stopBarButtonItem.isEnabled = false
            resetBarButtonItem.isEnabled = true
            modeSegmentedControl.isEnabled = true
            removeStaleSwitch.isEnabled = true
        }
    }

    private func appendLog(_ line: String) {
        let timestamp = Self.timeFormatter.string(from: Date())
        logs.append("[\(timestamp)] \(line)")
        if logs.count > 800 {
            logs.removeFirst(logs.count - 800)
        }
        logTextView.text = logs.joined(separator: "\n")
        let range = NSRange(location: max(logTextView.text.count - 1, 0), length: 1)
        logTextView.scrollRangeToVisible(range)
    }

    private func refreshStats() {
        let stats = (try? repository.fetchLocalHashIndexStats()) ?? LocalHashIndexStats(
            assetCount: 0,
            resourceCount: 0,
            totalFileSizeBytes: 0,
            oldestUpdatedAt: nil,
            newestUpdatedAt: nil
        )
        let totalSizeText = ByteCountFormatter.string(fromByteCount: stats.totalFileSizeBytes, countStyle: .file)
        statsSummaryLabel.text = "项目数 \(stats.assetCount) · 资源条目 \(stats.resourceCount) · \(totalSizeText)"
        if let oldest = stats.oldestUpdatedAt, let newest = stats.newestUpdatedAt {
            statsCoverageLabel.text = "覆盖范围（更新时间）\(Self.dateFormatter.string(from: oldest)) - \(Self.dateFormatter.string(from: newest))"
        } else {
            statsCoverageLabel.text = "覆盖范围：暂无"
        }
    }

    @objc
    private func startTapped() {
        if runState == .paused, !pendingAssetIDs.isEmpty {
            appendLog("继续运行本地 Hash 索引任务")
            startRun(continuePending: true)
            return
        }
        startRun(continuePending: false)
    }

    @objc
    private func pauseTapped() {
        guard runState == .running else { return }
        terminationIntent = .pause
        cancellationController?.cancel()
        runTask?.cancel()
        appendLog("请求暂停...")
    }

    @objc
    private func stopTapped() {
        guard runState == .running || runState == .paused else { return }
        if runState == .paused {
            pendingAssetIDs.removeAll()
            runState = .stopped
            progressTitleLabel.text = "已停止"
            appendLog("任务已停止")
            return
        }
        terminationIntent = .stop
        cancellationController?.cancel()
        runTask?.cancel()
        pendingAssetIDs.removeAll()
        appendLog("请求停止...")
    }

    @objc
    private func resetTapped() {
        guard runState != .running else { return }
        let alert = UIAlertController(
            title: "重置索引",
            message: "将清空本地 Hash 索引，是否继续？",
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: "取消", style: .cancel))
        alert.addAction(UIAlertAction(title: "重置", style: .destructive, handler: { [weak self] _ in
            guard let self else { return }
            do {
                try self.repository.clearLocalHashIndex()
                self.appendLog("已重置本地 Hash 索引")
                self.refreshStats()
            } catch {
                self.appendLog("重置失败：\(error.localizedDescription)")
            }
        }))
        present(alert, animated: true)
    }

    private func startRun(continuePending: Bool) {
        guard runTask == nil else { return }
        terminationIntent = .none
        let mode = BuildMode(rawValue: modeSegmentedControl.selectedSegmentIndex) ?? .update
        let pruneMissing = removeStaleSwitch.isOn

        runTask = Task { [weak self] in
            guard let self else { return }
            let cancellationController = BackupCancellationController()
            self.cancellationController = cancellationController
            self.runState = .running

            do {
                if !continuePending {
                    if mode == .create {
                        try self.repository.clearLocalHashIndex()
                        self.appendLog("已清空旧索引，开始创建")
                    } else {
                        self.appendLog("开始更新本地 Hash 索引")
                    }
                    self.pendingAssetIDs = try await self.buildInitialPendingAssetIDs()
                    self.processedAssetIDs.removeAll()
                    self.processedCount = 0
                    self.totalCount = self.pendingAssetIDs.count
                    self.progressView.progress = 0
                }

                try await self.runIndexLoop(
                    mode: mode,
                    pruneMissing: pruneMissing,
                    cancellationController: cancellationController
                )
            } catch {
                if error is CancellationError {
                    if self.terminationIntent == .pause {
                        self.runState = .paused
                        self.progressTitleLabel.text = "已暂停"
                        self.appendLog("任务已暂停")
                    } else {
                        self.runState = .stopped
                        self.progressTitleLabel.text = "已停止"
                        self.appendLog("任务已停止")
                    }
                } else {
                    self.runState = .failed
                    self.progressTitleLabel.text = "执行失败"
                    self.appendLog("执行失败：\(error.localizedDescription)")
                }
            }

            self.runTask = nil
            self.cancellationController = nil
            self.refreshStats()
        }
    }

    private func buildInitialPendingAssetIDs() async throws -> [String] {
        let status = dependencies.photoLibraryService.authorizationStatus()
        let authorized: Bool
        if status == .authorized || status == .limited {
            authorized = true
        } else {
            let requested = await dependencies.photoLibraryService.requestAuthorization()
            authorized = (requested == .authorized || requested == .limited)
        }
        guard authorized else {
            throw BackupError.photoPermissionDenied
        }
        let assets = dependencies.photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true)
        var result: [String] = []
        result.reserveCapacity(assets.count)
        for index in 0 ..< assets.count {
            result.append(assets.object(at: index).localIdentifier)
        }
        return result
    }

    private func runIndexLoop(
        mode: BuildMode,
        pruneMissing: Bool,
        cancellationController: BackupCancellationController
    ) async throws {
        while !pendingAssetIDs.isEmpty {
            try cancellationController.throwIfCancelled()
            try Task.checkCancellation()

            let assetID = pendingAssetIDs.removeFirst()
            guard let asset = PHAsset.fetchAssets(withLocalIdentifiers: [assetID], options: nil).firstObject else {
                processedCount += 1
                updateProgressUI(assetDisplayName: "缺失资产 \(assetID)")
                continue
            }

            let selectedResources = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(
                from: PHAssetResource.assetResources(for: asset)
            )
            if selectedResources.isEmpty {
                processedCount += 1
                processedAssetIDs.insert(assetID)
                updateProgressUI(assetDisplayName: "空资源 \(assetID)")
                continue
            }

            var roleSlotHashes: [(role: Int, slot: Int, contentHash: Data, fileSize: Int64)] = []
            roleSlotHashes.reserveCapacity(selectedResources.count)
            var totalFileSizeBytes: Int64 = 0

            for selected in selectedResources {
                try cancellationController.throwIfCancelled()
                try Task.checkCancellation()
                let exported = try await dependencies.photoLibraryService.exportResourceToTempFileAndDigest(
                    selected.resource,
                    cancellationController: cancellationController
                )
                defer { try? FileManager.default.removeItem(at: exported.fileURL) }
                let localFileSize = max(
                    PhotoLibraryService.resourceFileSize(selected.resource),
                    exported.fileSize
                )
                totalFileSizeBytes += max(localFileSize, 0)
                roleSlotHashes.append((
                    role: selected.role,
                    slot: selected.slot,
                    contentHash: exported.contentHash,
                    fileSize: localFileSize
                ))
            }

            let fingerprint = BackupAssetResourcePlanner.assetFingerprint(
                resourceRoleSlotHashes: roleSlotHashes.map { item in
                    (role: item.role, slot: item.slot, contentHash: item.contentHash)
                }
            )
            try repository.upsertAssetHashSnapshot(
                assetLocalIdentifier: assetID,
                assetFingerprint: fingerprint,
                resources: roleSlotHashes.map { item in
                    LocalAssetResourceHashRecord(
                        role: item.role,
                        slot: item.slot,
                        contentHash: item.contentHash,
                        fileSize: item.fileSize
                    )
                },
                totalFileSizeBytes: totalFileSizeBytes
            )

            processedCount += 1
            processedAssetIDs.insert(assetID)
            updateProgressUI(assetDisplayName: selectedResources.first?.resource.originalFilename ?? assetID)
        }

        if pruneMissing {
            let indexedIDs = try repository.fetchIndexedAssetIDs()
            let staleIDs = indexedIDs.filter { !processedAssetIDs.contains($0) }
            if !staleIDs.isEmpty {
                try repository.deleteIndexEntries(assetIDs: staleIDs)
                appendLog("已移除 \(staleIDs.count) 条本地已不存在索引项")
            }
        }

        runState = .completed
        progressTitleLabel.text = mode == .create ? "创建完成" : "更新完成"
        appendLog("完成：已处理 \(processedCount) 项")
    }

    private func updateProgressUI(assetDisplayName: String) {
        let total = max(totalCount, 1)
        let progress = Float(processedCount) / Float(total)
        progressView.progress = progress
        progressTitleLabel.text = assetDisplayName
        progressDetailLabel.text = "\(processedCount)/\(totalCount)"
    }

    private static let dateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
        return formatter
    }()

    private static let timeFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm:ss"
        return formatter
    }()
}
