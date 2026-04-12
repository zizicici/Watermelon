import SnapKit
import UIKit

final class SelectionActionPanel: UIView {

    enum CategoryPhase {
        case pending(total: Int)
        case running(completed: Int, failed: Int, total: Int)
        case paused(completed: Int, total: Int)
        case completed(total: Int)
        case failed(completed: Int, failed: Int, total: Int)
    }

    var onExecuteTapped: (() -> Void)?
    var onPauseTapped: (() -> Void)?
    var onStopTapped: (() -> Void)?
    var onResumeTapped: (() -> Void)?
    var onCompleteTapped: (() -> Void)?

    private var isExecuting = false
    private var isPaused = false
    private var isCompleted = false
    private var lastRenderedExecutionPhase: ExecutionPhase?
    private var lastRenderedControlState: ExecutionControlState?
    private var isStopTransitionLatched = false

    private let executionControlButtonWidth: CGFloat = 72
    private let executionControlButtonHeight: CGFloat = 36

    private let separator = UIView()
    private(set) var backupCategoryButton: UIButton = {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 15, weight: .bold)
        var cfg = UIButton.Configuration.plain()
        cfg.image = UIImage(systemName: "arrow.right", withConfiguration: iconConfig)
        cfg.imagePadding = 6
        cfg.titleAlignment = .leading
        cfg.subtitle = "备份"
        cfg.subtitleTextAttributesTransformer = .init { var a = $0; a.font = .preferredFont(forTextStyle: .caption1); return a }
        cfg.baseForegroundColor = .materialPrimary(light: .Material.Cyan._600, dark: .Material.Cyan._200)
        let btn = UIButton(configuration: cfg)
        btn.showsMenuAsPrimaryAction = true
        return btn
    }()
    private(set) var downloadCategoryButton: UIButton = {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 15, weight: .bold)
        var cfg = UIButton.Configuration.plain()
        cfg.image = UIImage(systemName: "arrow.left", withConfiguration: iconConfig)
        cfg.imagePadding = 6
        cfg.titleAlignment = .leading
        cfg.subtitle = "下载"
        cfg.subtitleTextAttributesTransformer = .init { var a = $0; a.font = .preferredFont(forTextStyle: .caption1); return a }
        cfg.baseForegroundColor = .materialPrimary(light: .Material.Orange._600, dark: .Material.Orange._200)
        let btn = UIButton(configuration: cfg)
        btn.showsMenuAsPrimaryAction = true
        return btn
    }()
    private(set) var syncCategoryButton: UIButton = {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 15, weight: .bold)
        var cfg = UIButton.Configuration.plain()
        cfg.image = UIImage(systemName: "arrow.left.arrow.right", withConfiguration: iconConfig)
        cfg.imagePadding = 6
        cfg.titleAlignment = .leading
        cfg.subtitle = "同步"
        cfg.subtitleTextAttributesTransformer = .init { var a = $0; a.font = .preferredFont(forTextStyle: .caption1); return a }
        cfg.baseForegroundColor = .materialPrimary(light: .Material.Purple._600, dark: .Material.Purple._200)
        let btn = UIButton(configuration: cfg)
        btn.showsMenuAsPrimaryAction = true
        return btn
    }()
    private let executeButton: UIButton = {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 14, weight: .bold)
        var cfg = UIButton.Configuration.filled()
        cfg.image = UIImage(systemName: "play.fill", withConfiguration: iconConfig)
        cfg.cornerStyle = .capsule
        cfg.baseBackgroundColor = .materialPrimary(light: .Material.Green._600, dark: .Material.Green._200)
        cfg.baseForegroundColor = .materialOnPrimary(dark: .Material.Green._800)
        cfg.contentInsets = .init(top: 8, leading: 14, bottom: 8, trailing: 14)
        return UIButton(configuration: cfg)
    }()
    private let stopButton: UIButton = {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 14, weight: .bold)
        var cfg = UIButton.Configuration.filled()
        cfg.image = UIImage(systemName: "stop.fill", withConfiguration: iconConfig)
        cfg.cornerStyle = .capsule
        cfg.baseBackgroundColor = .materialPrimary(light: .Material.Red._600, dark: .Material.Red._200)
        cfg.baseForegroundColor = .materialOnPrimary(dark: .Material.Red._800)
        cfg.contentInsets = .init(top: 8, leading: 14, bottom: 8, trailing: 14)
        return UIButton(configuration: cfg)
    }()

    override init(frame: CGRect) {
        super.init(frame: frame)
        setupUI()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError() }

    private(set) var failureSummaryButton: UIButton = {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 15, weight: .bold)
        var cfg = UIButton.Configuration.plain()
        cfg.image = UIImage(systemName: "exclamationmark.triangle.fill", withConfiguration: iconConfig)
        cfg.imagePadding = 6
        cfg.titleAlignment = .leading
        cfg.baseForegroundColor = .systemRed
        let btn = UIButton(configuration: cfg)
        btn.showsMenuAsPrimaryAction = true
        btn.isHidden = true
        return btn
    }()

    private let categoryScrollView: UIScrollView = {
        let sv = UIScrollView()
        sv.showsHorizontalScrollIndicator = false
        sv.showsVerticalScrollIndicator = false
        return sv
    }()

    private func setupUI() {
        backgroundColor = .appPaper

        separator.backgroundColor = .separator
        addSubview(separator)
        separator.snp.makeConstraints { make in
            make.top.leading.trailing.equalToSuperview()
            make.height.equalTo(0.5)
        }

        executeButton.addTarget(self, action: #selector(executeTapped), for: .touchUpInside)
        stopButton.addTarget(self, action: #selector(stopTappedAction), for: .touchUpInside)
        stopButton.isHidden = true
        executeButton.snp.makeConstraints { make in
            make.width.equalTo(executionControlButtonWidth)
            make.height.equalTo(executionControlButtonHeight)
        }
        stopButton.snp.makeConstraints { make in
            make.width.equalTo(executionControlButtonWidth)
            make.height.equalTo(executionControlButtonHeight)
        }

        // Scrollable category buttons (failure + backup + download + sync)
        let scrollContent = UIStackView(arrangedSubviews: [failureSummaryButton, backupCategoryButton, downloadCategoryButton, syncCategoryButton])
        scrollContent.axis = .horizontal
        scrollContent.spacing = 4
        scrollContent.alignment = .center

        categoryScrollView.addSubview(scrollContent)
        scrollContent.snp.makeConstraints { make in
            make.edges.equalToSuperview()
            make.height.equalToSuperview()
        }

        let spacer = UIView()
        spacer.setContentHuggingPriority(.defaultLow, for: .horizontal)

        let contentStack = UIStackView(arrangedSubviews: [categoryScrollView, spacer, stopButton, executeButton])
        contentStack.axis = .horizontal
        contentStack.spacing = 4
        contentStack.setCustomSpacing(12, after: stopButton)
        contentStack.alignment = .center

        let inset: CGFloat = 12
        addSubview(contentStack)
        contentStack.snp.makeConstraints { make in
            make.top.equalToSuperview().inset(inset)
            make.leading.trailing.equalToSuperview().inset(inset)
            make.bottom.equalTo(safeAreaLayoutGuide).inset(inset)
        }
    }

    @objc private func executeTapped() {
        if isCompleted {
            onCompleteTapped?()
        } else if isExecuting {
            if isPaused {
                onResumeTapped?()
            } else {
                onPauseTapped?()
            }
        } else {
            onExecuteTapped?()
        }
    }

    @objc private func stopTappedAction() { onStopTapped?() }

    // MARK: - Selection Mode

    func configure(backupCount: Int, downloadCount: Int, syncCount: Int) {
        backupCategoryButton.isHidden = backupCount == 0
        backupCategoryButton.configuration?.title = "\(backupCount)"
        downloadCategoryButton.isHidden = downloadCount == 0
        downloadCategoryButton.configuration?.title = "\(downloadCount)"
        syncCategoryButton.isHidden = syncCount == 0
        syncCategoryButton.configuration?.title = "\(syncCount)"
    }

    // MARK: - Execution Mode

    func updateFailureSummary(menu: UIMenu?, title: String?) {
        if let menu, let title {
            failureSummaryButton.isHidden = false
            failureSummaryButton.menu = menu
            failureSummaryButton.configuration?.title = title
        } else {
            failureSummaryButton.isHidden = true
            failureSummaryButton.menu = nil
        }
    }

    func enterExecution(backupTotal: Int, downloadTotal: Int, syncTotal: Int) {
        isExecuting = true
        isPaused = false
        isCompleted = false
        lastRenderedExecutionPhase = nil
        lastRenderedControlState = nil
        isStopTransitionLatched = false

        failureSummaryButton.isHidden = true
        failureSummaryButton.menu = nil
        backupCategoryButton.showsMenuAsPrimaryAction = false
        backupCategoryButton.menu = nil
        downloadCategoryButton.showsMenuAsPrimaryAction = false
        downloadCategoryButton.menu = nil
        syncCategoryButton.showsMenuAsPrimaryAction = false
        syncCategoryButton.menu = nil

        applyCategoryPhase(button: backupCategoryButton, phase: backupTotal > 0 ? .pending(total: backupTotal) : nil,
                           iconName: "arrow.right", color: .materialPrimary(light: .Material.Cyan._600, dark: .Material.Cyan._200))
        applyCategoryPhase(button: downloadCategoryButton, phase: downloadTotal > 0 ? .pending(total: downloadTotal) : nil,
                           iconName: "arrow.left", color: .materialPrimary(light: .Material.Orange._600, dark: .Material.Orange._200))
        applyCategoryPhase(button: syncCategoryButton, phase: syncTotal > 0 ? .pending(total: syncTotal) : nil,
                           iconName: "arrow.left.arrow.right", color: .materialPrimary(light: .Material.Purple._600, dark: .Material.Purple._200))

        applyPauseAppearance()
        stopButton.isHidden = false
        applyStopDefaultAppearance()
        stopButton.isEnabled = true
    }

    func updateExecution(
        backupPhase: CategoryPhase?,
        downloadPhase: CategoryPhase?,
        syncPhase: CategoryPhase?,
        phase: ExecutionPhase,
        controlState: ExecutionControlState
    ) {
        if controlState == .stopping {
            isStopTransitionLatched = true
        }

        applyCategoryPhase(button: backupCategoryButton, phase: backupPhase,
                           iconName: "arrow.right", color: .materialPrimary(light: .Material.Cyan._600, dark: .Material.Cyan._200))
        applyCategoryPhase(button: downloadCategoryButton, phase: downloadPhase,
                           iconName: "arrow.left", color: .materialPrimary(light: .Material.Orange._600, dark: .Material.Orange._200))
        applyCategoryPhase(button: syncCategoryButton, phase: syncPhase,
                           iconName: "arrow.left.arrow.right", color: .materialPrimary(light: .Material.Purple._600, dark: .Material.Purple._200))

        if isStopTransitionLatched, controlState != .stopping {
            return
        }

        guard lastRenderedExecutionPhase != phase || lastRenderedControlState != controlState else {
            return
        }
        lastRenderedExecutionPhase = phase
        lastRenderedControlState = controlState

        isPaused = false
        isCompleted = false
        stopButton.isHidden = false
        applyStopDefaultAppearance()
        stopButton.isEnabled = true

        switch controlState {
        case .starting:
            applyExecuteLoadingAppearance(
                light: .Material.Green._600,
                dark: .Material.Green._200,
                onDark: .Material.Green._800
            )
            executeButton.isEnabled = false
            stopButton.isEnabled = true
            return
        case .resuming:
            applyExecuteLoadingAppearance(
                light: .Material.Green._600,
                dark: .Material.Green._200,
                onDark: .Material.Green._800
            )
            executeButton.isEnabled = false
            stopButton.isEnabled = true
            return
        case .pausing:
            applyExecuteLoadingAppearance(
                light: .Material.Orange._600,
                dark: .Material.Orange._200,
                onDark: .Material.Orange._800
            )
            executeButton.isEnabled = false
            stopButton.isEnabled = true
            return
        case .stopping:
            applyPrimaryAppearance(for: phase)
            executeButton.isEnabled = false
            applyStopLoadingAppearance()
            stopButton.isEnabled = false
            return
        case .idle:
            break
        }

        switch phase {
        case .uploading, .downloading:
            applyPauseAppearance()
            executeButton.isEnabled = true
        case .uploadPaused, .downloadPaused:
            isPaused = true
            applyResumeAppearance()
            executeButton.isEnabled = true
        case .completed:
            isCompleted = true
            applyCompleteAppearance()
            executeButton.isEnabled = true
            stopButton.isHidden = true
        case .failed:
            applyFailedAppearance()
            executeButton.isEnabled = false
            stopButton.isEnabled = true
        }
    }

    func resetToSelection() {
        isExecuting = false
        isPaused = false
        isCompleted = false
        lastRenderedExecutionPhase = nil
        lastRenderedControlState = nil
        isStopTransitionLatched = false

        failureSummaryButton.isHidden = true
        failureSummaryButton.menu = nil
        backupCategoryButton.showsMenuAsPrimaryAction = true
        downloadCategoryButton.showsMenuAsPrimaryAction = true
        syncCategoryButton.showsMenuAsPrimaryAction = true

        let iconConfig = UIImage.SymbolConfiguration(pointSize: 15, weight: .bold)
        for (button, iconName, color) in [
            (backupCategoryButton, "arrow.right", UIColor.materialPrimary(light: .Material.Cyan._600, dark: .Material.Cyan._200)),
            (downloadCategoryButton, "arrow.left", UIColor.materialPrimary(light: .Material.Orange._600, dark: .Material.Orange._200)),
            (syncCategoryButton, "arrow.left.arrow.right", UIColor.materialPrimary(light: .Material.Purple._600, dark: .Material.Purple._200)),
        ] {
            button.isUserInteractionEnabled = true
            button.configuration?.showsActivityIndicator = false
            button.configuration?.image = UIImage(systemName: iconName, withConfiguration: iconConfig)
            button.configuration?.baseForegroundColor = color
        }

        let executeIconConfig = UIImage.SymbolConfiguration(pointSize: 14, weight: .bold)
        var cfg = UIButton.Configuration.filled()
        cfg.image = UIImage(systemName: "play.fill", withConfiguration: executeIconConfig)
        cfg.cornerStyle = .capsule
        cfg.baseBackgroundColor = .materialPrimary(light: .Material.Green._600, dark: .Material.Green._200)
        cfg.baseForegroundColor = .materialOnPrimary(dark: .Material.Green._800)
        cfg.contentInsets = .init(top: 8, leading: 14, bottom: 8, trailing: 14)
        executeButton.configuration = cfg
        executeButton.isEnabled = true

        stopButton.isHidden = true
        applyStopDefaultAppearance()
    }

    // MARK: - Private Helpers

    private func applyCategoryPhase(button: UIButton, phase: CategoryPhase?, iconName: String, color: UIColor) {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 15, weight: .bold)
        guard let phase else {
            button.isHidden = true
            return
        }
        button.isHidden = false
        button.isUserInteractionEnabled = false

        switch phase {
        case .pending(let total):
            button.configuration?.showsActivityIndicator = false
            button.configuration?.image = UIImage(systemName: iconName, withConfiguration: iconConfig)
            button.configuration?.title = "\(total)"
            button.configuration?.baseForegroundColor = color.withAlphaComponent(0.5)
        case .running(let completed, let failed, let total):
            button.configuration?.showsActivityIndicator = true
            if failed > 0 {
                button.configuration?.image = UIImage(systemName: "exclamationmark.triangle.fill", withConfiguration: iconConfig)
                button.configuration?.title = "\(completed)/\(total)"
                button.configuration?.baseForegroundColor = .systemOrange
            } else {
                button.configuration?.title = "\(completed)/\(total)"
                button.configuration?.baseForegroundColor = color
            }
        case .paused(let completed, let total):
            button.configuration?.showsActivityIndicator = false
            button.configuration?.image = UIImage(systemName: "pause.fill", withConfiguration: iconConfig)
            button.configuration?.title = "\(completed)/\(total)"
            button.configuration?.baseForegroundColor = color
        case .completed(let total):
            button.configuration?.showsActivityIndicator = false
            button.configuration?.image = UIImage(systemName: "checkmark", withConfiguration: iconConfig)
            button.configuration?.title = "\(total)/\(total)"
            button.configuration?.baseForegroundColor = color
        case .failed(let completed, let failed, let total):
            button.configuration?.showsActivityIndicator = false
            button.configuration?.image = UIImage(systemName: "exclamationmark.triangle.fill", withConfiguration: iconConfig)
            button.configuration?.title = "\(completed + failed)/\(total)"
            button.configuration?.baseForegroundColor = .systemRed
        }
    }

    private func applyPauseAppearance() {
        applyExecuteButtonStyle(iconName: "pause.fill", light: .Material.Orange._600, dark: .Material.Orange._200, onDark: .Material.Orange._800)
    }

    private func applyResumeAppearance() {
        applyExecuteButtonStyle(iconName: "play.fill", light: .Material.Green._600, dark: .Material.Green._200, onDark: .Material.Green._800)
    }

    private func applyFailedAppearance() {
        applyExecuteButtonStyle(iconName: "exclamationmark.triangle.fill", light: .Material.Red._600, dark: .Material.Red._200, onDark: .Material.Red._800)
    }

    private func applyCompleteAppearance() {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 14, weight: .bold)
        var cfg = UIButton.Configuration.filled()
        cfg.image = UIImage(systemName: "checkmark", withConfiguration: iconConfig)
        cfg.cornerStyle = .capsule
        cfg.baseBackgroundColor = .materialPrimary(light: .Material.Green._600, dark: .Material.Green._200)
        cfg.baseForegroundColor = .materialOnPrimary(dark: .Material.Green._800)
        cfg.contentInsets = .init(top: 8, leading: 14, bottom: 8, trailing: 14)
        executeButton.configuration = cfg
    }

    private func applyExecuteLoadingAppearance(light: UIColor, dark: UIColor, onDark: UIColor) {
        var cfg = UIButton.Configuration.filled()
        cfg.showsActivityIndicator = true
        cfg.cornerStyle = .capsule
        cfg.baseBackgroundColor = .materialPrimary(light: light, dark: dark)
        cfg.baseForegroundColor = .materialOnPrimary(dark: onDark)
        cfg.contentInsets = .init(top: 8, leading: 16, bottom: 8, trailing: 16)
        executeButton.configuration = cfg
    }

    private func applyStopDefaultAppearance() {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 14, weight: .bold)
        var cfg = UIButton.Configuration.filled()
        cfg.image = UIImage(systemName: "stop.fill", withConfiguration: iconConfig)
        cfg.cornerStyle = .capsule
        cfg.baseBackgroundColor = .materialPrimary(light: .Material.Red._600, dark: .Material.Red._200)
        cfg.baseForegroundColor = .materialOnPrimary(dark: .Material.Red._800)
        cfg.contentInsets = .init(top: 8, leading: 14, bottom: 8, trailing: 14)
        stopButton.configuration = cfg
    }

    private func applyStopLoadingAppearance() {
        var cfg = UIButton.Configuration.filled()
        cfg.showsActivityIndicator = true
        cfg.cornerStyle = .capsule
        cfg.baseBackgroundColor = .materialPrimary(light: .Material.Red._600, dark: .Material.Red._200)
        cfg.baseForegroundColor = .materialOnPrimary(dark: .Material.Red._800)
        cfg.contentInsets = .init(top: 8, leading: 14, bottom: 8, trailing: 14)
        stopButton.configuration = cfg
    }

    private func applyPrimaryAppearance(for phase: ExecutionPhase) {
        switch phase {
        case .uploading, .downloading:
            applyPauseAppearance()
        case .uploadPaused, .downloadPaused:
            isPaused = true
            applyResumeAppearance()
        case .completed:
            isCompleted = true
            applyCompleteAppearance()
        case .failed:
            applyFailedAppearance()
        }
    }

    private func applyExecuteButtonStyle(iconName: String, light: UIColor, dark: UIColor, onDark: UIColor) {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 14, weight: .bold)
        var cfg = UIButton.Configuration.filled()
        cfg.image = UIImage(systemName: iconName, withConfiguration: iconConfig)
        cfg.cornerStyle = .capsule
        cfg.baseBackgroundColor = .materialPrimary(light: light, dark: dark)
        cfg.baseForegroundColor = .materialOnPrimary(dark: onDark)
        cfg.contentInsets = .init(top: 8, leading: 14, bottom: 8, trailing: 14)
        executeButton.configuration = cfg
    }
}
