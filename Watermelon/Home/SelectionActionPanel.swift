import SnapKit
import UIKit

final class SelectionActionPanel: UIView {

    enum CategoryPhase {
        case pending(total: Int)
        case running(completed: Int, total: Int)
        case completed(total: Int)
    }

    var onExecuteTapped: (() -> Void)?
    var onPauseTapped: (() -> Void)?
    var onStopTapped: (() -> Void)?
    var onResumeTapped: (() -> Void)?
    var onCompleteTapped: (() -> Void)?

    private var isExecuting = false
    private var isPaused = false
    private var isCompleted = false

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
        var cfg = UIButton.Configuration.filled()
        cfg.title = "执行"
        cfg.cornerStyle = .capsule
        cfg.baseBackgroundColor = .materialPrimary(light: .Material.Green._600, dark: .Material.Green._200)
        cfg.baseForegroundColor = .materialOnPrimary(dark: .Material.Green._800)
        cfg.contentInsets = .init(top: 8, leading: 20, bottom: 8, trailing: 20)
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

        let spacer = UIView()
        spacer.setContentHuggingPriority(.defaultLow, for: .horizontal)

        let contentStack = UIStackView(arrangedSubviews: [backupCategoryButton, downloadCategoryButton, syncCategoryButton, spacer, stopButton, executeButton])
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

    func enterExecution(backupTotal: Int, downloadTotal: Int, syncTotal: Int) {
        isExecuting = true
        isPaused = false
        isCompleted = false

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
        stopButton.isEnabled = true
    }

    func updateExecution(
        backupPhase: CategoryPhase?,
        downloadPhase: CategoryPhase?,
        syncPhase: CategoryPhase?,
        state: BackupSessionController.State
    ) {
        applyCategoryPhase(button: backupCategoryButton, phase: backupPhase,
                           iconName: "arrow.right", color: .materialPrimary(light: .Material.Cyan._600, dark: .Material.Cyan._200))
        applyCategoryPhase(button: downloadCategoryButton, phase: downloadPhase,
                           iconName: "arrow.left", color: .materialPrimary(light: .Material.Orange._600, dark: .Material.Orange._200))
        applyCategoryPhase(button: syncCategoryButton, phase: syncPhase,
                           iconName: "arrow.left.arrow.right", color: .materialPrimary(light: .Material.Purple._600, dark: .Material.Purple._200))

        switch state {
        case .running:
            isPaused = false
            applyPauseAppearance()
            executeButton.isEnabled = true
            stopButton.isEnabled = true
        case .paused:
            isPaused = true
            applyResumeAppearance()
            executeButton.isEnabled = true
            stopButton.isEnabled = true
        case .completed:
            isPaused = false
            isCompleted = true
            applyCompleteAppearance()
            executeButton.isEnabled = true
            stopButton.isHidden = true
        case .failed:
            isPaused = false
            executeButton.isEnabled = false
            stopButton.isEnabled = true
        case .stopped:
            isPaused = false
            executeButton.isEnabled = false
            stopButton.isEnabled = false
        case .idle:
            isPaused = false
            executeButton.isEnabled = false
            stopButton.isEnabled = false
        }
    }

    func resetToSelection() {
        isExecuting = false
        isPaused = false
        isCompleted = false

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

        var cfg = UIButton.Configuration.filled()
        cfg.title = "执行"
        cfg.cornerStyle = .capsule
        cfg.baseBackgroundColor = .materialPrimary(light: .Material.Green._600, dark: .Material.Green._200)
        cfg.baseForegroundColor = .materialOnPrimary(dark: .Material.Green._800)
        cfg.contentInsets = .init(top: 8, leading: 20, bottom: 8, trailing: 20)
        executeButton.configuration = cfg
        executeButton.isEnabled = true

        stopButton.isHidden = true
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
        case .running(let completed, let total):
            button.configuration?.showsActivityIndicator = true
            button.configuration?.title = "\(completed)/\(total)"
            button.configuration?.baseForegroundColor = color
        case .completed(let total):
            button.configuration?.showsActivityIndicator = false
            button.configuration?.image = UIImage(systemName: "checkmark", withConfiguration: iconConfig)
            button.configuration?.title = "\(total)/\(total)"
            button.configuration?.baseForegroundColor = color
        }
    }

    private func applyPauseAppearance() {
        applyExecuteButtonStyle(iconName: "pause.fill", light: .Material.Orange._600, dark: .Material.Orange._200, onDark: .Material.Orange._800)
    }

    private func applyResumeAppearance() {
        applyExecuteButtonStyle(iconName: "play.fill", light: .Material.Green._600, dark: .Material.Green._200, onDark: .Material.Green._800)
    }

    private func applyCompleteAppearance() {
        var cfg = UIButton.Configuration.filled()
        cfg.title = "完成"
        cfg.cornerStyle = .capsule
        cfg.baseBackgroundColor = .materialPrimary(light: .Material.Green._600, dark: .Material.Green._200)
        cfg.baseForegroundColor = .materialOnPrimary(dark: .Material.Green._800)
        cfg.contentInsets = .init(top: 8, leading: 20, bottom: 8, trailing: 20)
        executeButton.configuration = cfg
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
