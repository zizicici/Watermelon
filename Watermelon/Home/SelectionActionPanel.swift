import SnapKit
import UIKit

final class SelectionActionPanel: UIView {

    enum CategoryPhase: Equatable {
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

    private var renderedState: SelectionActionPanelViewState?

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
        guard let renderedState else {
            onExecuteTapped?()
            return
        }

        switch renderedState.primaryAction {
        case .execute:
            onExecuteTapped?()
        case .pause:
            onPauseTapped?()
        case .resume:
            onResumeTapped?()
        case .complete:
            onCompleteTapped?()
        case .none:
            break
        }
    }

    @objc private func stopTappedAction() {
        guard case .execution(let state) = renderedState, state.stopAction != nil else { return }
        onStopTapped?()
    }

    func render(
        state: SelectionActionPanelViewState,
        menus: SelectionActionPanelMenus = .empty
    ) {
        applyMenus(menus, for: state)
        guard renderedState != state else { return }

        renderedState = state
        switch state {
        case .selection(let selectionState):
            renderSelection(selectionState)
        case .execution(let executionState):
            renderExecution(executionState)
        }
    }

    // MARK: - Private Helpers

    private func applyMenus(
        _ menus: SelectionActionPanelMenus,
        for state: SelectionActionPanelViewState
    ) {
        switch state {
        case .selection:
            failureSummaryButton.isHidden = true
            failureSummaryButton.menu = nil

            backupCategoryButton.showsMenuAsPrimaryAction = menus.backup != nil
            backupCategoryButton.menu = menus.backup
            downloadCategoryButton.showsMenuAsPrimaryAction = menus.download != nil
            downloadCategoryButton.menu = menus.download
            syncCategoryButton.showsMenuAsPrimaryAction = menus.sync != nil
            syncCategoryButton.menu = menus.sync
        case .execution(let executionState):
            if let title = executionState.failureSummaryTitle,
               let menu = menus.failureSummary {
                failureSummaryButton.isHidden = false
                failureSummaryButton.menu = menu
                failureSummaryButton.configuration?.title = title
            } else {
                failureSummaryButton.isHidden = true
                failureSummaryButton.menu = nil
            }

            backupCategoryButton.showsMenuAsPrimaryAction = false
            backupCategoryButton.menu = nil
            downloadCategoryButton.showsMenuAsPrimaryAction = false
            downloadCategoryButton.menu = nil
            syncCategoryButton.showsMenuAsPrimaryAction = false
            syncCategoryButton.menu = nil
        }
    }

    private func renderSelection(_ state: SelectionActionPanelSelectionState) {
        applySelectionCategory(
            button: backupCategoryButton,
            count: state.backupCount,
            iconName: "arrow.right",
            color: .materialPrimary(light: .Material.Cyan._600, dark: .Material.Cyan._200)
        )
        applySelectionCategory(
            button: downloadCategoryButton,
            count: state.downloadCount,
            iconName: "arrow.left",
            color: .materialPrimary(light: .Material.Orange._600, dark: .Material.Orange._200)
        )
        applySelectionCategory(
            button: syncCategoryButton,
            count: state.syncCount,
            iconName: "arrow.left.arrow.right",
            color: .materialPrimary(light: .Material.Purple._600, dark: .Material.Purple._200)
        )

        applyPrimaryButton(
            SelectionActionPanelButtonState(
                style: .execute,
                isEnabled: true,
                showsSpinner: false,
                isHidden: false
            )
        )
        applyStopButton(nil)
    }

    private func renderExecution(_ state: SelectionActionPanelExecutionState) {
        applyCategoryPhase(
            button: backupCategoryButton,
            phase: state.backupPhase,
            iconName: "arrow.right",
            color: .materialPrimary(light: .Material.Cyan._600, dark: .Material.Cyan._200)
        )
        applyCategoryPhase(
            button: downloadCategoryButton,
            phase: state.downloadPhase,
            iconName: "arrow.left",
            color: .materialPrimary(light: .Material.Orange._600, dark: .Material.Orange._200)
        )
        applyCategoryPhase(
            button: syncCategoryButton,
            phase: state.syncPhase,
            iconName: "arrow.left.arrow.right",
            color: .materialPrimary(light: .Material.Purple._600, dark: .Material.Purple._200)
        )
        applyPrimaryButton(state.primaryButton)
        applyStopButton(state.stopButton)
    }

    private func applySelectionCategory(
        button: UIButton,
        count: Int,
        iconName: String,
        color: UIColor
    ) {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 15, weight: .bold)
        button.isHidden = count == 0
        guard count > 0 else { return }

        button.isUserInteractionEnabled = true
        button.configuration?.showsActivityIndicator = false
        button.configuration?.image = UIImage(systemName: iconName, withConfiguration: iconConfig)
        button.configuration?.title = "\(count)"
        button.configuration?.baseForegroundColor = color
    }

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

    private func applyPrimaryButton(_ state: SelectionActionPanelButtonState) {
        executeButton.isEnabled = state.isEnabled
        executeButton.isHidden = state.isHidden
        if state.showsSpinner {
            applyExecuteLoadingAppearance(style: state.style)
            return
        }

        switch state.style {
        case .execute:
            applyExecuteButtonStyle(
                iconName: "play.fill",
                light: .Material.Green._600,
                dark: .Material.Green._200,
                onDark: .Material.Green._800
            )
        case .pause:
            applyExecuteButtonStyle(
                iconName: "pause.fill",
                light: .Material.Orange._600,
                dark: .Material.Orange._200,
                onDark: .Material.Orange._800
            )
        case .resume:
            applyExecuteButtonStyle(
                iconName: "play.fill",
                light: .Material.Green._600,
                dark: .Material.Green._200,
                onDark: .Material.Green._800
            )
        case .complete:
            applyExecuteButtonStyle(
                iconName: "checkmark",
                light: .Material.Green._600,
                dark: .Material.Green._200,
                onDark: .Material.Green._800
            )
        case .failed:
            applyExecuteButtonStyle(
                iconName: "exclamationmark.triangle.fill",
                light: .Material.Red._600,
                dark: .Material.Red._200,
                onDark: .Material.Red._800
            )
        case .stop:
            applyExecuteButtonStyle(
                iconName: "stop.fill",
                light: .Material.Red._600,
                dark: .Material.Red._200,
                onDark: .Material.Red._800
            )
        }
    }

    private func applyExecuteLoadingAppearance(style: SelectionActionPanelButtonStyle) {
        let palette: (UIColor, UIColor, UIColor)
        switch style {
        case .pause:
            palette = (.Material.Orange._600, .Material.Orange._200, .Material.Orange._800)
        case .failed, .stop:
            palette = (.Material.Red._600, .Material.Red._200, .Material.Red._800)
        case .execute, .resume, .complete:
            palette = (.Material.Green._600, .Material.Green._200, .Material.Green._800)
        }

        var cfg = UIButton.Configuration.filled()
        cfg.showsActivityIndicator = true
        cfg.cornerStyle = .capsule
        cfg.baseBackgroundColor = .materialPrimary(light: palette.0, dark: palette.1)
        cfg.baseForegroundColor = .materialOnPrimary(dark: palette.2)
        cfg.contentInsets = .init(top: 8, leading: 16, bottom: 8, trailing: 16)
        executeButton.configuration = cfg
    }

    private func applyStopButton(_ state: SelectionActionPanelButtonState?) {
        guard let state, !state.isHidden else {
            stopButton.isHidden = true
            return
        }

        stopButton.isHidden = false
        stopButton.isEnabled = state.isEnabled
        if state.showsSpinner {
            applyStopLoadingAppearance()
        } else {
            applyStopDefaultAppearance()
        }
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
