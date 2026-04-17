import SnapKit
import UIKit

final class SelectionActionPanel: UIView {
    private enum Layout {
        static let executionControlButtonWidth: CGFloat = 72
        static let executionControlButtonHeight: CGFloat = 36
        static let selectionCategoryRowHeight: CGFloat = 52
        static let selectionCategoryButtonInsets = NSDirectionalEdgeInsets(top: 6, leading: 0, bottom: 6, trailing: 0)
        static let executionCategoryButtonInsets = NSDirectionalEdgeInsets(top: 2, leading: 0, bottom: 2, trailing: 2)
        static let panelLeadingInset: CGFloat = 18
        static let panelTrailingInset: CGFloat = 12
        static let panelVerticalInset: CGFloat = 12
    }

    var onExecuteTapped: (() -> Void)?
    var onPauseTapped: (() -> Void)?
    var onStopTapped: (() -> Void)?
    var onResumeTapped: (() -> Void)?
    var onCompleteTapped: (() -> Void)?
    var onExecutionDetailsTapped: (() -> Void)?

    private var renderedState: SelectionActionPanelViewState?

    private let separator = UIView()
    private let leftContentStack = UIStackView()
    private let categoryScrollView = UIScrollView()
    private let categoryRowStack = UIStackView()
    private(set) var backupCategoryButton = SelectionActionPanel.makeSelectionCategoryButton(
        iconName: "arrow.right",
        subtitle: String(localized: "panel.backup"),
        color: .materialPrimary(light: .Material.Cyan._600, dark: .Material.Cyan._200)
    )
    private(set) var downloadCategoryButton = SelectionActionPanel.makeSelectionCategoryButton(
        iconName: "arrow.left",
        subtitle: String(localized: "panel.download"),
        color: .materialPrimary(light: .Material.Orange._600, dark: .Material.Orange._200)
    )
    private(set) var syncCategoryButton = SelectionActionPanel.makeSelectionCategoryButton(
        iconName: "arrow.left.arrow.right",
        subtitle: String(localized: "panel.sync"),
        color: .materialPrimary(light: .Material.Purple._600, dark: .Material.Purple._200)
    )
    private let executionInfoStack = UIStackView()
    private let executionCategoryRow = UIStackView()
    private let executionUploadCategoryButton = SelectionActionPanel.makeExecutionCategoryButton(
        iconName: "arrow.right",
        color: .materialPrimary(light: .Material.Cyan._600, dark: .Material.Cyan._200)
    )
    private let executionDownloadCategoryButton = SelectionActionPanel.makeExecutionCategoryButton(
        iconName: "arrow.left",
        color: .materialPrimary(light: .Material.Orange._600, dark: .Material.Orange._200)
    )
    private let executionSyncCategoryButton = SelectionActionPanel.makeExecutionCategoryButton(
        iconName: "arrow.left.arrow.right",
        color: .materialPrimary(light: .Material.Purple._600, dark: .Material.Purple._200)
    )
    private let executionStatusButton: UIButton = {
        var cfg = UIButton.Configuration.plain()
        cfg.title = "Log"
        cfg.image = UIImage(
            systemName: "chevron.right",
            withConfiguration: UIImage.SymbolConfiguration(pointSize: 10, weight: .semibold)
        )
        cfg.imagePlacement = .trailing
        cfg.imagePadding = 4
        cfg.titleAlignment = .leading
        cfg.baseForegroundColor = .secondaryLabel
        cfg.titleTextAttributesTransformer = .init {
            var a = $0; a.font = .systemFont(ofSize: 15, weight: .semibold); return a
        }
        cfg.contentInsets = .init(top: 0, leading: 0, bottom: 0, trailing: 0)
        cfg.cornerStyle = .fixed
        cfg.background = .clear()
        let button = UIButton(configuration: cfg)
        button.contentHorizontalAlignment = .leading
        button.isHidden = true
        return button
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

    private static func makeSelectionCategoryButton(
        iconName: String,
        subtitle: String,
        color: UIColor
    ) -> UIButton {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 15, weight: .bold)
        var cfg = UIButton.Configuration.plain()
        cfg.image = UIImage(systemName: iconName, withConfiguration: iconConfig)
        cfg.imagePadding = 6
        cfg.titleAlignment = .leading
        cfg.subtitle = subtitle
        cfg.contentInsets = Layout.selectionCategoryButtonInsets
        cfg.subtitleTextAttributesTransformer = .init {
            var attributes = $0
            attributes.font = .preferredFont(forTextStyle: .caption1)
            return attributes
        }
        cfg.baseForegroundColor = color
        let button = UIButton(configuration: cfg)
        button.contentHorizontalAlignment = .leading
        button.titleLabel?.adjustsFontForContentSizeCategory = true
        button.titleLabel?.lineBreakMode = .byTruncatingTail
        button.setContentCompressionResistancePriority(.required, for: .vertical)
        return button
    }

    private static func makeExecutionCategoryButton(
        iconName: String,
        color: UIColor
    ) -> UIButton {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 12, weight: .semibold)
        var cfg = UIButton.Configuration.plain()
        cfg.image = UIImage(systemName: iconName, withConfiguration: iconConfig)
        cfg.imagePadding = 3
        cfg.titleAlignment = .leading
        cfg.contentInsets = Layout.executionCategoryButtonInsets
        cfg.titleTextAttributesTransformer = .init {
            var attributes = $0
            attributes.font = .monospacedDigitSystemFont(ofSize: 13, weight: .semibold)
            return attributes
        }
        cfg.baseForegroundColor = color
        let button = UIButton(configuration: cfg)
        button.contentHorizontalAlignment = .leading
        button.titleLabel?.adjustsFontForContentSizeCategory = true
        button.titleLabel?.lineBreakMode = .byClipping
        button.setContentHuggingPriority(.required, for: .horizontal)
        button.setContentCompressionResistancePriority(.required, for: .vertical)
        button.clipsToBounds = false
        return button
    }

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
        executionUploadCategoryButton.addTarget(self, action: #selector(executionDetailsTapped), for: .touchUpInside)
        executionDownloadCategoryButton.addTarget(self, action: #selector(executionDetailsTapped), for: .touchUpInside)
        executionSyncCategoryButton.addTarget(self, action: #selector(executionDetailsTapped), for: .touchUpInside)
        executionStatusButton.addTarget(self, action: #selector(executionDetailsTapped), for: .touchUpInside)
        stopButton.isHidden = true
        executeButton.snp.makeConstraints { make in
            make.width.equalTo(Layout.executionControlButtonWidth)
            make.height.equalTo(Layout.executionControlButtonHeight)
        }
        stopButton.snp.makeConstraints { make in
            make.width.equalTo(Layout.executionControlButtonWidth)
            make.height.equalTo(Layout.executionControlButtonHeight)
        }

        categoryScrollView.showsHorizontalScrollIndicator = false
        categoryScrollView.showsVerticalScrollIndicator = false
        categoryScrollView.alwaysBounceHorizontal = true

        categoryRowStack.axis = .horizontal
        categoryRowStack.spacing = 4
        categoryRowStack.alignment = .fill
        categoryRowStack.addArrangedSubview(backupCategoryButton)
        categoryRowStack.addArrangedSubview(downloadCategoryButton)
        categoryRowStack.addArrangedSubview(syncCategoryButton)

        categoryScrollView.addSubview(categoryRowStack)
        categoryRowStack.snp.makeConstraints { make in
            make.edges.equalTo(categoryScrollView.contentLayoutGuide)
            make.height.equalTo(categoryScrollView.frameLayoutGuide)
        }
        categoryScrollView.snp.makeConstraints { make in
            make.height.equalTo(Layout.selectionCategoryRowHeight)
        }

        executionCategoryRow.axis = .horizontal
        executionCategoryRow.spacing = 6
        executionCategoryRow.alignment = .fill
        executionCategoryRow.setContentHuggingPriority(.required, for: .vertical)
        let executionCategorySpacer = UIView()
        executionCategorySpacer.setContentHuggingPriority(.defaultLow, for: .horizontal)
        executionCategoryRow.addArrangedSubview(executionUploadCategoryButton)
        executionCategoryRow.addArrangedSubview(executionDownloadCategoryButton)
        executionCategoryRow.addArrangedSubview(executionSyncCategoryButton)
        executionCategoryRow.addArrangedSubview(executionCategorySpacer)

        executionInfoStack.axis = .vertical
        executionInfoStack.spacing = 6
        executionInfoStack.alignment = .fill
        executionInfoStack.addArrangedSubview(executionCategoryRow)
        executionInfoStack.addArrangedSubview(executionStatusButton)
        executionInfoStack.isHidden = true
        executionInfoStack.snp.makeConstraints { make in
            make.height.equalTo(Layout.selectionCategoryRowHeight)
        }

        leftContentStack.axis = .vertical
        leftContentStack.alignment = .fill
        leftContentStack.distribution = .fill
        leftContentStack.spacing = 0
        leftContentStack.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        leftContentStack.setContentHuggingPriority(.defaultLow, for: .horizontal)
        leftContentStack.addArrangedSubview(categoryScrollView)
        leftContentStack.addArrangedSubview(executionInfoStack)

        let contentStack = UIStackView(arrangedSubviews: [leftContentStack, stopButton, executeButton])
        contentStack.axis = .horizontal
        contentStack.spacing = 4
        contentStack.setCustomSpacing(12, after: stopButton)
        contentStack.alignment = .center

        addSubview(contentStack)
        contentStack.snp.makeConstraints { make in
            make.top.equalToSuperview().inset(Layout.panelVerticalInset)
            make.leading.equalToSuperview().inset(Layout.panelLeadingInset)
            make.trailing.equalToSuperview().inset(Layout.panelTrailingInset)
            make.bottom.equalTo(safeAreaLayoutGuide).inset(Layout.panelVerticalInset)
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

    @objc private func executionDetailsTapped() {
        guard case .execution = renderedState else { return }
        onExecutionDetailsTapped?()
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
            backupCategoryButton.showsMenuAsPrimaryAction = menus.backup != nil
            backupCategoryButton.menu = menus.backup
            downloadCategoryButton.showsMenuAsPrimaryAction = menus.download != nil
            downloadCategoryButton.menu = menus.download
            syncCategoryButton.showsMenuAsPrimaryAction = menus.sync != nil
            syncCategoryButton.menu = menus.sync
        case .execution:
            backupCategoryButton.showsMenuAsPrimaryAction = false
            backupCategoryButton.menu = nil
            downloadCategoryButton.showsMenuAsPrimaryAction = false
            downloadCategoryButton.menu = nil
            syncCategoryButton.showsMenuAsPrimaryAction = false
            syncCategoryButton.menu = nil
        }
    }

    private func renderSelection(_ state: SelectionActionPanelSelectionState) {
        categoryScrollView.isHidden = false
        executionInfoStack.isHidden = true

        applySelectionCategory(
            button: backupCategoryButton,
            count: state.backupCount,
            iconName: "arrow.right",
            subtitle: String(localized: "panel.backup"),
            color: .materialPrimary(light: .Material.Cyan._600, dark: .Material.Cyan._200)
        )
        applySelectionCategory(
            button: downloadCategoryButton,
            count: state.downloadCount,
            iconName: "arrow.left",
            subtitle: String(localized: "panel.download"),
            color: .materialPrimary(light: .Material.Orange._600, dark: .Material.Orange._200)
        )
        applySelectionCategory(
            button: syncCategoryButton,
            count: state.syncCount,
            iconName: "arrow.left.arrow.right",
            subtitle: String(localized: "panel.sync"),
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
        categoryScrollView.isHidden = true
        executionInfoStack.isHidden = false

        applyExecutionCategory(
            button: executionUploadCategoryButton,
            count: state.uploadCount,
            iconName: "arrow.right",
            color: .materialPrimary(light: .Material.Cyan._600, dark: .Material.Cyan._200)
        )
        applyExecutionCategory(
            button: executionDownloadCategoryButton,
            count: state.downloadCount,
            iconName: "arrow.left",
            color: .materialPrimary(light: .Material.Orange._600, dark: .Material.Orange._200)
        )
        applyExecutionCategory(
            button: executionSyncCategoryButton,
            count: state.syncCount,
            iconName: "arrow.left.arrow.right",
            color: .materialPrimary(light: .Material.Purple._600, dark: .Material.Purple._200)
        )
        applyExecutionStatus(text: state.statusText, logAlertText: state.logAlertText)
        applyPrimaryButton(state.primaryButton)
        applyStopButton(state.stopButton)
    }

    private func applySelectionCategory(
        button: UIButton,
        count: Int,
        iconName: String,
        subtitle: String,
        color: UIColor
    ) {
        button.isHidden = count == 0
        guard count > 0, var cfg = button.configuration else { return }

        let iconConfig = UIImage.SymbolConfiguration(pointSize: 15, weight: .bold)
        button.isUserInteractionEnabled = true
        cfg.showsActivityIndicator = false
        cfg.image = UIImage(systemName: iconName, withConfiguration: iconConfig)
        cfg.title = "\(count)"
        cfg.subtitle = subtitle
        cfg.baseForegroundColor = color
        button.configuration = cfg
    }

    private func applyExecutionCategory(
        button: UIButton,
        count: Int,
        iconName: String,
        color: UIColor
    ) {
        guard var cfg = button.configuration else { return }

        let iconConfig = UIImage.SymbolConfiguration(pointSize: 10, weight: .semibold)
        button.isHidden = count == 0
        guard count > 0 else { return }
        button.isUserInteractionEnabled = true
        cfg.showsActivityIndicator = false
        cfg.image = UIImage(systemName: iconName, withConfiguration: iconConfig)
        cfg.title = "\(count)"
        cfg.subtitle = nil
        cfg.baseForegroundColor = color
        button.configuration = cfg
    }

    private func applyExecutionStatus(text: String, logAlertText: String?) {
        let trimmedStatus = text.trimmingCharacters(in: .whitespacesAndNewlines)
        let statusText = trimmedStatus.isEmpty ? "Log" : trimmedStatus
        let hasAlert = !(logAlertText?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ?? true)

        executionStatusButton.isHidden = false
        var cfg = executionStatusButton.configuration ?? .plain()
        cfg.title = statusText
        cfg.baseForegroundColor = hasAlert
            ? .materialPrimary(light: .Material.Orange._700, dark: .Material.Orange._200)
            : .materialOnSurfaceVariant(light: .Material.BlueGrey._700, dark: .Material.BlueGrey._200)
        executionStatusButton.configuration = cfg
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
        case .close:
            applyExecuteButtonStyle(
                iconName: "xmark",
                light: .Material.BlueGrey._600,
                dark: .Material.BlueGrey._200,
                onDark: .Material.BlueGrey._800
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
        case .close:
            palette = (.Material.BlueGrey._600, .Material.BlueGrey._200, .Material.BlueGrey._800)
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
