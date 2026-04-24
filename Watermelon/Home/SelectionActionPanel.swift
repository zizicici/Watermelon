import SnapKit
import UIKit

final class SelectionActionPanel: UIView {
    private enum SelectionCategorySection: Hashable {
        case main
    }

    private enum SelectionCategoryKind: Int, Hashable {
        case backup
        case download
        case complement

        var intent: MonthIntent {
            switch self {
            case .backup: return .backup
            case .download: return .download
            case .complement: return .complement
            }
        }
    }

    private struct SelectionCategoryItem: Hashable {
        let kind: SelectionCategoryKind
        let count: Int

        func hash(into hasher: inout Hasher) {
            hasher.combine(kind)
        }

        static func == (lhs: Self, rhs: Self) -> Bool {
            lhs.kind == rhs.kind
        }
    }

    private enum Layout {
        static let executionControlButtonWidth: CGFloat = 72
        static let executionControlButtonHeight: CGFloat = 36
        static let selectionCategoryRowHeight: CGFloat = 52
        static let selectionCategoryEstimatedWidth: CGFloat = 96
        static let selectionCategorySpacing: CGFloat = 10
        static let executionCategorySpacing: CGFloat = 10
        static let panelControlSpacing: CGFloat = 10
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
    private lazy var categoryCollectionView = UICollectionView(
        frame: .zero,
        collectionViewLayout: Self.makeSelectionCategoryLayout()
    )
    private var categoryDataSource: UICollectionViewDiffableDataSource<SelectionCategorySection, SelectionCategoryItem>?
    private var selectionCategoryMenus = SelectionActionPanelMenus.empty
    private let executionInfoStack = UIStackView()
    private let executionCategoryRow = UIStackView()
    private let executionBackupCategoryButton = SelectionActionPanel.makeExecutionCategoryButton(for: .backup)
    private let executionDownloadCategoryButton = SelectionActionPanel.makeExecutionCategoryButton(for: .download)
    private let executionComplementCategoryButton = SelectionActionPanel.makeExecutionCategoryButton(for: .complement)
    private let executionStatusButton: UIButton = {
        var cfg = UIButton.Configuration.plain()
        cfg.title = String(localized: "panel.log")
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

    private static func makeSelectionCategoryLayout() -> UICollectionViewLayout {
        let layout = UICollectionViewFlowLayout()
        layout.scrollDirection = .horizontal
        layout.minimumInteritemSpacing = Layout.selectionCategorySpacing
        layout.minimumLineSpacing = Layout.selectionCategorySpacing
        layout.sectionInset = .zero
        layout.estimatedItemSize = CGSize(
            width: Layout.selectionCategoryEstimatedWidth,
            height: Layout.selectionCategoryRowHeight
        )
        return layout
    }

    private static func makeSelectionCategoryButtonConfiguration(
        intent: MonthIntent,
        count: Int
    ) -> UIButton.Configuration {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 15, weight: .bold)
        var cfg = UIButton.Configuration.plain()
        cfg.image = UIImage(systemName: intent.iconSymbolName, withConfiguration: iconConfig)
        cfg.imagePadding = 6
        cfg.titleAlignment = .leading
        cfg.title = "\(count)"
        cfg.subtitle = intent.panelSubtitle
        cfg.contentInsets = Layout.selectionCategoryButtonInsets
        cfg.subtitleTextAttributesTransformer = .init {
            var attributes = $0
            attributes.font = .preferredFont(forTextStyle: .caption1)
            return attributes
        }
        cfg.baseForegroundColor = intent.tintColor
        return cfg
    }

    private static func makeExecutionCategoryButton(for intent: MonthIntent) -> UIButton {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 12, weight: .semibold)
        var cfg = UIButton.Configuration.plain()
        cfg.image = UIImage(systemName: intent.iconSymbolName, withConfiguration: iconConfig)
        cfg.imagePadding = 3
        cfg.titleAlignment = .leading
        cfg.contentInsets = Layout.executionCategoryButtonInsets
        cfg.titleTextAttributesTransformer = .init {
            var attributes = $0
            attributes.font = .monospacedDigitSystemFont(ofSize: 13, weight: .semibold)
            return attributes
        }
        cfg.baseForegroundColor = intent.tintColor
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
        executionBackupCategoryButton.addTarget(self, action: #selector(executionDetailsTapped), for: .touchUpInside)
        executionDownloadCategoryButton.addTarget(self, action: #selector(executionDetailsTapped), for: .touchUpInside)
        executionComplementCategoryButton.addTarget(self, action: #selector(executionDetailsTapped), for: .touchUpInside)
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

        configureCategoryCollectionView()
        categoryCollectionView.snp.makeConstraints { make in
            make.height.equalTo(Layout.selectionCategoryRowHeight)
        }

        executionCategoryRow.axis = .horizontal
        executionCategoryRow.spacing = Layout.executionCategorySpacing
        executionCategoryRow.alignment = .fill
        executionCategoryRow.setContentHuggingPriority(.required, for: .vertical)
        let executionCategorySpacer = UIView()
        executionCategorySpacer.setContentHuggingPriority(.defaultLow, for: .horizontal)
        executionCategoryRow.addArrangedSubview(executionBackupCategoryButton)
        executionCategoryRow.addArrangedSubview(executionDownloadCategoryButton)
        executionCategoryRow.addArrangedSubview(executionComplementCategoryButton)
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
        leftContentStack.addArrangedSubview(categoryCollectionView)
        leftContentStack.addArrangedSubview(executionInfoStack)

        let contentStack = UIStackView(arrangedSubviews: [leftContentStack, stopButton, executeButton])
        contentStack.axis = .horizontal
        contentStack.spacing = Layout.panelControlSpacing
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

    private func configureCategoryCollectionView() {
        categoryCollectionView.backgroundColor = .clear
        categoryCollectionView.showsHorizontalScrollIndicator = false
        categoryCollectionView.showsVerticalScrollIndicator = false
        categoryCollectionView.alwaysBounceHorizontal = true
        categoryCollectionView.delaysContentTouches = false

        let registration = UICollectionView.CellRegistration<SelectionCategoryCell, SelectionCategoryItem> { [weak self] cell, _, item in
            cell.configure(
                item: item,
                menu: self?.menu(for: item.kind)
            )
        }

        categoryDataSource = UICollectionViewDiffableDataSource<SelectionCategorySection, SelectionCategoryItem>(
            collectionView: categoryCollectionView
        ) { collectionView, indexPath, item in
            collectionView.dequeueConfiguredReusableCell(
                using: registration,
                for: indexPath,
                item: item
            )
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
            selectionCategoryMenus = menus
            reconfigureSelectionCategoryMenus()
        case .execution:
            selectionCategoryMenus = .empty
            reconfigureSelectionCategoryMenus()
        }
    }

    private func renderSelection(_ state: SelectionActionPanelSelectionState) {
        categoryCollectionView.isHidden = false
        executionInfoStack.isHidden = true

        applySelectionCategories(state)

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
        categoryCollectionView.isHidden = true
        executionInfoStack.isHidden = false

        applyExecutionCategory(button: executionBackupCategoryButton, count: state.backupCount, intent: .backup)
        applyExecutionCategory(button: executionDownloadCategoryButton, count: state.downloadCount, intent: .download)
        applyExecutionCategory(button: executionComplementCategoryButton, count: state.complementCount, intent: .complement)
        applyExecutionStatus(text: state.statusText, hasLogAlert: state.hasLogAlert)
        applyPrimaryButton(state.primaryButton)
        applyStopButton(state.stopButton)
    }

    private func applySelectionCategories(_ state: SelectionActionPanelSelectionState) {
        let existingItems = Set(categoryDataSource?.snapshot().itemIdentifiers ?? [])
        var items: [SelectionCategoryItem] = []
        items.reserveCapacity(3)
        if state.backupCount > 0 {
            items.append(SelectionCategoryItem(kind: .backup, count: state.backupCount))
        }
        if state.downloadCount > 0 {
            items.append(SelectionCategoryItem(kind: .download, count: state.downloadCount))
        }
        if state.complementCount > 0 {
            items.append(SelectionCategoryItem(kind: .complement, count: state.complementCount))
        }

        var snapshot = NSDiffableDataSourceSnapshot<SelectionCategorySection, SelectionCategoryItem>()
        snapshot.appendSections([.main])
        snapshot.appendItems(items, toSection: .main)
        let reconfigurableItems = items.filter { existingItems.contains($0) }
        if !reconfigurableItems.isEmpty {
            snapshot.reconfigureItems(reconfigurableItems)
        }
        categoryDataSource?.apply(snapshot, animatingDifferences: false)
    }

    private func reconfigureSelectionCategoryMenus() {
        guard var snapshot = categoryDataSource?.snapshot() else { return }
        let items = snapshot.itemIdentifiers
        guard !items.isEmpty else { return }
        snapshot.reconfigureItems(items)
        categoryDataSource?.apply(snapshot, animatingDifferences: false)
    }

    private func menu(for kind: SelectionCategoryKind) -> UIMenu? {
        switch kind {
        case .backup: return selectionCategoryMenus.backup
        case .download: return selectionCategoryMenus.download
        case .complement: return selectionCategoryMenus.complement
        }
    }

    private func applyExecutionCategory(button: UIButton, count: Int, intent: MonthIntent) {
        guard var cfg = button.configuration else { return }

        let iconConfig = UIImage.SymbolConfiguration(pointSize: 10, weight: .semibold)
        button.isHidden = count == 0
        guard count > 0 else { return }
        button.isUserInteractionEnabled = true
        cfg.showsActivityIndicator = false
        cfg.image = UIImage(systemName: intent.iconSymbolName, withConfiguration: iconConfig)
        cfg.title = "\(count)"
        cfg.subtitle = nil
        cfg.baseForegroundColor = intent.tintColor
        button.configuration = cfg
    }

    private func applyExecutionStatus(text: String, hasLogAlert: Bool) {
        let trimmedStatus = text.trimmingCharacters(in: .whitespacesAndNewlines)
        let statusText = trimmedStatus.isEmpty ? String(localized: "panel.log") : trimmedStatus

        executionStatusButton.isHidden = false
        var cfg = executionStatusButton.configuration ?? .plain()
        cfg.title = statusText
        cfg.baseForegroundColor = hasLogAlert
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

    private final class SelectionCategoryCell: UICollectionViewCell {
        private let button = UIButton(type: .system)

        override init(frame: CGRect) {
            super.init(frame: frame)

            contentView.backgroundColor = .clear
            button.contentHorizontalAlignment = .leading
            button.titleLabel?.adjustsFontForContentSizeCategory = true
            button.titleLabel?.lineBreakMode = .byTruncatingTail
            button.setContentCompressionResistancePriority(.required, for: .vertical)

            contentView.addSubview(button)
            button.snp.makeConstraints { make in
                make.edges.equalToSuperview()
            }
        }

        @available(*, unavailable)
        required init?(coder: NSCoder) { fatalError() }

        override func prepareForReuse() {
            super.prepareForReuse()
            button.configuration = nil
            button.menu = nil
            button.showsMenuAsPrimaryAction = false
        }

        func configure(item: SelectionCategoryItem, menu: UIMenu?) {
            button.configuration = SelectionActionPanel.makeSelectionCategoryButtonConfiguration(
                intent: item.kind.intent,
                count: item.count
            )
            button.menu = menu
            button.showsMenuAsPrimaryAction = menu != nil
        }

        override func preferredLayoutAttributesFitting(
            _ layoutAttributes: UICollectionViewLayoutAttributes
        ) -> UICollectionViewLayoutAttributes {
            let attributes = super.preferredLayoutAttributesFitting(layoutAttributes)
            let fittingSize = contentView.systemLayoutSizeFitting(
                CGSize(
                    width: UIView.layoutFittingCompressedSize.width,
                    height: Layout.selectionCategoryRowHeight
                ),
                withHorizontalFittingPriority: .fittingSizeLevel,
                verticalFittingPriority: .required
            )
            attributes.frame.size = CGSize(
                width: ceil(fittingSize.width),
                height: Layout.selectionCategoryRowHeight
            )
            return attributes
        }
    }
}
