import SnapKit
import UIKit

final class SMBSharePathPickerViewController: UIViewController {
    private enum DirectoryLoadState: Equatable {
        case idle
        case loading
        case loaded
        case failed(String)
    }

    private struct ShareSectionState {
        let share: SMBShareInfo
        var currentPath: String = "/"
        var directories: [RemoteStorageEntry] = []
        var loadState: DirectoryLoadState = .idle
        var isExpanded = false
    }

    private enum Item: Hashable {
        case share(String)
        case loading(shareName: String, path: String)
        case parent(shareName: String, path: String, targetPath: String)
        case directory(shareName: String, path: String, directoryName: String, directoryPath: String)
        case empty(shareName: String, path: String)
        case error(shareName: String, path: String, message: String)
    }

    private typealias SectionID = String
    private typealias DataSource = UITableViewDiffableDataSource<SectionID, Item>
    private typealias Snapshot = NSDiffableDataSourceSnapshot<SectionID, Item>

    private let dependencies: DependencyContainer
    private let setupService = SMBSetupService()
    private let auth: SMBServerAuthContext
    private let editingProfile: ServerProfileRecord?
    private let shouldPopToRootOnSave: Bool
    private let onSaved: (ServerProfileRecord, String) -> Void

    private let shareOrder: [String]
    private var shareStates: [String: ShareSectionState]
    private var selectedShareName: String?
    private var loadTask: Task<Void, Never>?
    private var loadRequestID: UInt64 = 0
    private var hasAppliedInitialSnapshot = false

    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private var dataSource: DataSource!
    private lazy var nextBarButtonItem = UIBarButtonItem(
        title: String(localized: "common.next"),
        style: .prominentStyle,
        target: self,
        action: #selector(nextTapped)
    )

    init(
        dependencies: DependencyContainer,
        auth: SMBServerAuthContext,
        initialShares: [SMBShareInfo],
        editingProfile: ServerProfileRecord? = nil,
        shouldPopToRootOnSave: Bool = true,
        onSaved: @escaping (ServerProfileRecord, String) -> Void
    ) {
        self.dependencies = dependencies
        self.auth = auth
        self.editingProfile = editingProfile
        self.shouldPopToRootOnSave = shouldPopToRootOnSave
        self.onSaved = onSaved

        let uniqueShares = Self.uniqueSharesByName(initialShares)
        self.shareOrder = uniqueShares.map(\.name)

        var states: [String: ShareSectionState] = [:]
        for share in uniqueShares {
            states[share.name] = ShareSectionState(share: share)
        }

        if let editingProfile,
           var matchedState = states[editingProfile.shareName] {
            matchedState.currentPath = RemotePathBuilder.normalizePath(editingProfile.basePath)
            matchedState.loadState = .loading
            matchedState.isExpanded = true
            states[editingProfile.shareName] = matchedState
            self.selectedShareName = editingProfile.shareName
        } else {
            self.selectedShareName = nil
        }

        self.shareStates = states
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = String(localized: "auth.smb.share.title")

        configureUI()
        configureDataSource()
        applySnapshot()

        if let selectedShareName {
            startLoadDirectories(for: selectedShareName)
        }
    }

    deinit {
        loadTask?.cancel()
    }

    private func configureUI() {
        navigationItem.rightBarButtonItem = nextBarButtonItem
        updateNextButtonState()

        tableView.backgroundColor = .appBackground
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "dir")
        tableView.delegate = self
        tableView.rowHeight = UITableView.automaticDimension
        tableView.estimatedRowHeight = 44

        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.edges.equalTo(view.safeAreaLayoutGuide)
        }
    }

    private func configureDataSource() {
        dataSource = DataSource(tableView: tableView) { [weak self] tableView, indexPath, item in
            self?.configuredCell(in: tableView, at: indexPath, for: item) ?? UITableViewCell()
        }
        dataSource.defaultRowAnimation = .fade
    }

    private static func uniqueSharesByName(_ shares: [SMBShareInfo]) -> [SMBShareInfo] {
        var seenNames = Set<String>()
        var uniqueShares: [SMBShareInfo] = []
        uniqueShares.reserveCapacity(shares.count)

        for share in shares where seenNames.insert(share.name).inserted {
            uniqueShares.append(share)
        }

        return uniqueShares
    }

    @objc
    private func nextTapped() {
        guard let selectedShareName,
              let selectedState = shareStates[selectedShareName] else {
            presentAlert(title: String(localized: "auth.smb.share.noShareSelected"), message: String(localized: "auth.smb.share.selectShareFirst"))
            return
        }
        guard case .loaded = selectedState.loadState else { return }

        let context = SMBServerPathContext(
            auth: auth,
            shareName: selectedState.share.name,
            basePath: selectedState.currentPath
        )
        let finalizeVC = AddSMBServerViewController(
            dependencies: dependencies,
            context: context,
            editingProfile: editingProfile,
            shouldPopToRootOnSave: shouldPopToRootOnSave,
            onSaved: onSaved
        )
        navigationController?.pushViewController(finalizeVC, animated: true)
    }

    private func startLoadDirectories(for shareName: String) {
        guard var state = shareStates[shareName] else { return }

        loadTask?.cancel()
        loadRequestID &+= 1
        let requestID = loadRequestID
        let targetPath = state.currentPath
        state.loadState = .loading
        shareStates[shareName] = state
        updateNextButtonState()
        applySnapshot()

        loadTask = Task { [weak self] in
            guard let self else { return }
            do {
                let dirs = try await self.setupService.listDirectories(auth: self.auth, shareName: shareName, path: targetPath)
                try Task.checkCancellation()
                await MainActor.run {
                    guard requestID == self.loadRequestID else { return }
                    guard var latestState = self.shareStates[shareName],
                          latestState.isExpanded,
                          self.selectedShareName == shareName,
                          latestState.currentPath == targetPath else { return }
                    latestState.directories = dirs
                    latestState.loadState = .loaded
                    self.shareStates[shareName] = latestState
                    self.updateNextButtonState()
                    self.applySnapshot()
                }
            } catch is CancellationError {
            } catch {
                let message = UserFacingErrorLocalizer.message(for: error, storageType: .smb)
                await MainActor.run {
                    guard requestID == self.loadRequestID else { return }
                    guard var latestState = self.shareStates[shareName],
                          latestState.currentPath == targetPath else { return }
                    latestState.directories = []
                    latestState.loadState = .failed(message)
                    self.shareStates[shareName] = latestState
                    self.updateNextButtonState()
                    self.applySnapshot()
                }
            }
        }
    }

    private func parentPath(of path: String) -> String {
        let normalized = RemotePathBuilder.normalizePath(path)
        if normalized == "/" { return "/" }
        let ns = normalized as NSString
        let parent = ns.deletingLastPathComponent
        return parent.isEmpty ? "/" : RemotePathBuilder.normalizePath(parent)
    }

    private func configureSelection(for shareName: String) {
        if selectedShareName == shareName {
            guard let state = shareStates[shareName] else { return }
            switch state.loadState {
            case .idle, .failed:
                startLoadDirectories(for: shareName)
            case .loading, .loaded:
                break
            }
            return
        }

        if let previousShareName = selectedShareName,
           var previousState = shareStates[previousShareName] {
            previousState.isExpanded = false
            if case .loading = previousState.loadState {
                previousState.loadState = .idle
            }
            shareStates[previousShareName] = previousState
        }

        selectedShareName = shareName
        if var nextState = shareStates[shareName] {
            nextState.isExpanded = true
            shareStates[shareName] = nextState
        }

        startLoadDirectories(for: shareName)
    }

    private func navigate(to path: String, in shareName: String) {
        guard var state = shareStates[shareName] else { return }
        state.currentPath = RemotePathBuilder.normalizePath(path)
        shareStates[shareName] = state
        startLoadDirectories(for: shareName)
    }

    @MainActor
    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }

    private func updateNextButtonState() {
        guard let selectedShareName,
              let selectedState = shareStates[selectedShareName],
              selectedState.isExpanded else {
            nextBarButtonItem.isEnabled = false
            return
        }

        if case .loaded = selectedState.loadState {
            nextBarButtonItem.isEnabled = true
        } else {
            nextBarButtonItem.isEnabled = false
        }
    }

    private func applySnapshot() {
        guard dataSource != nil else { return }

        var snapshot = Snapshot()
        for shareName in shareOrder {
            guard let state = shareStates[shareName] else { continue }

            snapshot.appendSections([shareName])

            var items: [Item] = [.share(shareName)]
            if state.isExpanded {
                switch state.loadState {
                case .loading:
                    items.append(.loading(shareName: shareName, path: state.currentPath))
                case .failed(let message):
                    items.append(.error(shareName: shareName, path: state.currentPath, message: message))
                case .idle, .loaded:
                    if state.currentPath != "/" {
                        items.append(.parent(
                            shareName: shareName,
                            path: state.currentPath,
                            targetPath: parentPath(of: state.currentPath)
                        ))
                    }

                    if state.directories.isEmpty {
                        items.append(.empty(shareName: shareName, path: state.currentPath))
                    } else {
                        items.append(contentsOf: state.directories.map { entry in
                            .directory(
                                shareName: shareName,
                                path: state.currentPath,
                                directoryName: entry.name,
                                directoryPath: entry.path
                            )
                        })
                    }
                }
            }

            snapshot.appendItems(items, toSection: shareName)
        }

        let shouldAnimate = hasAppliedInitialSnapshot
        dataSource.apply(snapshot, animatingDifferences: shouldAnimate)
        hasAppliedInitialSnapshot = true
    }

    private func configuredCell(in tableView: UITableView, at indexPath: IndexPath, for item: Item) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: "dir", for: indexPath)
        var content = UIListContentConfiguration.subtitleCell()

        cell.selectionStyle = .default
        cell.accessoryType = .none
        cell.accessoryView = nil

        switch item {
        case .share(let shareName):
            guard let state = shareStates[shareName] else { return cell }
            content.text = state.share.name
            content.secondaryText = state.share.comment.isEmpty ? nil : state.share.comment
            if state.isExpanded {
                switch state.loadState {
                case .loading:
                    cell.accessoryView = makeSpinner()
                case .idle, .loaded, .failed:
                    cell.accessoryType = .checkmark
                }
            } else {
                cell.accessoryType = .disclosureIndicator
            }
        case .loading(_, let path):
            content.text = path
            content.textProperties.color = .secondaryLabel
            cell.selectionStyle = .none
            cell.accessoryView = makeSpinner()
        case .parent:
            content.text = ".."
            content.secondaryText = String(localized: "auth.smb.share.parentDir")
            cell.accessoryType = .disclosureIndicator
        case .directory(_, _, let directoryName, let directoryPath):
            content.text = directoryName
            content.secondaryText = directoryPath
            cell.accessoryType = .disclosureIndicator
        case .empty(_, let path):
            content.text = String(localized: "common.none")
            content.secondaryText = path
            content.textProperties.color = .secondaryLabel
            cell.selectionStyle = .none
        case .error(_, _, let message):
            content.text = String(localized: "auth.smb.share.readFailed")
            content.secondaryText = message
            content.textProperties.color = .systemRed
            content.secondaryTextProperties.color = .secondaryLabel
            cell.selectionStyle = .none
        }

        content.secondaryTextProperties.numberOfLines = 0
        cell.contentConfiguration = content
        return cell
    }

    private func makeSpinner() -> UIActivityIndicatorView {
        let spinner = UIActivityIndicatorView(style: .medium)
        spinner.startAnimating()
        return spinner
    }

    private func footerText(for section: Int) -> String? {
        let sections = dataSource.snapshot().sectionIdentifiers
        guard section >= 0, section < sections.count,
              let state = shareStates[sections[section]],
              state.isExpanded else {
            return nil
        }
        return String(format: String(localized: "auth.smb.share.currentPath"), state.currentPath)
    }
}

extension SMBSharePathPickerViewController: UITableViewDelegate {
    func tableView(_ tableView: UITableView, viewForFooterInSection section: Int) -> UIView? {
        guard let text = footerText(for: section) else { return nil }

        let container = UIView()
        let label = UILabel()
        label.font = .preferredFont(forTextStyle: .footnote)
        label.textColor = .secondaryLabel
        label.numberOfLines = 0
        label.text = text

        container.addSubview(label)
        label.snp.makeConstraints { make in
            make.edges.equalToSuperview().inset(UIEdgeInsets(top: 0, left: 16, bottom: 8, right: 16))
        }
        return container
    }

    func tableView(_ tableView: UITableView, heightForFooterInSection section: Int) -> CGFloat {
        footerText(for: section) == nil ? .leastNormalMagnitude : UITableView.automaticDimension
    }

    func tableView(_ tableView: UITableView, estimatedHeightForFooterInSection section: Int) -> CGFloat {
        footerText(for: section) == nil ? .leastNormalMagnitude : 28
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard let item = dataSource.itemIdentifier(for: indexPath) else { return }

        switch item {
        case .share(let shareName):
            configureSelection(for: shareName)
        case .parent(let shareName, _, let targetPath):
            navigate(to: targetPath, in: shareName)
        case .directory(let shareName, _, _, let directoryPath):
            navigate(to: directoryPath, in: shareName)
        case .loading, .empty, .error:
            return
        }
    }
}
