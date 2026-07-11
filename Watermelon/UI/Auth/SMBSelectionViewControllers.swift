import SnapKit
import UIKit

final class SMBShareSelectionViewController: UITableViewController {
    private let shares: [SMBShareInfo]
    private let selectedShareName: String?
    private let onSelected: (String) -> Void
    private let cellID = "ShareCell"

    init(
        shares: [SMBShareInfo],
        selectedShareName: String?,
        onSelected: @escaping (String) -> Void
    ) {
        var seen = Set<String>()
        self.shares = shares.filter { seen.insert($0.name).inserted }
        self.selectedShareName = selectedShareName
        self.onSelected = onSelected
        super.init(style: .insetGrouped)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        title = String(localized: "auth.smb.share.title")
        view.backgroundColor = .appBackground
        navigationItem.leftBarButtonItem = UIBarButtonItem(
            title: String(localized: "common.cancel"),
            style: .plain,
            target: self,
            action: #selector(cancelTapped)
        )
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: cellID)
    }

    override func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        shares.count
    }

    override func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let share = shares[indexPath.row]
        let cell = tableView.dequeueReusableCell(withIdentifier: cellID, for: indexPath)
        var content = UIListContentConfiguration.subtitleCell()
        content.text = share.name
        content.secondaryText = share.comment.isEmpty ? nil : share.comment
        content.secondaryTextProperties.numberOfLines = 0
        cell.contentConfiguration = content
        cell.accessoryType = share.name == selectedShareName ? .checkmark : .none
        return cell
    }

    override func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        onSelected(shares[indexPath.row].name)
        dismiss(animated: true)
    }

    @objc
    private func cancelTapped() {
        dismiss(animated: true)
    }
}

final class SMBFolderSelectionViewController: UITableViewController {
    typealias DirectoryLoader = (
        SMBServerAuthContext,
        String,
        String
    ) async throws -> [RemoteStorageEntry]

    private enum LoadState {
        case loading
        case loaded([RemoteStorageEntry])
        case failed(String)
    }

    private enum Section {
        case selectCurrent
        case parent
        case directories
    }

    private let auth: SMBServerAuthContext
    private let shareName: String
    private let onSelected: (String) -> Void
    private let directoryLoader: DirectoryLoader
    private let actionCellID = "FolderActionCell"
    private let folderCellID = "FolderCell"
    private let statusCellID = "FolderStatusCell"

    private var currentPath: String
    private var loadState: LoadState = .loading
    private var loadTask: Task<Void, Never>?
    private var loadRequestID: UInt64 = 0

    init(
        auth: SMBServerAuthContext,
        shareName: String,
        initialPath: String,
        directoryLoader: DirectoryLoader? = nil,
        onSelected: @escaping (String) -> Void
    ) {
        self.auth = auth
        self.shareName = shareName
        self.currentPath = (try? SMBPathCanonicalizer.canonicalRawPath(initialPath)) ?? "/"
        if let directoryLoader {
            self.directoryLoader = directoryLoader
        } else {
            let setupService = SMBSetupService()
            self.directoryLoader = { auth, shareName, path in
                try await setupService.listDirectories(
                    auth: auth,
                    shareName: shareName,
                    path: path
                )
            }
        }
        self.onSelected = onSelected
        super.init(style: .insetGrouped)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        loadTask?.cancel()
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        title = String(localized: "auth.smb.folder.title")
        view.backgroundColor = .appBackground
        navigationItem.leftBarButtonItem = UIBarButtonItem(
            title: String(localized: "common.cancel"),
            style: .plain,
            target: self,
            action: #selector(cancelTapped)
        )
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: actionCellID)
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: folderCellID)
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: statusCellID)
        loadDirectories()
    }

    override func viewDidAppear(_ animated: Bool) {
        super.viewDidAppear(animated)
        if loadTask == nil, case .loading = loadState {
            loadDirectories()
        }
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        if isBeingDismissed || navigationController?.isBeingDismissed == true {
            cancelLoading()
        }
    }

    override func numberOfSections(in tableView: UITableView) -> Int {
        visibleSections.count
    }

    override func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        guard let section = resolvedSection(section) else { return 0 }
        switch section {
        case .selectCurrent, .parent:
            return 1
        case .directories:
            switch loadState {
            case .loading, .failed:
                return 1
            case .loaded(let directories):
                return max(1, directories.count)
            }
        }
    }

    override func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        guard resolvedSection(section) == .selectCurrent else { return nil }
        return String.localizedStringWithFormat(
            String(localized: "auth.smb.share.currentPath"),
            currentPath
        )
    }

    override func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        guard let section = resolvedSection(indexPath.section) else {
            return UITableViewCell()
        }
        switch section {
        case .selectCurrent, .parent:
            return actionCell(in: tableView, at: indexPath, section: section)
        case .directories:
            return directoryOrStatusCell(in: tableView, at: indexPath)
        }
    }

    override func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard let section = resolvedSection(indexPath.section) else { return }
        switch section {
        case .selectCurrent:
            guard case .loaded = loadState else { return }
            onSelected(currentPath)
            cancelLoading()
            dismiss(animated: true)
        case .parent:
            navigate(to: parentPath(of: currentPath))
        case .directories:
            switch loadState {
            case .loaded(let directories) where directories.indices.contains(indexPath.row):
                navigate(to: directories[indexPath.row].path)
            case .failed:
                loadDirectories()
            case .loading, .loaded:
                break
            }
        }
    }

    private var visibleSections: [Section] {
        var sections: [Section] = [.selectCurrent]
        if currentPath != "/" {
            sections.append(.parent)
        }
        sections.append(.directories)
        return sections
    }

    private func resolvedSection(_ index: Int) -> Section? {
        visibleSections.indices.contains(index) ? visibleSections[index] : nil
    }

    private func actionCell(in tableView: UITableView, at indexPath: IndexPath, section: Section) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: actionCellID, for: indexPath)
        cell.accessoryType = .none
        cell.accessoryView = nil

        var content = cell.defaultContentConfiguration()
        content.textProperties.alignment = .center
        switch section {
        case .selectCurrent:
            content.text = String(localized: "auth.smb.folder.selectCurrent")
            if case .loaded = loadState {
                content.textProperties.color = .systemBlue
                cell.selectionStyle = .default
            } else {
                content.textProperties.color = .secondaryLabel
                cell.selectionStyle = .none
            }
        case .parent:
            content.text = String(localized: "auth.smb.share.parentDir")
            content.textProperties.color = .systemBlue
            cell.selectionStyle = .default
        case .directories:
            break
        }
        cell.contentConfiguration = content
        return cell
    }

    private func directoryOrStatusCell(in tableView: UITableView, at indexPath: IndexPath) -> UITableViewCell {
        switch loadState {
        case .loading:
            let cell = tableView.dequeueReusableCell(withIdentifier: statusCellID, for: indexPath)
            var content = cell.defaultContentConfiguration()
            content.text = currentPath
            content.textProperties.color = .secondaryLabel
            cell.contentConfiguration = content
            cell.selectionStyle = .none
            cell.accessoryType = .none
            cell.accessoryView = makeSpinner()
            return cell
        case .failed(let message):
            let cell = tableView.dequeueReusableCell(withIdentifier: statusCellID, for: indexPath)
            var content = UIListContentConfiguration.subtitleCell()
            content.text = String(localized: "auth.smb.share.readFailed")
            content.secondaryText = message
            content.textProperties.color = .systemRed
            content.secondaryTextProperties.numberOfLines = 0
            cell.contentConfiguration = content
            cell.selectionStyle = .default
            cell.accessoryType = .none
            cell.accessoryView = nil
            return cell
        case .loaded(let directories):
            guard directories.indices.contains(indexPath.row) else {
                let cell = tableView.dequeueReusableCell(withIdentifier: statusCellID, for: indexPath)
                var content = cell.defaultContentConfiguration()
                content.text = String(localized: "common.none")
                content.textProperties.color = .secondaryLabel
                cell.contentConfiguration = content
                cell.selectionStyle = .none
                cell.accessoryType = .none
                cell.accessoryView = nil
                return cell
            }
            let directory = directories[indexPath.row]
            let cell = tableView.dequeueReusableCell(withIdentifier: folderCellID, for: indexPath)
            var content = UIListContentConfiguration.subtitleCell()
            content.text = directory.name
            content.secondaryText = directory.path
            content.secondaryTextProperties.numberOfLines = 0
            content.image = UIImage(systemName: "folder")
            content.imageProperties.tintColor = .secondaryLabel
            cell.contentConfiguration = content
            cell.selectionStyle = .default
            cell.accessoryType = .disclosureIndicator
            cell.accessoryView = nil
            return cell
        }
    }

    private func navigate(to path: String) {
        guard let canonical = try? SMBPathCanonicalizer.canonicalRawPath(path) else { return }
        currentPath = canonical
        loadDirectories()
    }

    private func loadDirectories() {
        loadTask?.cancel()
        loadRequestID &+= 1
        let requestID = loadRequestID
        let requestedPath = currentPath
        let directoryLoader = directoryLoader
        let auth = auth
        let shareName = shareName
        loadState = .loading
        tableView.reloadData()
        loadTask = Task { [weak self] in
            do {
                let directories = try await directoryLoader(auth, shareName, requestedPath)
                try Task.checkCancellation()
                await MainActor.run {
                    guard let self else { return }
                    guard self.loadRequestID == requestID,
                          self.currentPath == requestedPath else { return }
                    self.loadTask = nil
                    self.loadState = .loaded(directories)
                    self.tableView.reloadData()
                }
            } catch is CancellationError {
            } catch {
                let message = UserFacingErrorLocalizer.message(for: error, storageType: .smb)
                await MainActor.run {
                    guard let self else { return }
                    guard self.loadRequestID == requestID,
                          self.currentPath == requestedPath else { return }
                    self.loadTask = nil
                    self.loadState = .failed(message)
                    self.tableView.reloadData()
                }
            }
        }
    }

    private func cancelLoading() {
        loadRequestID &+= 1
        loadTask?.cancel()
        loadTask = nil
    }

    private func parentPath(of path: String) -> String {
        guard let normalized = try? SMBPathCanonicalizer.canonicalRawPath(path) else { return "/" }
        if normalized == "/" { return "/" }
        let parent = (normalized as NSString).deletingLastPathComponent
        return parent.isEmpty ? "/" : ((try? SMBPathCanonicalizer.canonicalRawPath(parent)) ?? "/")
    }

    private func makeSpinner() -> UIActivityIndicatorView {
        let spinner = UIActivityIndicatorView(style: .medium)
        spinner.startAnimating()
        return spinner
    }

    @objc
    private func cancelTapped() {
        cancelLoading()
        dismiss(animated: true)
    }
}
