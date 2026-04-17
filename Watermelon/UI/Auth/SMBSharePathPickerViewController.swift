import SnapKit
import UIKit

final class SMBSharePathPickerViewController: UIViewController {
    private let dependencies: DependencyContainer
    private let setupService = SMBSetupService()
    private let auth: SMBServerAuthContext
    private let editingProfile: ServerProfileRecord?
    private let shouldPopToRootOnSave: Bool
    private let onSaved: (ServerProfileRecord, String) -> Void

    private var shares: [SMBShareInfo]
    private var selectedShare: SMBShareInfo?
    private var currentPath: String = "/"
    private var directories: [RemoteStorageEntry] = []
    private var loadTask: Task<Void, Never>?
    private var loadRequestID: UInt64 = 0

    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private lazy var nextBarButtonItem = UIBarButtonItem(
        title: String(localized: "common.next"),
        style: .prominentStyle,
        target: self,
        action: #selector(nextTapped)
    )
    private lazy var loadingIndicatorView = UIActivityIndicatorView(style: .medium)
    private lazy var loadingBarButtonItem = UIBarButtonItem(customView: loadingIndicatorView)

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
        self.shares = initialShares
        self.editingProfile = editingProfile
        self.shouldPopToRootOnSave = shouldPopToRootOnSave
        self.onSaved = onSaved
        if let editingProfile,
           let matchedShare = initialShares.first(where: { $0.name == editingProfile.shareName }) {
            self.selectedShare = matchedShare
            self.currentPath = RemotePathBuilder.normalizePath(editingProfile.basePath)
        } else {
            self.selectedShare = initialShares.first
        }
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
        startLoadDirectories()
    }

    deinit {
        loadTask?.cancel()
    }

    private func configureUI() {
        navigationItem.rightBarButtonItem = nextBarButtonItem
        updateNextButtonState()

        tableView.backgroundColor = .appBackground
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "dir")
        tableView.dataSource = self
        tableView.delegate = self
        tableView.rowHeight = UITableView.automaticDimension
        tableView.estimatedRowHeight = 44

        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.edges.equalTo(view.safeAreaLayoutGuide)
        }
    }

    @objc
    private func nextTapped() {
        guard let selectedShare else {
            presentAlert(title: String(localized: "auth.smb.share.noShareSelected"), message: String(localized: "auth.smb.share.selectShareFirst"))
            return
        }

        let context = SMBServerPathContext(auth: auth, shareName: selectedShare.name, basePath: currentPath)
        let finalizeVC = AddSMBServerViewController(
            dependencies: dependencies,
            context: context,
            editingProfile: editingProfile,
            shouldPopToRootOnSave: shouldPopToRootOnSave,
            onSaved: onSaved
        )
        navigationController?.pushViewController(finalizeVC, animated: true)
    }

    private func startLoadDirectories() {
        loadTask?.cancel()
        loadRequestID &+= 1
        let requestID = loadRequestID

        guard let selectedShare else {
            directories = []
            tableView.reloadSections(IndexSet(integer: TableSection.paths.rawValue), with: .none)
            return
        }

        let shareName = selectedShare.name
        let targetPath = currentPath
        setLoading(true)

        loadTask = Task { [weak self] in
            guard let self else { return }
            do {
                let dirs = try await self.setupService.listDirectories(auth: self.auth, shareName: shareName, path: targetPath)
                try Task.checkCancellation()
                await MainActor.run {
                    guard requestID == self.loadRequestID else { return }
                    guard self.selectedShare?.name == shareName, self.currentPath == targetPath else { return }
                    self.directories = dirs
                    self.setLoading(false)
                    self.tableView.reloadData()
                }
            } catch is CancellationError {
                await MainActor.run {
                    guard requestID == self.loadRequestID else { return }
                    self.setLoading(false)
                }
            } catch {
                await MainActor.run {
                    guard requestID == self.loadRequestID else { return }
                    self.setLoading(false)
                    self.presentAlert(
                        title: String(localized: "auth.smb.share.readFailed"),
                        message: UserFacingErrorLocalizer.message(for: error, storageType: .smb)
                    )
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

    @MainActor
    private func setLoading(_ loading: Bool) {
        tableView.isUserInteractionEnabled = !loading
        if loading {
            loadingIndicatorView.startAnimating()
            navigationItem.rightBarButtonItem = loadingBarButtonItem
        } else {
            loadingIndicatorView.stopAnimating()
            navigationItem.rightBarButtonItem = nextBarButtonItem
            updateNextButtonState()
        }
    }

    private func updateNextButtonState() {
        nextBarButtonItem.isEnabled = selectedShare != nil && !shares.isEmpty
    }

    @MainActor
    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }
}

extension SMBSharePathPickerViewController: UITableViewDataSource, UITableViewDelegate {
    private enum TableSection: Int, CaseIterable {
        case shares
        case paths
    }

    func numberOfSections(in tableView: UITableView) -> Int {
        TableSection.allCases.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        guard let section = TableSection(rawValue: section) else { return 0 }
        switch section {
        case .shares:
            return max(shares.count, 1)
        case .paths:
            let hasParent = currentPath != "/"
            return directories.count + (hasParent ? 1 : 0)
        }
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        guard let section = TableSection(rawValue: section) else { return nil }
        switch section {
        case .shares:
            return String(localized: "auth.smb.share.sectionShare")
        case .paths:
            return String(localized: "auth.smb.share.sectionPath")
        }
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        guard let section = TableSection(rawValue: section) else { return nil }
        switch section {
        case .shares:
            return nil
        case .paths:
            return String(format: String(localized: "auth.smb.share.currentPath"), currentPath)
        }
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: "dir", for: indexPath)
        var content = cell.defaultContentConfiguration()

        guard let section = TableSection(rawValue: indexPath.section) else {
            cell.contentConfiguration = content
            return cell
        }

        switch section {
        case .shares:
            if shares.isEmpty {
                content.text = String(localized: "auth.smb.share.noSharesFound")
                content.secondaryText = String(localized: "auth.smb.share.noSharesHint")
                cell.selectionStyle = .none
                cell.accessoryType = .none
            } else {
                let share = shares[indexPath.row]
                content.text = share.name
                content.secondaryText = share.comment.isEmpty ? "(no comment)" : share.comment
                cell.selectionStyle = .default
                cell.accessoryType = selectedShare?.name == share.name ? .checkmark : .none
            }
        case .paths:
            let hasParent = currentPath != "/"
            if hasParent && indexPath.row == 0 {
                content.text = ".."
                content.secondaryText = String(localized: "auth.smb.share.parentDir")
                cell.accessoryType = .disclosureIndicator
            } else {
                let adjustedIndex = indexPath.row - (hasParent ? 1 : 0)
                let entry = directories[adjustedIndex]
                content.text = entry.name
                content.secondaryText = entry.path
                cell.accessoryType = .disclosureIndicator
            }
            cell.selectionStyle = .default
        }

        cell.contentConfiguration = content
        return cell
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard let section = TableSection(rawValue: indexPath.section) else { return }

        switch section {
        case .shares:
            guard !shares.isEmpty, indexPath.row < shares.count else { return }
            selectedShare = shares[indexPath.row]
            currentPath = "/"
            updateNextButtonState()
            tableView.reloadData()
            startLoadDirectories()
        case .paths:
            let hasParent = currentPath != "/"
            if hasParent && indexPath.row == 0 {
                currentPath = parentPath(of: currentPath)
                startLoadDirectories()
                return
            }

            let adjustedIndex = indexPath.row - (hasParent ? 1 : 0)
            guard adjustedIndex >= 0, adjustedIndex < directories.count else { return }
            currentPath = directories[adjustedIndex].path
            startLoadDirectories()
        }
    }
}
