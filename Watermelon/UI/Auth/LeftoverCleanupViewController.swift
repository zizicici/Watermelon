import Foundation
import SnapKit
import UIKit

// Self-contained modal that runs the leftover-file maintenance lifecycle: scan → review → delete → summary.
// Scanning and deleting are non-dismissible (block interactive dismissal) and offer a Stop button; the
// review and terminal states are dismissible. Progress is driven by the shared RemoteMaintenanceController.
final class LeftoverCleanupViewController: UIViewController {
    private enum State {
        case scanning
        case reviewing(LeftoverScanResult)
        case empty
        case deleting
        case summary(LeftoverDeleteResult)
        case failed(String)
    }

    private struct ReviewSection {
        let month: LibraryMonthKey
        let files: [LeftoverFile]
        var totalBytes: Int64 { files.reduce(0) { $0 + $1.size } }
    }

    private let dependencies: DependencyContainer
    private let profile: ServerProfileRecord
    private var state: State = .scanning
    private var reviewResult: LeftoverScanResult?
    private var reviewSections: [ReviewSection] = []
    private var isStopping = false
    private var maintenanceObserver: NSObjectProtocol?

    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private let bottomBar = UIView()
    private let deleteButton = UIButton(type: .system)
    private let statusContainer = UIView()
    private let activityIndicator = UIActivityIndicatorView(style: .large)
    private let statusLabel = UILabel()
    private let cellID = "leftover"

    init(dependencies: DependencyContainer, profile: ServerProfileRecord) {
        self.dependencies = dependencies
        self.profile = profile
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        if let maintenanceObserver {
            NotificationCenter.default.removeObserver(maintenanceObserver)
        }
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = String(localized: "storage.detail.leftover.title")
        configureViews()
        observeMaintenance()
        startScan()
    }

    // MARK: - Layout

    private func configureViews() {
        tableView.backgroundColor = .appBackground
        tableView.dataSource = self
        tableView.delegate = self
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: cellID)
        view.addSubview(tableView)

        bottomBar.backgroundColor = .appBackground
        view.addSubview(bottomBar)

        var config = UIButton.Configuration.filled()
        config.baseBackgroundColor = .systemRed
        config.cornerStyle = .large
        deleteButton.configuration = config
        deleteButton.addTarget(self, action: #selector(confirmDelete), for: .touchUpInside)
        bottomBar.addSubview(deleteButton)

        statusContainer.isHidden = true
        view.addSubview(statusContainer)
        statusLabel.numberOfLines = 0
        statusLabel.textAlignment = .center
        statusLabel.textColor = .secondaryLabel
        statusLabel.font = .preferredFont(forTextStyle: .body)
        let stack = UIStackView(arrangedSubviews: [activityIndicator, statusLabel])
        stack.axis = .vertical
        stack.alignment = .center
        stack.spacing = 16
        statusContainer.addSubview(stack)

        tableView.snp.makeConstraints { make in
            make.top.leading.trailing.equalToSuperview()
            make.bottom.equalTo(bottomBar.snp.top)
        }
        bottomBar.snp.makeConstraints { make in
            make.leading.trailing.bottom.equalToSuperview()
        }
        deleteButton.snp.makeConstraints { make in
            make.leading.trailing.equalToSuperview().inset(16)
            make.top.equalToSuperview().inset(8)
            make.bottom.equalTo(bottomBar.safeAreaLayoutGuide.snp.bottom).inset(8)
        }
        statusContainer.snp.makeConstraints { make in
            make.edges.equalTo(view.safeAreaLayoutGuide)
        }
        stack.snp.makeConstraints { make in
            make.center.equalToSuperview()
            make.leading.greaterThanOrEqualToSuperview().inset(32)
            make.trailing.lessThanOrEqualToSuperview().inset(32)
        }
    }

    // MARK: - State rendering

    private func render() {
        switch state {
        case .scanning, .deleting:
            setDismissBlocked(true)
            installStopButton()
            navigationItem.rightBarButtonItem = nil
            showStatus(activity: true, text: progressText())
        case .reviewing(let result):
            setDismissBlocked(false)
            navigationItem.leftBarButtonItem = UIBarButtonItem(
                barButtonSystemItem: .cancel, target: self, action: #selector(dismissSelf)
            )
            navigationItem.rightBarButtonItem = nil
            updateDeleteButtonTitle(result)
            showReview()
        case .empty:
            setDismissBlocked(false)
            installDoneButton()
            showStatus(activity: false, text: String(localized: "storage.detail.leftover.empty.message"))
        case .summary(let result):
            setDismissBlocked(false)
            installDoneButton()
            showStatus(activity: false, text: summaryText(result))
        case .failed(let message):
            setDismissBlocked(false)
            installDoneButton()
            showStatus(activity: false, text: message)
        }
    }

    private func setDismissBlocked(_ blocked: Bool) {
        isModalInPresentation = blocked
        navigationController?.isModalInPresentation = blocked
    }

    private func installStopButton() {
        let stop = UIBarButtonItem(
            title: String(localized: "common.stop"), style: .plain, target: self, action: #selector(stopTapped)
        )
        stop.tintColor = .systemRed
        stop.isEnabled = !isStopping
        navigationItem.leftBarButtonItem = stop
    }

    private func installDoneButton() {
        navigationItem.leftBarButtonItem = nil
        navigationItem.rightBarButtonItem = UIBarButtonItem(
            barButtonSystemItem: .done, target: self, action: #selector(dismissSelf)
        )
    }

    private func showStatus(activity: Bool, text: String) {
        tableView.isHidden = true
        bottomBar.isHidden = true
        statusContainer.isHidden = false
        statusLabel.text = text
        activityIndicator.isHidden = !activity
        if activity { activityIndicator.startAnimating() } else { activityIndicator.stopAnimating() }
    }

    private func showReview() {
        statusContainer.isHidden = true
        activityIndicator.stopAnimating()
        tableView.isHidden = false
        bottomBar.isHidden = false
        tableView.reloadData()
    }

    private func updateDeleteButtonTitle(_ result: LeftoverScanResult) {
        var config = deleteButton.configuration
        let count = result.totalCount + result.orphanThumbnailCount
        let bytes = result.totalBytes + result.orphanThumbnailBytes
        config?.title = String.localizedStringWithFormat(
            String(localized: "storage.detail.leftover.delete"),
            count,
            ByteCountFormatter.string(fromByteCount: bytes, countStyle: .file)
        )
        deleteButton.configuration = config
    }

    private func progressText() -> String {
        if isStopping { return String(localized: "backup.session.stopping") }
        let isDeleting: Bool
        if case .deleting = state { isDeleting = true } else { isDeleting = false }
        let progress = dependencies.remoteMaintenanceController.currentProgress
        if let progress, progress.total > 0 {
            let key: String.LocalizationValue = isDeleting
                ? "storage.detail.overview.placeholder.deletingLeftover"
                : "storage.detail.overview.placeholder.scanningLeftover"
            return String.localizedStringWithFormat(String(localized: key), progress.current, progress.total)
        }
        return String(localized: isDeleting
            ? "storage.detail.overview.placeholder.deletingLeftoverStarting"
            : "storage.detail.overview.placeholder.scanningLeftoverStarting")
    }

    private func summaryText(_ result: LeftoverDeleteResult) -> String {
        var parts: [String] = []
        let hadDataWork = result.deletedCount > 0 || result.failedCount > 0
        if hadDataWork || result.deletedThumbnailCount == 0 {
            if result.failedCount > 0 {
                parts.append(String.localizedStringWithFormat(
                    String(localized: "storage.detail.leftover.summary.withFailures"),
                    result.deletedCount,
                    result.failedCount
                ))
            } else {
                parts.append(String.localizedStringWithFormat(
                    String(localized: "storage.detail.leftover.summary.deleted"),
                    result.deletedCount
                ))
            }
        }
        if result.deletedThumbnailCount > 0 {
            parts.append(String.localizedStringWithFormat(
                String(localized: "storage.detail.leftover.summary.thumbnails"),
                result.deletedThumbnailCount
            ))
        }
        return parts.joined(separator: " ")
    }

    // MARK: - Operations

    private func startScan() {
        state = .scanning
        isStopping = false
        render()
        guard let password = dependencies.appSession.activePassword else {
            state = .failed(String(localized: "storage.detail.overview.placeholder.disconnected"))
            render()
            return
        }
        let started = dependencies.remoteMaintenanceController.startScanLeftover(
            profile: profile,
            password: password
        ) { [weak self] outcome in
            guard let self else { return }
            self.isStopping = false
            switch outcome {
            case .completed(let result):
                self.reviewResult = result
                self.reviewSections = Self.makeSections(result)
                self.state = result.hasAnythingToClean ? .reviewing(result) : .empty
                self.render()
            case .cancelled:
                self.dismissSelf()
            case .failed(let message):
                self.state = .failed(message)
                self.render()
            }
        }
        if !started {
            state = .failed(String(localized: "home.alert.maintenanceInProgress"))
            render()
        }
    }

    private func startDelete(_ targets: [LeftoverFile], includeThumbnails: Bool) {
        state = .deleting
        isStopping = false
        render()
        guard let password = dependencies.appSession.activePassword else {
            state = .failed(String(localized: "storage.detail.overview.placeholder.disconnected"))
            render()
            return
        }
        let started = dependencies.remoteMaintenanceController.startDeleteLeftover(
            profile: profile,
            password: password,
            targets: targets,
            includeThumbnails: includeThumbnails
        ) { [weak self] outcome in
            guard let self else { return }
            self.isStopping = false
            switch outcome {
            case .completed(let result):
                self.state = .summary(result)
                self.render()
            case .cancelled:
                self.dismissSelf()
            case .failed(let message):
                self.state = .failed(message)
                self.render()
            }
        }
        if !started {
            state = .failed(String(localized: "home.alert.maintenanceInProgress"))
            render()
        }
    }

    @objc private func stopTapped() {
        guard !isStopping else { return }
        isStopping = true
        dependencies.remoteMaintenanceController.cancel()
        render()
    }

    @objc private func confirmDelete() {
        // Guard re-entrancy: only from the review state, and not while a confirm alert is already up.
        guard case .reviewing = state, presentedViewController == nil else { return }
        guard let result = reviewResult, result.hasAnythingToClean else { return }
        let combinedCount = result.totalCount + result.orphanThumbnailCount
        let alert = UIAlertController(
            title: String(localized: "storage.detail.leftover.confirm.title"),
            message: String.localizedStringWithFormat(
                String(localized: "storage.detail.leftover.confirm.message"),
                combinedCount
            ),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        alert.addAction(UIAlertAction(title: String(localized: "common.delete"), style: .destructive) { [weak self] _ in
            self?.startDelete(result.allFiles, includeThumbnails: result.orphanThumbnailCount > 0)
        })
        present(alert, animated: true)
    }

    @objc private func dismissSelf() {
        dismiss(animated: true)
    }

    // MARK: - Progress observation

    private func observeMaintenance() {
        maintenanceObserver = NotificationCenter.default.addObserver(
            forName: .RemoteMaintenanceDidChange,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            self?.updateProgressIfRunning()
        }
    }

    private func updateProgressIfRunning() {
        switch state {
        case .scanning, .deleting:
            // Skip the brief nil window after the op resets to idle (before the terminal outcome arrives),
            // which would otherwise flicker the count back to the "starting" copy for one frame.
            guard isStopping || dependencies.remoteMaintenanceController.currentProgress != nil else { return }
            statusLabel.text = progressText()
        default:
            break
        }
    }

    private static func makeSections(_ result: LeftoverScanResult) -> [ReviewSection] {
        result.groups
            .sorted { $0.month > $1.month }
            .map { ReviewSection(month: $0.month, files: $0.files) }
    }
}

extension LeftoverCleanupViewController: UITableViewDataSource, UITableViewDelegate {
    private var showsThumbnailSummary: Bool { (reviewResult?.orphanThumbnailCount ?? 0) > 0 }
    private func isThumbnailSection(_ section: Int) -> Bool {
        showsThumbnailSummary && section == reviewSections.count
    }

    func numberOfSections(in tableView: UITableView) -> Int {
        reviewSections.count + (showsThumbnailSummary ? 1 : 0)
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        isThumbnailSection(section) ? 1 : reviewSections[section].files.count
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        if isThumbnailSection(section) {
            return String(localized: "storage.detail.leftover.thumbnails.header")
        }
        let group = reviewSections[section]
        return String.localizedStringWithFormat(
            String(localized: "storage.detail.leftover.monthHeader"),
            group.month.displayText,
            group.files.count,
            ByteCountFormatter.string(fromByteCount: group.totalBytes, countStyle: .file)
        )
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        section == numberOfSections(in: tableView) - 1 ? String(localized: "storage.detail.leftover.footer") : nil
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: cellID, for: indexPath)
        var content = cell.defaultContentConfiguration()
        if isThumbnailSection(indexPath.section) {
            let result = reviewResult
            content.text = String(localized: "storage.detail.leftover.thumbnails.label")
            content.secondaryText = String.localizedStringWithFormat(
                String(localized: "storage.detail.leftover.thumbnails.detail"),
                result?.orphanThumbnailCount ?? 0,
                ByteCountFormatter.string(fromByteCount: result?.orphanThumbnailBytes ?? 0, countStyle: .file)
            )
        } else {
            let file = reviewSections[indexPath.section].files[indexPath.row]
            content.text = file.fileName
            content.secondaryText = ByteCountFormatter.string(fromByteCount: file.size, countStyle: .file)
        }
        cell.contentConfiguration = content
        cell.selectionStyle = .none
        return cell
    }
}
