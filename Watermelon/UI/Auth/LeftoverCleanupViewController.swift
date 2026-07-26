import Foundation
import SnapKit
import UIKit

// Self-contained modal for scan → review / optional hash check → selected delete → summary.
// Remote work is non-dismissible and cancellable; review and terminal states are dismissible.
final class LeftoverCleanupViewController: UIViewController {
    private enum State {
        case scanning
        case reviewing(LeftoverScanResult)
        case checkingHashes(LeftoverScanResult)
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
    private var selectedPaths = Set<String>()
    private var selectsThumbnails = false
    private var hashStatusByPath: [String: LeftoverHashCheckStatus] = [:]
    private var isStopping = false
    private var maintenanceObserver: NSObjectProtocol?

    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private let statusContainer = UIView()
    private let activityIndicator = UIActivityIndicatorView(style: .large)
    private let statusLabel = UILabel()
    private let navigationTitleLabel = UILabel()
    private let selectionSummaryLabel = UILabel()
    private let cellID = "leftover"
    private lazy var reviewTitleView: UIStackView = {
        navigationTitleLabel.text = String(localized: "storage.detail.leftover.title")
        navigationTitleLabel.font = .preferredFont(forTextStyle: .headline)
        navigationTitleLabel.textAlignment = .center
        navigationTitleLabel.adjustsFontForContentSizeCategory = true
        selectionSummaryLabel.font = .preferredFont(forTextStyle: .caption1)
        selectionSummaryLabel.textColor = .secondaryLabel
        selectionSummaryLabel.textAlignment = .center
        selectionSummaryLabel.adjustsFontForContentSizeCategory = true
        let stack = UIStackView(arrangedSubviews: [navigationTitleLabel, selectionSummaryLabel])
        stack.axis = .vertical
        stack.alignment = .center
        stack.spacing = 0
        return stack
    }()
    private lazy var deleteBarButtonItem = UIBarButtonItem(
        title: String(localized: "common.delete"),
        style: .plain,
        target: self,
        action: #selector(confirmDelete)
    )

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
            make.edges.equalToSuperview()
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
        case .scanning, .checkingHashes, .deleting:
            setDismissBlocked(true)
            showDefaultNavigationTitle()
            installStopButton()
            navigationItem.rightBarButtonItem = nil
            showStatus(activity: true, text: progressText())
        case .reviewing:
            setDismissBlocked(false)
            navigationItem.titleView = reviewTitleView
            navigationItem.leftBarButtonItem = UIBarButtonItem(
                barButtonSystemItem: .cancel, target: self, action: #selector(dismissSelf)
            )
            navigationItem.rightBarButtonItem = deleteBarButtonItem
            updateSelectionUI()
            showReview()
        case .empty:
            setDismissBlocked(false)
            showDefaultNavigationTitle()
            installDoneButton()
            showStatus(activity: false, text: String(localized: "storage.detail.leftover.empty.message"))
        case .summary(let result):
            setDismissBlocked(false)
            showDefaultNavigationTitle()
            installDoneButton()
            showStatus(activity: false, text: summaryText(result))
        case .failed(let message):
            setDismissBlocked(false)
            showDefaultNavigationTitle()
            installDoneButton()
            showStatus(activity: false, text: message)
        }
    }

    private func showDefaultNavigationTitle() {
        navigationItem.titleView = nil
        title = String(localized: "storage.detail.leftover.title")
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
        statusContainer.isHidden = false
        statusLabel.text = text
        activityIndicator.isHidden = !activity
        if activity { activityIndicator.startAnimating() } else { activityIndicator.stopAnimating() }
    }

    private func showReview() {
        statusContainer.isHidden = true
        activityIndicator.stopAnimating()
        tableView.isHidden = false
        tableView.reloadData()
    }

    private func updateSelectionUI() {
        let selectedFiles = selectedDataFiles()
        let thumbnailCount = selectsThumbnails ? (reviewResult?.orphanThumbnailCount ?? 0) : 0
        let count = selectedFiles.count + thumbnailCount
        let bytes = selectedFiles.reduce(Int64(0)) { $0 + $1.size }
            + (selectsThumbnails ? (reviewResult?.orphanThumbnailBytes ?? 0) : 0)
        deleteBarButtonItem.isEnabled = count > 0
        deleteBarButtonItem.tintColor = count > 0 ? .systemRed : nil
        selectionSummaryLabel.text = String.localizedStringWithFormat(
            String(localized: "storage.detail.leftover.selectionSummary"),
            count,
            ByteCountFormatter.string(fromByteCount: bytes, countStyle: .file)
        )
    }

    private func progressText() -> String {
        if isStopping { return String(localized: "backup.session.stopping") }
        let progress = dependencies.remoteMaintenanceController.currentProgress
        if case .deleting = state {
            guard let progress else {
                return String(localized: "storage.detail.overview.placeholder.deletingLeftoverStarting")
            }
            switch progress.kind {
            case .leftoverMaintenance(.preparingThumbnailDeletion):
                return String.localizedStringWithFormat(
                    String(localized: "storage.detail.leftover.progress.preparingThumbnailDeletion"),
                    progress.current,
                    progress.total
                )
            case .leftoverMaintenance(.scanningThumbnailsForDeletion):
                return String.localizedStringWithFormat(
                    String(localized: "storage.detail.leftover.progress.thumbnails"),
                    progress.current,
                    progress.total
                )
            case .leftoverMaintenance(.finalizingDelete):
                return String(localized: "storage.detail.leftover.progress.finalizingDelete")
            default:
                guard progress.total > 0 else {
                    return String(localized: "storage.detail.overview.placeholder.deletingLeftoverStarting")
                }
                return String.localizedStringWithFormat(
                    String(localized: "storage.detail.overview.placeholder.deletingLeftover"),
                    progress.current,
                    progress.total
                )
            }
        }
        guard let progress else {
            return String(localized: "storage.detail.overview.placeholder.scanningLeftoverStarting")
        }
        switch progress.kind {
        case .leftoverMaintenance(.scanningThumbnails):
            return String.localizedStringWithFormat(
                String(localized: "storage.detail.leftover.progress.thumbnails"),
                progress.current,
                progress.total
            )
        case .leftoverMaintenance(.finalizingScan):
            return String(localized: "storage.detail.leftover.progress.finalizingScan")
        case .leftoverMaintenance(.checkingHashes):
            return String.localizedStringWithFormat(
                String(localized: "storage.detail.leftover.progress.hashes"),
                progress.current,
                progress.total
            )
        case .leftoverMaintenance(.finalizingHashCheck):
            return String(localized: "storage.detail.leftover.progress.finalizingHashes")
        default:
            guard progress.total > 0 else {
                return String(localized: "storage.detail.overview.placeholder.scanningLeftoverStarting")
            }
            return String.localizedStringWithFormat(
                String(localized: "storage.detail.overview.placeholder.scanningLeftover"),
                progress.current,
                progress.total
            )
        }
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
                self.selectedPaths.removeAll()
                self.selectsThumbnails = false
                self.hashStatusByPath.removeAll()
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

    private func startHashCheck(_ targets: [LeftoverFile]) {
        guard let result = reviewResult, !targets.isEmpty else { return }
        state = .checkingHashes(result)
        isStopping = false
        render()
        guard let password = dependencies.appSession.activePassword else {
            state = .reviewing(result)
            render()
            presentError(String(localized: "storage.detail.overview.placeholder.disconnected"))
            return
        }
        let started = dependencies.remoteMaintenanceController.startCheckLeftoverHashes(
            profile: profile,
            password: password,
            targets: targets,
            knownResourceCatalog: result.knownResourceCatalog
        ) { [weak self] outcome in
            guard let self else { return }
            self.isStopping = false
            switch outcome {
            case .completed(let hashResult):
                self.hashStatusByPath.merge(hashResult.statusByPath) { _, new in new }
                self.state = .reviewing(result)
                self.render()
            case .cancelled:
                self.state = .reviewing(result)
                self.render()
            case .failed(let message):
                self.state = .reviewing(result)
                self.render()
                self.presentError(message)
            }
        }
        if !started {
            state = .reviewing(result)
            render()
            presentError(String(localized: "home.alert.maintenanceInProgress"))
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
                self.reviewResult = nil
                self.reviewSections = []
                self.hashStatusByPath.removeAll()
                self.state = .summary(result)
                self.render()
            case .cancelled:
                self.startScan()
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

    private func presentError(_ message: String) {
        if let presentedViewController {
            presentedViewController.dismiss(animated: true) { [weak self] in
                self?.presentError(message)
            }
            return
        }
        let alert = UIAlertController(
            title: String(localized: "common.error"),
            message: message,
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }

    @objc private func stopTapped() {
        guard !isStopping else { return }
        isStopping = true
        dependencies.remoteMaintenanceController.cancel()
        render()
    }

    private func confirmHashCheck() {
        guard case .reviewing = state, presentedViewController == nil,
              let result = reviewResult else { return }
        let selected = selectedDataFiles()
        let targets = selected.isEmpty ? result.allFiles : selected
        guard !targets.isEmpty else { return }
        let alert = UIAlertController(
            title: String(localized: "storage.detail.leftover.hash.confirm.title"),
            message: String.localizedStringWithFormat(
                String(localized: "storage.detail.leftover.hash.confirm.message"),
                targets.count
            ),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        alert.addAction(UIAlertAction(
            title: String(localized: "storage.detail.leftover.hash.start"),
            style: .default
        ) { [weak self] _ in
            self?.startHashCheck(targets)
        })
        present(alert, animated: true)
    }

    @objc private func confirmDelete() {
        guard case .reviewing = state, presentedViewController == nil else { return }
        guard let result = reviewResult else { return }
        let targets = selectedDataFiles()
        let thumbnailCount = selectsThumbnails ? result.orphanThumbnailCount : 0
        let combinedCount = targets.count + thumbnailCount
        guard combinedCount > 0 else { return }
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
            self?.startDelete(targets, includeThumbnails: self?.selectsThumbnails == true)
        })
        present(alert, animated: true)
    }

    private func selectedDataFiles() -> [LeftoverFile] {
        reviewResult?.allFiles.filter { selectedPaths.contains($0.path) } ?? []
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
        case .scanning, .checkingHashes, .deleting:
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
    private var showsHashAction: Bool { (reviewResult?.totalCount ?? 0) > 0 }
    private var showsThumbnailSummary: Bool { (reviewResult?.orphanThumbnailCount ?? 0) > 0 }
    private var thumbnailSection: Int { reviewSections.count }
    private var hashActionSection: Int {
        thumbnailSection + (showsThumbnailSummary ? 1 : 0)
    }
    private var lastCleanupSection: Int? {
        if showsThumbnailSummary { return thumbnailSection }
        return reviewSections.indices.last
    }

    private func isHashActionSection(_ section: Int) -> Bool {
        showsHashAction && section == hashActionSection
    }

    private func monthIndex(for section: Int) -> Int? {
        reviewSections.indices.contains(section) ? section : nil
    }

    private func isThumbnailSection(_ section: Int) -> Bool {
        showsThumbnailSummary && section == thumbnailSection
    }

    func numberOfSections(in tableView: UITableView) -> Int {
        (showsHashAction ? 1 : 0) + reviewSections.count + (showsThumbnailSummary ? 1 : 0)
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        if isHashActionSection(section) || isThumbnailSection(section) { return 1 }
        return monthIndex(for: section).map { reviewSections[$0].files.count } ?? 0
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        if isHashActionSection(section) {
            return String(localized: "storage.detail.leftover.hash.header")
        }
        if isThumbnailSection(section) {
            return String(localized: "storage.detail.leftover.thumbnails.header")
        }
        guard let monthIndex = monthIndex(for: section) else { return nil }
        let group = reviewSections[monthIndex]
        return String.localizedStringWithFormat(
            String(localized: "storage.detail.leftover.monthHeader"),
            group.month.displayText,
            group.files.count,
            ByteCountFormatter.string(fromByteCount: group.totalBytes, countStyle: .file)
        )
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        if isHashActionSection(section) {
            return String(localized: "storage.detail.leftover.hash.footer")
        }
        return section == lastCleanupSection
            ? String(localized: "storage.detail.leftover.footer")
            : nil
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: cellID, for: indexPath)
        var content = cell.defaultContentConfiguration()
        content.secondaryTextProperties.numberOfLines = 0
        cell.accessoryType = .none
        if isHashActionSection(indexPath.section) {
            content.text = String(localized: "storage.detail.leftover.hash.action")
            content.secondaryText = hashActionDetail()
            cell.accessoryType = .disclosureIndicator
        } else if isThumbnailSection(indexPath.section) {
            let result = reviewResult
            content.text = String(localized: "storage.detail.leftover.thumbnails.label")
            content.secondaryText = String.localizedStringWithFormat(
                String(localized: "storage.detail.leftover.thumbnails.detail"),
                result?.orphanThumbnailCount ?? 0,
                ByteCountFormatter.string(fromByteCount: result?.orphanThumbnailBytes ?? 0, countStyle: .file)
            )
            cell.accessoryType = selectsThumbnails ? .checkmark : .none
        } else {
            guard let monthIndex = monthIndex(for: indexPath.section) else { return cell }
            let file = reviewSections[monthIndex].files[indexPath.row]
            content.text = file.fileName
            content.secondaryText = fileDetail(file)
            cell.accessoryType = selectedPaths.contains(file.path) ? .checkmark : .none
        }
        cell.contentConfiguration = content
        cell.selectionStyle = .default
        return cell
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        if isHashActionSection(indexPath.section) {
            confirmHashCheck()
            return
        }
        if isThumbnailSection(indexPath.section) {
            selectsThumbnails.toggle()
        } else if let monthIndex = monthIndex(for: indexPath.section) {
            let file = reviewSections[monthIndex].files[indexPath.row]
            if !selectedPaths.insert(file.path).inserted {
                selectedPaths.remove(file.path)
            }
        }
        updateSelectionUI()
        tableView.reloadRows(at: [indexPath], with: .none)
    }

    private func hashActionDetail() -> String {
        let checkedCount = hashStatusByPath.values.reduce(into: 0) { count, status in
            if case .failed = status {} else { count += 1 }
        }
        guard checkedCount > 0 else {
            return String(localized: "storage.detail.leftover.hash.action.detail")
        }
        let matchedCount = hashStatusByPath.values.reduce(into: 0) { count, status in
            if case .matched = status { count += 1 }
        }
        return String.localizedStringWithFormat(
            String(localized: "storage.detail.leftover.hash.action.summary"),
            checkedCount,
            matchedCount
        )
    }

    private func fileDetail(_ file: LeftoverFile) -> String {
        let size = ByteCountFormatter.string(fromByteCount: file.size, countStyle: .file)
        guard let status = hashStatusByPath[file.path] else {
            return ([size] + probableMatchDetails(file)).joined(separator: "\n")
        }
        switch status {
        case .matched(let hashHex, let resources):
            guard !resources.isEmpty else {
                return size + "\n" + String(localized: "storage.detail.leftover.hash.failed")
            }
            let details = resources.map { resource in
                String.localizedStringWithFormat(
                    String(localized: "storage.detail.leftover.hash.match"),
                    resource.month.displayText,
                    resource.fileName,
                    ByteCountFormatter.string(fromByteCount: resource.fileSize, countStyle: .file)
                )
            }.joined(separator: "\n")
            return size + "\n" + details + "\nSHA-256 " + String(hashHex.prefix(12)) + "…"
        case .noMatch(let hashHex):
            return size
                + "\n"
                + String(localized: "storage.detail.leftover.hash.noMatch")
                + "\nSHA-256 "
                + String(hashHex.prefix(12))
                + "…"
        case .failed:
            return ([size] + probableMatchDetails(file) + [
                String(localized: "storage.detail.leftover.hash.failed")
            ]).joined(separator: "\n")
        }
    }

    private func probableMatchDetails(_ file: LeftoverFile) -> [String] {
        guard let summary = reviewResult?.probableMatchesByPath[file.path] else { return [] }
        var details = summary.matches.map { match in
            var evidence = [String(localized: "storage.detail.leftover.probable.evidence.size")]
            if match.hasSimilarName {
                evidence.append(String(localized: "storage.detail.leftover.probable.evidence.name"))
            }
            if match.hasMatchingTime {
                evidence.append(String(localized: "storage.detail.leftover.probable.evidence.time"))
            }
            return String.localizedStringWithFormat(
                String(localized: "storage.detail.leftover.probable.match"),
                match.resource.month.displayText,
                match.resource.fileName,
                ByteCountFormatter.string(fromByteCount: match.resource.fileSize, countStyle: .file),
                evidence.joined(separator: " · ")
            )
        }
        let hiddenCount = summary.totalCount - summary.matches.count
        if hiddenCount > 0 {
            details.append(String.localizedStringWithFormat(
                String(localized: "storage.detail.leftover.probable.more"),
                hiddenCount
            ))
        }
        return details
    }
}
