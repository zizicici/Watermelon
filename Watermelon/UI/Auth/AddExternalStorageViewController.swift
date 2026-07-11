import SnapKit
import UniformTypeIdentifiers
import UIKit

final class AddExternalStorageViewController: UIViewController {
    private enum Section: Int, CaseIterable {
        case name
        case location
    }

    private let dependencies: DependencyContainer
    private let editingProfile: ServerProfileRecord?
    private let shouldPopToRootOnSave: Bool
    private let onSaved: (ServerProfileRecord, String) -> Void
    private let onPersistedWhileInactive: (ServerProfileRecord) -> Void

    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private lazy var saveBarButtonItem = UIBarButtonItem(
        title: String(localized: "common.save"),
        style: .prominentStyle,
        target: self,
        action: #selector(saveTapped)
    )
    private let loadingIndicatorView = UIActivityIndicatorView(style: .medium)
    private lazy var loadingBarButtonItem = UIBarButtonItem(customView: loadingIndicatorView)
    private lazy var keyboardToolbar: UIToolbar = {
        let toolbar = UIToolbar()
        toolbar.sizeToFit()
        toolbar.items = [
            UIBarButtonItem(barButtonSystemItem: .flexibleSpace, target: nil, action: nil),
            UIBarButtonItem(barButtonSystemItem: .done, target: self, action: #selector(dismissKeyboard))
        ]
        return toolbar
    }()

    private var keyboardObservers: [NSObjectProtocol] = []
    private var nameText = ""
    private var selectedDirectoryURL: URL?
    private var didAutoPresentDirectoryPicker = false
    private var commitGate = StorageProfileCommitGate()
    private var saveTask: Task<Void, Never>?
    private var activeSaveOperationID: UUID?
    private var screenPhase: ExternalStorageScreenPhase = .active
    private var pendingCommittedSave: (operationID: UUID, profile: ServerProfileRecord)?

    private var visibleSections: [Section] {
        editingProfile == nil ? Section.allCases : [.location]
    }

    private func resolvedSection(at index: Int) -> Section? {
        guard visibleSections.indices.contains(index) else { return nil }
        return visibleSections[index]
    }

    init(
        dependencies: DependencyContainer,
        editingProfile: ServerProfileRecord? = nil,
        shouldPopToRootOnSave: Bool = true,
        onPersistedWhileInactive: @escaping (ServerProfileRecord) -> Void,
        onSaved: @escaping (ServerProfileRecord, String) -> Void
    ) {
        self.dependencies = dependencies
        self.editingProfile = editingProfile
        self.shouldPopToRootOnSave = shouldPopToRootOnSave
        self.onPersistedWhileInactive = onPersistedWhileInactive
        self.onSaved = onSaved
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = editingProfile == nil ? String(localized: "auth.external.title") : String(localized: "auth.external.editTitle")

        fillInitialValues()
        configureUI()
        updateSaveButtonState()
        registerKeyboardNotifications()
        scheduleInitialDirectoryPickerPresentation()
    }

    deinit {
        saveTask?.cancel()
        if let pendingCommittedSave {
            onPersistedWhileInactive(pendingCommittedSave.profile)
        }
        keyboardObservers.forEach { NotificationCenter.default.removeObserver($0) }
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        guard isMovingFromParent || isBeingDismissed || navigationController?.isBeingDismissed == true,
              activeSaveOperationID != nil else { return }
        screenPhase = .departing
        saveTask?.cancel()
    }

    override func viewDidAppear(_ animated: Bool) {
        super.viewDidAppear(animated)
        screenPhase = .active
        completePendingCommittedSaveIfReady()
    }

    override func viewDidDisappear(_ animated: Bool) {
        super.viewDidDisappear(animated)
        guard screenPhase == .departing else { return }
        screenPhase = .inactive
        completePendingCommittedSaveIfReady()
    }

    private func fillInitialValues() {
        guard let editingProfile else { return }
        nameText = editingProfile.name
    }

    private func configureUI() {
        navigationItem.rightBarButtonItem = saveBarButtonItem

        tableView.backgroundColor = .appBackground
        tableView.dataSource = self
        tableView.delegate = self
        tableView.keyboardDismissMode = .interactive
        tableView.rowHeight = UITableView.automaticDimension
        tableView.estimatedRowHeight = 44
        tableView.register(SettingsTextFieldCell.self, forCellReuseIdentifier: SettingsTextFieldCell.reuseIdentifier)
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "ValueCell")

        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.edges.equalTo(view.safeAreaLayoutGuide)
        }
    }

    private func updateSaveButtonState() {
        let hasLocation = selectedDirectoryURL != nil || !(editingProfile?.externalVolumeParams?.displayPath ?? "").isEmpty
        saveBarButtonItem.isEnabled = hasLocation && !commitGate.isCommitting
    }

    private func currentDisplayPath() -> String? {
        if let selectedDirectoryURL {
            return selectedDirectoryURL.path
        }
        return editingProfile?.externalVolumeParams?.displayPath
    }

    @objc
    private func pickDirectoryTapped() {
        presentDirectoryPicker(animated: true)
    }

    private func scheduleInitialDirectoryPickerPresentation() {
        guard editingProfile == nil, !didAutoPresentDirectoryPicker else { return }
        didAutoPresentDirectoryPicker = true
        DispatchQueue.main.async { [weak self] in
            self?.presentInitialDirectoryPickerWhenReady(remainingAttempts: 30)
        }
    }

    private func presentInitialDirectoryPickerWhenReady(remainingAttempts: Int) {
        guard selectedDirectoryURL == nil, presentedViewController == nil else { return }
        guard view.window != nil else {
            guard remainingAttempts > 0 else { return }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.03) { [weak self] in
                self?.presentInitialDirectoryPickerWhenReady(remainingAttempts: remainingAttempts - 1)
            }
            return
        }

        let presentPicker = { [weak self] in
            guard let self,
                  self.selectedDirectoryURL == nil,
                  self.presentedViewController == nil,
                  self.view.window != nil else { return }
            self.presentDirectoryPicker(animated: true)
        }
        if let coordinator = transitionCoordinator {
            let registered = coordinator.animate(alongsideTransition: nil) { _ in presentPicker() }
            if !registered {
                presentPicker()
            }
        } else {
            presentPicker()
        }
    }

    private func presentDirectoryPicker(animated: Bool) {
        guard presentedViewController == nil, !commitGate.isCommitting else { return }
        dismissKeyboard()
        let picker = UIDocumentPickerViewController(forOpeningContentTypes: [.folder], asCopy: false)
        picker.delegate = self
        picker.allowsMultipleSelection = false
        present(picker, animated: animated)
    }

    @objc
    private func saveTapped() {
        dismissKeyboard()
        guard commitGate.begin() else { return }
        guard !rejectIfProfileMutationBlocked() else {
            commitGate.releaseAfterFailure()
            updateSaveButtonState()
            return
        }
        guard selectedDirectoryURL != nil || editingProfile?.externalVolumeParams != nil else {
            commitGate.releaseAfterFailure()
            updateSaveButtonState()
            presentAlert(
                title: String(localized: "auth.external.noDirSelected"),
                message: String(localized: "auth.external.noDirMessage")
            )
            return
        }

        let operationID = UUID()
        activeSaveOperationID = operationID
        screenPhase = .active
        pendingCommittedSave = nil
        setSaving(true)
        let intent = ExternalStorageProfileSaveWorker.Intent(
            editingProfile: editingProfile,
            selectedDirectoryURL: selectedDirectoryURL,
            name: nameText.trimmingCharacters(in: .whitespacesAndNewlines)
        )
        let databaseManager = dependencies.databaseManager
        let runtimeFlags = dependencies.appRuntimeFlags
        let persistedWhileInactiveCallback = onPersistedWhileInactive
        let workerTask = Task.detached(priority: .userInitiated) {
            try ExternalStorageProfileSaveWorker.save(
                intent: intent,
                databaseManager: databaseManager,
                runtimeFlags: runtimeFlags
            )
        }
        saveTask = Task { [weak self] in
            do {
                let savedProfile = try await withTaskCancellationHandler {
                    try await workerTask.value
                } onCancel: {
                    workerTask.cancel()
                }
                if let self {
                    self.finishSave(operationID: operationID, savedProfile: savedProfile)
                } else {
                    persistedWhileInactiveCallback(savedProfile)
                }
            } catch is CancellationError {
                self?.finishCancelledSave(operationID: operationID)
            } catch {
                self?.finishFailedSave(operationID: operationID, error: error)
            }
        }
    }

    private func setSaving(_ saving: Bool) {
        tableView.isUserInteractionEnabled = !saving
        if saving {
            loadingIndicatorView.startAnimating()
            navigationItem.rightBarButtonItem = loadingBarButtonItem
        } else {
            loadingIndicatorView.stopAnimating()
            navigationItem.rightBarButtonItem = saveBarButtonItem
            updateSaveButtonState()
        }
    }

    private func finishSave(operationID: UUID, savedProfile: ServerProfileRecord) {
        let operationIsCurrent = activeSaveOperationID == operationID
        let isNavigationTop = navigationController.map { $0.topViewController === self } ?? true
        let isActiveScreen = view.window != nil && isNavigationTop
        let completionMode = ExternalStorageSaveCompletionPolicy.mode(
            commitSucceeded: true,
            operationIsCurrent: operationIsCurrent,
            screenPhase: screenPhase,
            isScreenActive: isActiveScreen
        )
        guard completionMode != .none else { return }
        if completionMode == .deferred {
            pendingCommittedSave = (operationID, savedProfile)
            return
        }
        guard operationIsCurrent else {
            onPersistedWhileInactive(savedProfile)
            return
        }
        activeSaveOperationID = nil
        pendingCommittedSave = nil
        saveTask = nil
        guard completionMode == .normal else {
            if ExternalStorageSaveCompletionPolicy.shouldEndCommitGate(
                mode: completionMode,
                operationIsCurrent: operationIsCurrent
            ) {
                commitGate.end()
            }
            setSaving(false)
            onPersistedWhileInactive(savedProfile)
            return
        }
        if editingProfile == nil {
            let savedCallback = onSaved
            StorageProfileSaveTransition.completeCreate(
                from: self,
                shouldPopToRoot: shouldPopToRootOnSave
            ) {
                savedCallback(savedProfile, "")
            }
        } else {
            onSaved(savedProfile, "")
            popAfterSave()
        }
    }

    private func finishCancelledSave(operationID: UUID) {
        guard activeSaveOperationID == operationID else { return }
        activeSaveOperationID = nil
        pendingCommittedSave = nil
        saveTask = nil
        commitGate.releaseAfterFailure()
        setSaving(false)
    }

    private func finishFailedSave(operationID: UUID, error: Error) {
        guard activeSaveOperationID == operationID else { return }
        activeSaveOperationID = nil
        pendingCommittedSave = nil
        saveTask = nil
        commitGate.releaseAfterFailure()
        setSaving(false)
        let isNavigationTop = navigationController.map { $0.topViewController === self } ?? true
        guard view.window != nil, isNavigationTop else { return }
        if case ExternalStorageProfileSaveWorker.WorkerError.mutationBlocked = error {
            presentMutationBlockedAlert()
            return
        }
        presentAlert(
            title: String(localized: "auth.saveFailed"),
            message: UserFacingErrorLocalizer.message(for: error, storageType: .externalVolume)
        )
    }

    private func completePendingCommittedSaveIfReady() {
        guard let pendingCommittedSave,
              activeSaveOperationID == pendingCommittedSave.operationID else { return }
        if screenPhase == .departing { return }
        if screenPhase == .active {
            let isNavigationTop = navigationController.map { $0.topViewController === self } ?? true
            guard view.window != nil, isNavigationTop else { return }
        }
        self.pendingCommittedSave = nil
        finishSave(
            operationID: pendingCommittedSave.operationID,
            savedProfile: pendingCommittedSave.profile
        )
    }

    private func popAfterSave() {
        guard let navigationController else { return }
        if shouldPopToRootOnSave {
            navigationController.popToRootViewController(animated: true)
            return
        }
        navigationController.popViewController(animated: true)
    }

    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }

    private func rejectIfProfileMutationBlocked() -> Bool {
        let blocked = dependencies.appRuntimeFlags.isExecuting ||
            dependencies.remoteMaintenanceController.isBusy ||
            dependencies.appRuntimeFlags.isConnecting(profileID: editingProfile?.id)
        if blocked {
            presentMutationBlockedAlert()
        }
        return blocked
    }

    private func presentMutationBlockedAlert() {
        presentAlert(
            title: String(localized: "common.error"),
            message: String(localized: "home.alert.maintenanceInProgress")
        )
    }

    @objc
    private func dismissKeyboard() {
        view.endEditing(true)
    }

    private func registerKeyboardNotifications() {
        let center = NotificationCenter.default
        keyboardObservers.append(center.addObserver(
            forName: UIResponder.keyboardWillShowNotification,
            object: nil,
            queue: .main
        ) { [weak self] note in
            self?.handleKeyboard(note: note, showing: true)
        })
        keyboardObservers.append(center.addObserver(
            forName: UIResponder.keyboardWillHideNotification,
            object: nil,
            queue: .main
        ) { [weak self] note in
            self?.handleKeyboard(note: note, showing: false)
        })
    }

    private func handleKeyboard(note: Notification, showing: Bool) {
        guard let info = note.userInfo,
              let frame = info[UIResponder.keyboardFrameEndUserInfoKey] as? CGRect,
              let duration = info[UIResponder.keyboardAnimationDurationUserInfoKey] as? TimeInterval else {
            return
        }

        let keyboardFrame = view.convert(frame, from: nil)
        let overlap = max(0, view.bounds.maxY - keyboardFrame.minY - view.safeAreaInsets.bottom)
        let insetBottom = showing ? overlap : 0

        UIView.animate(withDuration: duration) {
            self.tableView.contentInset.bottom = insetBottom
            self.tableView.verticalScrollIndicatorInsets.bottom = insetBottom
        }
    }
}

extension AddExternalStorageViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        visibleSections.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        1
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        guard let section = resolvedSection(at: section) else { return nil }
        switch section {
        case .name:
            return String(localized: "auth.section.name")
        case .location:
            return String(localized: "auth.section.directory")
        }
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        guard let section = resolvedSection(at: section) else { return nil }
        switch section {
        case .name:
            return nil
        case .location:
            return String(localized: "auth.external.footerNoDir")
        }
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        guard let section = resolvedSection(at: indexPath.section) else { return UITableViewCell() }

        switch section {
        case .name:
            guard let cell = tableView.dequeueReusableCell(
                withIdentifier: SettingsTextFieldCell.reuseIdentifier,
                for: indexPath
            ) as? SettingsTextFieldCell else {
                return UITableViewCell()
            }
            cell.configure(
                title: nil,
                text: nameText,
                placeholder: String(localized: "auth.external.placeholder.name"),
                autocapitalizationType: .words,
                returnKeyType: .done,
                inputAccessoryView: keyboardToolbar
            )
            cell.onTextChanged = { [weak self] in self?.nameText = $0 }
            cell.onReturn = { [weak self] in self?.dismissKeyboard() }
            return cell
        case .location:
            let cell = tableView.dequeueReusableCell(withIdentifier: "ValueCell", for: indexPath)
            cell.accessoryType = .disclosureIndicator
            var content = UIListContentConfiguration.valueCell()
            if let path = currentDisplayPath() {
                content.text = URL(fileURLWithPath: path).lastPathComponent
            } else {
                content.text = String(localized: "auth.external.selectDir")
            }
            content.textProperties.color = .label
            cell.contentConfiguration = content
            return cell
        }
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard let section = resolvedSection(at: indexPath.section), section == .location else { return }
        pickDirectoryTapped()
    }
}

extension AddExternalStorageViewController: UIDocumentPickerDelegate {
    func documentPicker(_ controller: UIDocumentPickerViewController, didPickDocumentsAt urls: [URL]) {
        guard let url = urls.first else { return }

        selectedDirectoryURL = url
        if nameText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            nameText = url.lastPathComponent
        }
        updateSaveButtonState()
        tableView.reloadData()
    }
}
