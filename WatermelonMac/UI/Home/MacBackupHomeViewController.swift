import AppKit

struct MacPhotoBrowserRequest {
    let initialMonth: LibraryMonthKey?
    let initialSide: SelectionSide
    let localQuery: PhotoLibraryQuery
    let title: String
    let monthGroupingTimeZone: MonthGroupingTimeZonePreference
}

enum MacHomeSelectionPresentationPolicy {
    static func remoteSelectionEnabled(
        selectionEnabled: Bool,
        remoteSnapshotReady: Bool,
        isSpecificAlbums: Bool
    ) -> Bool {
        selectionEnabled
            && remoteSnapshotReady
            && !isSpecificAlbums
    }
}

struct MacHomeRemoteSnapshotApplication {
    let shouldApply: Bool
    let state: RemoteLibrarySnapshotState?
    let hasActiveConnection: Bool
}

enum MacHomeRemoteSnapshotPolicy {
    static func resolve(
        state: RemoteLibrarySnapshotState?,
        requestedActiveConnection: Bool,
        sourceSessionGeneration: UInt64,
        currentSessionGeneration: UInt64,
        activeSessionProfile: ServerProfileRecord?,
        selectedProfile: ServerProfileRecord?,
        connectedProfile: ServerProfileRecord?
    ) -> MacHomeRemoteSnapshotApplication {
        guard sourceSessionGeneration == currentSessionGeneration else {
            return MacHomeRemoteSnapshotApplication(
                shouldApply: false,
                state: nil,
                hasActiveConnection: false
            )
        }
        guard requestedActiveConnection,
              let state,
              let activeSessionProfile,
              let selectedProfile,
              let connectedProfile,
              activeSessionProfile.runtimeConnectionIdentity
                == connectedProfile.runtimeConnectionIdentity,
              selectedProfile.runtimeConnectionIdentity
                == connectedProfile.runtimeConnectionIdentity else {
            return MacHomeRemoteSnapshotApplication(
                shouldApply: true,
                state: nil,
                hasActiveConnection: false
            )
        }
        let expectedProfileKey =
            RemoteIndexSyncService.remoteProfileKey(
                connectedProfile
            )
        let activeSessionProfileKey =
            RemoteIndexSyncService.remoteProfileKey(
                activeSessionProfile
            )
        let selectedProfileKey =
            RemoteIndexSyncService.remoteProfileKey(
                selectedProfile
            )
        guard RemoteSnapshotOwnership.matches(
            ownerProfileKey: state.profileKey,
            expectedProfileKey: expectedProfileKey
        ), activeSessionProfileKey == expectedProfileKey,
           selectedProfileKey == expectedProfileKey else {
            return MacHomeRemoteSnapshotApplication(
                shouldApply: true,
                state: nil,
                hasActiveConnection: false
            )
        }
        return MacHomeRemoteSnapshotApplication(
            shouldApply: true,
            state: state,
            hasActiveConnection: true
        )
    }
}

enum MacHomeExecutionStartPolicy {
    static func isEnabled(
        selectionEnabled: Bool,
        hasSelection: Bool,
        hasRemoteSelection: Bool,
        remoteSnapshotReady: Bool
    ) -> Bool {
        selectionEnabled
            && hasSelection
            && remoteStateAllows(
                hasRemoteSelection: hasRemoteSelection,
                remoteSnapshotReady: remoteSnapshotReady
            )
    }

    static func remoteStateAllows(
        hasRemoteSelection: Bool,
        remoteSnapshotReady: Bool
    ) -> Bool {
        !hasRemoteSelection || remoteSnapshotReady
    }
}

enum MacHomeExecutionPresentationPolicy {
    static func shouldApplyExternalPresentation(
        manualExecutionActive: Bool
    ) -> Bool {
        !manualExecutionActive
    }
}

enum MacConnectionFailureRecoveryPolicy {
    static func editProfileID(
        profileID: Int64?,
        response: NSApplication.ModalResponse
    ) -> Int64? {
        guard response == .alertFirstButtonReturn else {
            return nil
        }
        return profileID
    }
}

@MainActor
final class MacBackupHomeViewController: NSViewController {
    var onExecutionActivityChanged: ((Bool) -> Void)?
    var onOpenPhotoBrowser: ((MacPhotoBrowserRequest) -> Void)?
    var onOpenExecutionLog: ((URL) -> Void)?
    var onConnectDestination: ((ServerProfileRecord) -> Void)?
    var onConfigureDestination: ((Int64) -> Void)?
    var onManageDestinations: (() -> Void)?
    var onCreateDestination: ((StorageType) -> Void)?
    var onMonthGroupingTimeZoneAvailabilityChange: (() -> Void)?

    private let photoLibraryAuthorizationProvider: any PhotoLibraryAuthorizationProviding
    private let photoLibraryService: PhotoLibraryService
    private let photoLibraryIndexController: MacPhotoLibraryIndexController
    private let remoteConnectionController: MacRemoteConnectionController
    private let appSession: AppSession
    private let profileReachabilityService: ProfileReachabilityService
    private let backupExecutionController: MacBackupExecutionController
    private let libraryListViewController = MacLibraryMonthListViewController()
    private let photoSymbol = NSImageView()
    private let photoSourceButton = NSButton()
    private let remoteSymbol = NSImageView()
    private let remoteDestinationButton = NSButton()
    private let connectionProgressIndicator = NSProgressIndicator()
    private let connectionButton = NSButton()
    private var selectedProfile: ServerProfileRecord?
    private var destinationProfiles: [ServerProfileRecord] = []
    private var destinationInteractionEnabled = true
    private var remoteSnapshotReady = false
    private var currentSnapshot = PhotoLibraryMonthlyIndexSnapshot.empty
    private var isPhotoLibraryIndexLoading = false
    private var isPhotoLibraryReloadPending = false
    private var isStartingExecution = false
    private var didRequestReviewForCurrentExecution = false
    private var photoLibraryAccessState: PhotoLibraryAccessState = .unknown
    private var externalExecutionActive = false
    private var albumDescriptorsByID: [String: LocalAlbumDescriptor] = [:]
    nonisolated(unsafe) private var activationObserver: NSObjectProtocol?
    #if DEBUG
    private var didLoadDemoPhotoLibrary = false
    private var didSeedDemoSelection = false
    #endif

    private lazy var selectionController = HomeSelectionController(
        hooks: HomeSelectionController.Hooks(
            isSelectable: { [weak self] in
                self?.canSelectMonths ?? false
            },
            isRemoteSelectionAllowed: { [weak self] in
                self?.photoLibraryIndexController.scope
                    .isSpecificAlbums == false
            },
            isRemoteReady: { [weak self] in
                self?.remoteSnapshotReady ?? false
            },
            sections: { [weak self] in
                self?.currentSnapshot.sections ?? []
            }
        )
    )

    init(
        photoLibraryAuthorizationProvider: any PhotoLibraryAuthorizationProviding,
        photoLibraryService: PhotoLibraryService,
        photoLibraryIndexController: MacPhotoLibraryIndexController,
        remoteConnectionController: MacRemoteConnectionController,
        appSession: AppSession,
        profileReachabilityService: ProfileReachabilityService,
        backupExecutionController: MacBackupExecutionController
    ) {
        self.photoLibraryAuthorizationProvider = photoLibraryAuthorizationProvider
        self.photoLibraryService = photoLibraryService
        self.photoLibraryIndexController = photoLibraryIndexController
        self.remoteConnectionController = remoteConnectionController
        self.appSession = appSession
        self.profileReachabilityService = profileReachabilityService
        self.backupExecutionController = backupExecutionController
        super.init(nibName: nil, bundle: nil)
        self.libraryListViewController.destinationTitleProvider = {
            [weak self] profile in
            self?.destinationMenuTitle(profile) ?? profile.name
        }
        self.photoLibraryIndexController.onChange = { [weak self] state in
            self?.applyPhotoLibraryIndexState(state)
        }
        self.libraryListViewController.onRefresh = { [weak self] in
            self?.reloadPhotoLibrary()
        }
        self.libraryListViewController.onLocalAccessAction = {
            [weak self] in
            self?.handlePhotoAccess()
        }
        self.libraryListViewController.onCreateDestination = {
            [weak self] type in
            self?.onCreateDestination?(type)
        }
        self.libraryListViewController.onConnectDestination = {
            [weak self] profile in
            self?.onConnectDestination?(profile)
        }
        self.libraryListViewController.onRemoteOverlayAction = {
            [weak self] in
            self?.handleConnection(nil)
        }
        self.libraryListViewController.onToggleMonth = {
            [weak self] month, side in
            guard let self,
                  self.selectionController.toggleMonth(
                    month,
                    side: side
                  ) else {
                return
            }
            self.updateSelectionPresentation()
        }
        self.libraryListViewController.onToggleYear = {
            [weak self] sectionIndex, side in
            guard let self,
                  self.selectionController.toggleYear(
                    sectionIndex: sectionIndex,
                    side: side
                  ) else {
                return
            }
            self.updateSelectionPresentation()
        }
        self.libraryListViewController.onToggleAll = { [weak self] side in
            guard let self,
                  self.selectionController.toggleAll(side: side) else {
                return
            }
            self.updateSelectionPresentation()
        }
        self.libraryListViewController.onStart = { [weak self] in
            self?.startSelection()
        }
        self.libraryListViewController.onPauseExecution = { [weak self] in
            self?.backupExecutionController.pause()
        }
        self.libraryListViewController.onResumeExecution = { [weak self] in
            self?.backupExecutionController.resume()
        }
        self.libraryListViewController.onCompleteExecution = {
            [weak self] in
            self?.selectionController.clear()
            self?.backupExecutionController.resetPresentation()
        }
        self.libraryListViewController.onStopExecution = { [weak self] in
            self?.confirmStopExecution()
        }
        self.libraryListViewController.onOpenExecutionLog = { [weak self] in
            guard let self,
                  let url = self.backupExecutionController
                    .currentSessionLogURL else {
                return
            }
            self.onOpenExecutionLog?(url)
        }
        self.libraryListViewController.onOpenYear = {
            [weak self] sectionIndex, side in
            guard let self,
                  self.currentSnapshot.sections.indices.contains(
                    sectionIndex
                  ),
                  let month = self.currentSnapshot
                    .sections[sectionIndex].rows.first?.month else {
                return
            }
            self.openPhotoBrowser(
                initialMonth: month,
                initialSide: side
            )
        }
        self.libraryListViewController.onOpenMonth = {
            [weak self] month, side in
            guard let self else { return }
            self.openPhotoBrowser(
                initialMonth: month,
                initialSide: side
            )
        }
        self.remoteConnectionController.onChange = { [weak self] state in
            self?.applyRemoteConnectionState(state)
        }
        self.remoteConnectionController.onRemoteSnapshot = {
            [weak self] state, hasActiveConnection, sessionGeneration in
            self?.applyRemoteSnapshot(
                state,
                hasActiveConnection: hasActiveConnection,
                sessionGeneration: sessionGeneration
            )
        }
        self.remoteConnectionController.onNeedsCredential = {
            [weak self] profile in
            guard let self else { return nil }
            return await self.promptForCredential(profile: profile)
        }
        self.remoteConnectionController.onNeedsSFTPHostKeyTrust = {
            [weak self] profile, decision, fingerprint in
            guard let self else { return false }
            return await self.confirmSFTPHostKey(
                profile: profile,
                decision: decision,
                fingerprint: fingerprint
            )
        }
        self.remoteConnectionController.onConnectionFailed = {
            [weak self] profile, message in
            self?.presentConnectionFailure(
                profile: profile,
                message: message
            )
        }
        self.backupExecutionController.onChange = { [weak self] state in
            if state.isActive {
                self?.externalExecutionActive = false
            }
            self?.applyBackupExecutionState(state)
            self?.onExecutionActivityChanged?(state.isActive)
        }
        self.backupExecutionController.onMonthExecutionChange = {
            [weak self] in
            guard let self else { return }
            self.libraryListViewController.applyMonthExecution(
                self.backupExecutionController
                    .monthExecutionPhases,
                progress: self.backupExecutionController
                    .monthExecutionProgress
            )
        }
        self.backupExecutionController.onRemoteSnapshot = {
            [weak self] state, sessionGeneration in
            self?.applyRemoteSnapshot(
                state,
                hasActiveConnection: true,
                sessionGeneration: sessionGeneration
            )
        }
        self.backupExecutionController.onLocalLibraryChanged = {
            [weak self] in
            self?.reloadPhotoLibrary()
        }
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        if let activationObserver {
            NotificationCenter.default.removeObserver(activationObserver)
        }
    }

    override func loadView() {
        view = NSView()

        photoSymbol.symbolConfiguration = NSImage.SymbolConfiguration(
            pointSize: 24,
            weight: .medium
        )
        photoSymbol.image = NSImage(
            systemSymbolName: "photo.on.rectangle.angled",
            accessibilityDescription: nil
        )
        photoSymbol.contentTintColor = .secondaryLabelColor

        let photoTitleLabel = NSTextField(
            labelWithString: String(
                localized: "home.photoLibrary",
                defaultValue: "Photos Library"
            )
        )
        photoTitleLabel.font = .systemFont(ofSize: 17, weight: .semibold)

        photoSourceButton.image = NSImage(
            systemSymbolName: "rectangle.stack",
            accessibilityDescription: nil
        )
        photoSourceButton.imagePosition = .imageLeading
        photoSourceButton.target = self
        photoSourceButton.action = #selector(choosePhotoSource(_:))
        photoSourceButton.bezelStyle = .rounded
        updatePhotoSourceButton()

        let photoHeader = NSStackView(
            views: [photoSymbol, photoTitleLabel]
        )
        photoHeader.orientation = .horizontal
        photoHeader.alignment = .centerY
        photoHeader.spacing = 12

        let photoContent = NSStackView(
            views: [
                photoHeader,
                NSView(),
                photoSourceButton
            ]
        )
        photoContent.orientation = .horizontal
        photoContent.alignment = .centerY
        photoContent.spacing = 10
        let photoCard = makeCard(content: photoContent)

        remoteSymbol.symbolConfiguration = NSImage.SymbolConfiguration(
            pointSize: 24,
            weight: .medium
        )
        remoteSymbol.image = NSImage(
            systemSymbolName: "externaldrive",
            accessibilityDescription: nil
        )
        remoteSymbol.contentTintColor = .secondaryLabelColor

        remoteDestinationButton.title = String(
            localized: "mediaBrowser.mode.remote",
            defaultValue: "Remote"
        )
        remoteDestinationButton.font = .systemFont(
            ofSize: 17,
            weight: .semibold
        )
        remoteDestinationButton.image = NSImage(
            systemSymbolName: "chevron.down",
            accessibilityDescription: nil
        )
        remoteDestinationButton.imagePosition = .imageTrailing
        remoteDestinationButton.isBordered = false
        remoteDestinationButton.contentTintColor = .labelColor
        remoteDestinationButton.target = self
        remoteDestinationButton.action = #selector(
            chooseDestination(_:)
        )
        remoteDestinationButton.isEnabled =
            destinationInteractionEnabled
        remoteDestinationButton.setContentCompressionResistancePriority(
            .defaultLow,
            for: .horizontal
        )

        connectionProgressIndicator.style = .spinning
        connectionProgressIndicator.controlSize = .small
        connectionProgressIndicator.isDisplayedWhenStopped = false

        connectionButton.target = self
        connectionButton.action = #selector(handleConnection(_:))
        connectionButton.bezelStyle = .rounded

        let remoteHeader = NSStackView(
            views: [remoteSymbol, remoteDestinationButton]
        )
        remoteHeader.orientation = .horizontal
        remoteHeader.alignment = .centerY
        remoteHeader.spacing = 12

        let remoteContent = NSStackView(
            views: [
                remoteHeader,
                NSView(),
                connectionProgressIndicator,
                connectionButton
            ]
        )
        remoteContent.orientation = .horizontal
        remoteContent.alignment = .centerY
        remoteContent.spacing = 10
        let remoteCard = makeCard(content: remoteContent)

        let sourceCards = NSStackView(views: [photoCard, remoteCard])
        sourceCards.orientation = .horizontal
        sourceCards.alignment = .centerY
        sourceCards.distribution = .fillEqually
        sourceCards.spacing = 12

        addChild(libraryListViewController)
        let libraryView = libraryListViewController.view
        let content = NSStackView(
            views: [
                sourceCards,
                libraryView
            ]
        )
        content.orientation = .vertical
        content.alignment = .leading
        content.spacing = 12
        content.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(content)

        photoCard.translatesAutoresizingMaskIntoConstraints = false
        remoteCard.translatesAutoresizingMaskIntoConstraints = false
        sourceCards.translatesAutoresizingMaskIntoConstraints = false
        libraryView.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            content.topAnchor.constraint(
                equalTo: view.safeAreaLayoutGuide.topAnchor,
                constant: 24
            ),
            content.leadingAnchor.constraint(equalTo: view.leadingAnchor, constant: 44),
            content.trailingAnchor.constraint(equalTo: view.trailingAnchor, constant: -44),
            sourceCards.widthAnchor.constraint(equalTo: content.widthAnchor),
            photoCard.widthAnchor.constraint(equalTo: remoteCard.widthAnchor),
            photoCard.heightAnchor.constraint(equalTo: remoteCard.heightAnchor),
            libraryView.widthAnchor.constraint(equalTo: content.widthAnchor),
            libraryView.heightAnchor.constraint(greaterThanOrEqualToConstant: 250),
            content.bottomAnchor.constraint(
                equalTo: view.safeAreaLayoutGuide.bottomAnchor,
                constant: -20
            ),
            photoSymbol.widthAnchor.constraint(equalToConstant: 34),
            photoSymbol.heightAnchor.constraint(equalToConstant: 34),
            remoteSymbol.widthAnchor.constraint(equalToConstant: 34),
            remoteSymbol.heightAnchor.constraint(equalToConstant: 34)
        ])

        show(profile: selectedProfile)
        refreshPhotoLibraryAccess()

        activationObserver = NotificationCenter.default.addObserver(
            forName: NSApplication.didBecomeActiveNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            MainActor.assumeIsolated {
                self?.refreshPhotoLibraryAccess()
            }
        }
        applyBackupExecutionState(backupExecutionController.state)
    }

    func show(profile: ServerProfileRecord?) {
        guard !executionIsActive else { return }
        let previousProfileID = selectedProfile?.id
        selectedProfile = profile
        let didChangeProfile = previousProfileID != profile?.id
        if didChangeProfile {
            remoteSnapshotReady = false
        }
        guard isViewLoaded else { return }
        if didChangeProfile {
            updateSelectionPresentation()
        }
        remoteDestinationButton.title = profile?.name ?? String(
            localized: "mediaBrowser.mode.remote",
            defaultValue: "Remote"
        )

        guard profile != nil else {
            connectionButton.isHidden = true
            connectionProgressIndicator.stopAnimation(nil)
            updateRemoteOverlay()
            return
        }

        connectionButton.isHidden = false
        applyRemoteConnectionState(remoteConnectionController.state)
    }

    func reloadPhotoLibrary() {
        guard !executionIsActive,
              !isPhotoLibraryIndexLoading else {
            isPhotoLibraryReloadPending = true
            return
        }
        isPhotoLibraryReloadPending = false
        photoLibraryIndexController.reload()
    }

    func setDestinations(
        _ profiles: [ServerProfileRecord],
        selectedProfileID: Int64?
    ) {
        destinationProfiles = profiles
        updateReachabilityProfiles()
        guard isViewLoaded else { return }
        if let profile = profiles.first(where: {
            $0.id == selectedProfileID
        }) {
            remoteDestinationButton.title = profile.name
        } else {
            remoteDestinationButton.title = String(
                localized: "mediaBrowser.mode.remote",
                defaultValue: "Remote"
            )
        }
        updateRemoteOverlay()
    }

    func setDestinationInteractionEnabled(_ enabled: Bool) {
        destinationInteractionEnabled = enabled
        guard isViewLoaded else { return }
        remoteDestinationButton.isEnabled = enabled
        updateRemoteOverlay()
    }

    func applyCurrentRemoteSnapshot(
        _ state: RemoteLibrarySnapshotState,
        sessionGeneration: UInt64
    ) {
        applyRemoteSnapshot(
            state,
            hasActiveConnection: isRemoteConnected,
            sessionGeneration: sessionGeneration
        )
    }

    private func makeCard(content: NSView) -> NSView {
        let card = NSView()
        card.wantsLayer = true
        card.layer?.backgroundColor = NSColor.controlBackgroundColor.cgColor
        card.layer?.borderColor = NSColor.separatorColor.cgColor
        card.layer?.borderWidth = 1
        card.layer?.cornerRadius = 10
        content.translatesAutoresizingMaskIntoConstraints = false
        card.addSubview(content)
        NSLayoutConstraint.activate([
            content.topAnchor.constraint(equalTo: card.topAnchor, constant: 12),
            content.leadingAnchor.constraint(equalTo: card.leadingAnchor, constant: 14),
            content.trailingAnchor.constraint(equalTo: card.trailingAnchor, constant: -14),
            content.bottomAnchor.constraint(equalTo: card.bottomAnchor, constant: -12)
        ])
        return card
    }

    private func refreshPhotoLibraryAccess() {
        applyPhotoLibraryAccessState(
            photoLibraryAuthorizationProvider.currentAccessState()
        )
    }

    private func applyPhotoLibraryAccessState(_ state: PhotoLibraryAccessState) {
        #if DEBUG
        let arguments = ProcessInfo.processInfo.arguments
        let isDemoLibrary =
            MacDemoPhotoLibraryPolicy.usesSyntheticLibrary(
                arguments: arguments
            )
        let isDemoAccessDenied = arguments.contains(
            "--demo-photo-access-denied"
        )
        let resolvedState: PhotoLibraryAccessState = isDemoAccessDenied
            ? .denied
            : (isDemoLibrary ? .authorized : state)
        #else
        let resolvedState = state
        #endif

        let previousState = photoLibraryAccessState
        let wasReadable = previousState.canReadLibrary
        photoLibraryAccessState = resolvedState
        libraryListViewController.applyLocalAccessState(resolvedState)
        photoSourceButton.isHidden = !resolvedState.canReadLibrary
        photoSourceButton.isEnabled =
            resolvedState.canReadLibrary && !executionIsActive
        if previousState != resolvedState {
            selectionController.clear()
            updateSelectionPresentation()
        }

        #if DEBUG
        if isDemoLibrary || isDemoAccessDenied {
            if !didLoadDemoPhotoLibrary {
                didLoadDemoPhotoLibrary = true
                reloadPhotoLibrary()
            }
            return
        }
        #endif

        if resolvedState.canReadLibrary {
            if !wasReadable || previousState != resolvedState {
                reloadPhotoLibrary()
            }
        } else {
            if wasReadable || photoLibraryIndexControllerIsIdle {
                reloadPhotoLibrary()
            }
        }
    }

    private func applyPhotoLibraryIndexState(
        _ state: MacPhotoLibraryIndexController.State
    ) {
        guard isViewLoaded else { return }
        let previousTimeZoneAvailability =
            canChangeMonthGroupingTimeZone
        switch state {
        case .idle:
            break
        case .loading:
            isPhotoLibraryIndexLoading = true
            libraryListViewController.showLoading()
            updateSelectionPresentation()
        case .unavailable(let accessState):
            isPhotoLibraryIndexLoading = false
            libraryListViewController.applyLocalAccessState(
                accessState
            )
            libraryListViewController.showUnavailable("")
            updateSelectionPresentation()
        case .loaded(let snapshot):
            isPhotoLibraryIndexLoading = false
            currentSnapshot = snapshot
            updatePhotoSourceButton()
            reconcileSelection()
            seedDemoSelectionIfNeeded()
            libraryListViewController.applyLocalAccessState(
                photoLibraryAccessState
            )
            libraryListViewController.apply(snapshot: snapshot)
            updateSelectionPresentation()
        case .failed(let message):
            isPhotoLibraryIndexLoading = false
            libraryListViewController.applyLocalAccessState(
                photoLibraryAccessState
            )
            libraryListViewController.showError(message)
            updateSelectionPresentation()
        }
        if previousTimeZoneAvailability
            != canChangeMonthGroupingTimeZone {
            onMonthGroupingTimeZoneAvailabilityChange?()
        }
        performPendingPhotoLibraryReloadIfPossible()
    }

    private var photoLibraryIndexControllerIsIdle: Bool {
        if case .idle = photoLibraryIndexController.state {
            return true
        }
        return false
    }

    private var photoLibraryIndexControllerIsReady: Bool {
        if case .loaded = photoLibraryIndexController.state {
            return true
        }
        return false
    }

    private var isRemoteConnected: Bool {
        if case .connected = remoteConnectionController.state {
            return true
        }
        return false
    }

    private var canSelectMonths: Bool {
        #if DEBUG
        let canReadLibrary = photoLibraryAccessState.canReadLibrary
            || MacDemoPhotoLibraryPolicy.usesSyntheticLibrary(
                arguments: ProcessInfo.processInfo.arguments
            )
        #else
        let canReadLibrary = photoLibraryAccessState.canReadLibrary
        #endif
        return canReadLibrary
            && isRemoteConnected
            && !remoteConnectionController.state.isConnecting
            && photoLibraryIndexControllerIsReady
            && !isStartingExecution
            && !executionIsActive
    }

    private var executionIsActive: Bool {
        backupExecutionController.state.isActive
            || externalExecutionActive
    }

    var canChangeMonthGroupingTimeZone: Bool {
        !executionIsActive && !isPhotoLibraryIndexLoading
    }

    func setExternalExecutionActive(_ active: Bool) {
        let manualExecutionActive =
            backupExecutionController.state.isActive
        externalExecutionActive = active && !manualExecutionActive
        guard isViewLoaded else { return }
        guard MacHomeExecutionPresentationPolicy
            .shouldApplyExternalPresentation(
                manualExecutionActive: manualExecutionActive
            ) else {
            connectionButton.isEnabled = false
            photoSourceButton.isEnabled = false
            updateSelectionPresentation()
            performPendingPhotoLibraryReloadIfPossible()
            return
        }
        if externalExecutionActive {
            libraryListViewController.applyExecution(
                status: String(
                    localized: "mediaBrowser.action.uploading",
                    defaultValue: "Backing up…"
                ),
                progress: nil,
                active: true,
                canPause: false,
                canResume: false,
                canComplete: false,
                canStop: false,
                canOpenLog: false
            )
        } else {
            libraryListViewController.applyExecution(
                status: nil,
                progress: nil,
                active: false,
                canPause: false,
                canResume: false,
                canComplete: false,
                canStop: false,
                canOpenLog: false
            )
            applyRemoteConnectionState(
                remoteConnectionController.state
            )
        }
        connectionButton.isEnabled = !executionIsActive
        photoSourceButton.isEnabled = !executionIsActive
        updateSelectionPresentation()
        performPendingPhotoLibraryReloadIfPossible()
    }

    private func applyRemoteSnapshot(
        _ state: RemoteLibrarySnapshotState?,
        hasActiveConnection: Bool,
        sessionGeneration: UInt64
    ) {
        let currentSession = appSession.snapshot
        let application = MacHomeRemoteSnapshotPolicy.resolve(
            state: state,
            requestedActiveConnection: hasActiveConnection,
            sourceSessionGeneration: sessionGeneration,
            currentSessionGeneration: currentSession.generation,
            activeSessionProfile: currentSession.activeProfile,
            selectedProfile: selectedProfile,
            connectedProfile:
                remoteConnectionController.state.connectedProfile
        )
        guard application.shouldApply else { return }
        remoteSnapshotReady = false
        updateSelectionPresentation()
        updateRemoteOverlay()
        photoLibraryIndexController.applyRemoteSnapshot(
            application.state,
            hasActiveConnection:
                application.hasActiveConnection
        ) { [weak self] in
            guard let self else { return }
            let currentSession = self.appSession.snapshot
            let currentApplication =
                MacHomeRemoteSnapshotPolicy.resolve(
                    state: application.state,
                    requestedActiveConnection:
                        application.hasActiveConnection,
                    sourceSessionGeneration: sessionGeneration,
                    currentSessionGeneration:
                        currentSession.generation,
                    activeSessionProfile:
                        currentSession.activeProfile,
                    selectedProfile: self.selectedProfile,
                    connectedProfile:
                        self.remoteConnectionController
                            .state.connectedProfile
                )
            guard currentApplication.shouldApply else { return }
            self.remoteSnapshotReady =
                currentApplication.hasActiveConnection
            self.updateSelectionPresentation()
            self.updateRemoteOverlay()
        }
    }

    private func applyRemoteConnectionState(
        _ state: MacRemoteConnectionState
    ) {
        updateReachabilityProfiles(
            activeProfileID: state.connectedProfile?.id
        )
        guard isViewLoaded, let selectedProfile else { return }
        connectionProgressIndicator.stopAnimation(nil)
        connectionButton.isEnabled = !executionIsActive

        switch state {
        case .idle:
            remoteSnapshotReady = false
            connectionButton.title = String(
                localized: "common.connect",
                defaultValue: "Connect"
            )
        case .connecting(let profile, _):
            guard profile.id == selectedProfile.id else { return }
            remoteSnapshotReady = false
            connectionProgressIndicator.startAnimation(nil)
            connectionButton.title = String(
                localized: "common.cancel",
                defaultValue: "Cancel"
            )
        case .connected(let profile, _):
            guard profile.id == selectedProfile.id else { return }
            connectionButton.title = String(
                localized: "home.menu.disconnect",
                defaultValue: "Disconnect"
            )
        case .failed(let profile, _):
            guard profile.id == selectedProfile.id else { return }
            remoteSnapshotReady = false
            connectionButton.title = String(
                localized: "common.retry",
                defaultValue: "Retry"
            )
        }

        #if DEBUG
        if ProcessInfo.processInfo.arguments.contains("--demo-photo-library") {
            connectionButton.isEnabled = false
            connectionButton.toolTip = nil
        }
        if ProcessInfo.processInfo.arguments.contains("--demo-connected") {
            remoteSnapshotReady = true
        }
        #endif
        seedDemoSelectionIfNeeded()
        updateSelectionPresentation()
        updateRemoteOverlay()
    }

    private func updateRemoteOverlay() {
        guard isViewLoaded else { return }
        let mode: MacLibraryMonthListViewController.RemoteOverlayMode
        if destinationProfiles.isEmpty {
            mode = .emptySetup
        } else {
            switch remoteConnectionController.state {
            case .idle, .failed:
                mode = .profileSelection
            case .connecting(let profile, let progress)
                where profile.id == selectedProfile?.id:
                mode = .progress(
                    message: connectionProgressText(progress),
                    actionTitle: String(
                        localized: "common.cancel",
                        defaultValue: "Cancel"
                    )
                )
            case .connected(let profile, _)
                where profile.id == selectedProfile?.id:
                if remoteSnapshotReady {
                    mode = .hidden
                } else {
                    mode = .progress(
                        message: String(
                            localized: "home.overlay.scanningIndex",
                            defaultValue: "Scanning backup index…"
                        ),
                        actionTitle: String(
                            localized: "home.menu.disconnect",
                            defaultValue: "Disconnect"
                        )
                    )
                }
            case .connecting, .connected:
                mode = .profileSelection
            }
        }
        libraryListViewController.applyRemoteOverlay(
            mode: mode,
            profiles: destinationProfiles,
            interactionEnabled: destinationInteractionEnabled
                && !executionIsActive
        )
        let showsConnectedAction: Bool
        if case .connected(let profile, _) =
            remoteConnectionController.state {
            showsConnectedAction =
                profile.id == selectedProfile?.id && remoteSnapshotReady
        } else {
            showsConnectedAction = false
        }
        connectionButton.isHidden = !showsConnectedAction
    }

    private func connectionProgressText(
        _ progress: RemoteSyncProgress?
    ) -> String {
        guard let progress else {
            return String(localized: "link.connection.connecting")
        }
        return RemoteSyncProgressPresentation.message(for: progress)
    }

    private func reconcileSelection() {
        let rows = currentSnapshot.sections.flatMap(\.rows)
        let localMonths = Set(
            rows.lazy.filter { $0.local != nil }.map(\.month)
        )
        let remoteMonths = Set(
            rows.lazy.filter { $0.remote != nil }.map(\.month)
        )
        _ = selectionController.intersect(
            localMonths: localMonths,
            remoteMonths: remoteMonths
        )
    }

    private func seedDemoSelectionIfNeeded() {
        #if DEBUG
        guard !didSeedDemoSelection,
              ProcessInfo.processInfo.arguments.contains("--demo-selection"),
              canSelectMonths else {
            return
        }
        didSeedDemoSelection = true
        _ = selectionController.toggleMonth(
            LibraryMonthKey(year: 2026, month: 7),
            side: .local
        )
        _ = selectionController.toggleMonth(
            LibraryMonthKey(year: 2026, month: 7),
            side: .remote
        )
        _ = selectionController.toggleMonth(
            LibraryMonthKey(year: 2026, month: 5),
            side: .remote
        )
        _ = selectionController.toggleMonth(
            LibraryMonthKey(year: 2026, month: 4),
            side: .local
        )
        #endif
    }

    private func updateSelectionPresentation() {
        guard isViewLoaded else { return }
        if !isRemoteConnected {
            selectionController.clear()
        }
        libraryListViewController.applySelection(
            selectionController.state,
            selectionEnabled: canSelectMonths,
            remoteSelectionEnabled:
                MacHomeSelectionPresentationPolicy
                    .remoteSelectionEnabled(
                        selectionEnabled: canSelectMonths,
                        remoteSnapshotReady: remoteSnapshotReady,
                        isSpecificAlbums:
                            photoLibraryIndexController.scope
                                .isSpecificAlbums
                    )
        )
        let counts = selectionController.state.counts()
        let selectedCount = counts.backup
            + counts.download
            + counts.complement
        libraryListViewController.setStartButton(
            title: String(
                localized: "common.start",
                defaultValue: "Start"
            ),
            enabled: MacHomeExecutionStartPolicy.isEnabled(
                selectionEnabled: canSelectMonths,
                hasSelection: selectedCount > 0,
                hasRemoteSelection:
                    !selectionController.state.remoteMonths.isEmpty,
                remoteSnapshotReady: remoteSnapshotReady
            )
        )
    }

    private func openPhotoBrowser(
        initialMonth: LibraryMonthKey,
        initialSide: SelectionSide
    ) {
        onOpenPhotoBrowser?(
            MacPhotoBrowserRequest(
                initialMonth: initialMonth,
                initialSide: initialSide,
                localQuery: .allAssets,
                title: String(
                    localized: "home.photoLibrary"
                ),
                monthGroupingTimeZone:
                    currentSnapshot.monthGroupingTimeZone
            )
        )
    }

    private func startSelection() {
        guard !isStartingExecution else { return }
        let counts = selectionController.state.counts()
        let selectedCount = counts.backup
            + counts.download
            + counts.complement
        guard MacHomeExecutionStartPolicy.isEnabled(
            selectionEnabled: canSelectMonths,
            hasSelection: selectedCount > 0,
            hasRemoteSelection:
                !selectionController.state.remoteMonths.isEmpty,
            remoteSnapshotReady: remoteSnapshotReady
        ) else {
            return
        }
        guard let selectedProfile,
              isRemoteConnected,
              !executionIsActive else {
            return
        }
        #if DEBUG
        guard !MacDemoPhotoLibraryPolicy.usesSyntheticLibrary(
            arguments: ProcessInfo.processInfo.arguments
        ) else {
            return
        }
        #endif

        let backupMonths = Set(
            selectionController.state.months(for: .backup)
        )
        let downloadMonths = Set(
            selectionController.state.months(for: .download)
        )
        let complementMonths = Set(
            selectionController.state.months(for: .complement)
        )
        let allMonths = backupMonths
            .union(downloadMonths)
            .union(complementMonths)
        let localAssetIDsByMonth = Dictionary(
            uniqueKeysWithValues: allMonths.map {
                (
                    $0,
                    photoLibraryIndexController.localAssetIDs(for: $0)
                )
            }
        )
        let plan = MacBackupExecutionPlan(
            backupMonths: backupMonths,
            downloadMonths: downloadMonths,
            complementMonths: complementMonths,
            localAssetIDsByMonth: localAssetIDsByMonth,
            monthGroupingTimeZone:
                currentSnapshot.monthGroupingTimeZone,
            incompleteDownloadPolicy: .skip
        )
        var lines: [String] = []
        if !backupMonths.isEmpty {
            lines.append(
                String.localizedStringWithFormat(
                    String(localized: "home.confirm.backupMonths"),
                    backupMonths.count
                )
            )
        }
        if !downloadMonths.isEmpty {
            lines.append(
                String.localizedStringWithFormat(
                    String(localized: "home.confirm.downloadMonths"),
                    downloadMonths.count
                )
            )
        }
        if !complementMonths.isEmpty {
            lines.append(
                String.localizedStringWithFormat(
                    String(
                        localized:
                            "home.confirm.complementMonths"
                    ),
                    complementMonths.count
                )
            )
        }
        let alert = NSAlert()
        alert.messageText = String(
            localized: "home.alert.confirmExecute"
        )
        alert.informativeText = lines.joined(separator: "\n")
        alert.addButton(
            withTitle: String(
                localized: "common.start",
                defaultValue: "Start"
            )
        )
        alert.addButton(
            withTitle: String(
                localized: "common.cancel",
                defaultValue: "Cancel"
            )
        )
        guard alert.runModal() == .alertFirstButtonReturn else {
            return
        }
        isStartingExecution = true
        updateSelectionPresentation()
        Task { [weak self] in
            await self?.startResolvingIncomplete(
                profileID: selectedProfile.id,
                plan: plan
            )
        }
    }

    private func startResolvingIncomplete(
        profileID: Int64?,
        plan: MacBackupExecutionPlan
    ) async {
        defer {
            isStartingExecution = false
            updateSelectionPresentation()
        }
        guard var scanPlan = revalidatedExecutionPlan(
            plan,
            incompleteDownloadPolicy: .skip
        ) else {
            return
        }

        while true {
            let scanSignature =
                scanPlan.incompleteDownloadScanSignature
            let count = await backupExecutionController
                .incompleteDownloadItemCount(for: scanPlan)
            guard !Task.isCancelled,
                  executionStartContextIsValid(
                      profileID: profileID,
                      plan: scanPlan
                  ),
                  let currentPlan = revalidatedExecutionPlan(
                      scanPlan,
                      incompleteDownloadPolicy: .skip
                  ) else {
                return
            }
            guard currentPlan.incompleteDownloadScanSignature
                    == scanSignature else {
                scanPlan = currentPlan
                continue
            }

            let policy: IncompleteDownloadPolicy
            if count == 0 {
                policy = .skip
            } else {
                guard let selected =
                        presentIncompleteDownloadPrompt(
                            count: count
                        ) else {
                    return
                }
                policy = selected
            }
            guard executionStartContextIsValid(
                      profileID: profileID,
                      plan: currentPlan
                  ),
                  let livePlan = revalidatedExecutionPlan(
                      currentPlan,
                      incompleteDownloadPolicy: policy
                  ) else {
                return
            }
            if count == 0,
               livePlan.incompleteDownloadScanSignature
                    != scanSignature {
                scanPlan = livePlan
                continue
            }
            guard backupExecutionController.start(
                profileID: profileID,
                plan: livePlan
            ) else {
                presentTaskInProgress()
                return
            }
            return
        }
    }

    private func executionStartContextIsValid(
        profileID: Int64?,
        plan: MacBackupExecutionPlan
    ) -> Bool {
        selectedProfile?.id == profileID
            && isRemoteConnected
            && MacHomeExecutionStartPolicy.remoteStateAllows(
                hasRemoteSelection:
                    !plan.downloadMonths.isEmpty
                    || !plan.complementMonths.isEmpty,
                remoteSnapshotReady: remoteSnapshotReady
            )
            && photoLibraryAccessState.canReadLibrary
            && photoLibraryIndexControllerIsReady
            && !executionIsActive
    }

    private func revalidatedExecutionPlan(
        _ plan: MacBackupExecutionPlan,
        incompleteDownloadPolicy: IncompleteDownloadPolicy
    ) -> MacBackupExecutionPlan? {
        let live = selectionController.state.revalidated(
            backup: plan.backupMonths.sorted(),
            download: plan.downloadMonths.sorted(),
            complement: plan.complementMonths.sorted()
        )
        let backupMonths = Set(live.backup)
        let downloadMonths = Set(live.download)
        let complementMonths = Set(live.complement)
        let allMonths = backupMonths
            .union(downloadMonths)
            .union(complementMonths)
        guard !allMonths.isEmpty else { return nil }

        let localAssetIDsByMonth = Dictionary(
            uniqueKeysWithValues: allMonths.map {
                (
                    $0,
                    photoLibraryIndexController.localAssetIDs(for: $0)
                )
            }
        )
        return MacBackupExecutionPlan(
            backupMonths: backupMonths,
            downloadMonths: downloadMonths,
            complementMonths: complementMonths,
            localAssetIDsByMonth: localAssetIDsByMonth,
            monthGroupingTimeZone:
                currentSnapshot.monthGroupingTimeZone,
            incompleteDownloadPolicy: incompleteDownloadPolicy
        )
    }

    private func presentIncompleteDownloadPrompt(
        count: Int
    ) -> IncompleteDownloadPolicy? {
        let alert = NSAlert()
        alert.messageText = String(
            localized: "home.incompleteDownload.title"
        )
        alert.informativeText = String.localizedStringWithFormat(
            String(localized: "home.incompleteDownload.message"),
            count
        )
        alert.addButton(
            withTitle: String(
                localized: "home.incompleteDownload.createAll"
            )
        )
        alert.addButton(
            withTitle: String(
                localized: "home.incompleteDownload.skip"
            )
        )
        alert.addButton(
            withTitle: String(
                localized: "home.incompleteDownload.abort"
            )
        )
        switch alert.runModal() {
        case .alertFirstButtonReturn:
            return .createNewAsset
        case .alertSecondButtonReturn:
            return .skip
        default:
            return nil
        }
    }

    private func presentTaskInProgress() {
        let alert = NSAlert()
        alert.messageText = String(localized: "common.error")
        alert.informativeText = String(
            localized: "mediaBrowser.action.taskInProgress"
        )
        alert.addButton(
            withTitle: String(
                localized: "common.ok",
                defaultValue: "OK"
            )
        )
        if let window = view.window {
            alert.beginSheetModal(for: window)
        } else {
            alert.runModal()
        }
    }

    private func applyBackupExecutionState(
        _ state: MacBackupExecutionState
    ) {
        guard isViewLoaded else { return }
        libraryListViewController.applyMonthExecution(
            backupExecutionController.monthExecutionPhases,
            progress: backupExecutionController.monthExecutionProgress
        )
        let status: String?
        let progress: Double?
        let canPause: Bool
        let canResume: Bool
        let canComplete: Bool
        let canStop: Bool

        switch state {
        case .idle:
            didRequestReviewForCurrentExecution = false
            status = nil
            progress = nil
            canPause = false
            canResume = false
            canComplete = false
            canStop = false
        case .preflighting(let processed, let total, _):
            status = String(
                localized: "backup.session.preparingBackup",
                defaultValue: "Preparing backup…"
            ) + " · " + String.localizedStringWithFormat(
                String(
                    localized: "home.localIndex.indexedCountValue",
                    defaultValue: "%1$lld / %2$lld"
                ),
                Int64(processed),
                Int64(total)
            )
            progress = total > 0
                ? Double(processed) / Double(total)
                : 0
            canPause = true
            canResume = false
            canComplete = false
            canStop = true
        case .uploading(let value):
            if let value {
                let processed = value.succeeded
                    + value.failed
                    + value.skipped
                status = String.localizedStringWithFormat(
                    String(
                        localized: "mac.execution.uploadProgress",
                        defaultValue: "Backing up %1$lld of %2$lld"
                    ),
                    Int64(processed),
                    Int64(value.total)
                )
                progress = value.total > 0
                    ? Double(processed) / Double(value.total)
                    : 0
            } else {
                status = String(
                    localized: "backup.session.preparingBackup",
                    defaultValue: "Preparing the backup repository…"
                )
                progress = 0
            }
            canPause = true
            canResume = false
            canComplete = false
            canStop = true
        case .downloading(
            let month,
            let itemPosition,
            let totalItems
        ):
            status = totalItems > 0
                ? String.localizedStringWithFormat(
                    String(
                        localized: "mac.execution.downloadProgress",
                        defaultValue: "Downloading %1$@ · %2$lld of %3$lld"
                    ),
                    month.displayText,
                    Int64(itemPosition),
                    Int64(totalItems)
                )
                : String(
                    localized: "home.execution.preparingDownload",
                    defaultValue: "Preparing download…"
                )
            progress = totalItems > 0
                ? Double(itemPosition) / Double(totalItems)
                : 0
            canPause = true
            canResume = false
            canComplete = false
            canStop = true
        case .pausing:
            status = String(
                localized: "backup.session.pausing",
                defaultValue: "Pausing safely…"
            )
            progress = nil
            canPause = false
            canResume = false
            canComplete = false
            canStop = false
        case .paused:
            status = String(
                localized: "backup.session.paused",
                defaultValue: "Backup paused"
            )
            progress = nil
            canPause = false
            canResume = true
            canComplete = false
            canStop = true
        case .resuming:
            status = String(
                localized: "backup.session.resuming"
            )
            progress = nil
            canPause = false
            canResume = false
            canComplete = false
            canStop = true
        case .stopping:
            status = String(
                localized: "backup.session.stopping",
                defaultValue: "Stopping safely…"
            )
            progress = nil
            canPause = false
            canResume = false
            canComplete = false
            canStop = false
        case .completed(let result):
            let uploadFailed = result.upload?.failed ?? 0
            let hasIssues = uploadFailed > 0
                || result.failedDownloadMonths > 0
                || result.skippedIncompleteCount > 0
            if !hasIssues,
               !didRequestReviewForCurrentExecution {
                didRequestReviewForCurrentExecution = true
                MacRatingPromptService.requestReviewIfEligible(in: self)
            }
            status = hasIssues
                ? String(
                    localized: "home.execution.partialFailed",
                    defaultValue: "Some months failed"
                )
                : String(
                    localized: "backup.session.completed"
                )
            progress = nil
            canPause = false
            canResume = false
            canComplete = true
            canStop = false
        case .failed(let message):
            status = message
            progress = nil
            canPause = false
            canResume = false
            canComplete = true
            canStop = false
        case .cancelled:
            status = nil
            progress = nil
            canPause = false
            canResume = false
            canComplete = false
            canStop = false
            selectionController.clear()
        }

        libraryListViewController.applyExecution(
            status: status,
            progress: progress,
            active: state.isActive,
            canPause: canPause,
            canResume: canResume,
            canComplete: canComplete,
            canStop: canStop,
            canOpenLog:
                backupExecutionController.currentSessionLogURL != nil
        )
        if !state.isActive {
            applyRemoteConnectionState(
                remoteConnectionController.state
            )
        }
        connectionButton.isEnabled = !state.isActive
        photoSourceButton.isEnabled = !state.isActive
        updateSelectionPresentation()
        performPendingPhotoLibraryReloadIfPossible()
    }

    private func performPendingPhotoLibraryReloadIfPossible() {
        guard isPhotoLibraryReloadPending,
              !executionIsActive,
              !isPhotoLibraryIndexLoading else {
            return
        }
        isPhotoLibraryReloadPending = false
        photoLibraryIndexController.reload()
    }

    private func confirmStopExecution() {
        guard backupExecutionController.state
            .acceptsStopRequest else {
            return
        }
        let alert = NSAlert()
        alert.alertStyle = .warning
        alert.messageText = String(
            localized: "home.alert.confirmStop",
            defaultValue: "Confirm Stop"
        )
        alert.informativeText = String(
            localized: "home.alert.confirmStopMessage",
            defaultValue: "You will need to re-select months after stopping."
        )
        alert.addButton(
            withTitle: String(
                localized: "common.stop",
                defaultValue: "Stop"
            )
        )
        alert.addButton(
            withTitle: String(
                localized: "common.cancel",
                defaultValue: "Cancel"
            )
        )
        let finish: (NSApplication.ModalResponse) -> Void = {
            [weak self] response in
            guard response == .alertFirstButtonReturn,
                  let self,
                  backupExecutionController.state
                    .acceptsStopRequest else {
                return
            }
            backupExecutionController.stop()
        }
        if let window = view.window {
            alert.beginSheetModal(
                for: window,
                completionHandler: finish
            )
        } else {
            finish(alert.runModal())
        }
    }

    private func updatePhotoSourceButton() {
        switch photoLibraryIndexController.scope {
        case .allPhotos:
            photoSourceButton.title = String(
                localized: "home.localSource.allPhotos",
                defaultValue: "All"
            )
        case .albums(let ids):
            if ids.count == 1,
               let id = ids.first,
               let descriptor = albumDescriptorsByID[id] {
                photoSourceButton.title = descriptor.title
            } else {
                photoSourceButton.title =
                    String.localizedStringWithFormat(
                        String(
                            localized: "home.localSource.albumCount",
                            defaultValue: "%1$lld Albums"
                        ),
                        Int64(ids.count)
                    )
            }
        }
    }

    private func promptForCredential(
        profile: ServerProfileRecord
    ) async -> String? {
        guard let window = view.window else { return nil }
        let alert = NSAlert()
        alert.messageText = String(
            localized: "mac.connection.passwordTitle",
            defaultValue: "Enter the credential for \(profile.name)"
        )
        alert.informativeText = String(
            localized: "mac.connection.passwordMessage",
            defaultValue: "The credential will be saved in your login keychain."
        )
        alert.addButton(
            withTitle: String(
                localized: "common.connect",
                defaultValue: "Connect"
            )
        )
        alert.addButton(
            withTitle: String(localized: "common.cancel", defaultValue: "Cancel")
        )
        let field = NSSecureTextField(
            frame: NSRect(x: 0, y: 0, width: 320, height: 24)
        )
        alert.accessoryView = field
        window.makeFirstResponder(field)
        return await withCheckedContinuation { continuation in
            alert.beginSheetModal(for: window) { response in
                continuation.resume(
                    returning: response == .alertFirstButtonReturn
                        ? field.stringValue
                        : nil
                )
            }
        }
    }

    private func confirmSFTPHostKey(
        profile _: ServerProfileRecord,
        decision: SFTPHostKeyPromptPolicy.Decision,
        fingerprint: String
    ) async -> Bool {
        guard let window = view.window else { return false }
        let alert = NSAlert()
        alert.alertStyle = decision == .firstTrust ? .informational : .warning
        let confirmTitle: String
        let isDestructive: Bool
        switch decision {
        case .none:
            return true
        case .firstTrust:
            alert.messageText = String(
                localized: "auth.sftp.hostKey.confirmTitle"
            )
            alert.informativeText = String.localizedStringWithFormat(
                String(localized: "auth.sftp.hostKey.confirmBody"),
                fingerprint
            )
            confirmTitle = String(
                localized: "auth.sftp.hostKey.confirmAction"
            )
            isDestructive = false
        case .changedKey(let expected):
            alert.messageText = String(
                localized: "auth.sftp.hostKey.changedTitle"
            )
            alert.informativeText = String.localizedStringWithFormat(
                String(localized: "auth.sftp.hostKey.changedBody"),
                expected,
                fingerprint
            )
            confirmTitle = String(
                localized: "auth.sftp.hostKey.changedAction"
            )
            isDestructive = true
        }
        alert.addButton(withTitle: confirmTitle)
        alert.addButton(
            withTitle: String(localized: "common.cancel", defaultValue: "Cancel")
        )
        alert.buttons.first?.hasDestructiveAction = isDestructive
        return await withCheckedContinuation { continuation in
            alert.beginSheetModal(for: window) {
                continuation.resume(
                    returning: $0 == .alertFirstButtonReturn
                )
            }
        }
    }

    @objc private func chooseDestination(_ sender: NSButton) {
        let menu = NSMenu()
        var selectedItem: NSMenuItem?
        for profile in destinationProfiles {
            guard let profileID = profile.id else { continue }
            let item = NSMenuItem(
                title: destinationMenuTitle(profile),
                action: #selector(connectDestination(_:)),
                keyEquivalent: ""
            )
            item.target = self
            item.representedObject = NSNumber(value: profileID)
            item.image = NSImage(
                systemSymbolName: profile.resolvedStorageType.symbolName,
                accessibilityDescription: nil
            )
            if profileID == selectedProfile?.id {
                item.state = .on
                selectedItem = item
            }
            menu.addItem(item)
        }
        if !destinationProfiles.isEmpty {
            menu.addItem(.separator())
        }
        if let connectedProfile =
                remoteConnectionController.state.connectedProfile,
           connectedProfile.id == selectedProfile?.id,
           let profileID = connectedProfile.id {
            let configureItem = NSMenuItem(
                title: String(
                    localized: "home.menu.configureCurrentNode",
                    defaultValue: "Configure Current Node"
                ) + "…",
                action: #selector(configureDestination(_:)),
                keyEquivalent: ""
            )
            configureItem.target = self
            configureItem.representedObject = NSNumber(value: profileID)
            configureItem.image = NSImage(
                systemSymbolName: "slider.horizontal.3",
                accessibilityDescription: nil
            )
            menu.addItem(configureItem)
        }
        let manageItem = NSMenuItem(
            title: String(
                localized: "more.item.manageStorage",
                defaultValue: "Manage Nodes"
            ) + "…",
            action: #selector(manageDestinations(_:)),
            keyEquivalent: ""
        )
        manageItem.target = self
        menu.addItem(manageItem)
        menu.popUp(
            positioning: selectedItem,
            at: NSPoint(x: 0, y: sender.bounds.height + 4),
            in: sender
        )
    }

    private func updateReachabilityProfiles(
        activeProfileID: Int64? = nil
    ) {
        let connectedProfileID =
            activeProfileID
            ?? remoteConnectionController.state.connectedProfile?.id
        profileReachabilityService.setProfiles(
            destinationProfiles,
            activeProfileID: connectedProfileID
        )
    }

    private func destinationMenuTitle(
        _ profile: ServerProfileRecord
    ) -> String {
        guard let profileID = profile.id,
              profileReachabilityService.reachability(
                  for: profileID
              ) == .unreachable else {
            return profile.name
        }
        return String(localized: "home.menu.offlineMarker")
            + profile.name
    }

    @objc private func connectDestination(_ sender: NSMenuItem) {
        guard let profileID = (
            sender.representedObject as? NSNumber
        )?.int64Value,
              let profile = destinationProfiles.first(where: {
                  $0.id == profileID
              }) else {
            return
        }
        onConnectDestination?(profile)
    }

    @objc private func configureDestination(_ sender: NSMenuItem) {
        guard let profileID = (
            sender.representedObject as? NSNumber
        )?.int64Value else {
            return
        }
        onConfigureDestination?(profileID)
    }

    private func presentConnectionFailure(
        profile: ServerProfileRecord,
        message: String
    ) {
        let alert = NSAlert()
        alert.alertStyle = .warning
        alert.messageText = String(
            localized: "home.alert.connectionFailed"
        )
        alert.informativeText = message
        let editableProfileID = profile.id
        if editableProfileID != nil {
            alert.addButton(
                withTitle: String(
                    localized: "common.edit",
                    defaultValue: "Edit"
                )
            )
        }
        let dismissButton = alert.addButton(
            withTitle: String(
                localized: "common.ok",
                defaultValue: "OK"
            )
        )
        dismissButton.keyEquivalent = "\u{1b}"
        let handleResponse: (NSApplication.ModalResponse) -> Void = {
            [weak self] response in
            guard let profileID =
                    MacConnectionFailureRecoveryPolicy.editProfileID(
                        profileID: editableProfileID,
                        response: response
                    ) else {
                return
            }
            self?.onConfigureDestination?(profileID)
        }
        if let window = view.window {
            alert.beginSheetModal(
                for: window,
                completionHandler: handleResponse
            )
        } else {
            handleResponse(alert.runModal())
        }
    }

    @objc private func manageDestinations(_ sender: Any?) {
        onManageDestinations?()
    }

    private func handlePhotoAccess() {
        if photoLibraryAccessState == .notDetermined {
            libraryListViewController.showLocalAccessRequestInProgress()
            Task {
                let state = await photoLibraryAuthorizationProvider.requestAccess()
                applyPhotoLibraryAccessState(state)
            }
            return
        }

        MacPhotoLibrarySettings.open()
    }

    @objc private func choosePhotoSource(_ sender: Any?) {
        guard !executionIsActive,
              photoLibraryAccessState.canReadLibrary
                || MacDemoPhotoLibraryPolicy.usesSyntheticLibrary(
                    arguments: ProcessInfo.processInfo.arguments
                ),
              let button = sender as? NSButton else {
            return
        }

        let menu = NSMenu()
        let allPhotos = NSMenuItem(
            title: String(
                localized: "home.localSource.allPhotos",
                defaultValue: "Entire Library"
            ),
            action: #selector(chooseEntireLibrary(_:)),
            keyEquivalent: ""
        )
        allPhotos.target = self
        allPhotos.state = photoLibraryIndexController.scope
            .isSpecificAlbums ? .off : .on
        menu.addItem(allPhotos)

        let albums = NSMenuItem(
            title: String(
                localized: "home.localSource.specificAlbums",
                defaultValue: "Specific Albums"
            ),
            action: #selector(chooseSpecificAlbums(_:)),
            keyEquivalent: ""
        )
        albums.target = self
        albums.state = photoLibraryIndexController.scope
            .isSpecificAlbums ? .on : .off
        menu.addItem(albums)
        menu.popUp(
            positioning: nil,
            at: NSPoint(x: 0, y: button.bounds.maxY + 4),
            in: button
        )
    }

    @objc private func chooseEntireLibrary(_ sender: Any?) {
        albumDescriptorsByID.removeAll()
        selectionController.clear()
        photoLibraryIndexController.setScope(.allPhotos)
        updatePhotoSourceButton()
        updateSelectionPresentation()
    }

    @objc private func chooseSpecificAlbums(_ sender: Any?) {
        guard let window = view.window else { return }
        presentAlbumPicker(in: window)
    }

    private func presentAlbumPicker(in window: NSWindow) {
        let picker = MacAlbumPickerViewController(
            photoLibraryService: photoLibraryService,
            selectedAlbumIDs:
                photoLibraryIndexController.scope
                    .selectedAlbumIdentifiers
        )
        let sheet = NSWindow(contentViewController: picker)
        sheet.title = String(
            localized: "home.localAlbums.title",
            defaultValue: "Choose Albums"
        )
        sheet.styleMask = [.titled]
        picker.onCancel = { [weak window, weak sheet] in
            guard let sheet else { return }
            window?.endSheet(sheet)
        }
        picker.onOpenAlbum = { [weak self] album in
            guard let self else { return }
            onOpenPhotoBrowser?(
                MacPhotoBrowserRequest(
                    initialMonth: nil,
                    initialSide: .local,
                    localQuery: .albums([
                        album.localIdentifier
                    ]),
                    title: album.title,
                    monthGroupingTimeZone:
                        currentSnapshot.monthGroupingTimeZone
                )
            )
        }
        picker.onApply = { [weak self, weak window, weak sheet] albums in
            guard let self, let sheet else { return }
            window?.endSheet(sheet)
            albumDescriptorsByID = Dictionary(
                uniqueKeysWithValues: albums.map {
                    ($0.localIdentifier, $0)
                }
            )
            selectionController.clear()
            photoLibraryIndexController.setScope(
                .albums(Set(albums.map(\.localIdentifier)))
            )
            updatePhotoSourceButton()
            updateSelectionPresentation()
        }
        window.beginSheet(sheet)
    }

    #if DEBUG
    func showDemoAlbumPicker() {
        guard let window = view.window else { return }
        presentAlbumPicker(in: window)
    }
    #endif

    @objc private func handleConnection(_ sender: Any?) {
        guard !executionIsActive else { return }
        switch remoteConnectionController.state {
        case .connecting, .connected:
            remoteConnectionController.disconnect()
        case .idle, .failed:
            remoteConnectionController.connectSelected()
        }
    }

}
