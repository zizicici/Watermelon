import AppKit
import AVFoundation
import AVKit
import Photos
import PhotosUI

@MainActor
final class MacPhotoBrowserViewController: NSViewController {
    var onBackUpItems: (([LibraryMonthKey: Set<String>]) -> Bool)?
    var onRemoteLibraryChanged: ((UInt64) -> Void)?
    var onLocalLibraryChanged: (() -> Void)?

    private enum Mode: Int {
        case local
        case merged
        case remote
    }

    private enum DemoPresence {
        case both
        case onMac
        case backupOnly
    }

    private struct Item: @unchecked Sendable {
        let id: String
        var month: LibraryMonthKey
        let remoteDisplayMonth: LibraryMonthKey?
        let remoteStorageMonth: LibraryMonthKey?
        let localAsset: PHAsset?
        let remote: RemoteAlbumItem?
        let fingerprint: Data?
        let fileName: String
        var creationDate: Date
        let kind: AlbumMediaKind
        let demoPresence: DemoPresence?

        var presenceText: String {
            if let demoPresence {
                switch demoPresence {
                case .both:
                    return String(
                        localized: "mac.browser.presence.both"
                    )
                case .onMac:
                    return String(
                        localized: "mac.selection.mac"
                    )
                case .backupOnly:
                    return String(
                        localized:
                            "mac.browser.filter.backupOnly"
                    )
                }
            }
            switch (localAsset != nil, remote != nil) {
            case (true, true):
                return String(
                    localized: "mac.browser.presence.both"
                )
            case (true, false):
                return String(
                    localized: "mac.selection.mac"
                )
            case (false, true):
                return String(
                    localized:
                        "mac.browser.filter.backupOnly"
                )
            case (false, false):
                return ""
            }
        }

        var isRemoteOnly: Bool {
            if let demoPresence {
                return demoPresence == .backupOnly
            }
            return localAsset == nil && remote != nil
        }

        var isOnMac: Bool {
            if let demoPresence {
                return demoPresence != .backupOnly
            }
            return localAsset != nil
        }

        var actionPresence: MediaLibraryPresence? {
            switch (localAsset != nil, remote != nil) {
            case (true, false): return .localOnly
            case (false, true): return .remoteOnly
            case (true, true): return .both
            case (false, false): return nil
            }
        }

        var badgeText: String {
            if let demoPresence {
                switch demoPresence {
                case .both:
                    return ""
                case .onMac:
                    return String(localized: "mac.selection.mac")
                case .backupOnly:
                    return String(
                        localized: "mac.browser.filter.backupOnly"
                    )
                }
            }
            if isRemoteOnly {
                return String(localized: "mac.browser.filter.backupOnly")
            }
            if remote == nil {
                return String(localized: "mac.selection.mac")
            }
            return ""
        }

        func projected(for mode: Mode) -> Item {
            guard mode != .local,
                  remote != nil,
                  let remoteDisplayMonth else {
                return self
            }
            var projected = self
            projected.month = remoteDisplayMonth
            projected.creationDate =
                remote?.creationDate ?? creationDate
            return projected
        }

        var withoutRemote: Item? {
            guard localAsset != nil else { return nil }
            return Item(
                id: id,
                month: month,
                remoteDisplayMonth: nil,
                remoteStorageMonth: nil,
                localAsset: localAsset,
                remote: nil,
                fingerprint: fingerprint,
                fileName: fileName,
                creationDate: creationDate,
                kind: kind,
                demoPresence: demoPresence
            )
        }
    }

    private struct Section {
        let month: LibraryMonthKey
        let items: [Item]
    }

    private struct LiveRemoteItem {
        let month: LibraryMonthKey
        let item: RemoteAlbumItem
    }

    private let initialMonth: LibraryMonthKey?
    private let initialMode: Mode
    private let localQuery: PhotoLibraryQuery
    private let allowsRemoteModes: Bool
    private let monthGroupingCalendar: Calendar
    private var profile: ServerProfileRecord?
    private var credential: String?
    private let photoLibraryService: PhotoLibraryService
    private let backupCoordinator: BackupCoordinator
    private let hashIndexRepository: ContentHashIndexRepository
    private let downloadWorkflowHelper: DownloadWorkflowHelper
    private let storageClientFactory: StorageClientFactory
    private let appSession: AppSession
    private let appRuntimeFlags: AppRuntimeFlags
    private var sessionGeneration: UInt64
    private let collectionView = NSCollectionView()
    private let flowLayout = NSCollectionViewFlowLayout()
    private let filterControl = NSSegmentedControl(
        labels: [
            String(localized: "mediaBrowser.mode.local"),
            String(localized: "mediaBrowser.mode.merged"),
            String(localized: "mediaBrowser.mode.remote")
        ],
        trackingMode: .selectOne,
        target: nil,
        action: nil
    )
    private let zoomSlider = NSSlider()
    private let selectionBar = NSStackView()
    private let selectionSummaryLabel =
        NSTextField(labelWithString: "")
    private let batchBackupButton = NSButton()
    private let batchRestoreButton = NSButton()
    private let batchDeleteButton = NSButton()
    private let previewImageView = NSImageView()
    private let previewPlayerView = AVPlayerView()
    private let previewLivePhotoView = PHLivePhotoView()
    private let previewTitle = NSTextField(labelWithString: "")
    private let previewMetadata = NSTextField(wrappingLabelWithString: "")
    private let previewStatus = NSTextField(wrappingLabelWithString: "")
    private let previewProgress = NSProgressIndicator()
    private let previewActionButton = NSButton()
    private let infoButton = NSButton()
    private let shareButton = NSButton()
    private let deleteLocalButton = NSButton()
    private let deleteRemoteButton = NSButton()
    private weak var previewPane: NSView?
    private var allItems: [Item] = []
    private var visibleSections: [Section] = []
    private var thumbnailCache: [String: NSImage] = [:]
    private var remoteThumbnailData: [Data: Data] = [:]
    private var thumbnailTasks: [IndexPath: Task<Void, Never>] = [:]
    private var previewTask: Task<Void, Never>?
    private var remoteThumbnailLoader: MacRemoteThumbnailLoader?
    private var libraryLoadTask: Task<Void, Never>?
    private var actionTask: Task<Void, Never>?
    private var previewTemporaryURL: URL?
    private var previewLivePhotoTemporaryURLs: [URL] = []
    private var sharingTemporaryURLs: [URL] = []
    private var sharingServicePicker: NSSharingServicePicker?
    private var isRunningAction = false
    private var closeWhenActionFinishes = false
    private var pendingPhotoLibraryReload = false
    private var pendingLocalProjectionInvalidation = false
    private var photoLibraryAccessState: PhotoLibraryAccessState
    private var didScrollToInitialMonth = false
    private var libraryLoadGeneration: UInt64 = 0
    private var displayedSessionGeneration: UInt64?
    nonisolated(unsafe) private var executionLifecycleObserver: NSObjectProtocol?
    nonisolated(unsafe) private var sessionObserver: NSObjectProtocol?
    nonisolated(unsafe) private var remoteSnapshotObserver: NSObjectProtocol?
    nonisolated(unsafe) private var activationObserver: NSObjectProtocol?
    #if DEBUG
    private let demoMode: Bool
    #endif

    init(
        request: MacPhotoBrowserRequest,
        photoLibraryService: PhotoLibraryService,
        backupCoordinator: BackupCoordinator,
        hashIndexRepository: ContentHashIndexRepository,
        downloadWorkflowHelper: DownloadWorkflowHelper,
        storageClientFactory: StorageClientFactory,
        appSession: AppSession,
        appRuntimeFlags: AppRuntimeFlags
    ) {
        initialMonth = request.initialMonth
        localQuery = request.localQuery
        allowsRemoteModes = request.localQuery == .allAssets
        initialMode = request.localQuery == .allAssets
            && request.initialSide == .remote
            ? .remote
            : .local
        monthGroupingCalendar = LibraryMonthKey.monthCalendar(
            preference: request.monthGroupingTimeZone
        )
        let sessionState = MacPhotoBrowserSessionState(
            snapshot: appSession.snapshot
        )
        self.profile = sessionState.profile
        self.credential = sessionState.credential
        self.photoLibraryService = photoLibraryService
        self.backupCoordinator = backupCoordinator
        self.hashIndexRepository = hashIndexRepository
        self.downloadWorkflowHelper = downloadWorkflowHelper
        self.storageClientFactory = storageClientFactory
        self.appSession = appSession
        self.appRuntimeFlags = appRuntimeFlags
        self.sessionGeneration = sessionState.generation
        self.photoLibraryAccessState = PhotoLibraryAccessState(
            photoLibraryService.authorizationStatus()
        )
        if let profile = sessionState.profile,
           let credential = sessionState.credential {
            self.remoteThumbnailLoader = MacRemoteThumbnailLoader(
                profile: profile,
                credential: credential,
                storageClientFactory: storageClientFactory
            )
        }
        #if DEBUG
        self.demoMode = ProcessInfo.processInfo.arguments.contains {
            $0 == "--demo-photo-browser"
                || $0 == "--demo-photo-metadata"
        }
        #endif
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        if let executionLifecycleObserver {
            NotificationCenter.default.removeObserver(
                executionLifecycleObserver
            )
        }
        if let sessionObserver {
            NotificationCenter.default.removeObserver(sessionObserver)
        }
        if let remoteSnapshotObserver {
            NotificationCenter.default.removeObserver(
                remoteSnapshotObserver
            )
        }
        if let activationObserver {
            NotificationCenter.default.removeObserver(activationObserver)
        }
        thumbnailTasks.values.forEach { $0.cancel() }
        previewTask?.cancel()
        libraryLoadTask?.cancel()
        actionTask?.cancel()
        if let remoteThumbnailLoader {
            Task {
                await remoteThumbnailLoader.disconnect()
            }
        }
        if let previewTemporaryURL {
            try? FileManager.default.removeItem(
                at: previewTemporaryURL
            )
        }
        for url in previewLivePhotoTemporaryURLs {
            try? FileManager.default.removeItem(at: url)
        }
        for url in sharingTemporaryURLs {
            try? FileManager.default.removeItem(at: url)
        }
        PHPhotoLibrary.shared().unregisterChangeObserver(self)
    }

    override func loadView() {
        view = NSView()
        executionLifecycleObserver = NotificationCenter.default
            .addObserver(
                forName: .ExecutionLifecycleDidChange,
                object: nil,
                queue: .main
            ) { [weak self] _ in
                MainActor.assumeIsolated {
                    self?.executionLifecycleDidChange()
                }
            }
        sessionObserver = NotificationCenter.default.addObserver(
            forName: .AppSessionChanged,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            MainActor.assumeIsolated {
                self?.sessionDidChange()
            }
        }
        remoteSnapshotObserver = NotificationCenter.default
            .addObserver(
                forName: .RemoteLibrarySnapshotDidChange,
                object: nil,
                queue: .main
            ) { [weak self] _ in
                MainActor.assumeIsolated {
                    self?.remoteSnapshotDidChange()
                }
            }
        activationObserver = NotificationCenter.default.addObserver(
            forName: NSApplication.didBecomeActiveNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            MainActor.assumeIsolated {
                self?.applicationDidBecomeActive()
            }
        }
        PHPhotoLibrary.shared().register(self)

        let hasRemoteSession = refreshRemoteModeAvailability()
        filterControl.selectedSegment =
            hasRemoteSession ? initialMode.rawValue : Mode.local.rawValue
        filterControl.isHidden = !allowsRemoteModes
        filterControl.target = self
        filterControl.action = #selector(changeFilter(_:))

        let smallSymbol = NSImageView(
            image: NSImage(
                systemSymbolName: "square.grid.3x3.fill",
                accessibilityDescription: nil
            ) ?? NSImage()
        )
        let largeSymbol = NSImageView(
            image: NSImage(
                systemSymbolName: "square.grid.2x2",
                accessibilityDescription: nil
            ) ?? NSImage()
        )
        smallSymbol.contentTintColor = .secondaryLabelColor
        largeSymbol.contentTintColor = .secondaryLabelColor
        zoomSlider.minValue = 100
        zoomSlider.maxValue = 230
        zoomSlider.doubleValue = 150
        zoomSlider.target = self
        zoomSlider.action = #selector(changeZoom(_:))

        let zoomStack = NSStackView(
            views: [smallSymbol, zoomSlider, largeSymbol]
        )
        zoomStack.orientation = .horizontal
        zoomStack.alignment = .centerY
        zoomStack.spacing = 7

        let toolbar = NSStackView(
            views: [
                NSView(),
                filterControl,
                zoomStack
            ]
        )
        toolbar.orientation = .horizontal
        toolbar.alignment = .centerY
        toolbar.spacing = 14
        configureSelectionBar()

        configureCollectionView()
        let collectionScroll = NSScrollView()
        collectionScroll.documentView = collectionView
        collectionScroll.hasVerticalScroller = true
        collectionScroll.autohidesScrollers = true
        collectionScroll.drawsBackground = false

        let preview = makePreviewPane()
        previewPane = preview
        let content = NSStackView(views: [collectionScroll, preview])
        content.orientation = .horizontal
        content.alignment = .height
        content.spacing = 16

        let root = NSStackView(
            views: [toolbar, selectionBar, content]
        )
        root.orientation = .vertical
        root.alignment = .width
        root.spacing = 14
        root.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(root)

        NSLayoutConstraint.activate([
            root.topAnchor.constraint(equalTo: view.topAnchor, constant: 22),
            root.leadingAnchor.constraint(
                equalTo: view.leadingAnchor,
                constant: 22
            ),
            root.trailingAnchor.constraint(
                equalTo: view.trailingAnchor,
                constant: -22
            ),
            root.bottomAnchor.constraint(
                equalTo: view.bottomAnchor,
                constant: -22
            ),
            content.heightAnchor.constraint(
                greaterThanOrEqualToConstant: 480
            ),
            collectionScroll.widthAnchor.constraint(
                greaterThanOrEqualToConstant: 560
            ),
            preview.widthAnchor.constraint(equalToConstant: 320),
            zoomSlider.widthAnchor.constraint(equalToConstant: 110)
        ])

        showEmptyPreview()
        loadItems()
    }

    override func viewDidAppear() {
        super.viewDidAppear()
        view.window?.delegate = self
    }

    private func configureSelectionBar() {
        selectionSummaryLabel.font =
            .systemFont(ofSize: 12, weight: .medium)
        selectionSummaryLabel.textColor = .secondaryLabelColor

        batchBackupButton.title = String(
            localized: "panel.backup",
            defaultValue: "Back Up"
        )
        batchBackupButton.bezelStyle = .rounded
        batchBackupButton.target = self
        batchBackupButton.action = #selector(backUpSelection(_:))

        batchRestoreButton.title = String(
            localized: "mediaBrowser.action.download",
            defaultValue: "Download"
        )
        batchRestoreButton.bezelStyle = .rounded
        batchRestoreButton.target = self
        batchRestoreButton.action = #selector(restoreSelection(_:))

        batchDeleteButton.title = String(
            localized: "common.delete",
            defaultValue: "Delete"
        ) + "…"
        batchDeleteButton.bezelStyle = .rounded
        batchDeleteButton.contentTintColor = .wmMaterialError
        batchDeleteButton.target = self
        batchDeleteButton.action = #selector(deleteSelectedItems(_:))

        selectionBar.setViews(
            [
                selectionSummaryLabel,
                NSView(),
                batchBackupButton,
                batchRestoreButton,
                batchDeleteButton
            ],
            in: .leading
        )
        selectionBar.orientation = .horizontal
        selectionBar.alignment = .centerY
        selectionBar.spacing = 9
        selectionBar.edgeInsets = NSEdgeInsets(
            top: 7,
            left: 10,
            bottom: 7,
            right: 10
        )
        selectionBar.wantsLayer = true
        selectionBar.layer?.backgroundColor =
            NSColor.controlBackgroundColor.cgColor
        selectionBar.layer?.cornerRadius = 8
        selectionBar.isHidden = true
    }

    private func configureCollectionView() {
        flowLayout.itemSize = NSSize(width: 150, height: 150)
        flowLayout.minimumInteritemSpacing = 12
        flowLayout.minimumLineSpacing = 14
        flowLayout.sectionInset = NSEdgeInsets(
            top: 4,
            left: 4,
            bottom: 16,
            right: 4
        )
        flowLayout.headerReferenceSize = NSSize(
            width: 0,
            height: 34
        )
        collectionView.collectionViewLayout = flowLayout
        collectionView.isSelectable = true
        collectionView.allowsMultipleSelection = true
        collectionView.backgroundColors = [.clear]
        collectionView.dataSource = self
        collectionView.delegate = self
        collectionView.register(
            MacPhotoBrowserCollectionItem.self,
            forItemWithIdentifier:
                MacPhotoBrowserCollectionItem.identifier
        )
        collectionView.register(
            MacPhotoBrowserMonthHeaderView.self,
            forSupplementaryViewOfKind:
                NSCollectionView.elementKindSectionHeader,
            withIdentifier:
                MacPhotoBrowserMonthHeaderView.identifier
        )
    }

    private func makePreviewPane() -> NSView {
        let pane = NSView()
        pane.wantsLayer = true
        pane.layer?.backgroundColor =
            NSColor.controlBackgroundColor.cgColor
        pane.layer?.borderColor = NSColor.separatorColor.cgColor
        pane.layer?.borderWidth = 1
        pane.layer?.cornerRadius = 10

        previewImageView.imageScaling = .scaleProportionallyUpOrDown
        previewImageView.wantsLayer = true
        previewImageView.layer?.backgroundColor =
            NSColor.windowBackgroundColor.cgColor
        previewImageView.layer?.cornerRadius = 8
        previewImageView.layer?.masksToBounds = true

        previewPlayerView.controlsStyle = .inline
        previewPlayerView.videoGravity = .resizeAspect
        previewPlayerView.isHidden = true
        previewPlayerView.wantsLayer = true
        previewPlayerView.layer?.backgroundColor = NSColor.black.cgColor
        previewPlayerView.layer?.cornerRadius = 8
        previewPlayerView.layer?.masksToBounds = true

        previewLivePhotoView.contentMode = .aspectFit
        previewLivePhotoView.isMuted = false
        previewLivePhotoView.isHidden = true
        previewLivePhotoView.wantsLayer = true
        previewLivePhotoView.layer?.backgroundColor =
            NSColor.windowBackgroundColor.cgColor
        previewLivePhotoView.layer?.cornerRadius = 8
        previewLivePhotoView.layer?.masksToBounds = true

        previewTitle.font = .systemFont(ofSize: 16, weight: .semibold)
        previewTitle.lineBreakMode = .byTruncatingMiddle
        previewTitle.alignment = .center

        previewMetadata.font = .systemFont(ofSize: 12)
        previewMetadata.textColor = .secondaryLabelColor
        previewMetadata.alignment = .center

        previewStatus.font = .systemFont(ofSize: 12)
        previewStatus.textColor = .tertiaryLabelColor
        previewStatus.alignment = .center

        previewProgress.style = .spinning
        previewProgress.controlSize = .small
        previewProgress.isDisplayedWhenStopped = false

        previewActionButton.bezelStyle = .rounded
        previewActionButton.target = self
        previewActionButton.action = #selector(runPreviewAction(_:))
        previewActionButton.isHidden = true

        infoButton.title = String(
            localized: "mediaBrowser.info.title"
        )
        infoButton.image = NSImage(
            systemSymbolName: "info.circle",
            accessibilityDescription: nil
        )
        infoButton.imagePosition = .imageLeading
        infoButton.bezelStyle = .rounded
        infoButton.target = self
        infoButton.action = #selector(showMetadata(_:))
        infoButton.isHidden = true

        shareButton.title = String(
            localized: "mediaBrowser.action.share",
            defaultValue: "Share"
        ) + "…"
        shareButton.image = NSImage(
            systemSymbolName: "square.and.arrow.up",
            accessibilityDescription: nil
        )
        shareButton.imagePosition = .imageLeading
        shareButton.bezelStyle = .rounded
        shareButton.target = self
        shareButton.action = #selector(shareSelectedItem(_:))
        shareButton.isHidden = true

        deleteLocalButton.title = String(
            localized: "common.delete",
            defaultValue: "Delete"
        ) + "…"
        deleteLocalButton.bezelStyle = .rounded
        deleteLocalButton.contentTintColor = .wmMaterialError
        deleteLocalButton.target = self
        deleteLocalButton.action = #selector(showPreviewDeleteMenu(_:))
        deleteLocalButton.isHidden = true

        let secondaryActions = NSStackView(
            views: [infoButton, shareButton]
        )
        secondaryActions.orientation = .horizontal
        secondaryActions.alignment = .centerY
        secondaryActions.spacing = 8

        let destructiveActions = NSStackView(
            views: [deleteLocalButton]
        )
        destructiveActions.orientation = .horizontal
        destructiveActions.alignment = .centerY
        destructiveActions.spacing = 8

        let stack = NSStackView(
            views: [
                previewImageView,
                previewPlayerView,
                previewLivePhotoView,
                previewProgress,
                previewTitle,
                previewMetadata,
                previewStatus,
                previewActionButton,
                secondaryActions,
                destructiveActions,
                NSView()
            ]
        )
        stack.orientation = .vertical
        stack.alignment = .centerX
        stack.spacing = 10
        stack.translatesAutoresizingMaskIntoConstraints = false
        pane.addSubview(stack)

        NSLayoutConstraint.activate([
            stack.topAnchor.constraint(
                equalTo: pane.topAnchor,
                constant: 18
            ),
            stack.leadingAnchor.constraint(
                equalTo: pane.leadingAnchor,
                constant: 18
            ),
            stack.trailingAnchor.constraint(
                equalTo: pane.trailingAnchor,
                constant: -18
            ),
            stack.bottomAnchor.constraint(
                equalTo: pane.bottomAnchor,
                constant: -18
            ),
            previewImageView.widthAnchor.constraint(
                equalTo: stack.widthAnchor
            ),
            previewImageView.heightAnchor.constraint(
                equalTo: previewImageView.widthAnchor
            ),
            previewPlayerView.widthAnchor.constraint(
                equalTo: stack.widthAnchor
            ),
            previewPlayerView.heightAnchor.constraint(
                equalTo: previewPlayerView.widthAnchor
            ),
            previewLivePhotoView.widthAnchor.constraint(
                equalTo: stack.widthAnchor
            ),
            previewLivePhotoView.heightAnchor.constraint(
                equalTo: previewLivePhotoView.widthAnchor
            ),
            previewTitle.widthAnchor.constraint(
                equalTo: stack.widthAnchor
            ),
            previewMetadata.widthAnchor.constraint(
                equalTo: stack.widthAnchor
            ),
            previewStatus.widthAnchor.constraint(
                equalTo: stack.widthAnchor
            )
        ])
        return pane
    }

    private func loadItems(
        selectingLocalIdentifiers:
            Set<String> = []
    ) {
        #if DEBUG
        if demoMode {
            let demoMonth = initialMonth ?? LibraryMonthKey.from(
                date: Date(),
                calendar: monthGroupingCalendar
            )
            allItems = Self.demoMonths(endingAt: demoMonth)
                .flatMap(Self.demoItems(month:))
            for (index, item) in allItems.enumerated() {
                thumbnailCache[item.id] = Self.demoImage(
                    index: index,
                    kind: item.kind
                )
            }
            applyMode()
            return
        }
        #endif

        libraryLoadTask?.cancel()
        libraryLoadGeneration &+= 1
        let generation = libraryLoadGeneration
        let contentSessionGeneration = sessionGeneration
        let remoteDeltas = Array(
            currentRemoteDeltasByMonth().values
        )
        let photoLibraryService = photoLibraryService
        let hashIndexRepository = hashIndexRepository
        let monthGroupingCalendar = monthGroupingCalendar
        let localQuery = localQuery
        libraryLoadTask = Task { [weak self] in
            let items = await withCancellableDetachedValue(
                priority: .userInitiated
            ) {
                Self.buildItems(
                    photoLibraryService: photoLibraryService,
                    hashIndexRepository: hashIndexRepository,
                    localQuery: localQuery,
                    remoteDeltas: remoteDeltas,
                    monthGroupingCalendar:
                        monthGroupingCalendar
                )
            }
            guard !Task.isCancelled,
                  let self,
                  libraryLoadGeneration == generation else {
                return
            }
            libraryLoadTask = nil
            allItems = items
            displayedSessionGeneration = contentSessionGeneration
            applyMode()
            if !selectingLocalIdentifiers.isEmpty {
                selectItems(
                    localIdentifiers:
                        selectingLocalIdentifiers
                )
            }
        }
    }

    private nonisolated static func buildItems(
        photoLibraryService: PhotoLibraryService,
        hashIndexRepository: ContentHashIndexRepository,
        localQuery: PhotoLibraryQuery,
        remoteDeltas: [RemoteLibraryMonthDelta],
        monthGroupingCalendar: Calendar
    ) -> [Item] {
        let localAssets = photoLibraryService.fetchAssets(
            for: localQuery,
            shouldCancel: { Task.isCancelled }
        )
        guard !Task.isCancelled else { return [] }
        let localAssetIDs = Set(
            localAssets.map(\.localIdentifier)
        )
        let fingerprintRecords = (
            try? hashIndexRepository.fetchAssetFingerprintRecords(
                assetIDs: localAssetIDs
            )
        ) ?? [:]
        guard !Task.isCancelled else { return [] }

        var remoteByFingerprint:
            [Data: (LibraryMonthKey, RemoteAlbumItem)] = [:]
        for delta in remoteDeltas.sorted(
            by: { $0.month > $1.month }
        ) {
            guard !Task.isCancelled else { return [] }
            let remoteItems = HomeAlbumMatching.buildRemoteItems(
                assets: delta.assets,
                resources: delta.resources,
                links: delta.assetResourceLinks
            )
            for remote in remoteItems.sorted(by: { $0.id < $1.id })
                where remoteByFingerprint[remote.assetFingerprint] == nil {
                remoteByFingerprint[remote.assetFingerprint] =
                    (delta.month, remote)
            }
        }

        var items: [Item] = []
        items.reserveCapacity(
            localAssets.count + remoteByFingerprint.count
        )
        var usedRemoteFingerprints = Set<Data>()
        for asset in localAssets {
            guard !Task.isCancelled else { return [] }
            let id = asset.localIdentifier
            let record = fingerprintRecords[id]
            let fingerprint: Data? = record.flatMap {
                if let modified = asset.modificationDate,
                   modified > $0.updatedAt {
                    return nil
                }
                return $0.fingerprint
            }
            let remoteEntry = fingerprint.flatMap {
                remoteByFingerprint[$0]
            }
            if let fingerprint, remoteEntry != nil {
                usedRemoteFingerprints.insert(fingerprint)
            }
            let creationDate = LibraryCreationDate.normalized(
                asset.creationDate
            ).date
            items.append(
                Item(
                    id: "local:\(id)",
                    month: LibraryMonthKey.from(
                        date: creationDate,
                        calendar: monthGroupingCalendar
                    ),
                    remoteDisplayMonth: remoteEntry.map {
                        LibraryMonthKey.from(
                            date: $0.1.creationDate,
                            calendar: monthGroupingCalendar
                        )
                    },
                    remoteStorageMonth: remoteEntry?.0,
                    localAsset: asset,
                    remote: remoteEntry?.1,
                    fingerprint: fingerprint,
                    fileName: Self.fileName(for: asset),
                    creationDate: creationDate,
                    kind: Self.kind(for: asset),
                    demoPresence: nil
                )
            )
        }
        for (fingerprint, entry) in remoteByFingerprint
            where !usedRemoteFingerprints.contains(fingerprint) {
            guard !Task.isCancelled else { return [] }
            let (storageMonth, remote) = entry
            items.append(
                Item(
                    id: "remote:\(storageMonth.text):\(remote.id)",
                    month: LibraryMonthKey.from(
                        date: remote.creationDate,
                        calendar: monthGroupingCalendar
                    ),
                    remoteDisplayMonth: LibraryMonthKey.from(
                        date: remote.creationDate,
                        calendar: monthGroupingCalendar
                    ),
                    remoteStorageMonth: storageMonth,
                    localAsset: nil,
                    remote: remote,
                    fingerprint: fingerprint,
                    fileName: remote.representative.fileName,
                    creationDate: remote.creationDate,
                    kind: remote.mediaKind,
                    demoPresence: nil
                )
            )
        }
        return items.sorted {
            if $0.month != $1.month {
                return $0.month > $1.month
            }
            if $0.creationDate != $1.creationDate {
                return $0.creationDate > $1.creationDate
            }
            return $0.id < $1.id
        }
    }

    @discardableResult
    private func refreshRemoteModeAvailability() -> Bool {
        let hasRemoteSession = allowsRemoteModes
            && profile != nil
            && credential != nil
        filterControl.setEnabled(
            hasRemoteSession,
            forSegment: Mode.merged.rawValue
        )
        filterControl.setEnabled(
            hasRemoteSession,
            forSegment: Mode.remote.rawValue
        )
        return hasRemoteSession
    }

    private func currentRemoteDeltasByMonth()
        -> [LibraryMonthKey: RemoteLibraryMonthDelta] {
        guard let profile, credential != nil else { return [:] }
        let state = backupCoordinator.currentRemoteSnapshotState(
            since: nil
        )
        guard state.profileKey
                == RemoteIndexSyncService.remoteProfileKey(profile)
        else {
            return [:]
        }
        return Dictionary(
            uniqueKeysWithValues: state.monthDeltas.map {
                ($0.month, $0)
            }
        )
    }

    private func currentRemoteItemsByFingerprint(
        expectedProfileKey: String,
        fingerprints: Set<Data>
    ) -> [Data: LiveRemoteItem]? {
        guard !fingerprints.isEmpty else { return [:] }
        let state = backupCoordinator.currentRemoteSnapshotState(
            since: nil
        )
        guard RemoteSnapshotOwnership.matches(
            ownerProfileKey: state.profileKey,
            expectedProfileKey: expectedProfileKey
        ) else {
            return nil
        }
        var result: [Data: LiveRemoteItem] = [:]
        for delta in state.monthDeltas.sorted(
            by: { $0.month > $1.month }
        ) {
            let assets = delta.assets.filter {
                fingerprints.contains($0.assetFingerprint)
                    && result[$0.assetFingerprint] == nil
            }
            guard !assets.isEmpty else { continue }
            let assetIDs = Set(assets.map(\.id))
            let links = delta.assetResourceLinks.filter {
                assetIDs.contains($0.assetID)
            }
            let hashes = Set(links.map(\.resourceHash))
            for item in HomeAlbumMatching.buildRemoteItems(
                assets: assets,
                resources: delta.resources.filter {
                    hashes.contains($0.contentHash)
                },
                links: links
            ).sorted(by: { $0.id < $1.id })
            where result[item.assetFingerprint] == nil {
                result[item.assetFingerprint] = LiveRemoteItem(
                    month: delta.month,
                    item: item
                )
            }
            if result.count == fingerprints.count { break }
        }
        return result
    }

    private func localFingerprintIsCurrent(
        asset: PHAsset,
        expected: Data
    ) -> Bool {
        guard let record = try? hashIndexRepository
            .fetchAssetFingerprintRecords(
                assetIDs: [asset.localIdentifier]
            )[asset.localIdentifier],
              record.fingerprint == expected else {
            return false
        }
        if let modificationDate = asset.modificationDate,
           modificationDate > record.updatedAt {
            return false
        }
        return true
    }

    private func sessionDidChange() {
        let snapshot = appSession.snapshot
        guard snapshot.generation != sessionGeneration else { return }

        let previousProfileKey = profile.map {
            RemoteIndexSyncService.remoteProfileKey($0)
        }
        let sessionState = MacPhotoBrowserSessionState(
            snapshot: snapshot
        )
        profile = sessionState.profile
        credential = sessionState.credential
        sessionGeneration = sessionState.generation
        let activeProfileKey = profile.map {
            RemoteIndexSyncService.remoteProfileKey($0)
        }

        thumbnailTasks.values.forEach { $0.cancel() }
        thumbnailTasks.removeAll()
        previewTask?.cancel()
        previewTask = nil
        replaceRemoteThumbnailLoader()
        remoteThumbnailData.removeAll()
        thumbnailCache.removeAll()

        let hasRemoteSession = refreshRemoteModeAvailability()
        if !hasRemoteSession,
           filterControl.selectedSegment != Mode.local.rawValue {
            filterControl.selectedSegment = Mode.local.rawValue
        }
        if previousProfileKey != activeProfileKey {
            displayedSessionGeneration = nil
            allItems = allItems.compactMap(\.withoutRemote)
            applyMode()
        } else {
            updateSelectionPresentation()
            if let selectedItem {
                configurePreviewAction(selectedItem)
            }
        }

        loadItems()
    }

    private func applyMode() {
        let mode = selectedMode
        let visibleItems: [Item]
        switch mode {
        case .local:
            visibleItems = allItems
                .filter { $0.localAsset != nil }
                .map { $0.projected(for: mode) }
        case .merged:
            visibleItems = allItems.map {
                $0.projected(for: mode)
            }
        case .remote:
            visibleItems = allItems
                .filter { $0.remote != nil }
                .map { $0.projected(for: mode) }
        }
        let sortedItems = visibleItems.sorted {
            if $0.month != $1.month {
                return $0.month > $1.month
            }
            if $0.creationDate != $1.creationDate {
                return $0.creationDate > $1.creationDate
            }
            return $0.id < $1.id
        }
        visibleSections = Dictionary(
            grouping: sortedItems,
            by: \.month
        )
        .map { Section(month: $0.key, items: $0.value) }
        .sorted { $0.month > $1.month }
        thumbnailTasks.values.forEach { $0.cancel() }
        thumbnailTasks.removeAll()
        collectionView.reloadData()
        collectionView.deselectAll(nil)
        updateSelectionPresentation()
        showEmptyPreview()
        scrollToInitialMonthIfNeeded()
        #if DEBUG
        if demoMode, !visibleSections.isEmpty {
            let selectionCount = ProcessInfo.processInfo.arguments
                .contains("--demo-browser-selection")
                ? min(3, visibleSections[0].items.count)
                : 1
            let paths = Set((0 ..< selectionCount).map {
                IndexPath(item: $0, section: 0)
            })
            collectionView.selectItems(
                at: paths,
                scrollPosition: []
            )
            updateSelectionPresentation()
            if selectionCount == 1,
               let first = paths.first {
                showPreview(for: first)
                if ProcessInfo.processInfo.arguments.contains(
                    "--demo-photo-metadata"
                ) {
                    DispatchQueue.main.async { [weak self] in
                        self?.showMetadata(nil)
                    }
                }
            }
        }
        #endif
    }

    private var selectedMode: Mode {
        Mode(rawValue: filterControl.selectedSegment) ?? .local
    }

    private func scrollToInitialMonthIfNeeded() {
        guard !didScrollToInitialMonth,
              let initialMonth,
              let section = visibleSections.firstIndex(
                where: { $0.month == initialMonth }
              ),
              !visibleSections[section].items.isEmpty else {
            return
        }
        didScrollToInitialMonth = true
        DispatchQueue.main.async { [weak self] in
            self?.collectionView.scrollToItems(
                at: [IndexPath(item: 0, section: section)],
                scrollPosition: .top
            )
        }
    }

    private func updateSelectionPresentation() {
        let items = selectedItems
        let canMutate = !isRunningAction
            && !appRuntimeFlags.isExecuting
        selectionBar.isHidden = items.count < 2
        selectionSummaryLabel.stringValue = String(
            localized: "mac.browser.selectedCount",
            defaultValue: "\(items.count) selected"
        )
        let remoteReady = displayedRemoteSessionIsCurrent
        let batchSummary = MediaLibraryActionPolicy.batchSummary(
            for: items.compactMap { item in
                item.actionPresence.map {
                    MediaLibraryBatchItem(
                        presence: $0,
                        canDeleteLocal: item.localAsset != nil,
                        canDeleteRemote: selectedMode != .local
                            && item.remote != nil
                    )
                }
            }
        )
        let showsBackup = batchSummary.showsUpload
            && remoteReady
            && onBackUpItems != nil
        let showsRestore = batchSummary.showsDownload
            && remoteReady
        batchBackupButton.isHidden = !showsBackup
        batchRestoreButton.isHidden = !showsRestore
        batchBackupButton.isEnabled =
            canMutate && showsBackup
        batchRestoreButton.isEnabled =
            canMutate && showsRestore
        batchDeleteButton.isEnabled =
            canMutate && batchSummary.showsDelete

        if items.count > 1 {
            previewPane?.isHidden = true
            showMultipleSelectionPreview()
        } else {
            previewPane?.isHidden = false
        }
    }

    private func showMultipleSelectionPreview() {
        previewTask?.cancel()
        clearPreviewTemporaryFile()
        previewPlayerView.player?.pause()
        previewPlayerView.player = nil
        previewPlayerView.isHidden = true
        previewLivePhotoView.stopPlayback()
        previewLivePhotoView.livePhoto = nil
        previewLivePhotoView.isHidden = true
        previewImageView.isHidden = false
        previewImageView.image = nil
        previewTitle.stringValue = ""
        previewMetadata.stringValue = ""
        previewStatus.stringValue = ""
        previewProgress.stopAnimation(nil)
        previewActionButton.isHidden = true
        infoButton.isHidden = true
        shareButton.isHidden = true
        deleteLocalButton.isHidden = true
        deleteRemoteButton.isHidden = true
    }

    @objc private func backUpSelection(_ sender: Any?) {
        let items = selectedItems.compactMap {
            item -> (LibraryMonthKey, String)? in
                guard item.remote == nil else { return nil }
                guard let identifier =
                    item.localAsset?.localIdentifier else {
                    return nil
                }
                return (item.month, identifier)
        }
        let identifiersByMonth = Dictionary(
            grouping: items,
            by: \.0
        ).mapValues { Set($0.map(\.1)) }
        guard !identifiersByMonth.isEmpty else { return }
        guard onBackUpItems?(identifiersByMonth) == true else { return }
        view.window?.close()
    }

    @objc private func restoreSelection(_ sender: Any?) {
        let items = selectedItems.filter(\.isRemoteOnly)
        guard !items.isEmpty else { return }
        restore(items: items)
    }

    @objc private func deleteSelectedItems(_ sender: Any?) {
        guard !isRunningAction else { return }
        let items = selectedItems
        let localItems = items.filter { $0.localAsset != nil }
        var seenFingerprints = Set<Data>()
        let remoteItems: [(LibraryMonthKey, RemoteAlbumItem, String?)] =
            selectedMode == .local
                ? []
                : items.compactMap { item in
                    guard let remote = item.remote,
                          let month = item.remoteStorageMonth,
                          seenFingerprints.insert(
                              remote.assetFingerprint
                          ).inserted else {
                        return nil
                    }
                    return (
                        month,
                        remote,
                        item.localAsset?.localIdentifier
                    )
                }
        guard !localItems.isEmpty || !remoteItems.isEmpty else {
            return
        }
        let capturedProfile = profile
        let capturedCredential = credential
        let capturedGeneration = sessionGeneration
        let expectedProfileKey = capturedProfile.map {
            RemoteIndexSyncService.remoteProfileKey($0)
        }
        if !remoteItems.isEmpty {
            let lines = [
                String.localizedStringWithFormat(
                    String(
                        localized:
                            "mediaBrowser.batch.delete.remoteLine"
                    ),
                    Int64(remoteItems.count)
                ),
                String.localizedStringWithFormat(
                    String(
                        localized:
                            "mediaBrowser.batch.delete.deviceLine"
                    ),
                    Int64(localItems.count)
                )
            ]
            let alert = NSAlert()
            alert.alertStyle = .critical
            alert.messageText = String(
                localized: "mediaBrowser.batch.delete.confirmTitle"
            )
            alert.informativeText = lines.joined(separator: "\n")
            alert.addButton(
                withTitle: String(
                    localized: "mediaBrowser.action.delete"
                )
            )
            alert.addButton(
                withTitle: String(localized: "common.cancel")
            )
            guard alert.runModal() == .alertFirstButtonReturn else {
                return
            }
            guard let capturedProfile,
                  capturedCredential != nil,
                  let expectedProfileKey,
                  sessionStillMatches(
                      profile: capturedProfile,
                      generation: capturedGeneration
                  ),
                  currentRemoteItemsByFingerprint(
                      expectedProfileKey: expectedProfileKey,
                      fingerprints: Set(
                          remoteItems.map {
                              $0.1.assetFingerprint
                          }
                      )
                  ) != nil else {
                presentActionError(
                    RemoteStorageClientError.notConnected
                )
                return
            }
        }

        beginAction(
            status: String(
                localized: "mediaBrowser.action.deletingRemote"
            )
        )
        actionTask = Task { [weak self] in
            guard let self else { return }
            defer { finishAction() }
            do {
                let outcome = try await appRuntimeFlags
                    .withExecutionLease(
                        cancellationHandler:
                            executionCancellationHandler()
                    ) {
                        var unresolvedLocalIDs = Set<String>()
                        var localChanged = false
                        var remoteChanged = false

                        if !remoteItems.isEmpty {
                            guard let capturedProfile,
                                  capturedCredential != nil,
                                  let expectedProfileKey,
                                  self.sessionStillMatches(
                                      profile: capturedProfile,
                                      generation:
                                          capturedGeneration
                                  ),
                                  self.currentRemoteItemsByFingerprint(
                                      expectedProfileKey:
                                          expectedProfileKey,
                                      fingerprints: Set(
                                          remoteItems.map {
                                              $0.1
                                                  .assetFingerprint
                                          }
                                      )
                                  ) != nil else {
                                throw RemoteStorageClientError
                                    .notConnected
                            }
                        }

                        if !localItems.isEmpty {
                            let localIDs = localItems.compactMap {
                                $0.localAsset?.localIdentifier
                            }
                            let fetched = PHAsset.fetchAssets(
                                withLocalIdentifiers: localIDs,
                                options: nil
                            )
                            var currentAssets: [PHAsset] = []
                            var fetchedIDs = Set<String>()
                            fetched.enumerateObjects { asset, _, _ in
                                currentAssets.append(asset)
                                fetchedIDs.insert(asset.localIdentifier)
                            }
                            let missingIDs = Set(localIDs)
                                .subtracting(fetchedIDs)
                            if self.photoLibraryService
                                .authorizationStatus() != .authorized {
                                unresolvedLocalIDs = missingIDs
                            }

                            let remoteFingerprintsBeingDeleted = Set(
                                remoteItems.map { $0.1.assetFingerprint }
                            )
                            let retainedClaims = localItems.compactMap {
                                item -> (PHAsset, Data)? in
                                guard let asset = item.localAsset,
                                      let fingerprint = item.fingerprint,
                                      item.remote != nil,
                                      !remoteFingerprintsBeingDeleted
                                        .contains(fingerprint),
                                      fetchedIDs.contains(
                                          asset.localIdentifier
                                      ) else {
                                    return nil
                                }
                                return (asset, fingerprint)
                            }
                            let claimsAreCurrent = retainedClaims
                                .allSatisfy {
                                    self.localFingerprintIsCurrent(
                                        asset: $0.0,
                                        expected: $0.1
                                    )
                                }
                            guard claimsAreCurrent else {
                                throw RemoteStorageClientError.unavailable
                            }

                            if !retainedClaims.isEmpty {
                                let remoteItemsByFingerprint =
                                    expectedProfileKey.flatMap {
                                        self.currentRemoteItemsByFingerprint(
                                            expectedProfileKey: $0,
                                            fingerprints: Set(
                                                retainedClaims.map {
                                                    $0.1
                                                }
                                            )
                                        )
                                    }
                                let atRiskCount = retainedClaims.count {
                                    remoteItemsByFingerprint?[$0.1]?
                                        .item.isIncomplete != false
                                }
                                if atRiskCount > 0 {
                                    let alert = NSAlert()
                                    alert.alertStyle = .critical
                                    alert.messageText = String(
                                        localized:
                                            "mediaBrowser.action.incompleteDownload.confirmTitle"
                                    )
                                    alert.informativeText =
                                        String.localizedStringWithFormat(
                                            String(
                                                localized:
                                                    "mediaBrowser.batch.deleteLocal.incompleteConfirm"
                                            ),
                                            Int64(atRiskCount)
                                        )
                                    alert.addButton(
                                        withTitle: String(
                                            localized:
                                                "mediaBrowser.action.delete"
                                        )
                                    )
                                    alert.addButton(
                                        withTitle: String(
                                            localized: "common.cancel"
                                        )
                                    )
                                    guard alert.runModal()
                                        == .alertFirstButtonReturn else {
                                        return MacPhotoBrowserBatchDeleteOutcome
                                            .cancelled(
                                                localChanged: false,
                                                remoteChanged: false
                                            )
                                    }
                                }
                            }

                            if !currentAssets.isEmpty {
                                do {
                                    try await Self.deletePhotoAssets(
                                        currentAssets
                                    )
                                } catch {
                                    let nsError = error as NSError
                                    if nsError.domain
                                        == PHPhotosErrorDomain,
                                       nsError.code
                                        == PHPhotosError.userCancelled
                                            .rawValue {
                                        return MacPhotoBrowserBatchDeleteOutcome
                                            .cancelled(
                                                localChanged: false,
                                                remoteChanged: false
                                            )
                                    }
                                    throw error
                                }
                                localChanged = true
                            }
                            let purgeIDs =
                                self.photoLibraryService
                                    .authorizationStatus()
                                    == .authorized
                                ? localIDs
                                : Array(fetchedIDs)
                            try? self.hashIndexRepository
                                .deleteIndexEntries(assetIDs: purgeIDs)
                        }

                        var failed = 0
                        if !remoteItems.isEmpty {
                            guard let capturedProfile,
                                  let capturedCredential,
                                  let expectedProfileKey else {
                                return MacPhotoBrowserBatchDeleteOutcome
                                    .completed(
                                        localChanged: localChanged,
                                        remoteChanged: false,
                                        failed: remoteItems.count
                                    )
                            }
                            for (
                                index,
                                (month, remote, localIdentifier)
                            ) in remoteItems.enumerated() {
                                if Task.isCancelled {
                                    return MacPhotoBrowserBatchDeleteOutcome
                                        .cancelled(
                                            localChanged: localChanged,
                                            remoteChanged: remoteChanged
                                        )
                                }
                                self.previewStatus.stringValue =
                                    String.localizedStringWithFormat(
                                        String(
                                            localized:
                                                "mediaBrowser.batch.deleting"
                                        ),
                                        Int64(index + 1),
                                        Int64(remoteItems.count)
                                    )
                                if let localIdentifier,
                                   unresolvedLocalIDs.contains(
                                       localIdentifier
                                   ) {
                                    failed += 1
                                    continue
                                }
                                guard self.sessionStillMatches(
                                    profile: capturedProfile,
                                    generation: capturedGeneration
                                ) else {
                                    failed += 1
                                    continue
                                }
                                guard let liveItems =
                                    self.currentRemoteItemsByFingerprint(
                                        expectedProfileKey:
                                            expectedProfileKey,
                                        fingerprints: [
                                            remote.assetFingerprint
                                        ]
                                    ) else {
                                    failed += 1
                                    continue
                                }
                                guard liveItems[
                                    remote.assetFingerprint
                                ] != nil else {
                                    continue
                                }
                                do {
                                    try await self.backupCoordinator
                                        .deleteRemoteAsset(
                                            profile: capturedProfile,
                                            password:
                                                capturedCredential,
                                            month: month,
                                            assetFingerprint:
                                                remote
                                                    .assetFingerprint
                                        )
                                    remoteChanged = true
                                } catch is CancellationError {
                                    return MacPhotoBrowserBatchDeleteOutcome
                                        .cancelled(
                                            localChanged: localChanged,
                                            remoteChanged: remoteChanged
                                        )
                                } catch {
                                    failed += 1
                                }
                            }
                        }
                        return MacPhotoBrowserBatchDeleteOutcome
                            .completed(
                                localChanged: localChanged,
                                remoteChanged: remoteChanged,
                                failed: failed
                            )
                    }
                guard let outcome else {
                    presentActionMessage(
                        String(
                            localized:
                                "mediaBrowser.action.taskInProgress"
                        )
                    )
                    return
                }
                if outcome.localChanged {
                    onLocalLibraryChanged?()
                }
                if outcome.remoteChanged {
                    onRemoteLibraryChanged?(capturedGeneration)
                }
                if outcome.failedCount > 0 {
                    presentActionMessage(
                        String.localizedStringWithFormat(
                            String(
                                localized:
                                    "mediaBrowser.batch.delete.failed"
                            ),
                            Int64(outcome.failedCount)
                        )
                    )
                }
                if outcome.shouldReload {
                    loadItems()
                }
            } catch is CancellationError {
            } catch {
                presentActionError(error)
            }
        }
    }

    private func thumbnail(
        for item: Item,
        targetSize: CGSize
    ) async -> NSImage? {
        if let cached = thumbnailCache[item.id] {
            return cached
        }
        let image: NSImage?
        if let asset = item.localAsset {
            image = await Self.requestImage(
                asset: asset,
                targetSize: targetSize,
                contentMode: .aspectFill,
                networkAccessAllowed: false
            )
        } else if let fingerprint = item.fingerprint,
                  let data = remoteThumbnailData[fingerprint] {
            image = NSImage(data: data)
        } else if let fingerprint = item.fingerprint,
                  MacRemoteThumbnailFetchPolicy.shouldFetch(
                      hasLocalAsset: item.localAsset != nil,
                      hasFingerprint: true,
                      hasCachedData: false,
                      sessionIsCurrent: displayedRemoteSessionIsCurrent
                  ),
                  let loader = remoteThumbnailLoader {
            let generation = sessionGeneration
            let data = await loader.data(for: fingerprint)
            guard !Task.isCancelled,
                  generation == sessionGeneration,
                  displayedRemoteSessionIsCurrent,
                  let data else {
                return nil
            }
            remoteThumbnailData[fingerprint] = data
            image = NSImage(data: data)
        } else {
            image = nil
        }
        if let image {
            thumbnailCache[item.id] = image
        }
        return image
    }

    private func replaceRemoteThumbnailLoader() {
        let previous = remoteThumbnailLoader
        if let profile, let credential {
            remoteThumbnailLoader = MacRemoteThumbnailLoader(
                profile: profile,
                credential: credential,
                storageClientFactory: storageClientFactory
            )
        } else {
            remoteThumbnailLoader = nil
        }
        if let previous {
            Task {
                await previous.disconnect()
            }
        }
    }

    private func showPreview(for indexPath: IndexPath) {
        guard let item = item(at: indexPath) else {
            showEmptyPreview()
            return
        }
        let remoteReadContext = currentRemoteReadContext()
        previewTask?.cancel()
        clearPreviewTemporaryFile()
        previewPlayerView.player?.pause()
        previewPlayerView.player = nil
        previewPlayerView.isHidden = true
        previewLivePhotoView.stopPlayback()
        previewLivePhotoView.livePhoto = nil
        previewLivePhotoView.isHidden = true
        previewImageView.isHidden = false
        previewTitle.stringValue = item.fileName
        previewMetadata.stringValue = [
            Self.dateFormatter.string(from: item.creationDate),
            Self.kindText(item.kind),
            item.presenceText
        ].joined(separator: "\n")
        previewStatus.stringValue = String(
            localized: "mediaBrowser.loading.message",
            defaultValue: "Preparing media…"
        )
        previewImageView.image = thumbnailCache[item.id]
        previewProgress.startAnimation(nil)
        configurePreviewAction(item)

        previewTask = Task { [weak self] in
            guard let self else { return }
            if item.demoPresence != nil {
                previewProgress.stopAnimation(nil)
                previewImageView.image = thumbnailCache[item.id]
                previewStatus.stringValue = ""
                return
            }
            if item.kind == .livePhoto {
                let livePhoto: PHLivePhoto?
                var temporaryURLs: [URL] = []
                if let asset = item.localAsset {
                    livePhoto = await Self.requestLivePhoto(
                        asset: asset,
                        targetSize: CGSize(width: 1_400, height: 1_400)
                    )
                } else if let photo = Self.preferredInstance(
                    for: item,
                    side: .photo
                ),
                let video = Self.preferredInstance(
                    for: item,
                    side: .video
                ),
                let remoteReadContext,
                remoteReadContext.isCurrent(
                    displayedSessionGeneration:
                        displayedSessionGeneration,
                    current: appSession.snapshot
                ) {
                    do {
                        let photoURL = try await Self
                            .materializeRemoteOriginal(
                                instance: photo,
                                profile: remoteReadContext.profile,
                                credential:
                                    remoteReadContext.credential,
                                storageClientFactory:
                                    storageClientFactory
                            )
                        temporaryURLs.append(photoURL)
                        guard !Task.isCancelled,
                              remoteReadContext.isCurrent(
                                  displayedSessionGeneration:
                                      displayedSessionGeneration,
                                  current: appSession.snapshot
                              ) else {
                            for url in temporaryURLs {
                                try? FileManager.default.removeItem(
                                    at: url
                                )
                            }
                            return
                        }
                        let videoURL = try await Self
                            .materializeRemoteOriginal(
                                instance: video,
                                profile: remoteReadContext.profile,
                                credential:
                                    remoteReadContext.credential,
                                storageClientFactory:
                                    storageClientFactory
                            )
                        temporaryURLs.append(videoURL)
                        let placeholder = NSImage(
                            data: try Data(contentsOf: photoURL)
                        )
                        livePhoto = await Self.buildLivePhoto(
                            resourceURLs: [photoURL, videoURL],
                            placeholder: placeholder,
                            targetSize: CGSize(
                                width: 1_400,
                                height: 1_400
                            )
                        )
                    } catch {
                        livePhoto = nil
                    }
                } else {
                    livePhoto = nil
                }
                guard !Task.isCancelled,
                      item.localAsset != nil
                        || remoteReadContext?.isCurrent(
                            displayedSessionGeneration:
                                displayedSessionGeneration,
                            current: appSession.snapshot
                        ) == true,
                      collectionView.selectionIndexPaths.contains(
                        indexPath
                      ) else {
                    for url in temporaryURLs {
                        try? FileManager.default.removeItem(at: url)
                    }
                    return
                }
                if let livePhoto {
                    previewLivePhotoTemporaryURLs = temporaryURLs
                    previewProgress.stopAnimation(nil)
                    previewImageView.isHidden = true
                    previewPlayerView.isHidden = true
                    previewLivePhotoView.isHidden = false
                    previewLivePhotoView.livePhoto = livePhoto
                    previewLivePhotoView.startPlayback(with: .hint)
                    previewStatus.stringValue = ""
                    return
                }
                for url in temporaryURLs {
                    try? FileManager.default.removeItem(at: url)
                }
            }
            if item.demoPresence == nil,
               item.kind == .video,
               let asset = item.localAsset,
               let playerItem = await Self.requestPlayerItem(asset: asset) {
                guard !Task.isCancelled,
                      collectionView.selectionIndexPaths.contains(
                        indexPath
                      ) else {
                    return
                }
                previewProgress.stopAnimation(nil)
                previewImageView.isHidden = true
                previewPlayerView.isHidden = false
                previewPlayerView.player = AVPlayer(
                    playerItem: playerItem
                )
                previewStatus.stringValue = ""
                return
            }

            if item.kind == .video,
               item.localAsset == nil,
               let instance = Self.preferredInstance(
                   for: item,
                   side: .video
               ),
               let remoteReadContext,
               remoteReadContext.isCurrent(
                   displayedSessionGeneration:
                       displayedSessionGeneration,
                   current: appSession.snapshot
               ) {
                do {
                    let url = try await Self.materializeRemoteOriginal(
                        instance: instance,
                        profile: remoteReadContext.profile,
                        credential: remoteReadContext.credential,
                        storageClientFactory: storageClientFactory
                    )
                    guard !Task.isCancelled,
                          remoteReadContext.isCurrent(
                              displayedSessionGeneration:
                                  displayedSessionGeneration,
                              current: appSession.snapshot
                          ),
                          collectionView.selectionIndexPaths.contains(
                            indexPath
                          ) else {
                        try? FileManager.default.removeItem(at: url)
                        return
                    }
                    previewTemporaryURL = url
                    previewProgress.stopAnimation(nil)
                    previewImageView.isHidden = true
                    previewPlayerView.isHidden = false
                    previewPlayerView.player = AVPlayer(url: url)
                    previewStatus.stringValue = ""
                    return
                } catch is CancellationError {
                    return
                } catch {
                    previewStatus.stringValue =
                        String(
                            localized:
                                "mac.browser.preview.originalUnavailable"
                        )
                }
            }

            let image: NSImage?
            var verifiedRemoteOriginal = false
            if let asset = item.localAsset {
                image = await Self.requestImage(
                    asset: asset,
                    targetSize: CGSize(width: 1_400, height: 1_400),
                    contentMode: .aspectFit,
                    networkAccessAllowed: true
                )
            } else if let instance = Self.preferredInstance(
                for: item,
                side: .photo
            ),
            let remoteReadContext,
            remoteReadContext.isCurrent(
                displayedSessionGeneration:
                    displayedSessionGeneration,
                current: appSession.snapshot
            ) {
                do {
                    let url = try await Self.materializeRemoteOriginal(
                        instance: instance,
                        profile: remoteReadContext.profile,
                        credential: remoteReadContext.credential,
                        storageClientFactory: storageClientFactory
                    )
                    defer {
                        try? FileManager.default.removeItem(at: url)
                    }
                    image = try NSImage(
                        data: Data(contentsOf: url)
                    )
                    guard remoteReadContext.isCurrent(
                        displayedSessionGeneration:
                            displayedSessionGeneration,
                        current: appSession.snapshot
                    ) else {
                        return
                    }
                    verifiedRemoteOriginal = image != nil
                } catch {
                    if let fingerprint = item.fingerprint,
                       let data = remoteThumbnailData[fingerprint] {
                        image = NSImage(data: data)
                    } else {
                        image = nil
                    }
                }
            } else if let fingerprint = item.fingerprint,
                      let data = remoteThumbnailData[fingerprint] {
                image = NSImage(data: data)
            } else {
                image = nil
            }
            guard !Task.isCancelled,
                  collectionView.selectionIndexPaths.contains(
                    indexPath
                  ) else {
                return
            }
            previewProgress.stopAnimation(nil)
            previewImageView.image = image ?? Self.placeholderImage(
                kind: item.kind
            )
            if item.localAsset != nil {
                previewStatus.stringValue = ""
            } else if verifiedRemoteOriginal {
                previewStatus.stringValue = ""
            } else if image == nil {
                previewStatus.stringValue =
                    String(
                        localized:
                            "mac.browser.preview.originalAndThumbnailUnavailable"
                    )
            } else {
                previewStatus.stringValue =
                    String(
                        localized:
                            "mac.browser.preview.thumbnailFallback"
                    )
            }
        }
    }

    private func showEmptyPreview() {
        previewTask?.cancel()
        clearPreviewTemporaryFile()
        previewPlayerView.player?.pause()
        previewPlayerView.player = nil
        previewPlayerView.isHidden = true
        previewLivePhotoView.stopPlayback()
        previewLivePhotoView.livePhoto = nil
        previewLivePhotoView.isHidden = true
        previewImageView.isHidden = false
        previewProgress.stopAnimation(nil)
        previewImageView.image = NSImage(
            systemSymbolName: "photo.on.rectangle.angled",
            accessibilityDescription: nil
        )
        previewTitle.stringValue = String(
            localized: "mediaBrowser.select",
            defaultValue: "Select"
        )
        previewMetadata.stringValue = ""
        previewStatus.stringValue = ""
        previewActionButton.isHidden = true
        infoButton.isHidden = true
        shareButton.isHidden = true
        deleteLocalButton.isHidden = true
        deleteRemoteButton.isHidden = true
    }

    private func configurePreviewAction(_ item: Item) {
        #if DEBUG
        if demoMode {
            previewActionButton.title = String(
                localized: "mediaBrowser.action.download",
                defaultValue: "Download"
            ) + "…"
            previewActionButton.isHidden = item.demoPresence
                != .backupOnly
            previewActionButton.isEnabled = false
            infoButton.isHidden = false
            infoButton.isEnabled = true
            shareButton.isHidden = false
            shareButton.isEnabled = false
            deleteLocalButton.isHidden = false
            deleteLocalButton.isEnabled = false
            deleteRemoteButton.isHidden = true
            return
        }
        #endif
        let remoteReady = displayedRemoteSessionIsCurrent
        let actionScope: MediaLibraryActionScope =
            selectedMode == .local ? .local : .unified
        let actions = item.actionPresence.map {
            MediaLibraryActionPolicy.actions(
                for: $0,
                scope: actionScope
            )
        } ?? []
        if actions.contains(.upload),
           let localIdentifier = item.localAsset?.localIdentifier,
           onBackUpItems != nil,
           remoteReady {
            previewActionButton.title = String(
                localized: "panel.backup",
                defaultValue: "Back Up"
            ) + "…"
            previewActionButton.identifier = NSUserInterfaceItemIdentifier(
                localIdentifier
            )
            previewActionButton.isHidden = false
        } else if actions.contains(.download),
                  remoteReady {
            previewActionButton.title = String(
                localized: "mediaBrowser.action.download",
                defaultValue: "Download"
            ) + "…"
            previewActionButton.identifier = NSUserInterfaceItemIdentifier(
                "restore-item"
            )
            previewActionButton.isHidden = false
        } else {
            previewActionButton.identifier = nil
            previewActionButton.isHidden = true
        }
        let canDeleteLocal = actions.contains(.deleteLocal)
        let canDeleteRemote = actions.contains(.deleteRemote)
            && remoteReady
        deleteLocalButton.isHidden =
            !canDeleteLocal && !canDeleteRemote
        if canDeleteLocal, canDeleteRemote {
            deleteLocalButton.title = String(
                localized: "common.delete",
                defaultValue: "Delete"
            ) + "…"
            deleteLocalButton.action =
                #selector(showPreviewDeleteMenu(_:))
        } else if canDeleteLocal {
            deleteLocalButton.title = String(
                localized: "mediaBrowser.action.deleteLocal"
            ) + "…"
            deleteLocalButton.action = #selector(deleteLocal(_:))
        } else if canDeleteRemote {
            deleteLocalButton.title = String(
                localized: "mediaBrowser.action.deleteRemote"
            ) + "…"
            deleteLocalButton.action = #selector(deleteRemote(_:))
        }
        deleteRemoteButton.isHidden = true
        let canRead = item.localAsset != nil || remoteReady
        infoButton.isHidden = !canRead
        shareButton.isHidden = !actions.contains(.share) || !canRead
        let canMutate = !isRunningAction
            && !appRuntimeFlags.isExecuting
        previewActionButton.isEnabled = canMutate
        infoButton.isEnabled = !isRunningAction
        shareButton.isEnabled = !isRunningAction
        deleteLocalButton.isEnabled = canMutate
        deleteRemoteButton.isEnabled = canMutate
    }

    private func executionLifecycleDidChange() {
        updateSelectionPresentation()
        if let selectedItem {
            configurePreviewAction(selectedItem)
        }
        if !appRuntimeFlags.isExecuting {
            loadItems()
        }
    }

    private func applicationDidBecomeActive() {
        let state = PhotoLibraryAccessState(
            photoLibraryService.authorizationStatus()
        )
        handlePhotoLibraryAccessChange(state)
    }

    private func handlePhotoLibraryAccessChange(
        _ state: PhotoLibraryAccessState
    ) {
        let previous = photoLibraryAccessState
        guard previous != state else { return }
        photoLibraryAccessState = state
        guard MacPhotoBrowserPhotoAccessTransitionPolicy
                .invalidatesLocalProjection(
                    previous: previous,
                    current: state
                ) else {
            return
        }
        pendingLocalProjectionInvalidation = true
        schedulePhotoLibraryReload()
    }

    private func schedulePhotoLibraryReload() {
        guard !isRunningAction else {
            pendingPhotoLibraryReload = true
            return
        }
        pendingPhotoLibraryReload = false
        if pendingLocalProjectionInvalidation {
            pendingLocalProjectionInvalidation = false
            allItems.removeAll { $0.localAsset != nil }
            applyMode()
        }
        thumbnailCache.removeAll()
        loadItems()
    }

    private func remoteSnapshotDidChange() {
        thumbnailTasks.values.forEach { $0.cancel() }
        thumbnailTasks.removeAll()
        remoteThumbnailData.removeAll()
        thumbnailCache.removeAll()
        loadItems()
    }

    @objc private func showMetadata(_ sender: Any?) {
        guard let item = selectedItem else { return }
        let capturedProfile = displayedRemoteSessionIsCurrent
            ? profile : nil
        let capturedCredential = displayedRemoteSessionIsCurrent
            ? credential : nil
        let capturedGeneration = sessionGeneration
        let controller = MacMediaMetadataViewController {
            [weak self] in
            guard let self else { return nil }
            return try await self.metadataDocument(
                for: item,
                profile: capturedProfile,
                credential: capturedCredential,
                sessionGeneration: capturedGeneration
            )
        }
        presentAsSheet(controller)
    }

    private func metadataDocument(
        for item: Item,
        profile: ServerProfileRecord?,
        credential: String?,
        sessionGeneration: UInt64
    ) async throws -> MediaMetadataDocument? {
        #if DEBUG
        if item.demoPresence != nil {
            return Self.demoMetadataDocument(for: item)
        }
        #endif

        var localSummary: MediaMetadataDocument?
        if let asset = item.localAsset {
            let localDocument = await MediaMetadataLoader
                .localDocument(
                    asset: asset,
                    kind: item.kind,
                    allowNetworkAccess: item.remote == nil
                )
            if localDocument?.isSummaryOnly == false {
                return localDocument
            }
            localSummary = localDocument
        }

        guard let profile,
              let credential,
              sessionStillMatches(
                  profile: profile,
                  generation: sessionGeneration
              ),
              let instance = Self.preferredInstance(
                  for: item,
                  side: item.kind == .video ? .video : .photo
              ) else {
            return localSummary
        }
        let url = try await Self.materializeRemoteOriginal(
            instance: instance,
            profile: profile,
            credential: credential,
            storageClientFactory: storageClientFactory
        )
        defer {
            try? FileManager.default.removeItem(at: url)
        }
        guard sessionStillMatches(
            profile: profile,
            generation: sessionGeneration
        ) else {
            return localSummary
        }
        if item.kind == .video {
            return await MediaMetadataLoader.remoteVideoDocument(
                at: url,
                kind: item.kind,
                creationDate: item.creationDate,
                relativePath: instance.fileName
            )
        }
        return MediaMetadataLoader.remoteImageDocument(
            at: url,
            kind: item.kind,
            creationDate: item.creationDate,
            relativePath: instance.fileName
        )
    }

    @objc private func showPreviewDeleteMenu(_ sender: NSButton) {
        guard let item = selectedItem else { return }
        let remoteReady = displayedRemoteSessionIsCurrent
        let menu = NSMenu()
        if item.localAsset != nil {
            let local = NSMenuItem(
                title: String(
                    localized: "mediaBrowser.action.deleteLocal"
                ) + "…",
                action: #selector(deleteLocal(_:)),
                keyEquivalent: ""
            )
            local.target = self
            menu.addItem(local)
        }
        if item.remote != nil, remoteReady {
            let remote = NSMenuItem(
                title: String(
                    localized: "mediaBrowser.action.deleteRemote"
                ) + "…",
                action: #selector(deleteRemote(_:)),
                keyEquivalent: ""
            )
            remote.target = self
            menu.addItem(remote)
        }
        if item.localAsset != nil, item.remote != nil, remoteReady {
            let all = NSMenuItem(
                title: String(
                    localized: "mediaBrowser.action.deleteAll"
                ) + "…",
                action: #selector(deleteSelectedItems(_:)),
                keyEquivalent: ""
            )
            all.target = self
            menu.addItem(all)
        }
        menu.popUp(
            positioning: nil,
            at: NSPoint(x: 0, y: sender.bounds.maxY + 4),
            in: sender
        )
    }

    @objc private func runPreviewAction(_ sender: NSButton) {
        guard let value = sender.identifier?.rawValue else { return }
        if value == "restore-item" {
            restoreSelectedItem()
            return
        }
        guard let item = selectedItem,
              item.localAsset?.localIdentifier == value else {
            return
        }
        guard onBackUpItems?([item.month: [value]]) == true else { return }
        view.window?.close()
    }

    private func restoreSelectedItem() {
        guard !isRunningAction,
              let item = selectedItem else {
            return
        }
        restore(items: [item])
    }

    private func restore(items: [Item]) {
        let projectedRemoteItems = items.compactMap { item in
            item.isRemoteOnly ? item.remote : nil
        }
        guard !isRunningAction,
              !projectedRemoteItems.isEmpty,
              let profile,
              let credential else {
            return
        }
        let capturedGeneration = sessionGeneration
        guard sessionStillMatches(
            profile: profile,
            generation: capturedGeneration
        ) else {
            presentActionError(RemoteStorageClientError.notConnected)
            return
        }
        let expectedProfileKey =
            RemoteIndexSyncService.remoteProfileKey(profile)
        guard let liveItems = currentRemoteItemsByFingerprint(
            expectedProfileKey: expectedProfileKey,
            fingerprints: Set(
                projectedRemoteItems.map(\.assetFingerprint)
            )
        ) else {
            presentActionError(RemoteStorageClientError.notConnected)
            return
        }
        var seenFingerprints = Set<Data>()
        let remoteItems = projectedRemoteItems.compactMap {
            remote -> RemoteAlbumItem? in
            guard seenFingerprints.insert(
                remote.assetFingerprint
            ).inserted else {
                return nil
            }
            return liveItems[remote.assetFingerprint]?.item
        }
        guard !remoteItems.isEmpty else {
            presentActionError(RemoteStorageClientError.unavailable)
            return
        }
        let incompleteCount = remoteItems.count(
            where: \.isIncomplete
        )
        let incompletePolicy: IncompleteDownloadPolicy
        if incompleteCount > 0 {
            let alert = NSAlert()
            alert.alertStyle = .warning
            if incompleteCount == 1 {
                alert.messageText = String(
                    localized:
                        "mediaBrowser.action.incompleteDownload.confirmTitle",
                    defaultValue: "Incomplete backup"
                )
                alert.informativeText = String(
                    localized:
                        "mediaBrowser.action.incompleteDownload.confirmMessage",
                    defaultValue: "This backup is incomplete. Restoring creates a separate asset that may be backed up again as new."
                )
            } else {
                alert.messageText = String(
                    localized: "home.incompleteDownload.title",
                    defaultValue: "Incomplete Backup Items"
                )
                alert.informativeText = String.localizedStringWithFormat(
                    String(
                        localized: "home.incompleteDownload.message",
                        defaultValue: "%lld incomplete backup items were found."
                    ),
                    incompleteCount
                )
            }
            alert.addButton(
                withTitle: incompleteCount == 1
                    ? String(
                        localized:
                            "mediaBrowser.action.incompleteDownload.create",
                        defaultValue: "Create New Asset"
                    )
                    : String(
                        localized: "home.incompleteDownload.createAll",
                        defaultValue: "Create New Assets"
                    )
            )
            alert.addButton(
                withTitle: String(
                    localized: "home.incompleteDownload.skip",
                    defaultValue: "Skip Them"
                )
            )
            alert.addButton(
                withTitle: String(
                    localized: "common.cancel",
                    defaultValue: "Cancel"
                )
            )
            switch alert.runModal() {
            case .alertFirstButtonReturn:
                incompletePolicy = .createNewAsset
            case .alertSecondButtonReturn:
                incompletePolicy = .skip
            default:
                return
            }
        } else {
            incompletePolicy = .skip
        }
        guard sessionStillMatches(
            profile: profile,
            generation: capturedGeneration
        ) else {
            presentActionError(RemoteStorageClientError.notConnected)
            return
        }
        beginAction(
            status: String(
                localized: "mediaBrowser.action.saving",
                defaultValue: "Saving…"
            )
        )
        actionTask = Task { [weak self] in
            guard let self else { return }
            defer { finishAction() }
            do {
                let accessState = PhotoLibraryAccessState(
                    await photoLibraryService.requestAuthorization()
                )
                handlePhotoLibraryAccessChange(accessState)
                guard accessState.canReadLibrary else {
                    presentActionMessage(
                        String(
                            localized:
                                "mediaBrowser.action.noPhotoAccess"
                        )
                    )
                    return
                }
                let changed = try await appRuntimeFlags
                    .withExecutionLease(
                        cancellationHandler:
                            executionCancellationHandler()
                    ) {
                        guard self.sessionStillMatches(
                            profile: profile,
                            generation: capturedGeneration
                        ) else {
                            throw RemoteStorageClientError.notConnected
                        }
                        var existingLocalIDs: [String] = []
                        var pending: [RemoteAlbumItem] = []
                        for remote in remoteItems {
                            if let localID = try self
                                .existingLocalIdentifier(
                                    for: remote.assetFingerprint
                                ) {
                                existingLocalIDs.append(localID)
                            } else {
                                pending.append(remote)
                            }
                        }
                        guard !pending.isEmpty else {
                            return existingLocalIDs
                        }
                        var restoredLocalIDs: [String] = []
                        let result = await self.downloadWorkflowHelper
                            .downloadItems(
                                pending,
                                context: DownloadWorkflowHelper.Context(
                                    profile: profile,
                                    password: credential
                                ),
                                incompletePolicy: incompletePolicy,
                                onTransferState: {
                                    [weak self] transfer in
                                    self?.previewStatus.stringValue =
                                        transfer.stageDescription
                                },
                                onItemRestored: { localID in
                                    restoredLocalIDs.append(localID)
                                }
                            )
                        switch result {
                        case .success:
                            return existingLocalIDs
                                + restoredLocalIDs
                        case .failed(let message):
                            throw NSError(
                                domain: "MacPhotoBrowser.Restore",
                                code: 1,
                                userInfo: [
                                    NSLocalizedDescriptionKey: message
                                ]
                            )
                        case .fatal(let message, _):
                            throw NSError(
                                domain: "MacPhotoBrowser.Restore",
                                code: 2,
                                userInfo: [
                                    NSLocalizedDescriptionKey: message
                                ]
                            )
                        case .cancelled:
                            throw CancellationError()
                        }
                    }
                guard let localIDs = changed else {
                    throw RemoteStorageClientError.unavailable
                }
                onLocalLibraryChanged?()
                loadItems(
                    selectingLocalIdentifiers: Set(localIDs)
                )
            } catch is CancellationError {
                return
            } catch {
                presentActionError(error)
            }
        }
    }

    @objc private func shareSelectedItem(_ sender: Any?) {
        guard !isRunningAction,
              let item = selectedItem else {
            return
        }
        let remoteReady = displayedRemoteSessionIsCurrent
        guard item.localAsset != nil || remoteReady else { return }
        let capturedProfile = remoteReady ? profile : nil
        let capturedCredential = remoteReady ? credential : nil
        let capturedGeneration = sessionGeneration
        beginAction(
            status: String(
                localized: "mediaBrowser.loading.message",
                defaultValue: "Preparing media…"
            )
        )
        actionTask = Task { [weak self] in
            guard let self else { return }
            defer { finishAction() }
            do {
                let shareItems = try await materializeShareItems(
                    item,
                    profile: capturedProfile,
                    credential: capturedCredential,
                    sessionGeneration: capturedGeneration
                )
                guard !Task.isCancelled else {
                    for url in shareItems.compactMap({
                        $0 as? URL
                    }) where url.lastPathComponent.hasPrefix(
                        "wm-preview-"
                    ) {
                        try? FileManager.default.removeItem(at: url)
                    }
                    return
                }
                clearSharingTemporaryFile()
                sharingTemporaryURLs = shareItems.compactMap {
                    $0 as? URL
                }.filter {
                    $0.lastPathComponent.hasPrefix("wm-preview-")
                }
                let picker = NSSharingServicePicker(
                    items: shareItems
                )
                picker.delegate = self
                sharingServicePicker = picker
                picker.show(
                    relativeTo: shareButton.bounds,
                    of: shareButton,
                    preferredEdge: .minY
                )
            } catch is CancellationError {
                return
            } catch {
                presentActionError(error)
            }
        }
    }

    private func materializeShareItems(
        _ item: Item,
        profile: ServerProfileRecord?,
        credential: String?,
        sessionGeneration: UInt64
    ) async throws -> [Any] {
        if let asset = item.localAsset {
            return try await Self.materializeLocalOriginals(
                asset: asset
            )
        }
        let side: ResourceRole.DisplaySide = item.kind == .video
            ? .video : .photo
        if let instance = Self.preferredInstance(
            for: item,
            side: side
        ),
        let profile,
        let credential,
        sessionStillMatches(
            profile: profile,
            generation: sessionGeneration
        ) {
            let url = try await Self.materializeRemoteOriginal(
                instance: instance,
                profile: profile,
                credential: credential,
                storageClientFactory: storageClientFactory
            )
            guard sessionStillMatches(
                profile: profile,
                generation: sessionGeneration
            ) else {
                try? FileManager.default.removeItem(at: url)
                throw RemoteStorageClientError.notConnected
            }
            return [url]
        }
        throw RemoteStorageClientError.unavailable
    }

    @objc private func deleteLocal(_ sender: Any?) {
        guard !isRunningAction,
              let item = selectedItem,
              let asset = item.localAsset else {
            return
        }
        let expectedProfileKey = profile.map {
            RemoteIndexSyncService.remoteProfileKey($0)
        }
        var acceptedBackupRisk = false
        if item.remote != nil,
           let fingerprint = item.fingerprint,
           !remoteBackupIsComplete(
               fingerprint: fingerprint,
               expectedProfileKey: expectedProfileKey
           ) {
            guard confirmIncompleteLocalDelete() else { return }
            acceptedBackupRisk = true
        }
        beginAction()
        let localIdentifier = asset.localIdentifier
        actionTask = Task { [weak self] in
            guard let self else { return }
            defer { finishAction() }
            do {
                let changed = try await appRuntimeFlags
                    .withExecutionLease(
                        cancellationHandler:
                            executionCancellationHandler()
                    ) {
                        let fetch = PHAsset.fetchAssets(
                            withLocalIdentifiers: [localIdentifier],
                            options: nil
                        )
                        guard let currentAsset = fetch.firstObject else {
                            guard self.photoLibraryService
                                .authorizationStatus() == .authorized else {
                                throw RemoteStorageClientError.unavailable
                            }
                            try? self.hashIndexRepository
                                .deleteIndexEntries(
                                    assetIDs: [localIdentifier]
                                )
                            return true
                        }
                        if item.remote != nil,
                           let fingerprint = item.fingerprint {
                            guard self.localFingerprintIsCurrent(
                                asset: currentAsset,
                                expected: fingerprint
                            ) else {
                                throw RemoteStorageClientError.unavailable
                            }
                            if !acceptedBackupRisk,
                               !self.remoteBackupIsComplete(
                                   fingerprint: fingerprint,
                                   expectedProfileKey:
                                       expectedProfileKey
                               ) {
                                guard self.confirmIncompleteLocalDelete()
                                else {
                                    return false
                                }
                            }
                        }
                        try await Self.deletePhotoAsset(currentAsset)
                        try? self.hashIndexRepository.deleteIndexEntries(
                            assetIDs: [localIdentifier]
                        )
                        return true
                    }
                guard let changed else {
                    throw RemoteStorageClientError.unavailable
                }
                guard changed else { return }
                onLocalLibraryChanged?()
                loadItems()
            } catch is CancellationError {
                return
            } catch {
                presentActionError(error)
            }
        }
    }

    private func remoteBackupIsComplete(
        fingerprint: Data,
        expectedProfileKey: String?
    ) -> Bool {
        guard let expectedProfileKey,
              let live = currentRemoteItemsByFingerprint(
                  expectedProfileKey: expectedProfileKey,
                  fingerprints: [fingerprint]
              )?[fingerprint] else {
            return false
        }
        return !live.item.isIncomplete
    }

    private func confirmIncompleteLocalDelete() -> Bool {
        let alert = NSAlert()
        alert.alertStyle = .critical
        alert.messageText = String(
            localized:
                "mediaBrowser.action.incompleteDownload.confirmTitle"
        )
        alert.informativeText = String(
            localized:
                "mediaBrowser.action.incompleteDeleteLocal.confirmMessage"
        )
        alert.addButton(
            withTitle: String(
                localized: "mediaBrowser.action.deleteLocal"
            )
        )
        alert.addButton(
            withTitle: String(localized: "common.cancel")
        )
        return alert.runModal() == .alertFirstButtonReturn
    }

    @objc private func deleteRemote(_ sender: Any?) {
        guard !isRunningAction,
              let item = selectedItem,
              let remote = item.remote,
              let remoteStorageMonth = item.remoteStorageMonth,
              let profile,
              let credential else {
            return
        }
        let capturedGeneration = sessionGeneration
        guard sessionStillMatches(
            profile: profile,
            generation: capturedGeneration
        ) else {
            presentActionError(
                RemoteStorageClientError.notConnected
            )
            return
        }
        let alert = NSAlert()
        alert.alertStyle = .critical
        alert.messageText = String(
            localized:
                "mediaBrowser.action.deleteRemote.confirmTitle"
        )
        alert.informativeText = String(
            localized:
                "mediaBrowser.action.deleteRemote.confirmMessage"
        )
        alert.addButton(
            withTitle: String(
                localized: "mediaBrowser.action.deleteRemote"
            )
        )
        alert.addButton(
            withTitle: String(localized: "common.cancel")
        )
        guard alert.runModal() == .alertFirstButtonReturn else {
            return
        }
        guard sessionStillMatches(
            profile: profile,
            generation: capturedGeneration
        ) else {
            presentActionError(
                RemoteStorageClientError.notConnected
            )
            return
        }
        let expectedProfileKey =
            RemoteIndexSyncService.remoteProfileKey(profile)
        guard let liveItems = currentRemoteItemsByFingerprint(
            expectedProfileKey: expectedProfileKey,
            fingerprints: [remote.assetFingerprint]
        ) else {
            presentActionError(
                RemoteStorageClientError.notConnected
            )
            return
        }
        guard liveItems[remote.assetFingerprint] != nil else {
            onRemoteLibraryChanged?(capturedGeneration)
            loadItems()
            return
        }
        beginAction()
        actionTask = Task { [weak self] in
            guard let self else { return }
            defer { finishAction() }
            do {
                let changed = try await appRuntimeFlags
                    .withExecutionLease(
                        cancellationHandler:
                            executionCancellationHandler()
                    ) {
                        guard self.sessionStillMatches(
                            profile: profile,
                            generation: capturedGeneration
                        ),
                              let liveItems =
                                self.currentRemoteItemsByFingerprint(
                                    expectedProfileKey:
                                        expectedProfileKey,
                                    fingerprints: [
                                        remote.assetFingerprint
                                    ]
                                ) else {
                            throw RemoteStorageClientError.notConnected
                        }
                        guard liveItems[
                            remote.assetFingerprint
                        ] != nil else {
                            return true
                        }
                        try await self.backupCoordinator
                            .deleteRemoteAsset(
                                profile: profile,
                                password: credential,
                                month: remoteStorageMonth,
                                assetFingerprint:
                                    remote.assetFingerprint
                            )
                        return true
                    }
                guard changed == true else {
                    throw RemoteStorageClientError.unavailable
                }
                onRemoteLibraryChanged?(capturedGeneration)
                loadItems()
            } catch is CancellationError {
                return
            } catch {
                presentActionError(error)
            }
        }
    }

    private func item(at indexPath: IndexPath) -> Item? {
        guard visibleSections.indices.contains(indexPath.section),
              visibleSections[indexPath.section].items.indices
                .contains(indexPath.item) else {
            return nil
        }
        return visibleSections[indexPath.section].items[indexPath.item]
    }

    private var selectedItem: Item? {
        guard let indexPath =
                collectionView.selectionIndexPaths.first else {
            return nil
        }
        return item(at: indexPath)
    }

    private var selectedItems: [Item] {
        collectionView.selectionIndexPaths
            .sorted {
                if $0.section != $1.section {
                    return $0.section < $1.section
                }
                return $0.item < $1.item
            }
            .compactMap(item(at:))
    }

    private func beginAction(status: String? = nil) {
        isRunningAction = true
        previewStatus.textColor = .secondaryLabelColor
        previewStatus.stringValue = status ?? ""
        previewProgress.startAnimation(nil)
        if selectedItems.count > 1 {
            batchBackupButton.isEnabled = false
            batchRestoreButton.isEnabled = false
            batchDeleteButton.isEnabled = false
        } else if let selectedItem {
            configurePreviewAction(selectedItem)
        }
    }

    private func executionCancellationHandler()
        -> @Sendable () -> Void {
        appRuntimeFlags.makeExecutionCancellationHandler(for: self) {
            $0.actionTask?.cancel()
        }
    }

    private func finishAction() {
        isRunningAction = false
        actionTask = nil
        previewProgress.stopAnimation(nil)
        if selectedItems.count > 1 {
            updateSelectionPresentation()
        } else if let selectedItem {
            configurePreviewAction(selectedItem)
        }
        if pendingPhotoLibraryReload,
           !closeWhenActionFinishes {
            schedulePhotoLibraryReload()
        }
        if closeWhenActionFinishes {
            closeWhenActionFinishes = false
            view.window?.performClose(nil)
        }
    }

    private func existingLocalIdentifier(
        for fingerprint: Data
    ) throws -> String? {
        let candidates = try hashIndexRepository
            .fetchAssetIDsByFingerprints([fingerprint])[fingerprint]
            ?? []
        guard !candidates.isEmpty else { return nil }
        let records = try hashIndexRepository
            .fetchAssetFingerprintRecords(
                assetIDs: Set(candidates)
            )
        let fetch = PHAsset.fetchAssets(
            withLocalIdentifiers: candidates,
            options: nil
        )
        var currentIdentifier: String?
        fetch.enumerateObjects { asset, _, stop in
            guard let record = records[asset.localIdentifier],
                  record.fingerprint == fingerprint else {
                return
            }
            if let modified = asset.modificationDate,
               modified > record.updatedAt {
                return
            }
            currentIdentifier = asset.localIdentifier
            stop.pointee = true
        }
        return currentIdentifier
    }

    private func selectItems(localIdentifiers: Set<String>) {
        guard !localIdentifiers.isEmpty else { return }
        if filterControl.selectedSegment == Mode.remote.rawValue {
            filterControl.selectedSegment = Mode.merged.rawValue
            applyMode()
        }
        var indexPaths = Set<IndexPath>()
        for (sectionIndex, section) in visibleSections.enumerated() {
            for (itemIndex, item) in section.items.enumerated() {
                guard let localID =
                    item.localAsset?.localIdentifier,
                      localIdentifiers.contains(localID) else {
                    continue
                }
                indexPaths.insert(
                    IndexPath(
                        item: itemIndex,
                        section: sectionIndex
                    )
                )
            }
        }
        guard !indexPaths.isEmpty else { return }
        collectionView.selectItems(
            at: indexPaths,
            scrollPosition: .centeredVertically
        )
        updateSelectionPresentation()
        if indexPaths.count == 1,
           let indexPath = indexPaths.first {
            showPreview(for: indexPath)
        }
    }

    private func presentActionError(_ error: Error) {
        guard let window = view.window else { return }
        let alert = NSAlert()
        alert.alertStyle = .warning
        alert.messageText = String(
            localized: "mediaBrowser.action.error"
        )
        alert.informativeText = profile?
            .userFacingStorageErrorMessage(error)
            ?? error.localizedDescription
        alert.beginSheetModal(for: window)
    }

    private func presentActionMessage(_ message: String) {
        guard let window = view.window else { return }
        let alert = NSAlert()
        alert.alertStyle = .warning
        alert.messageText = String(
            localized: "mediaBrowser.action.error"
        )
        alert.informativeText = message
        alert.beginSheetModal(for: window)
    }

    private func sessionStillMatchesCapturedProfile() -> Bool {
        guard let profile else { return false }
        return sessionStillMatches(
            profile: profile,
            generation: sessionGeneration
        )
    }

    private var displayedRemoteSessionIsCurrent: Bool {
        displayedSessionGeneration == sessionGeneration
            && sessionStillMatchesCapturedProfile()
    }

    private func currentRemoteReadContext()
        -> MacPhotoBrowserRemoteReadContext? {
        guard displayedRemoteSessionIsCurrent,
              let profile,
              let credential else {
            return nil
        }
        return MacPhotoBrowserRemoteReadContext(
            profile: profile,
            credential: credential,
            sessionGeneration: sessionGeneration
        )
    }

    private func sessionStillMatches(
        profile: ServerProfileRecord,
        generation: UInt64
    ) -> Bool {
        MacPhotoBrowserSessionPolicy.matches(
            capturedProfile: profile,
            capturedGeneration: generation,
            current: appSession.snapshot
        )
    }

    private func clearPreviewTemporaryFile() {
        if let url = previewTemporaryURL {
            self.previewTemporaryURL = nil
            try? FileManager.default.removeItem(at: url)
        }
        let liveURLs = previewLivePhotoTemporaryURLs
        previewLivePhotoTemporaryURLs.removeAll()
        for url in liveURLs {
            try? FileManager.default.removeItem(at: url)
        }
    }

    private func clearSharingTemporaryFile() {
        let urls = sharingTemporaryURLs
        sharingTemporaryURLs.removeAll()
        for url in urls {
            try? FileManager.default.removeItem(at: url)
        }
    }

    @objc private func changeFilter(_ sender: Any?) {
        didScrollToInitialMonth = false
        applyMode()
    }

    @objc private func changeZoom(_ sender: NSSlider) {
        let width = CGFloat(sender.doubleValue)
        flowLayout.itemSize = NSSize(width: width, height: width + 28)
        flowLayout.invalidateLayout()
    }

    private nonisolated static func preferredInstance(
        for item: Item,
        side: ResourceRole.DisplaySide
    ) -> RemoteAssetResourceInstance? {
        guard let remote = item.remote else { return nil }
        let ranked = remote.instances.compactMap { instance in
            ResourceRole.displaySelectionRank(
                role: instance.role,
                slot: instance.slot,
                side: side
            ).map { ($0, instance) }
        }
        return ranked.min { $0.0 < $1.0 }?.1
            ?? remote.instances.first {
                $0.resourceHash
                    == remote.representative.contentHash
            }
    }

    private nonisolated static func materializeRemoteOriginal(
        instance: RemoteAssetResourceInstance,
        profile: ServerProfileRecord,
        credential: String,
        storageClientFactory: StorageClientFactory
    ) async throws -> URL {
        let ext = (instance.fileName as NSString)
            .pathExtension
        var localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(
                "wm-preview-\(UUID().uuidString)"
            )
        if !ext.isEmpty {
            localURL.appendPathExtension(
                RemotePathBuilder.sanitizeFilename(ext)
            )
        }
        let client = try storageClientFactory.makeClient(
            profile: profile,
            credentialPayload: credential
        )
        do {
            try await client.connect()
            try Task.checkCancellation()
            let remotePath = RemotePathBuilder.absolutePath(
                basePath: profile.basePath,
                remoteRelativePath:
                    instance.remoteRelativePath
            )
            if let directURL = await client.directReadURL(
                forRemotePath: remotePath
            ) {
                try FileManager.default.copyItem(
                    at: directURL,
                    to: localURL
                )
            } else {
                try await client.download(
                    remotePath: remotePath,
                    localURL: localURL
                )
            }
            try Task.checkCancellation()
            let digest = try FileDigestService.sha256AndSize(
                of: localURL
            )
            guard digest.hash == instance.resourceHash,
                  digest.size == instance.fileSize else {
                throw RemoteStorageClientError.underlying(
                    NSError(
                        domain: "MacPhotoBrowser",
                        code: 1,
                        userInfo: [
                            NSLocalizedDescriptionKey:
                                String(
                                    localized:
                                        "mac.browser.originalMismatch"
                                )
                        ]
                    )
                )
            }
            await client.disconnectSafely()
            return localURL
        } catch {
            await client.disconnectSafely()
            try? FileManager.default.removeItem(at: localURL)
            throw error
        }
    }

    private nonisolated static func materializeLocalOriginals(
        asset: PHAsset
    ) async throws -> [Any] {
        let resources = PHAssetResource.assetResources(for: asset)
        let selected: [PHAssetResource]
        if asset.mediaSubtypes.contains(.photoLive) {
            let photo = resources.first {
                $0.type == .fullSizePhoto
            } ?? resources.first {
                $0.type == .photo
            }
            let video = resources.first {
                $0.type == .fullSizePairedVideo
            } ?? resources.first {
                $0.type == .pairedVideo
            }
            selected = [photo, video].compactMap { $0 }
        } else if asset.mediaType == .video {
            selected = [
                resources.first {
                    $0.type == .fullSizeVideo
                } ?? resources.first {
                    $0.type == .video
                }
            ].compactMap { $0 }
        } else {
            selected = [
                resources.first {
                    $0.type == .fullSizePhoto
                } ?? resources.first {
                    $0.type == .photo
                } ?? resources.first {
                    $0.type == .alternatePhoto
                }
            ].compactMap { $0 }
        }
        guard !selected.isEmpty else {
            throw RemoteStorageClientError.unavailable
        }

        var urls: [URL] = []
        do {
            for resource in selected {
                let fileName = RemotePathBuilder.sanitizeFilename(
                    resource.originalFilename
                )
                let url = FileManager.default.temporaryDirectory
                    .appendingPathComponent(
                        "wm-preview-\(UUID().uuidString)-\(fileName)"
                    )
                try await writeAssetResource(
                    resource,
                    to: url
                )
                try Task.checkCancellation()
                urls.append(url)
            }
            return urls.map { $0 as Any }
        } catch {
            for url in urls {
                try? FileManager.default.removeItem(at: url)
            }
            throw error
        }
    }

    private nonisolated static func writeAssetResource(
        _ resource: PHAssetResource,
        to url: URL
    ) async throws {
        let options = PHAssetResourceRequestOptions()
        options.isNetworkAccessAllowed = true
        try await withCheckedThrowingContinuation {
            (continuation: CheckedContinuation<Void, Error>) in
            PHAssetResourceManager.default().writeData(
                for: resource,
                toFile: url,
                options: options
            ) { error in
                if let error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume()
                }
            }
        }
    }

    private nonisolated static func deletePhotoAsset(
        _ asset: PHAsset
    ) async throws {
        try await deletePhotoAssets([asset])
    }

    private nonisolated static func deletePhotoAssets(
        _ assets: [PHAsset]
    ) async throws {
        try MacPhotoLibraryDeletionPreparationPolicy
            .ensureCommitAllowed(
                isCancelled: Task.isCancelled
            )
        try await withCheckedThrowingContinuation {
            (continuation: CheckedContinuation<Void, Error>) in
            PHPhotoLibrary.shared().performChanges {
                PHAssetChangeRequest.deleteAssets(
                    assets as NSFastEnumeration
                )
            } completionHandler: { success, error in
                if success {
                    continuation.resume()
                } else {
                    continuation.resume(
                        throwing: error
                            ?? RemoteStorageClientError
                                .unavailable
                    )
                }
            }
        }
    }

    private nonisolated static func requestImage(
        asset: PHAsset,
        targetSize: CGSize,
        contentMode: PHImageContentMode,
        networkAccessAllowed: Bool
    ) async -> NSImage? {
        let manager = PHImageManager.default()
        let state = MacPhotosRequestState<NSImage>(
            manager: manager
        )
        return await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                guard state.bind(continuation) else {
                    continuation.resume(returning: nil)
                    return
                }
                let options = PHImageRequestOptions()
                options.deliveryMode = .highQualityFormat
                options.resizeMode = .fast
                options.isNetworkAccessAllowed =
                    networkAccessAllowed
                options.version = .current
                let requestID = manager.requestImage(
                    for: asset,
                    targetSize: targetSize,
                    contentMode: contentMode,
                    options: options
                ) { image, info in
                    if (
                        info?[
                            PHImageResultIsDegradedKey
                        ] as? NSNumber
                    )?.boolValue == true {
                        return
                    }
                    guard (
                        info?[PHImageCancelledKey]
                            as? NSNumber
                    )?.boolValue != true,
                    info?[PHImageErrorKey] == nil,
                    networkAccessAllowed
                        || (
                            info?[PHImageResultIsInCloudKey]
                                as? NSNumber
                        )?.boolValue != true else {
                        state.complete(nil)
                        return
                    }
                    state.complete(image)
                }
                state.attach(requestID)
            }
        } onCancel: {
            state.cancel()
        }
    }

    private nonisolated static func requestLivePhoto(
        asset: PHAsset,
        targetSize: CGSize
    ) async -> PHLivePhoto? {
        let manager = PHImageManager.default()
        let state = MacPhotosRequestState<PHLivePhoto>(
            manager: manager
        )
        return await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                guard state.bind(continuation) else {
                    continuation.resume(returning: nil)
                    return
                }
                let options = PHLivePhotoRequestOptions()
                options.deliveryMode = .highQualityFormat
                options.isNetworkAccessAllowed = true
                let requestID = manager.requestLivePhoto(
                    for: asset,
                    targetSize: targetSize,
                    contentMode: .aspectFit,
                    options: options
                ) { livePhoto, info in
                    if (
                        info?[PHImageResultIsDegradedKey]
                            as? NSNumber
                    )?.boolValue == true {
                        return
                    }
                    guard (
                        info?[PHImageCancelledKey]
                            as? NSNumber
                    )?.boolValue != true,
                    info?[PHImageErrorKey] == nil else {
                        state.complete(nil)
                        return
                    }
                    state.complete(livePhoto)
                }
                state.attach(requestID)
            }
        } onCancel: {
            state.cancel()
        }
    }

    private nonisolated static func buildLivePhoto(
        resourceURLs: [URL],
        placeholder: NSImage?,
        targetSize: CGSize
    ) async -> PHLivePhoto? {
        let state = MacPhotosRequestState<PHLivePhoto>(
            cancelRequest: { requestID in
                PHLivePhoto.cancelRequest(
                    withRequestID: requestID
                )
            }
        )
        return await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                guard state.bind(continuation) else {
                    continuation.resume(returning: nil)
                    return
                }
                let requestID = PHLivePhoto.request(
                    withResourceFileURLs: resourceURLs,
                    placeholderImage: placeholder,
                    targetSize: targetSize,
                    contentMode: .aspectFit
                ) { livePhoto, info in
                    if (
                        info[PHLivePhotoInfoIsDegradedKey]
                            as? NSNumber
                    )?.boolValue == true {
                        return
                    }
                    guard (
                        info[PHLivePhotoInfoCancelledKey]
                            as? NSNumber
                    )?.boolValue != true,
                    info[PHLivePhotoInfoErrorKey] == nil else {
                        state.complete(nil)
                        return
                    }
                    state.complete(livePhoto)
                }
                state.attach(requestID)
            }
        } onCancel: {
            state.cancel()
        }
    }

    private nonisolated static func requestPlayerItem(
        asset: PHAsset
    ) async -> AVPlayerItem? {
        let manager = PHImageManager.default()
        let state = MacPhotosRequestState<AVPlayerItem>(
            manager: manager
        )
        return await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                guard state.bind(continuation) else {
                    continuation.resume(returning: nil)
                    return
                }
                let options = PHVideoRequestOptions()
                options.deliveryMode = .highQualityFormat
                options.isNetworkAccessAllowed = true
                options.version = .current
                let requestID = manager.requestPlayerItem(
                    forVideo: asset,
                    options: options
                ) { playerItem, info in
                    guard (
                        info?[PHImageCancelledKey]
                            as? NSNumber
                    )?.boolValue != true,
                    info?[PHImageErrorKey] == nil else {
                        state.complete(nil)
                        return
                    }
                    state.complete(playerItem)
                }
                state.attach(requestID)
            }
        } onCancel: {
            state.cancel()
        }
    }

    private nonisolated static func fileName(
        for asset: PHAsset
    ) -> String {
        PHAssetResource.assetResources(for: asset).first?
            .originalFilename
            ?? String(localized: "media.type.photo")
    }

    private nonisolated static func kind(
        for asset: PHAsset
    ) -> AlbumMediaKind {
        if asset.mediaType == .video {
            return .video
        }
        if asset.mediaSubtypes.contains(.photoLive) {
            return .livePhoto
        }
        return .photo
    }

    private static func kindText(_ kind: AlbumMediaKind) -> String {
        switch kind {
        case .photo:
            return String(localized: "media.type.photo")
        case .video:
            return String(localized: "media.type.video")
        case .livePhoto:
            return String(localized: "media.type.livePhoto")
        }
    }

    private static func placeholderImage(
        kind: AlbumMediaKind
    ) -> NSImage? {
        NSImage(
            systemSymbolName: kind == .video
                ? "video.fill"
                : "photo.fill",
            accessibilityDescription: nil
        )
    }

    private static let dateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateStyle = .medium
        formatter.timeStyle = .short
        return formatter
    }()

    #if DEBUG
    private static func demoMetadataDocument(
        for item: Item
    ) -> MediaMetadataDocument {
        MediaMetadataDocument(
            sections: [
                .init(
                    title: String(
                        localized:
                            "mediaMetadata.section.file"
                    ),
                    rows: [
                        .init(label: "FileName", value: item.fileName),
                        .init(
                            label: "MediaType",
                            value: kindText(item.kind)
                        ),
                        .init(
                            label: "FileType",
                            value: item.kind == .video
                                ? "MOV"
                                : "HEIC"
                        ),
                        .init(label: "FileSize", value: "3.2 MB"),
                        .init(
                            label: "ImageSize",
                            value: item.kind == .video
                                ? "3840 × 2160"
                                : "4032 × 3024"
                        ),
                        .init(
                            label: "CreateDate",
                            value: dateFormatter.string(
                                from: item.creationDate
                            )
                        )
                    ]
                ),
                .init(
                    title: "EXIF",
                    rows: [
                        .init(
                            label: "ExposureTime",
                            value: "1/120 s"
                        ),
                        .init(label: "FNumber", value: "ƒ/1.8"),
                        .init(label: "ISOSpeedRatings", value: "80"),
                        .init(
                            label: "FocalLength",
                            value: "6.8 mm"
                        ),
                        .init(
                            label: "LensModel",
                            value: "iPhone back camera"
                        )
                    ]
                ),
                .init(
                    title: "TIFF",
                    rows: [
                        .init(label: "Make", value: "Apple"),
                        .init(label: "Model", value: "iPhone"),
                        .init(
                            label: "Software",
                            value: "Watermelon Metadata Demo"
                        )
                    ]
                )
            ],
            detailLevel: .original
        )
    }

    private static func demoMonths(
        endingAt month: LibraryMonthKey
    ) -> [LibraryMonthKey] {
        let calendar = Calendar.current
        guard let date = calendar.date(
            from: DateComponents(
                year: month.year,
                month: month.month,
                day: 1
            )
        ) else {
            return [month]
        }
        return (0 ..< 3).compactMap { offset in
            calendar.date(
                byAdding: .month,
                value: -offset,
                to: date
            ).map {
                LibraryMonthKey.from(
                    date: $0,
                    calendar: calendar
                )
            }
        }
    }

    private static func demoItems(month: LibraryMonthKey) -> [Item] {
        let calendar = Calendar.current
        let base = calendar.date(
            from: DateComponents(
                year: month.year,
                month: month.month,
                day: 24,
                hour: 14
            )
        ) ?? Date()
        return (0 ..< 28).map { index in
            let presence = index % 7
            let kind: AlbumMediaKind = index % 6 == 0
                ? .video
                : index % 5 == 0 ? .livePhoto : .photo
            let fingerprint = Data(
                repeating: UInt8(index),
                count: 32
            )
            let remote = presence == 0 ? RemoteAlbumItem(
                id: "demo-\(month.year)-\(month.month)-\(index)",
                assetFingerprint: fingerprint,
                creationDate: base.addingTimeInterval(
                    TimeInterval(-index * 4_800)
                ),
                resources: [],
                instances: [],
                representative: RemoteManifestResource(
                    year: month.year,
                    month: month.month,
                    fileName: "IMG_\(2048 + index).HEIC",
                    contentHash: fingerprint,
                    fileSize: 3_000_000,
                    resourceType: 1,
                    creationDateMs: nil,
                    backedUpAtMs: 0
                ),
                mediaKind: kind,
                contentHashes: [fingerprint],
                isIncomplete: false,
                missingResourceCount: 0
            ) : nil
            return Item(
                id: "demo:\(month.year):\(month.month):\(index)",
                month: month,
                remoteDisplayMonth: remote == nil ? nil : month,
                remoteStorageMonth: remote == nil ? nil : month,
                localAsset: nil,
                remote: remote,
                fingerprint: fingerprint,
                fileName: kind == .video
                    ? "VID_\(9100 + index).MOV"
                    : "IMG_\(2048 + index).HEIC",
                creationDate: base.addingTimeInterval(
                    TimeInterval(-index * 4_800)
                ),
                kind: kind,
                demoPresence: presence == 0
                    ? .backupOnly
                    : presence < 4 ? .both : .onMac
            )
        }
    }

    private static func demoImage(
        index: Int,
        kind: AlbumMediaKind
    ) -> NSImage {
        let size = NSSize(width: 360, height: 360)
        return NSImage(
            size: size,
            flipped: false
        ) { rect in
            NSColor.wmMaterialPhotoSurfaces[
                index % NSColor.wmMaterialPhotoSurfaces.count
            ].setFill()
            rect.fill()
            let symbol = NSImage(
                systemSymbolName: kind == .video
                    ? "play.rectangle.fill"
                    : kind == .livePhoto
                        ? "livephoto"
                        : "photo.fill",
                accessibilityDescription: nil
            )
            symbol?.draw(
                in: rect.insetBy(dx: 115, dy: 115),
                from: .zero,
                operation: .sourceOver,
                fraction: 0.75
            )
            return true
        }
    }
    #endif
}

extension MacPhotoBrowserViewController:
    NSSharingServicePickerDelegate,
    NSSharingServiceDelegate
{
    func sharingServicePicker(
        _ sharingServicePicker: NSSharingServicePicker,
        delegateFor sharingService: NSSharingService
    ) -> (any NSSharingServiceDelegate)? {
        self
    }

    func sharingServicePicker(
        _ sharingServicePicker: NSSharingServicePicker,
        didChoose sharingService: NSSharingService?
    ) {
        if sharingService == nil {
            sharingServicePicker.delegate = nil
            self.sharingServicePicker = nil
            clearSharingTemporaryFile()
        }
    }

    func sharingService(
        _ sharingService: NSSharingService,
        didShareItems items: [Any]
    ) {
        sharingServicePicker?.delegate = nil
        sharingServicePicker = nil
        clearSharingTemporaryFile()
    }

    func sharingService(
        _ sharingService: NSSharingService,
        didFailToShareItems items: [Any],
        error: any Error
    ) {
        sharingServicePicker?.delegate = nil
        sharingServicePicker = nil
        clearSharingTemporaryFile()
        presentActionError(error)
    }
}

extension MacPhotoBrowserViewController: PHPhotoLibraryChangeObserver {
    nonisolated func photoLibraryDidChange(_ changeInstance: PHChange) {
        Task { @MainActor [weak self] in
            self?.schedulePhotoLibraryReload()
        }
    }
}

extension MacPhotoBrowserViewController: NSWindowDelegate {
    func windowShouldClose(_ sender: NSWindow) -> Bool {
        guard isRunningAction else { return true }
        let alert = NSAlert()
        alert.alertStyle = .warning
        alert.messageText = String(
            localized: "mediaBrowser.action.taskInProgress",
            defaultValue: "A task is already running. Try again later."
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
        let stopConfirmed =
            alert.runModal() == .alertFirstButtonReturn
        switch MacRunningTaskWindowClosePolicy.action(
            stopConfirmed: stopConfirmed,
            isTaskRunning: isRunningAction
        ) {
        case .keepOpen:
            return false
        case .close:
            return true
        case .stopThenClose:
            closeWhenActionFinishes = true
            actionTask?.cancel()
            previewStatus.stringValue = String(
                localized: "backup.session.stopping",
                defaultValue: "Stopping safely…"
            )
            return false
        }
    }
}

extension MacPhotoBrowserViewController:
    NSCollectionViewDataSource,
    NSCollectionViewDelegate
{
    func numberOfSections(
        in collectionView: NSCollectionView
    ) -> Int {
        visibleSections.count
    }

    func collectionView(
        _ collectionView: NSCollectionView,
        numberOfItemsInSection section: Int
    ) -> Int {
        guard visibleSections.indices.contains(section) else {
            return 0
        }
        return visibleSections[section].items.count
    }

    func collectionView(
        _ collectionView: NSCollectionView,
        itemForRepresentedObjectAt indexPath: IndexPath
    ) -> NSCollectionViewItem {
        let cell = collectionView.makeItem(
            withIdentifier: MacPhotoBrowserCollectionItem.identifier,
            for: indexPath
        ) as? MacPhotoBrowserCollectionItem
            ?? MacPhotoBrowserCollectionItem()
        guard let item = item(at: indexPath) else {
            return cell
        }
        let placeholder = Self.placeholderImage(kind: item.kind)
        cell.configure(
            id: item.id,
            image: thumbnailCache[item.id] ?? placeholder,
            presence: item.badgeText
        )
        thumbnailTasks[indexPath]?.cancel()
        let target = flowLayout.itemSize.width * (
            view.window?.backingScaleFactor ?? 2
        )
        thumbnailTasks[indexPath] = Task { [weak self, weak cell] in
            guard let self else { return }
            let image = await thumbnail(
                for: item,
                targetSize: CGSize(width: target, height: target)
            )
            guard !Task.isCancelled,
                  cell?.representedID == item.id,
                  let image else {
                return
            }
            cell?.setImage(image)
        }
        return cell
    }

    func collectionView(
        _ collectionView: NSCollectionView,
        viewForSupplementaryElementOfKind kind:
            NSCollectionView.SupplementaryElementKind,
        at indexPath: IndexPath
    ) -> NSView {
        let header = collectionView.makeSupplementaryView(
            ofKind: kind,
            withIdentifier:
                MacPhotoBrowserMonthHeaderView.identifier,
            for: indexPath
        ) as? MacPhotoBrowserMonthHeaderView
            ?? MacPhotoBrowserMonthHeaderView()
        guard visibleSections.indices.contains(indexPath.section) else {
            header.configure(month: nil, itemCount: 0)
            return header
        }
        let section = visibleSections[indexPath.section]
        header.configure(
            month: section.month,
            itemCount: section.items.count
        )
        return header
    }

    func collectionView(
        _ collectionView: NSCollectionView,
        didSelectItemsAt indexPaths: Set<IndexPath>
    ) {
        updateSelectionPresentation()
        guard collectionView.selectionIndexPaths.count == 1,
              let indexPath =
                collectionView.selectionIndexPaths.first else {
            if collectionView.selectionIndexPaths.isEmpty {
                showEmptyPreview()
            }
            return
        }
        showPreview(for: indexPath)
    }

    func collectionView(
        _ collectionView: NSCollectionView,
        didDeselectItemsAt indexPaths: Set<IndexPath>
    ) {
        updateSelectionPresentation()
        guard collectionView.selectionIndexPaths.count == 1,
              let indexPath =
                collectionView.selectionIndexPaths.first else {
            if collectionView.selectionIndexPaths.isEmpty {
                showEmptyPreview()
            }
            return
        }
        showPreview(for: indexPath)
    }

    func collectionView(
        _ collectionView: NSCollectionView,
        didEndDisplaying item: NSCollectionViewItem,
        forRepresentedObjectAt indexPath: IndexPath
    ) {
        thumbnailTasks[indexPath]?.cancel()
        thumbnailTasks[indexPath] = nil
    }
}

@MainActor
private final class MacPhotoBrowserMonthHeaderView: NSView {
    static let identifier = NSUserInterfaceItemIdentifier(
        "MacPhotoBrowserMonthHeaderView"
    )

    private let titleLabel = NSTextField(labelWithString: "")
    private let countLabel = NSTextField(labelWithString: "")

    override init(frame frameRect: NSRect) {
        super.init(frame: frameRect)

        titleLabel.font = .systemFont(ofSize: 14, weight: .semibold)
        countLabel.font = .monospacedDigitSystemFont(
            ofSize: 11,
            weight: .regular
        )
        countLabel.textColor = .secondaryLabelColor

        let row = NSStackView(
            views: [titleLabel, countLabel, NSView()]
        )
        row.orientation = .horizontal
        row.alignment = .centerY
        row.spacing = 8
        row.translatesAutoresizingMaskIntoConstraints = false
        addSubview(row)
        NSLayoutConstraint.activate([
            row.leadingAnchor.constraint(
                equalTo: leadingAnchor,
                constant: 4
            ),
            row.trailingAnchor.constraint(
                equalTo: trailingAnchor,
                constant: -4
            ),
            row.centerYAnchor.constraint(equalTo: centerYAnchor),
        ])
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func configure(
        month: LibraryMonthKey?,
        itemCount: Int
    ) {
        guard let month else {
            titleLabel.stringValue = ""
            countLabel.stringValue = ""
            return
        }
        titleLabel.stringValue = month.displayText
        titleLabel.textColor = NSColor.wmMaterialMonthTitle(
            for: month.month
        )
        countLabel.stringValue = String.localizedStringWithFormat(
            String(localized: "home.localAlbums.assetCount"),
            itemCount
        )
    }
}

@MainActor
private final class MacPhotoBrowserCollectionItem:
    NSCollectionViewItem
{
    static let identifier = NSUserInterfaceItemIdentifier(
        "MacPhotoBrowserCollectionItem"
    )

    private let photoView = NSImageView()
    private let presenceLabel = NSTextField(labelWithString: "")
    private(set) var representedID: String?

    override func loadView() {
        view = NSView()
        view.wantsLayer = true
        view.layer?.backgroundColor =
            NSColor.controlBackgroundColor.cgColor
        view.layer?.borderColor = NSColor.clear.cgColor
        view.layer?.borderWidth = 2
        view.layer?.cornerRadius = 8
        view.layer?.masksToBounds = true

        photoView.imageScaling = .scaleProportionallyUpOrDown
        photoView.wantsLayer = true
        photoView.layer?.contentsGravity = .resizeAspectFill
        photoView.layer?.backgroundColor =
            NSColor.windowBackgroundColor.cgColor

        presenceLabel.font = .systemFont(ofSize: 10, weight: .semibold)
        presenceLabel.textColor = .white
        presenceLabel.backgroundColor = NSColor.black.withAlphaComponent(0.62)
        presenceLabel.drawsBackground = true
        presenceLabel.wantsLayer = true
        presenceLabel.layer?.cornerRadius = 4
        presenceLabel.alignment = .center

        for subview in [photoView, presenceLabel] {
            subview.translatesAutoresizingMaskIntoConstraints = false
            view.addSubview(subview)
        }
        NSLayoutConstraint.activate([
            photoView.topAnchor.constraint(equalTo: view.topAnchor),
            photoView.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            photoView.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            photoView.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            presenceLabel.leadingAnchor.constraint(
                equalTo: view.leadingAnchor,
                constant: 7
            ),
            presenceLabel.topAnchor.constraint(
                equalTo: view.topAnchor,
                constant: 7
            ),
            presenceLabel.heightAnchor.constraint(equalToConstant: 20)
        ])
    }

    override var isSelected: Bool {
        didSet {
            view.layer?.borderColor = isSelected
                ? NSColor.controlAccentColor.cgColor
                : NSColor.clear.cgColor
        }
    }

    override func prepareForReuse() {
        super.prepareForReuse()
        representedID = nil
        photoView.image = nil
    }

    func configure(
        id: String,
        image: NSImage?,
        presence: String
    ) {
        representedID = id
        photoView.image = image
        presenceLabel.stringValue = "  \(presence)  "
        presenceLabel.isHidden = presence.isEmpty
    }

    func setImage(_ image: NSImage) {
        photoView.image = image
    }
}
