import Foundation
import Photos

enum LocalIndexBuildError: LocalizedError {
    case photoPermissionDenied

    var errorDescription: String? {
        switch self {
        case .photoPermissionDenied:
            return String(localized: "home.localIndex.error.permissionDenied")
        }
    }
}

@MainActor
final class LocalIndexBuildCoordinator {
    private static let automaticRevalidationBatchSize = 16

    enum Mode {
        case incremental
        case rebuild
    }

    struct State {
        let mode: Mode
        var totalCount: Int
        var initialIndexed: Int
        var processedInRun: Int

        var displayedIndexed: Int {
            min(initialIndexed + processedInRun, totalCount)
        }
    }

    private let buildService: LocalHashIndexBuildService
    private let photoLibraryService: PhotoLibraryService
    private let hashIndexRepository: ContentHashIndexRepository
    private let changePublisher: LocalIndexChangePublisher
    private let appRuntimeFlags: AppRuntimeFlags

    private(set) var state: State?
    private(set) var lastError: Error?
    private var executionClaim: AppRuntimeFlags.ExecutionClaim?
    private var task: Task<Void, Never>?
    private var automaticRevalidationTask: Task<Void, Never>?
    private var pendingRevalidationAssetIDs = Set<String>()
    private var observers: [UUID: () -> Void] = [:]

    var isRunning: Bool { state != nil }
    var canStart: Bool { state == nil && !appRuntimeFlags.isExecuting }

    nonisolated init(
        buildService: LocalHashIndexBuildService,
        photoLibraryService: PhotoLibraryService,
        hashIndexRepository: ContentHashIndexRepository,
        changePublisher: LocalIndexChangePublisher,
        appRuntimeFlags: AppRuntimeFlags
    ) {
        self.buildService = buildService
        self.photoLibraryService = photoLibraryService
        self.hashIndexRepository = hashIndexRepository
        self.changePublisher = changePublisher
        self.appRuntimeFlags = appRuntimeFlags
    }

    deinit {
        task?.cancel()
        automaticRevalidationTask?.cancel()
        if let executionClaim {
            appRuntimeFlags.exitExecution(executionClaim)
        }
    }

    @discardableResult
    func addObserver(_ block: @escaping () -> Void) -> UUID {
        let id = UUID()
        observers[id] = block
        return id
    }

    func removeObserver(_ id: UUID) {
        observers.removeValue(forKey: id)
    }

    private func notify() {
        for block in observers.values {
            block()
        }
    }

    private func notifyRunningStateChanged() {
        NotificationCenter.default.post(name: .LocalIndexBuildStateDidChange, object: self)
    }

    @discardableResult
    func start(mode: Mode, initialIndexed: Int) -> Bool {
        guard state == nil,
              let executionClaim = appRuntimeFlags.tryEnterExecution() else { return false }
        self.executionClaim = executionClaim
        automaticRevalidationTask?.cancel()
        pendingRevalidationAssetIDs.removeAll()
        lastError = nil
        state = State(
            mode: mode,
            totalCount: 0,
            initialIndexed: initialIndexed,
            processedInRun: 0
        )
        notify()
        notifyRunningStateChanged()

        task = Task { [weak self] in
            await self?.runWork()
            self?.finish()
        }
        return true
    }

    func cancel() {
        task?.cancel()
    }

    func scheduleFingerprintRevalidation(for assetIDs: Set<String>) {
        guard !assetIDs.isEmpty else { return }
        pendingRevalidationAssetIDs.formUnion(assetIDs)
        scheduleAutomaticRevalidationIfNeeded()
    }

    func handleExecutionLifecycleChange() {
        if !appRuntimeFlags.isExecuting {
            scheduleAutomaticRevalidationIfNeeded()
        } else {
            automaticRevalidationTask?.cancel()
        }
    }

    private func runWork() async {
        guard let initialState = state else { return }
        let mode = initialState.mode
        var didClear = false
        var processedIDs = Set<String>()

        do {
            try await Self.ensureAuthorization(photoLibraryService: photoLibraryService)
            try Task.checkCancellation()

            if mode == .rebuild {
                try await Self.clearIndex(repository: hashIndexRepository)
                didClear = true
                try Task.checkCancellation()
                updateState { $0.initialIndexed = 0 }
            }

            let allIDs = await Self.collectAllAssetIDs(photoLibraryService: photoLibraryService)
            try Task.checkCancellation()
            updateState { $0.totalCount = allIDs.count }

            guard !allIDs.isEmpty else { return }

            let processIDs: Set<String>
            switch mode {
            case .rebuild:
                processIDs = allIDs
            case .incremental:
                processIDs = await Self.computeIncrementalProcessIDs(
                    repository: hashIndexRepository,
                    photoLibraryService: photoLibraryService,
                    assetIDs: allIDs
                )
                try Task.checkCancellation()
            }

            guard !processIDs.isEmpty else { return }
            processedIDs = processIDs

            _ = try await buildService.buildIndex(
                for: processIDs,
                workerCount: 2,
                allowNetworkAccess: false,
                tickHandler: { [weak self] processed, _ in
                    await self?.applyProgress(processed: processed)
                }
            )
        } catch is CancellationError {
        } catch {
            lastError = error
        }

        if didClear {
            changePublisher.publish(.bulkInvalidation)
        } else if !processedIDs.isEmpty {
            changePublisher.publish(.touched(assetIDs: processedIDs))
        }
    }

    private func updateState(_ block: (inout State) -> Void) {
        guard var current = state else { return }
        block(&current)
        state = current
        notify()
    }

    private func applyProgress(processed: Int) {
        updateState { $0.processedInRun = processed }
    }

    private func finish() {
        state = nil
        task = nil
        if let executionClaim {
            self.executionClaim = nil
            appRuntimeFlags.exitExecution(executionClaim)
        }
        notify()
        notifyRunningStateChanged()
        scheduleAutomaticRevalidationIfNeeded()
    }

    private func scheduleAutomaticRevalidationIfNeeded() {
        guard automaticRevalidationTask == nil,
              state == nil,
              !pendingRevalidationAssetIDs.isEmpty,
              !appRuntimeFlags.isExecuting else { return }
        automaticRevalidationTask = Task { [weak self] in
            try? await Task.sleep(nanoseconds: 500_000_000)
            await self?.runAutomaticRevalidationBatch()
        }
    }

    private func runAutomaticRevalidationBatch() async {
        defer { finishAutomaticRevalidationBatch() }
        guard !Task.isCancelled,
              state == nil,
              !appRuntimeFlags.isExecuting else { return }

        let assetIDs = Set(
            pendingRevalidationAssetIDs.prefix(Self.automaticRevalidationBatchSize)
        )
        pendingRevalidationAssetIDs.subtract(assetIDs)

        guard let result = try? await buildService.buildIndex(
            for: assetIDs,
            workerCount: min(2, max(assetIDs.count, 1)),
            allowNetworkAccess: false
        ) else { return }
        if !result.readyAssetIDs.isEmpty {
            changePublisher.publish(.touched(assetIDs: result.readyAssetIDs))
        }
    }

    private func finishAutomaticRevalidationBatch() {
        automaticRevalidationTask = nil
        scheduleAutomaticRevalidationIfNeeded()
    }

    private nonisolated static func ensureAuthorization(photoLibraryService: PhotoLibraryService) async throws {
        let status = photoLibraryService.authorizationStatus()
        if status == .authorized || status == .limited { return }
        let requested = await photoLibraryService.requestAuthorization()
        guard requested == .authorized || requested == .limited else {
            throw LocalIndexBuildError.photoPermissionDenied
        }
    }

    private nonisolated static func clearIndex(repository: ContentHashIndexRepository) async throws {
        let result: Result<Void, Error> = await withCancellableDetachedValue(priority: .userInitiated) {
            do {
                try repository.clearLocalHashIndex()
                return .success(())
            } catch {
                return .failure(error)
            }
        }
        try result.get()
    }

    private nonisolated static func collectAllAssetIDs(
        photoLibraryService: PhotoLibraryService
    ) async -> Set<String> {
        await withCancellableDetachedValue(priority: .userInitiated) {
            photoLibraryService.collectAssetIDs(query: .allAssets)
        }
    }

    private nonisolated static func computeIncrementalProcessIDs(
        repository: ContentHashIndexRepository,
        photoLibraryService: PhotoLibraryService,
        assetIDs: Set<String>
    ) async -> Set<String> {
        await withCancellableDetachedValue(priority: .userInitiated) {
            let cached = (try? repository.fetchAssetHashCaches(assetIDs: assetIDs)) ?? [:]
            let unfingerprinted = assetIDs.subtracting(cached.keys)

            var modified = Set<String>()
            if !cached.isEmpty {
                let phAssets = photoLibraryService.fetchAssets(localIdentifiers: Set(cached.keys))
                for asset in phAssets {
                    guard let cache = cached[asset.localIdentifier],
                          let modificationDate = asset.modificationDate,
                          modificationDate > cache.updatedAt
                    else { continue }
                    modified.insert(asset.localIdentifier)
                }
            }

            return unfingerprinted.union(modified)
        }
    }
}
