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

    private(set) var state: State?
    private(set) var lastError: Error?
    private var task: Task<Void, Never>?
    private var observers: [UUID: () -> Void] = [:]

    var isRunning: Bool { state != nil }

    nonisolated init(
        buildService: LocalHashIndexBuildService,
        photoLibraryService: PhotoLibraryService,
        hashIndexRepository: ContentHashIndexRepository,
        changePublisher: LocalIndexChangePublisher
    ) {
        self.buildService = buildService
        self.photoLibraryService = photoLibraryService
        self.hashIndexRepository = hashIndexRepository
        self.changePublisher = changePublisher
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

    func start(mode: Mode, initialIndexed: Int) {
        guard state == nil else { return }
        lastError = nil
        state = State(
            mode: mode,
            totalCount: 0,
            initialIndexed: initialIndexed,
            processedInRun: 0
        )
        notify()

        task = Task { [weak self] in
            await self?.runWork()
            self?.finish()
        }
    }

    func cancel() {
        task?.cancel()
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
        notify()
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
