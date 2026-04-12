import Foundation

@MainActor
final class HomeExecutionDataRefresher {
    typealias RemoteSync = () async -> Set<LibraryMonthKey>
    typealias LocalRefresh = (Set<String>) async -> Set<LibraryMonthKey>

    var onStateChanged: (() -> Void)?

    private let syncRemoteData: RemoteSync
    private let refreshLocalIndex: LocalRefresh

    private var remoteSyncTask: Task<Void, Never>?
    private var remoteSyncRequested = false
    private var remoteSyncWaiters: [UUID: CheckedContinuation<Set<LibraryMonthKey>, Never>] = [:]
    private var pendingChangedMonths = Set<LibraryMonthKey>()

    init(
        syncRemoteData: @escaping RemoteSync,
        refreshLocalIndex: @escaping LocalRefresh
    ) {
        self.syncRemoteData = syncRemoteData
        self.refreshLocalIndex = refreshLocalIndex
    }

    func reset() {
        remoteSyncRequested = false
        pendingChangedMonths.removeAll()
    }

    func cancel() {
        remoteSyncTask?.cancel()
        remoteSyncTask = nil
        remoteSyncRequested = false
        pendingChangedMonths.removeAll()

        let waiters = Array(remoteSyncWaiters.values)
        remoteSyncWaiters.removeAll()
        for waiter in waiters {
            waiter.resume(returning: [])
        }
    }

    func consumePendingChangedMonths() -> Set<LibraryMonthKey> {
        defer { pendingChangedMonths.removeAll() }
        return pendingChangedMonths
    }

    func scheduleRemoteSync() {
        remoteSyncRequested = true
        ensureRemoteSyncTask()
    }

    func syncRemoteDataAndWait() async -> Set<LibraryMonthKey> {
        let waiterID = UUID()
        return await withTaskCancellationHandler {
            if Task.isCancelled {
                return []
            }

            remoteSyncRequested = true
            ensureRemoteSyncTask()
            return await withCheckedContinuation { continuation in
                if Task.isCancelled {
                    continuation.resume(returning: [])
                    return
                }
                remoteSyncWaiters[waiterID] = continuation
            }
        } onCancel: {
            Task { @MainActor [weak self] in
                self?.resumeRemoteSyncWaiter(id: waiterID, returning: [])
            }
        }
    }

    func refreshLocalIndexAndNotify(_ assetIDs: Set<String>) async {
        let changedMonths = await refreshLocalIndex(assetIDs)
        guard !changedMonths.isEmpty else { return }
        pendingChangedMonths.formUnion(changedMonths)
        onStateChanged?()
    }

    private func ensureRemoteSyncTask() {
        guard remoteSyncTask == nil else { return }

        remoteSyncTask = Task { [weak self] in
            guard let self else { return }

            var aggregatedChangedMonths = Set<LibraryMonthKey>()
            while self.remoteSyncRequested {
                self.remoteSyncRequested = false
                if Task.isCancelled { break }

                let changedMonths = await self.syncRemoteData()
                if Task.isCancelled { break }
                aggregatedChangedMonths.formUnion(changedMonths)
                self.pendingChangedMonths.formUnion(changedMonths)

                if !changedMonths.isEmpty {
                    self.onStateChanged?()
                }
            }

            let waiters = Array(self.remoteSyncWaiters.values)
            self.remoteSyncWaiters.removeAll()
            self.remoteSyncTask = nil

            for waiter in waiters {
                waiter.resume(returning: aggregatedChangedMonths)
            }
        }
    }

    private func resumeRemoteSyncWaiter(id: UUID, returning changedMonths: Set<LibraryMonthKey>) {
        guard let waiter = remoteSyncWaiters.removeValue(forKey: id) else { return }
        waiter.resume(returning: changedMonths)
    }
}
