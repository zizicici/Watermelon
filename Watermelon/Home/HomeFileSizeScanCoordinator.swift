import Foundation
import UIKit
import os.log

private let fileSizeScanLog = Logger(subsystem: "com.zizicici.watermelon", category: "HomeData")

/// Owns the asset-size scan working set for `HomeIncrementalDataManager`.
///
/// Two scan paths run independently here:
/// - **Startup / forceReload scan** (`startFullScan`): walks every month in the
///   library, emits by-year notifications. Cancels any in-flight rescan and
///   invalidates the size snapshot before starting.
/// - **PHChange-driven rescan** (`enqueueRescan`): partial, coalesced. Single
///   in-flight; pending months accumulate while one runs.
///
/// A refcount (`activeFileSizeScanCount`) gates `releaseAssetSizeSnapshotIfIdle`
/// so that a fast rescan completing while startup is still walking months can't
/// pull the snapshot out from under startup.
@MainActor
final class HomeFileSizeScanCoordinator {
    private static let fileSizeUpdateCoalescingDelayNs: UInt64 = 250_000_000
    private static let assetSizeWriteBackBatchSize = 200

    private enum FileSizeScanNotificationMode {
        case coalesced
        case byYear
    }

    struct Hooks {
        let localMonthsForScan: () -> [LibraryMonthKey]
        let updateFileSize: (LibraryMonthKey, [String: AssetSizeSnapshot]) async -> [AssetSizeUpdate]
    }

    private let hooks: Hooks
    private let contentHashIndexRepository: ContentHashIndexRepository

    private var pendingFileSizeMonths = Set<LibraryMonthKey>()
    private var fileSizeUpdateTask: Task<Void, Never>?
    private var assetSizeSnapshot: [String: AssetSizeSnapshot] = [:]
    private var assetSizeSnapshotLoaded = false
    private var assetSizeSnapshotLoadTask: Task<[String: AssetSizeSnapshot], Never>?
    private var assetSizeSnapshotGeneration = 0
    // Startup scan (full library, by-year notifications). At most one runs at a time; a new
    // startup cancels its predecessor. Separate from the PHChange-driven rescan below so the
    // two cannot clobber each other.
    private var fileSizeScanTask: Task<Void, Never>?
    // PHChange rescan (partial, coalesced notifications). Single in-flight:
    // pendingRescanMonths accumulates while a rescan runs; the running task drains
    // it and auto-restarts if more arrived.
    private var fileSizeRescanTask: Task<Void, Never>?
    private var pendingRescanMonths = Set<LibraryMonthKey>()
    // Refcount so invalidateAssetSizeSnapshot() at the end of a scan fires only once all
    // in-flight scans have finished. Otherwise a fast rescan completing while startup is
    // still walking months would wipe the cache out from under startup.
    private var activeFileSizeScanCount = 0
    private var memoryWarningObserver: NSObjectProtocol?

    var onFileSizesUpdated: ((Set<LibraryMonthKey>) -> Void)?

    init(hooks: Hooks, contentHashIndexRepository: ContentHashIndexRepository) {
        self.hooks = hooks
        self.contentHashIndexRepository = contentHashIndexRepository
        memoryWarningObserver = NotificationCenter.default.addObserver(
            forName: UIApplication.didReceiveMemoryWarningNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            MainActor.assumeIsolated {
                self?.handleMemoryWarning()
            }
        }
    }

    deinit {
        if let memoryWarningObserver {
            NotificationCenter.default.removeObserver(memoryWarningObserver)
        }
    }

    // MARK: - Public API (called by HomeIncrementalDataManager)

    /// Called after a successful `loadLocalIndex` with `didReload=true`. Cancels any
    /// in-flight rescan, invalidates the size snapshot (DB is the more up-to-date
    /// source after a reload), and starts a full library scan.
    func startFullScan() {
        fileSizeScanTask?.cancel()
        // A full startup scan supersedes any in-flight PHChange rescan: startup's months
        // come from localMonthsForScan() — the complete library — so rescan's
        // partial progress is a subset that startup will revisit anyway. Leaving rescan
        // running would make it continue against the invalidated snapshot below, falling
        // back to cold PHAssetResource recomputation for its remaining months. Cancel it
        // and drop pending so the refcount-guarded cache-release logic isn't fighting
        // against the startup's explicit invalidate.
        fileSizeRescanTask?.cancel()
        fileSizeRescanTask = nil
        pendingRescanMonths.removeAll()
        resetPendingFileSizeUpdates()
        // Drop the in-memory snapshot: hash-builder paths (AssetProcessor) write
        // mtime into local_assets during execution without notifying us, so after
        // a forceReload the DB is more up-to-date than our cached dict.
        invalidateAssetSizeSnapshot()
        let months = hooks.localMonthsForScan()
        fileSizeScanTask = Task { [weak self] in
            await self?.runFileSizeScan(months: months, notificationMode: .byYear)
        }
    }

    /// Called from `applyPhotoLibraryChangeNow` when the index reconciled some
    /// months. The rescan is single-in-flight and coalesces concurrent enqueues.
    func enqueueRescan(for months: Set<LibraryMonthKey>) {
        guard !months.isEmpty else { return }
        pendingRescanMonths.formUnion(months)
        guard fileSizeRescanTask == nil else { return }
        startPendingRescan()
    }

    /// Called on the auth-loss branch of `loadLocalIndex`. Cancels both scans and
    /// clears pending state. Deliberately does NOT call `invalidateAssetSizeSnapshot`:
    /// the snapshot survives auth flicker, and the next reauth will hit
    /// `startFullScan()` which invalidates anyway.
    func reset() {
        fileSizeScanTask?.cancel()
        fileSizeScanTask = nil
        fileSizeRescanTask?.cancel()
        fileSizeRescanTask = nil
        pendingRescanMonths.removeAll()
        resetPendingFileSizeUpdates()
    }

    // MARK: - Memory-warning trim

    // Cheap, UX-preserving trim on system memory pressure:
    // - cancel the PHChange rescan (it's cache-rebuild work; next PHChange re-triggers).
    // - drop pending size-coalesce + rescan month buffers.
    // - try to release the asset-size snapshot via the refcount-guarded path so we don't
    //   yank it out from under an in-flight startup scan (that would force every remaining
    //   month to cold PHAssetResource recomputation, *worsening* pressure).
    private func handleMemoryWarning() {
        fileSizeScanLog.warning("[HomeData] memory warning: cancelling rescan, releasing size snapshot if idle")
        fileSizeRescanTask?.cancel()
        fileSizeRescanTask = nil
        pendingRescanMonths.removeAll()
        resetPendingFileSizeUpdates()
        releaseAssetSizeSnapshotIfIdle()
    }

    // MARK: - Scan internals

    private func startPendingRescan() {
        guard fileSizeRescanTask == nil, !pendingRescanMonths.isEmpty else { return }
        let months = Array(pendingRescanMonths)
        pendingRescanMonths.removeAll()
        // `priority:` is a no-op here: runFileSizeScan is @MainActor, and MainActor ignores
        // task priority on its queue. The work inside the scan yields cooperatively via
        // Task.yield() between months, which is what actually lets UI interleave.
        fileSizeRescanTask = Task { [weak self] in
            await self?.runFileSizeScan(months: months, notificationMode: .coalesced)
            // If we were cancelled externally (startup supersede or auth loss), the caller
            // has already cleared the slot and the pending set; whatever lives in the slot
            // now belongs to a successor rescan, so leave it alone.
            guard let self, !Task.isCancelled else { return }
            self.fileSizeRescanTask = nil
            if !self.pendingRescanMonths.isEmpty {
                self.startPendingRescan()
            }
        }
    }

    private func invalidateAssetSizeSnapshot() {
        assetSizeSnapshot.removeAll()
        assetSizeSnapshotLoaded = false
        assetSizeSnapshotLoadTask?.cancel()
        assetSizeSnapshotLoadTask = nil
        assetSizeSnapshotGeneration &+= 1
    }

    private func releaseAssetSizeSnapshotIfIdle() {
        // Free the ~5 MB working set only once every in-flight scan has finished.
        // Releasing while another scan is mid-walk would make it recompute sizes from
        // PHAssetResource for every remaining month.
        guard activeFileSizeScanCount == 0 else { return }
        invalidateAssetSizeSnapshot()
    }

    private func runFileSizeScan(
        months: [LibraryMonthKey],
        notificationMode: FileSizeScanNotificationMode
    ) async {
        activeFileSizeScanCount += 1
        defer {
            activeFileSizeScanCount -= 1
            releaseAssetSizeSnapshotIfIdle()
        }

        let repository = contentHashIndexRepository
        await ensureAssetSizeSnapshotLoaded(repository: repository)

        let orderedMonths = notificationMode == .byYear ? months.sorted(by: >) : months
        var pendingYearMonths = Set<LibraryMonthKey>()
        var writeBackBuffer: [AssetSizeUpdate] = []

        for (index, month) in orderedMonths.enumerated() {
            if Task.isCancelled {
                Self.flushAssetSizeWriteBack(&writeBackBuffer, repository: repository)
                return
            }
            let updates = await hooks.updateFileSize(month, assetSizeSnapshot)
            if Task.isCancelled {
                mergeIntoAssetSizeSnapshot(updates)
                writeBackBuffer.append(contentsOf: updates)
                Self.flushAssetSizeWriteBack(&writeBackBuffer, repository: repository)
                return
            }

            mergeIntoAssetSizeSnapshot(updates)
            writeBackBuffer.append(contentsOf: updates)
            if writeBackBuffer.count >= Self.assetSizeWriteBackBatchSize {
                Self.flushAssetSizeWriteBack(&writeBackBuffer, repository: repository)
            }

            switch notificationMode {
            case .coalesced:
                enqueueFileSizeUpdate(for: month)
            case .byYear:
                pendingYearMonths.insert(month)
                let nextYear = index + 1 < orderedMonths.count ? orderedMonths[index + 1].year : nil
                if nextYear != month.year {
                    onFileSizesUpdated?(pendingYearMonths)
                    pendingYearMonths.removeAll(keepingCapacity: true)
                }
            }

            await Task.yield()
        }

        Self.flushAssetSizeWriteBack(&writeBackBuffer, repository: repository)

        if Task.isCancelled { return }
        if notificationMode == .coalesced {
            flushPendingFileSizeUpdates()
        }
    }

    private func ensureAssetSizeSnapshotLoaded(repository: ContentHashIndexRepository) async {
        guard !assetSizeSnapshotLoaded else { return }
        let generation = assetSizeSnapshotGeneration

        let task: Task<[String: AssetSizeSnapshot], Never>
        if let existing = assetSizeSnapshotLoadTask {
            task = existing
        } else {
            task = Task.detached(priority: .utility) {
                (try? repository.fetchAssetSizes()) ?? [:]
            }
            assetSizeSnapshotLoadTask = task
        }

        let loaded = await task.value
        // If invalidated mid-flight, a newer generation is already loading fresh data; drop ours.
        guard !assetSizeSnapshotLoaded, generation == assetSizeSnapshotGeneration else { return }
        assetSizeSnapshot = loaded
        assetSizeSnapshotLoaded = true
        assetSizeSnapshotLoadTask = nil
    }

    private func mergeIntoAssetSizeSnapshot(_ updates: [AssetSizeUpdate]) {
        guard !updates.isEmpty else { return }
        for update in updates {
            assetSizeSnapshot[update.assetLocalIdentifier] = AssetSizeSnapshot(
                totalFileSizeBytes: update.totalFileSizeBytes,
                modificationDateMs: update.modificationDateMs
            )
        }
    }

    private static func flushAssetSizeWriteBack(
        _ buffer: inout [AssetSizeUpdate],
        repository: ContentHashIndexRepository
    ) {
        guard !buffer.isEmpty else { return }
        let entries = buffer
        buffer.removeAll(keepingCapacity: true)
        Task.detached(priority: .background) {
            do {
                try repository.upsertAssetSizes(entries)
            } catch {
                fileSizeScanLog.error("[HomeData] upsertAssetSizes failed: \(String(describing: error))")
            }
        }
    }

    private func enqueueFileSizeUpdate(for month: LibraryMonthKey) {
        pendingFileSizeMonths.insert(month)
        guard fileSizeUpdateTask == nil else { return }

        fileSizeUpdateTask = Task { [weak self] in
            do {
                try await Task.sleep(nanoseconds: Self.fileSizeUpdateCoalescingDelayNs)
            } catch {
                return
            }

            guard let self, !Task.isCancelled else { return }
            self.flushPendingFileSizeUpdates()
        }
    }

    private func flushPendingFileSizeUpdates() {
        let months = pendingFileSizeMonths
        pendingFileSizeMonths.removeAll()
        fileSizeUpdateTask?.cancel()
        fileSizeUpdateTask = nil

        guard !months.isEmpty else { return }
        onFileSizesUpdated?(months)
    }

    private func resetPendingFileSizeUpdates() {
        pendingFileSizeMonths.removeAll()
        fileSizeUpdateTask?.cancel()
        fileSizeUpdateTask = nil
    }
}
