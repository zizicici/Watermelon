import Foundation
import UIKit
import os.log

private let fileSizeScanLog = Logger(subsystem: "com.zizicici.watermelon", category: "HomeData")

@MainActor
final class HomeFileSizeScanCoordinator {
    struct Hooks {
        let months: () async -> [LibraryMonthKey]
        let loadSizes: () async -> [String: AssetSizeSnapshot]
        let scan: (LibraryMonthKey, [String: AssetSizeSnapshot]) async -> [AssetSizeUpdate]
        let persist: ([AssetSizeUpdate]) async -> Void
    }

    private let hooks: Hooks
    private var isActive: Bool
    private var needsFullScan = false
    private var pendingMonths = Set<LibraryMonthKey>()
    private var pendingNotifications = Set<LibraryMonthKey>()
    private var revision: UInt64 = 0
    private var task: Task<Void, Never>?
    private var memoryWarningObserver: NSObjectProtocol?

    var onFileSizesUpdated: ((Set<LibraryMonthKey>) -> Void)?

    convenience init(worker: HomeDataProcessingWorker, contentHashIndexRepository repository: ContentHashIndexRepository,
                     initiallyActive: Bool = true) {
        self.init(initiallyActive: initiallyActive, hooks: Hooks(
            months: { await worker.localMonthsForFileSizeScan() },
            loadSizes: {
                await Task.detached(priority: .utility) { (try? repository.fetchAssetSizes()) ?? [:] }.value
            },
            scan: { await worker.updateFileSize(for: $0, sizeCache: $1) },
            persist: { entries in
                await Task.detached(priority: .utility) {
                    do { try repository.upsertAssetSizes(entries) }
                    catch { fileSizeScanLog.error("[HomeData] upsertAssetSizes failed: \(String(describing: error))") }
                }.value
            }
        ))
    }

    init(initiallyActive: Bool = true, hooks: Hooks) {
        self.hooks = hooks
        self.isActive = initiallyActive
        memoryWarningObserver = NotificationCenter.default.addObserver(
            forName: UIApplication.didReceiveMemoryWarningNotification, object: nil, queue: .main
        ) { [weak self] _ in
            MainActor.assumeIsolated {
                self?.setActive(false)
                self?.startFullScan()
            }
        }
    }

    deinit {
        if let memoryWarningObserver { NotificationCenter.default.removeObserver(memoryWarningObserver) }
    }

    func setActive(_ active: Bool) {
        isActive = active
        if active { flushNotifications() }
        startIfNeeded()
    }

    func startFullScan() {
        reset()
        needsFullScan = true
        startIfNeeded()
    }

    func enqueueRescan(for months: Set<LibraryMonthKey>) {
        pendingMonths.formUnion(months)
        startIfNeeded()
    }

    func reset() {
        revision &+= 1
        needsFullScan = false
        pendingMonths.removeAll()
        pendingNotifications.removeAll()
    }

    private func startIfNeeded() {
        guard isActive, task == nil, needsFullScan || !pendingMonths.isEmpty else { return }
        task = Task { [weak self] in
            guard let self else { return }
            await self.drain()
            self.task = nil
            self.startIfNeeded()
        }
    }

    private func drain() async {
        var sizes: [String: AssetSizeSnapshot] = [:]
        var loadedRevision: UInt64?
        var updates: [AssetSizeUpdate] = []
        let clock = ContinuousClock()
        var lastNotification = clock.now

        while isActive, needsFullScan || !pendingMonths.isEmpty {
            let revision = self.revision
            if loadedRevision != revision {
                updates.removeAll()
                sizes = await hooks.loadSizes()
                guard revision == self.revision else { continue }
                loadedRevision = revision
            }
            if needsFullScan {
                needsFullScan = false
                let months = await hooks.months()
                guard revision == self.revision else { continue }
                pendingMonths.formUnion(months)
            }
            // Finish each captured batch so repeated edits cannot starve older months.
            for month in pendingMonths.sorted(by: >) {
                guard isActive, revision == self.revision else { break }
                pendingMonths.remove(month)
                let computed = await hooks.scan(month, sizes)
                guard revision == self.revision else { break }
                for update in computed {
                    sizes[update.assetLocalIdentifier] = AssetSizeSnapshot(
                        totalFileSizeBytes: update.totalFileSizeBytes, modificationDateMs: update.modificationDateMs
                    )
                }
                updates.append(contentsOf: computed)
                pendingNotifications.insert(month)
                if updates.count >= 200 {
                    await hooks.persist(updates)
                    updates.removeAll(keepingCapacity: true)
                    guard revision == self.revision else { break }
                }
                if clock.now - lastNotification >= .milliseconds(250) {
                    flushNotifications()
                    lastNotification = clock.now
                }
                await Task.yield()
            }
        }
        if loadedRevision == revision, !updates.isEmpty { await hooks.persist(updates) }
        flushNotifications()
    }

    private func flushNotifications() {
        guard isActive, !pendingNotifications.isEmpty else { return }
        let months = pendingNotifications
        pendingNotifications.removeAll()
        onFileSizesUpdated?(months)
    }

    #if DEBUG
    func _testWaitUntilIdle() async {
        while let task { await task.value }
    }
    #endif
}
