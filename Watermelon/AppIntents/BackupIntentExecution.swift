import Foundation

enum BackupIntentExecution {
    static func run<T: Sendable>(
        cancellation: BackupCancellationController,
        operation: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        try cancellation.throwIfCancelled()
        return try await withTaskCancellationHandler {
            let task = Task {
                try Task.checkCancellation()
                try cancellation.throwIfCancelled()
                return try await operation()
            }
            let handlerID = cancellation.addCancellationHandler { task.cancel() }
            defer {
                if let handlerID { cancellation.removeCancellationHandler(handlerID) }
            }
            // Keep the runtime extension until the pipeline finishes flushing and releasing its lease.
            let result = try await task.value
            try cancellation.throwIfCancelled()
            try Task.checkCancellation()
            return result
        } onCancel: {
            cancellation.cancel()
        }
    }
}

actor BackupIntentProgressReporter {
    private let progress: Progress
    private let clock = ContinuousClock()
    private let updateInterval: Duration
    private var processed = 0
    private var total = 0
    private var completedUnits: Int64 = 0
    private var retryUnits: Int64 = 0
    private var lastResourceProgress: (key: ResourceProgressKey, units: Int64, assetUnits: Int64)?
    private var preparationCurrent = 0
    private var preparationTotal = 0
    private var isFinishing = false
    private var isStopped = false
    private var lastUpdate: ContinuousClock.Instant
    private var pendingUpdate: Task<Void, Never>?
    private static let unitsPerAsset: Int64 = 1_000_000_000

    private struct ResourceProgressKey: Equatable {
        let assetID: String
        let position: Int
        let isUpload: Bool
    }

    init(progress: Progress, nodeName: String, updateInterval: Duration = .seconds(1)) {
        self.progress = progress
        self.updateInterval = max(.zero, updateInterval)
        lastUpdate = clock.now
        progress.totalUnitCount = -1
        progress.completedUnitCount = 0
        progress.localizedDescription = String(format: String(localized: "backupIntent.progress.title"), nodeName)
        progress.localizedAdditionalDescription = String(localized: "backupIntent.progress.preparing")
    }

    deinit {
        pendingUpdate?.cancel()
    }

    func receive(_ event: BackupEvent) {
        guard !isStopped else { return }
        switch event {
        case .preparationProgress(let current, let count):
            guard total == 0, !isFinishing else { return }
            preparationTotal = max(0, count)
            preparationCurrent = min(max(0, current), preparationTotal)
        case .started(let count, _):
            guard count > 0 else { return }
            total = count
        case .progress(let update):
            let count = update.succeeded + update.failed + update.skipped
            if count != processed { lastResourceProgress = nil }
            processed = count
            advance(to: Int64(processed) * Self.unitsPerAsset)
        case .transferState(let transfer):
            guard transfer.kind == .upload, total > 0 else { return }
            receiveTransfer(transfer)
        case .finished:
            isFinishing = true
        case .log, .monthChanged:
            return
        }
        publishOrSchedule()
    }

    func stop() {
        isStopped = true
        pendingUpdate?.cancel()
        pendingUpdate = nil
    }

    private var subtitle: String {
        if isFinishing || total > 0 && processed >= total {
            return String(localized: "backupIntent.progress.finishing")
        }
        if total > 0 {
            return String(format: String(localized: "backupIntent.progress.backingUp"), processed, total)
        }
        if preparationTotal > 0 {
            return String(format: String(localized: "backupIntent.progress.preparingCount"), preparationCurrent, preparationTotal)
        }
        return String(localized: "backupIntent.progress.preparing")
    }

    func complete() {
        stop()
        progress.totalUnitCount = max(1, totalUnits)
        progress.completedUnitCount = progress.totalUnitCount
        progress.localizedAdditionalDescription = String(localized: "backupIntent.progress.completed")
    }

    private func publishOrSchedule() {
        let deadline = lastUpdate.advanced(by: updateInterval)
        if clock.now >= deadline {
            publish()
        } else if pendingUpdate == nil {
            pendingUpdate = Task { [weak self] in
                do {
                    try await Task.sleep(until: deadline, clock: .continuous)
                } catch {
                    return
                }
                await self?.publishPending()
            }
        }
    }

    private func publishPending() {
        guard !Task.isCancelled, !isStopped else { return }
        pendingUpdate = nil
        publishOrSchedule()
    }

    private func publish() {
        guard !isStopped else { return }
        pendingUpdate?.cancel()
        pendingUpdate = nil
        let text = subtitle
        guard progress.totalUnitCount != totalUnits
            || progress.completedUnitCount != completedUnits
            || progress.localizedAdditionalDescription != text else { return }
        lastUpdate = clock.now
        if progress.totalUnitCount != totalUnits { progress.totalUnitCount = totalUnits }
        if progress.completedUnitCount != completedUnits { progress.completedUnitCount = completedUnits }
        if progress.localizedAdditionalDescription != text { progress.localizedAdditionalDescription = text }
    }

    private var totalUnits: Int64 {
        total > 0 ? Int64(total) * Self.unitsPerAsset + retryUnits + 1 : -1
    }

    private func receiveTransfer(_ transfer: BackupTransferState) {
        // Final skip/failure estimates aren't a new preparation attempt.
        guard transfer.countsTowardTransferSpeed || transfer.resourceBytesTransferred == nil else { return }
        let rawFraction: Double
        if let bytes = transfer.resourceBytesTransferred,
           let size = transfer.resourceTotalBytes, size > 0 {
            rawFraction = Double(bytes) / Double(size)
        } else {
            rawFraction = Double(transfer.resourceFraction)
        }
        guard rawFraction.isFinite else { return }
        let fraction = min(1, max(0, rawFraction))
        let resourceCount = max(1, transfer.totalResources)
        let position = min(max(1, transfer.resourcePosition), resourceCount)
        let phaseUnits = Self.unitsPerAsset / 2
        let resourceUnits = Int64(fraction * Double(phaseUnits) / Double(resourceCount))
        let key = ResourceProgressKey(
            assetID: transfer.assetLocalIdentifier,
            position: position,
            isUpload: transfer.countsTowardTransferSpeed
        )
        let phaseOffset = transfer.countsTowardTransferSpeed ? phaseUnits : 0
        let resourceOffset = Int64(position - 1) * phaseUnits / Int64(resourceCount)
        let assetUnits = phaseOffset + resourceOffset + resourceUnits
        if let previous = lastResourceProgress, previous.key.assetID == key.assetID {
            // Reconnection can restart preparation of the entire asset.
            if !key.isUpload, position == 1, assetUnits < previous.assetUnits {
                retryUnits += previous.assetUnits
            } else if previous.key == key, resourceUnits < previous.units {
                retryUnits += previous.units
            }
        }
        lastResourceProgress = (key, resourceUnits, assetUnits)
        advance(to: Int64(processed) * Self.unitsPerAsset + assetUnits)
    }

    private func advance(to units: Int64) {
        guard total > 0 else { return }
        // The final unit belongs to durable manifest flush and lease release.
        completedUnits = max(completedUnits, min(units, Int64(total) * Self.unitsPerAsset) + retryUnits)
    }
}
