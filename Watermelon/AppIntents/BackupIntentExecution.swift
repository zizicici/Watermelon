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
    private var stage = BackupStage.preparing
    private var overallCompleted: Int64 = 0
    private var retryFractionFloor = 0.0
    private var retryWorkFloor: Int64 = 0
    private var startedAt: ContinuousClock.Instant?
    private var elapsedUpdate: Task<Void, Never>?
    private var isStopped = false
    private var lastUpdate: ContinuousClock.Instant
    private var pendingUpdate: Task<Void, Never>?
    private static let unitsPerAsset: Int64 = 1_000_000_000
    private static let overallUnits: Int64 = 1_000_000_000_000

    private struct ResourceProgressKey: Equatable {
        let assetID: String
        let position: Int
        let isUpload: Bool
    }

    init(progress: Progress, nodeName: String, updateInterval: Duration = .seconds(1)) {
        self.progress = progress
        self.updateInterval = max(.zero, updateInterval)
        lastUpdate = clock.now
        progress.totalUnitCount = Self.overallUnits
        progress.completedUnitCount = 0
        progress.localizedDescription = String(format: String(localized: "backupIntent.progress.title"), nodeName)
        progress.localizedAdditionalDescription = String(localized: "backupIntent.progress.preparing")
    }

    deinit {
        pendingUpdate?.cancel()
        elapsedUpdate?.cancel()
    }

    func start() {
        guard !isStopped, startedAt == nil else { return }
        startedAt = clock.now
        publish()
        elapsedUpdate = Task { [weak self] in
            while !Task.isCancelled {
                do {
                    try await Task.sleep(for: .seconds(1))
                } catch {
                    return
                }
                guard let self else { return }
                await self.refreshEstimate()
            }
        }
    }

    func receive(_ event: BackupEvent) {
        guard !isStopped else { return }
        switch event {
        case .stageChanged(let stage):
            enter(stage)
        case .preparationProgress(let current, let count):
            guard stage.rawValue < BackupStage.backingUp.rawValue else { return }
            preparationTotal = max(0, count)
            preparationCurrent = min(max(0, current), preparationTotal)
        case .started(let count, _):
            guard count > 0 else { return }
            total = count
            enter(.backingUp)
        case .progress(let update):
            let count = update.succeeded + update.failed + update.skipped
            if count != processed { lastResourceProgress = nil }
            processed = count
            advance(to: Int64(processed) * Self.unitsPerAsset)
        case .transferState(let transfer):
            guard transfer.kind == .upload, total > 0 else { return }
            receiveTransfer(transfer)
        case .finished:
            enter(.finishing)
        case .log, .monthChanged:
            return
        }
        publishOrSchedule()
    }

    func stop() {
        isStopped = true
        pendingUpdate?.cancel()
        pendingUpdate = nil
        elapsedUpdate?.cancel()
        elapsedUpdate = nil
    }

    private func enter(_ next: BackupStage) {
        guard next.rawValue > stage.rawValue else { return }
        stage = next
        preparationCurrent = 0
        preparationTotal = 0
        overallCompleted = max(overallCompleted, stageRange.lowerBound)
    }

    private var stageRange: Range<Int64> {
        let percentages: Range<Int64>
        switch stage {
        case .preparing: percentages = 0..<10
        case .connecting: percentages = 10..<20
        case .preparingRepository: percentages = 20..<30
        case .scanningRemote: percentages = 30..<40
        case .scanningLocal: percentages = 40..<45
        case .backingUp: percentages = 45..<95
        case .finishing: percentages = 95..<100
        }
        let percent = Self.overallUnits / 100
        return (percentages.lowerBound * percent)..<(percentages.upperBound * percent)
    }

    private var stageSubtitle: String {
        switch stage {
        case .preparing:
            return String(localized: "backupIntent.progress.preparing")
        case .connecting:
            return String(localized: "backupIntent.progress.connecting")
        case .preparingRepository:
            return String(localized: "backupIntent.progress.repository")
        case .scanningRemote:
            return String(localized: "backupIntent.progress.scanningRemote")
        case .scanningLocal:
            return String(localized: "backupIntent.progress.scanningLocal")
        case .backingUp:
            return String(format: String(localized: "backupIntent.progress.backingUp"), processed, total)
        case .finishing:
            return String(localized: "backupIntent.progress.finishing")
        }
    }

    private var subtitle: String {
        var text = stageSubtitle
        if stage.rawValue < BackupStage.backingUp.rawValue, preparationTotal > 0 {
            text += " (\(preparationCurrent)/\(preparationTotal))"
        }
        if let startedAt {
            let elapsed = startedAt.duration(to: clock.now).formatted(.time(pattern: .hourMinuteSecond))
            text = String(format: String(localized: "backupIntent.progress.elapsed"), text, elapsed)
        }
        return text
    }

    func complete() {
        stop()
        progress.totalUnitCount = Self.overallUnits
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
        updateMeasuredProgress()
        let text = subtitle
        guard progress.totalUnitCount != Self.overallUnits
            || progress.completedUnitCount != overallCompleted
            || progress.localizedAdditionalDescription != text else { return }
        lastUpdate = clock.now
        if progress.totalUnitCount != Self.overallUnits { progress.totalUnitCount = Self.overallUnits }
        if progress.completedUnitCount != overallCompleted { progress.completedUnitCount = overallCompleted }
        if progress.localizedAdditionalDescription != text { progress.localizedAdditionalDescription = text }
    }

    private var backupFraction: Double {
        let remaining = Int64(total) * Self.unitsPerAsset + retryUnits - retryWorkFloor
        guard remaining > 0 else { return retryFractionFloor }
        let fraction = min(1, max(0, Double(completedUnits - retryWorkFloor) / Double(remaining)))
        return retryFractionFloor + (1 - retryFractionFloor) * fraction
    }

    private func updateMeasuredProgress() {
        let fraction: Double
        if stage == .backingUp {
            fraction = backupFraction
        } else if preparationTotal > 0 {
            fraction = Double(preparationCurrent) / Double(preparationTotal)
        } else {
            fraction = 0
        }
        let range = stageRange
        let measured = range.lowerBound + Int64(Double(range.upperBound - range.lowerBound) * fraction * 0.9)
        overallCompleted = max(overallCompleted, min(range.upperBound - 1, measured))
    }

    func refreshEstimate() {
        guard !isStopped else { return }
        updateMeasuredProgress()
        let range = stageRange
        // Estimated movement stays inside the current stage; only real events advance to the next one.
        let ceiling = range.upperBound - (range.upperBound - range.lowerBound) / 100
        if overallCompleted < ceiling {
            overallCompleted += max(1, (ceiling - overallCompleted) / 60)
        }
        publish()
    }

    private func addRetryWork(_ units: Int64) {
        retryFractionFloor = backupFraction
        retryWorkFloor = completedUnits
        retryUnits += units
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
                addRetryWork(previous.assetUnits)
            } else if previous.key == key, resourceUnits < previous.units {
                addRetryWork(previous.units)
            }
        }
        lastResourceProgress = (key, resourceUnits, assetUnits)
        advance(to: Int64(processed) * Self.unitsPerAsset + assetUnits)
    }

    private func advance(to units: Int64) {
        guard total > 0 else { return }
        completedUnits = max(completedUnits, min(units, Int64(total) * Self.unitsPerAsset) + retryUnits)
    }
}
