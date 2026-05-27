import Foundation
import Photos

typealias MonthKey = LibraryMonthKey

enum BackupMonthScheduler {

    static func buildMonthAssetIDsByMonth(
        from assetsResult: PHFetchResult<PHAsset>
    ) -> [MonthKey: [PhotoKitLocalIdentifier]] {
        var assetsByMonth: [MonthKey: [PhotoKitLocalIdentifier]] = [:]
        assetsByMonth.reserveCapacity(32)

        for index in 0 ..< assetsResult.count {
            let asset = assetsResult.object(at: index)
            let monthKey = AssetProcessor.monthKey(for: asset.creationDate)
            assetsByMonth[monthKey, default: []].append(PhotoKitLocalIdentifier(asset))
        }
        return assetsByMonth
    }

    static func buildMonthAssetIDsByMonth(
        from assets: [PHAsset]
    ) -> [MonthKey: [PhotoKitLocalIdentifier]] {
        var assetsByMonth: [MonthKey: [PhotoKitLocalIdentifier]] = [:]
        assetsByMonth.reserveCapacity(32)

        for asset in assets {
            let monthKey = AssetProcessor.monthKey(for: asset.creationDate)
            assetsByMonth[monthKey, default: []].append(PhotoKitLocalIdentifier(asset))
        }
        return assetsByMonth
    }

    static func buildMonthPlans(
        assetLocalIdentifiersByMonth: [MonthKey: [PhotoKitLocalIdentifier]],
        estimatedBytesByMonth: [MonthKey: Int64]
    ) -> [MonthWorkItem] {

        var plans: [MonthWorkItem] = []
        plans.reserveCapacity(assetLocalIdentifiersByMonth.count)
        for (month, monthAssetIDs) in assetLocalIdentifiersByMonth {
            plans.append(MonthWorkItem(
                month: month,
                assetLocalIdentifiers: monthAssetIDs,
                estimatedBytes: estimatedBytesByMonth[month] ?? 0
            ))
        }

        plans.sort { lhs, rhs in
            if lhs.estimatedBytes != rhs.estimatedBytes {
                return lhs.estimatedBytes > rhs.estimatedBytes
            }
            if lhs.assetLocalIdentifiers.count != rhs.assetLocalIdentifiers.count {
                return lhs.assetLocalIdentifiers.count > rhs.assetLocalIdentifiers.count
            }
            return lhs.month < rhs.month
        }

        return plans
    }

    static func resolveWorkerCount(
        profile: ServerProfileRecord,
        monthCount: Int,
        override: Int?
    ) -> Int {
        let lowerBound = 1
        let upperBound = 4
        let protocolDefault: Int
        switch profile.resolvedStorageType {
        case .smb:
            protocolDefault = 2
        case .webdav:
            protocolDefault = 2
        case .externalVolume:
            protocolDefault = 3
        case .s3:
            protocolDefault = 2
        case .sftp:
            protocolDefault = 2
        }

        let requested = override ?? protocolDefault
        let clampedByPolicy = max(lowerBound, min(upperBound, requested))
        let clampedByWorkload = max(lowerBound, min(clampedByPolicy, max(monthCount, 1)))
        return clampedByWorkload
    }

    static func resolveConnectionPoolSize(
        profile: ServerProfileRecord,
        workerCount: Int,
        override: Int?
    ) -> Int {
        switch profile.resolvedStorageType {
        case .smb, .webdav, .s3, .sftp:
            if override != nil {
                return max(1, workerCount)
            }
            return max(1, min(workerCount, 2))
        case .externalVolume:
            return max(1, workerCount)
        }
    }
}

// MARK: - Month Work Types

struct MonthWorkItem: Sendable {
    let month: MonthKey
    let assetLocalIdentifiers: [PhotoKitLocalIdentifier]
    let estimatedBytes: Int64
}

struct WorkerRunState: Sendable {
    var paused: Bool = false
}

struct AggregatedProgressState: Sendable {
    let state: BackupRunState
    let position: Int
    let timingSummary: String?
}

struct DispatchSlot: Sendable {
    let position: Int
    let total: Int
}

actor MonthWorkQueue {
    private let months: [MonthWorkItem]
    private var nextIndex: Int = 0

    init(months: [MonthWorkItem]) {
        self.months = months
    }

    func next() -> MonthWorkItem? {
        guard nextIndex < months.count else { return nil }
        let month = months[nextIndex]
        nextIndex += 1
        return month
    }
}

// MARK: - Progress Aggregation

enum ProvisionalRecordStatus: Sendable, Equatable {
    case success
    case skipped
}

actor ParallelBackupProgressAggregator {
    private var state: BackupRunState
    private var stageTimingWindow = StageTimingWindow()
    private var scheduledCount = 0
    /// Per-month, per-fingerprint, per-`assetLocalIdentifier` buffer of provisional success/skipped
    /// records. Two local assets that hash to the same content fingerprint each contribute their
    /// own row (matching the hash-index intent queue's `(month, fingerprint, assetLocalIdentifier)`
    /// key); collapsing by fingerprint would under-count the rollback when one batch holds both
    /// duplicates and the commit fails. Entries stay until either `markBatchDurable` (the batch
    /// commit covering them landed) or `rollBackProvisionalBatch` (hard-abort flush failure)
    /// processes them. Counters advance at `record(result:)` time and revert on rollback so the
    /// visible progress matches durable outcomes after reconciliation.
    private var provisionalByMonth: [LibraryMonthKey: [AssetFingerprint: [PhotoKitLocalIdentifier: ProvisionalRecordStatus]]] = [:]

    init(total: Int) {
        state = BackupRunState(total: total)
    }

    func allocateDispatchSlot() -> DispatchSlot {
        scheduledCount += 1
        return DispatchSlot(position: max(scheduledCount, 1), total: max(state.total, 1))
    }

    func reduceTotalForEmptyAsset() {
        state.total = max(state.total - 1, 0)
    }

    func record(result: AssetProcessResult) -> AggregatedProgressState {
        switch result.status {
        case .success:
            state.succeeded += 1
        case .failed:
            state.failed += 1
        case .skipped:
            state.skipped += 1
        }

        stageTimingWindow.record(result)
        let summary = stageTimingWindow.takeSummaryIfNeeded(
            processed: state.processed,
            total: state.total
        )
        return AggregatedProgressState(
            state: state,
            position: max(state.processed, 1),
            timingSummary: summary
        )
    }

    func recordFailure() -> AggregatedProgressState {
        state.failed += 1
        stageTimingWindow.record(nil)
        let summary = stageTimingWindow.takeSummaryIfNeeded(
            processed: state.processed,
            total: state.total
        )
        return AggregatedProgressState(
            state: state,
            position: max(state.processed, 1),
            timingSummary: summary
        )
    }

    func recordMonthSkipped(count: Int) -> AggregatedProgressState {
        guard count > 0 else {
            return AggregatedProgressState(
                state: state,
                position: max(state.processed, 1),
                timingSummary: nil
            )
        }
        state.skipped += count
        stageTimingWindow.recordSkipped(count: count)
        let summary = stageTimingWindow.takeSummaryIfNeeded(
            processed: state.processed,
            total: state.total
        )
        return AggregatedProgressState(
            state: state,
            position: max(state.processed, 1),
            timingSummary: summary
        )
    }

    func recordProvisional(
        month: LibraryMonthKey,
        fingerprint: AssetFingerprint,
        assetLocalIdentifier: PhotoKitLocalIdentifier,
        status: ProvisionalRecordStatus
    ) {
        provisionalByMonth[month, default: [:]][fingerprint, default: [:]][assetLocalIdentifier] = status
    }

    func markBatchDurable(
        month: LibraryMonthKey,
        committedAssetFingerprints: Set<AssetFingerprint>
    ) {
        guard var byFingerprint = provisionalByMonth[month] else { return }
        for fp in committedAssetFingerprints {
            byFingerprint.removeValue(forKey: fp)
        }
        if byFingerprint.isEmpty {
            provisionalByMonth.removeValue(forKey: month)
        } else {
            provisionalByMonth[month] = byFingerprint
        }
    }

    /// Hard-abort reconciliation. Returns the affected fingerprints so callers can roll back the
    /// matching hash-index intents on the AssetProcessor side. Counter decrements happen per
    /// `(fingerprint, assetLocalIdentifier)` cell so duplicate-fingerprint local assets each
    /// revert their provisional row exactly once.
    @discardableResult
    func rollBackProvisionalBatch(month: LibraryMonthKey) -> Set<AssetFingerprint> {
        guard let byFingerprint = provisionalByMonth.removeValue(forKey: month) else {
            return []
        }
        for (_, byLocalID) in byFingerprint {
            for (_, status) in byLocalID {
                switch status {
                case .success:
                    state.succeeded = max(state.succeeded - 1, 0)
                case .skipped:
                    state.skipped = max(state.skipped - 1, 0)
                }
                state.failed += 1
            }
        }
        return Set(byFingerprint.keys)
    }

    func provisionalCountForTest(month: LibraryMonthKey) -> Int {
        provisionalByMonth[month]?.count ?? 0
    }

    func markPaused() {
        state.paused = true
    }

    func finalTimingSummary() -> String? {
        stageTimingWindow.takeSummaryIfNeeded(
            processed: state.processed,
            total: state.total,
            force: true
        )
    }

    func snapshot() -> BackupRunState {
        state
    }
}

// MARK: - Stage Timing

struct StageTimingWindow {
    private static let batchSize = 200

    private var processedCount = 0
    private var timedCount = 0
    private var exportHashSeconds: TimeInterval = 0
    private var collisionCheckSeconds: TimeInterval = 0
    private var uploadBodySeconds: TimeInterval = 0
    private var setModificationDateSeconds: TimeInterval = 0
    private var databaseSeconds: TimeInterval = 0
    private var totalFileSizeBytes: Int64 = 0
    private var uploadedFileSizeBytes: Int64 = 0
    private var firstRecordAt: CFAbsoluteTime?
    private var lastRecordAt: CFAbsoluteTime?
    private var firstUploadRecordAt: CFAbsoluteTime?
    private var lastUploadRecordAt: CFAbsoluteTime?

    mutating func recordSkipped(count: Int) {
        guard count > 0 else { return }
        let now = CFAbsoluteTimeGetCurrent()
        if firstRecordAt == nil {
            firstRecordAt = now
        }
        lastRecordAt = now
        processedCount += count
    }

    mutating func record(_ result: AssetProcessResult?) {
        let now = CFAbsoluteTimeGetCurrent()
        if firstRecordAt == nil {
            firstRecordAt = now
        }
        lastRecordAt = now

        processedCount += 1
        guard let result else { return }
        timedCount += 1
        exportHashSeconds += result.timing.exportHashSeconds
        collisionCheckSeconds += result.timing.collisionCheckSeconds
        uploadBodySeconds += result.timing.uploadBodySeconds
        setModificationDateSeconds += result.timing.setModificationDateSeconds
        databaseSeconds += result.timing.databaseSeconds
        totalFileSizeBytes += max(result.totalFileSizeBytes, 0)
        uploadedFileSizeBytes += max(result.uploadedFileSizeBytes, 0)

        if result.uploadedFileSizeBytes > 0 {
            if firstUploadRecordAt == nil {
                firstUploadRecordAt = now
            }
            lastUploadRecordAt = now
        }
    }

    mutating func takeSummaryIfNeeded(
        processed: Int,
        total: Int,
        force: Bool = false
    ) -> String? {
        guard processedCount > 0 else { return nil }
        guard force || processedCount >= Self.batchSize else { return nil }

        let perAssetDivisor = max(Double(timedCount), 1)
        let uploadWallSeconds: TimeInterval = {
            guard let start = firstUploadRecordAt ?? firstRecordAt else { return 0 }
            guard let end = lastUploadRecordAt ?? lastRecordAt else { return 0 }
            return max(end - start, 0)
        }()
        let wallRateBytesPerSecond: Double = {
            guard uploadedFileSizeBytes > 0 else { return 0 }
            guard uploadWallSeconds > 0 else { return 0 }
            return Double(uploadedFileSizeBytes) / uploadWallSeconds
        }()
        let summedBodyRateBytesPerSecond: Double = {
            guard uploadedFileSizeBytes > 0 else { return 0 }
            guard uploadBodySeconds > 0 else { return 0 }
            return Double(uploadedFileSizeBytes) / uploadBodySeconds
        }()
        let formatKey: String.LocalizationValue = force && processedCount < Self.batchSize
            ? "backup.scheduler.stageTimingFinal"
            : "backup.scheduler.stageTimingRecent"
        let summary = String.localizedStringWithFormat(
            String(localized: formatKey),
            Int64(processedCount),
            Int64(processed),
            Int64(max(total, 1)),
            Self.formatBytes(totalFileSizeBytes),
            Self.formatBytes(uploadedFileSizeBytes),
            Self.formatBytes(Int64(wallRateBytesPerSecond.rounded())),
            Self.formatBytes(Int64(summedBodyRateBytesPerSecond.rounded())),
            uploadWallSeconds,
            exportHashSeconds,
            exportHashSeconds * 1_000 / perAssetDivisor,
            collisionCheckSeconds,
            collisionCheckSeconds * 1_000 / perAssetDivisor,
            uploadBodySeconds,
            uploadBodySeconds * 1_000 / perAssetDivisor,
            setModificationDateSeconds,
            setModificationDateSeconds * 1_000 / perAssetDivisor,
            databaseSeconds,
            databaseSeconds * 1_000 / perAssetDivisor,
            Int64(timedCount)
        )
        reset()
        return summary
    }

    private mutating func reset() {
        processedCount = 0
        timedCount = 0
        exportHashSeconds = 0
        collisionCheckSeconds = 0
        uploadBodySeconds = 0
        setModificationDateSeconds = 0
        databaseSeconds = 0
        totalFileSizeBytes = 0
        uploadedFileSizeBytes = 0
        firstRecordAt = nil
        lastRecordAt = nil
        firstUploadRecordAt = nil
        lastUploadRecordAt = nil
    }

    private static let byteFormatter: ByteCountFormatter = {
        let formatter = ByteCountFormatter()
        formatter.allowedUnits = [.useKB, .useMB, .useGB, .useTB]
        formatter.countStyle = .file
        formatter.includesUnit = true
        formatter.isAdaptive = true
        return formatter
    }()

    static func formatBytes(_ value: Int64) -> String {
        byteFormatter.string(fromByteCount: max(0, value))
    }
}
