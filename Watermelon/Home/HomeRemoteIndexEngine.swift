import Foundation

struct HomeRemoteDelta {
    let changedMonths: Set<LibraryMonthKey>
}

/// In-memory mirror of remote month manifests, keyed by `RemoteLibrarySnapshotState.revision`.
///
/// **Concurrency contract**: same as `HomeLocalIndexEngine` — callers must
/// serialize access. `@unchecked Sendable` is granted because
/// `HomeDataProcessingWorker` runs all engine calls on its `processingQueue`.
final class HomeRemoteIndexEngine: @unchecked Sendable {
    private var remoteFingerprintsByMonth: [LibraryMonthKey: Set<Data>] = [:]
    private var summaryByMonth: [LibraryMonthKey: HomeMonthSummary] = [:]

    private(set) var snapshotRevision: UInt64?

    var allMonths: Set<LibraryMonthKey> {
        Set(remoteFingerprintsByMonth.keys)
    }

    func fingerprints(for month: LibraryMonthKey) -> Set<Data> {
        remoteFingerprintsByMonth[month] ?? []
    }

    func summary(for month: LibraryMonthKey) -> HomeMonthSummary? {
        summaryByMonth[month]
    }

    func apply(
        state: RemoteLibrarySnapshotState,
        hasActiveConnection: Bool
    ) -> HomeRemoteDelta {
        var changedMonths = Set<LibraryMonthKey>()

        guard hasActiveConnection else {
            if !remoteFingerprintsByMonth.isEmpty {
                changedMonths.formUnion(remoteFingerprintsByMonth.keys)
                clearRemoteState()
            }
            snapshotRevision = state.revision
            return HomeRemoteDelta(changedMonths: changedMonths)
        }

        if snapshotRevision == state.revision, !state.isFullSnapshot {
            return HomeRemoteDelta(changedMonths: changedMonths)
        }

        if state.isFullSnapshot {
            changedMonths.formUnion(remoteFingerprintsByMonth.keys)
            clearRemoteState()
        }

        // Resolve here (on the worker queue, off-main) via the shared resolver, then map onto Home types.
        for resolved in RemoteMonthResolver.resolveMany(state.monthDeltas) {
            let month = resolved.month
            changedMonths.insert(month)
            remoteFingerprintsByMonth[month] = resolved.fingerprints.isEmpty ? nil : resolved.fingerprints
            summaryByMonth[month] = resolved.assetCount > 0
                ? HomeMonthSummary(
                    month: month,
                    assetCount: resolved.assetCount,
                    photoCount: resolved.photoCount,
                    videoCount: resolved.videoCount,
                    backedUpCount: nil,
                    totalSizeBytes: resolved.totalSizeBytes
                )
                : nil
        }

        snapshotRevision = state.revision
        return HomeRemoteDelta(changedMonths: changedMonths)
    }

    private func clearRemoteState() {
        remoteFingerprintsByMonth.removeAll()
        summaryByMonth.removeAll()
    }

}
