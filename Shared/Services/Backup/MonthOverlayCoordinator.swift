import Foundation

/// Binds the optimistic-presence/overlay lifecycle to transaction boundaries. A thin,
/// behavior-preserving indirection over `RemoteIndexSyncService`: every method reproduces the
/// exact call (and order, when grouped) of the manual sites it replaces — same conditions, same
/// `month` argument. It does not change WHEN any overlay operation fires.
struct MonthOverlayCoordinator {
    let remoteIndexService: RemoteIndexSyncService

    init(remoteIndexService: RemoteIndexSyncService) {
        self.remoteIndexService = remoteIndexService
    }

    /// Hard-abort / stale-eviction boundary: drop the in-process optimistic month overlay so stale
    /// per-asset `appendAsset` rows from an aborted batch stop surfacing through the committed view.
    /// Mirrors a bare `dropOptimisticMonthIfStale(month:)` call exactly.
    func onHardAbort(month: LibraryMonthKey) {
        remoteIndexService.dropOptimisticMonthIfStale(month: month)
    }
}
