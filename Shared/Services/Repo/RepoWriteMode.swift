import Foundation

// Lives beside WriteLockService rather than in the iOS target: the subsystems that write under a lease
// (MonthManifestStore, VersionManifestLite, OrphanCleanupLite, V1ToLiteMigration) are all shared, and when
// this type was app-only they could reach the lease only through an erased `() async throws -> Void`. That
// erasure is what discarded the hazard class of each write and forced every call site onto the strongest gate.
struct RepoWriteMode: Sendable {
    let session: AnyRepoWriteSession
    let liteMonthsListing: LiteMonthsListingSnapshot?

    static func lite<Session: RepoWriteSession>(
        _ session: Session,
        _ monthsListing: LiteMonthsListingSnapshot?
    ) -> RepoWriteMode {
        RepoWriteMode(session: AnyRepoWriteSession(session), liteMonthsListing: monthsListing)
    }

    var manifestLayout: MonthManifestStore.ManifestLayout {
        .lite
    }

    // Both lease gates for the subsystems that write under this mode. Neither writes the lock — the refresh
    // task remains the sole lock writer.
    var ownershipGates: RepoOwnershipGates? {
        RepoWriteGuard.ownershipGates(session)
    }

    func stopAndRelease() async {
        await session.release()
    }
}
