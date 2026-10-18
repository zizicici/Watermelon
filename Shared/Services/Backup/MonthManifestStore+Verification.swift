import Foundation
import os.log

private let manifestVerifyLog = Logger(subsystem: "com.zizicici.watermelon", category: "MonthManifestVerify")

extension MonthManifestStore {
    @concurrent
    static func verifyMonth(client: RemoteStorageClientProtocol, basePath: String, month: LibraryMonthKey,
                            layout: ManifestLayout, assertOwnership: RepoOwnershipGates? = nil,
                            snapshotObserver: SnapshotObserver? = nil) async throws {
        do {
            try await verifyLoadedMonth(client: client, basePath: basePath, month: month, layout: layout,
                assertOwnership: assertOwnership, snapshotObserver: snapshotObserver)
        } catch {
            snapshotObserver?(.uncertain)
            throw error
        }
    }

    private static func verifyLoadedMonth(
        client: RemoteStorageClientProtocol,
        basePath: String,
        month: LibraryMonthKey,
        layout: MonthManifestStore.ManifestLayout,
        assertOwnership: RepoOwnershipGates? = nil,
        snapshotObserver: SnapshotObserver? = nil
    ) async throws {
        let monthRelativePath = String(format: "%04d/%02d", month.year, month.month)
        let manifestPath = layout.manifestAbsolutePath(basePath: basePath, year: month.year, month: month.month)
        func missingManifestError() -> NSError {
            NSError(
                domain: "RemoteIndexSyncService",
                code: -1,
                userInfo: [
                    NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                        String(localized: "backup.manifest.error.downloadExistingManifest"),
                        monthRelativePath
                    )
                ]
            )
        }
        // Confirmed-absent canonical (evicted): the download must fail this month closed, unlike the transient
        // -1 above which keeps last-known-good cache and stays continuable.
        func confirmedMissingManifestError() -> NSError {
            NSError(
                domain: "RemoteIndexSyncService",
                code: -2,
                userInfo: [
                    NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                        String(localized: "backup.manifest.error.downloadExistingManifest"),
                        monthRelativePath
                    )
                ]
            )
        }
        // Reconcile proved rows invalid in memory but the correction could not be durably persisted/published —
        // any fault in the prove→publish window (data-directory listing, listing reconcile, or corrective flush),
        // so the cache still holds the un-pruned rows: the download must fail this month closed (like -2) rather
        // than continue over a manifest this verify just proved invalid.
        func reconcileFlushFailedError() -> NSError {
            NSError(
                domain: "RemoteIndexSyncService",
                code: -3,
                userInfo: [
                    NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                        String(localized: "backup.manifest.error.downloadExistingManifest"),
                        monthRelativePath
                    )
                ]
            )
        }
        // Pre-check distinguishes "manifest gone" (drop stale cache entry) from "download failed" (error);
        // `loadManifestDirect` collapses both into nil. A directory at the canonical slot is damaged/foreign
        // control state: an owned verify fails it closed (existingLiteManifestConflict, which is not a
        // continuable download-verify signal) so the month is never certified completed over it; a read-only
        // verify evicts the stale cache entry like a genuinely absent manifest.
        let manifestMetadata = try await client.metadata(path: manifestPath)
        if manifestMetadata == nil || manifestMetadata?.isDirectory == true {
            if let assertOwnership {
                try await assertOwnership.assertWrite()
                if manifestMetadata?.isDirectory == true {
                    throw LiteRepoError.existingLiteManifestConflict(month: month.text)
                }
                // Owned proof the canonical is absent: evict the stale cached month so a download/restore
                // consumer can't read it as current truth, and fail the month closed (confirmed, not transient)
                // so the download never falsely completes it from the cache this verify just evicted.
                snapshotObserver?(.removed(month))
                throw confirmedMissingManifestError()
            }
            snapshotObserver?(.removed(month))
            return
        }

        let store: MonthManifestStore
        do {
            guard let loaded = try await MonthManifestStore.loadManifestDirect(
                client: client,
                basePath: basePath,
                year: month.year,
                month: month.month,
                layout: layout,
                manifestAbsolutePath: manifestPath,
                // An owned verify must persist a schema-only migration; the upgrade is gated inside
                // loadManifestDirect by `assertOwnership != nil`, so a read-only verify still never writes.
                pushSchemaUpgrade: true,
                assertOwnership: assertOwnership,
                // Surface a clear download not-found (canonical deleted between probe and fetch) as a throw so
                // the catch can evict, rather than collapsing it into the transient nil path below.
                surfaceDownloadNotFound: true,
                snapshotObserver: snapshotObserver
            ) else {
                // metadata proved the canonical present but the download faulted transiently (not a clear
                // not-found): the canonical still exists, so keep the cache so the download can use last-known-good.
                throw missingManifestError()
            }
            store = loaded
        } catch {
            // Evict the stale cached month only when the verify proves the *current canonical itself* unusable, so
            // the download path can't restore from it: a confirmed-corrupt load (-34/-35) or a clear not-found on
            // the initial canonical fetch (deleted between the metadata probe and the download). A later owned
            // schema-flush/read-back failure — even one whose error chain wraps a not-found — must surface, not
            // evict; a transient initial download fault (loadManifestDirect → nil → missingManifestError) keeps
            // the cache. Match the dedicated marker, never a chain-classified not-found.
            if isConfirmedInvalidManifest(error) {
                snapshotObserver?(.removed(month))
                throw error
            }
            if MonthManifestStore.isManifestDownloadNotFoundError(error) {
                snapshotObserver?(.removed(month))
                throw confirmedMissingManifestError()
            }
            throw error
        }

        let internalResult = try store.reconcileMonth()

        // From the in-memory prune above until the corrected manifest is published, the store may already hold
        // proof that rows are invalid (store.dirty). A non-cancellation/non-lease-loss fault anywhere in this
        // window — the data-directory listing probe, the V1 list, the listing reconcile, or the corrective flush —
        // must fail the download closed (-3); the raw retryable fault would otherwise read continuable and let a
        // consumer restore from the still-un-pruned cache this verify just proved invalid.
        do {
            let monthAbsolutePath = RemotePathBuilder.absolutePath(
                basePath: basePath,
                remoteRelativePath: monthRelativePath
            )
            let remoteFileNames: Set<String>
            if layout == .lite {
                // Lite stores month truth in .watermelon/months; a confirmed missing data directory collapses
                // to an empty listing, but any other fault surfaces. A destructive prune (whole-month clear or
                // large-ratio) of the manifest must be confirmed by a second listing before it is applied.
                let listing = try await LiteDataDirectoryProbe.probe(client: client, monthAbsolutePath: monthAbsolutePath)
                switch try await LiteDataDirectoryProbe.confirmPrune(
                    client: client,
                    monthAbsolutePath: monthAbsolutePath,
                    initial: listing,
                    manifestFileNames: store.manifestFileNames()
                ) {
                case .reconcile(let names, _):
                    remoteFileNames = names
                case .skip:
                    remoteFileNames = store.manifestFileNames()   // no listing-based prune this run
                }
            } else {
                let entries = try await client.list(path: monthAbsolutePath)
                remoteFileNames = Set(entries
                    .filter { !$0.isDirectory && $0.name != MonthManifestStore.manifestFileName }
                    .map(\.name))
            }
            let listingMissing = store.manifestFileNames().subtracting(remoteFileNames)
            let listingResult = try store.reconcileMonth(missingFileNames: listingMissing)

            let touched = internalResult.removedResourceCount + internalResult.removedAssetCount
                + internalResult.removedOrphanLinkCount
                + listingResult.removedResourceCount + listingResult.removedAssetCount
                + listingResult.removedOrphanLinkCount

            // Flush only a reconcile prune (touched > 0). A read-only verify never writes, and an owned schema-only
            // upgrade was already flushed inside loadManifestDirect. The store-owned ownership gate inside
            // flushToRemote re-asserts the lease and fails closed if it is lost.
            if touched > 0, store.dirty {
                try await store.flushToRemote()
            }
            if touched > 0 {
                manifestVerifyLog.info("[verify] \(month.text): internal=\(internalResult.removedAssetCount)+\(internalResult.removedOrphanLinkCount)L, listing=\(listingResult.removedAssetCount)+\(listingResult.removedOrphanLinkCount)L")
            }
        } catch {
            // Cancellation and lease-loss carry their own download dispositions (cancelled / upload-fail-fast);
            // surface them unchanged. Otherwise, once reconcile proved rows invalid (store.dirty) but the
            // correction could not be durably published, fail closed (-3) rather than surface the raw retryable
            // fault. With nothing proven invalid yet (not dirty), a transient listing fault stays continuable over
            // the still-valid cache (a genuinely missing data file is caught later as an incomplete-item / 404).
            if store.dirty,
               RemoteFaultLite.classify(error) != .cancelled,
               !((error as? LiteRepoError)?.isUploadFailFast ?? false) {
                throw reconcileFlushFailedError()
            }
            throw error
        }

        store.publishSnapshot()
    }

    private static func isConfirmedInvalidManifest(_ error: Error) -> Bool {
        let ns = error as NSError
        return ns.domain == "MonthManifestStore" && (ns.code == -34 || ns.code == -35)
    }
}
