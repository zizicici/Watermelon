import Photos
import UIKit
import os.log

private let actionLog = Logger(subsystem: "com.zizicici.watermelon", category: "MediaBrowserActions")

// Executes the viewer's per-item actions (share / save / upload / delete). Kept out of the viewer so the
// action set can grow without touching the paging UI. Each action self-presents its own confirmation,
// progress, and result. Network/write actions are serialized and gated on no other task running; a
// per-run `onRemoved` (the presenting viewer's own closure) reloads the grid + closes once the asset is gone.
@MainActor
final class MediaBrowserActionRunner {
    struct Environment {
        let appSession: AppSession
        let backupCoordinator: BackupCoordinator
        // Download restores the FULL asset (all resources, hash-verified) instead of importing a subset.
        let restoreService: RestoreService
        let photoLibraryService: PhotoLibraryService
        let hashIndexRepository: ContentHashIndexRepository
        // Single source of truth for local/remote/both — invalidated after a library change, queried for
        // upload success. Shared with every source so a mutation is reflected everywhere on the next reload.
        let presenceIndex: LibraryPresenceIndex
        // The app-wide single-execution mutex — an on-demand upload / remote-delete takes it so it's
        // mutually exclusive with a Home backup / maintenance (and vice versa).
        let appRuntimeFlags: AppRuntimeFlags
        // True while a backup / download / maintenance task is running.
        let isTaskActive: () -> Bool
        // Inherited so an on-demand upload behaves exactly like a normal backup.
        let iCloudPhotoBackupMode: () -> ICloudPhotoBackupMode
        let monthGroupingTimeZone: () -> MonthGroupingTimeZonePreference
    }

    private let env: Environment
    private var isRunningAction = false
    private var isSharing = false

    // True while a browser-initiated action/batch is mid-flight (distinct from a Home task via `isTaskActive`).
    // Lets the grid block a mode switch that would strand a running batch's HUD over unrelated content.
    var isActionRunning: Bool { isRunningAction }

    init(env: Environment) {
        self.env = env
    }

    // Share is always available. Upload / download / delete are disallowed while a task is running (a
    // backup/download in progress, or another browser action mid-flight).
    func canRun(_ kind: MediaBrowserActionKind) -> Bool {
        switch kind {
        case .share: return true
        case .upload, .download, .deleteLocal, .deleteRemote:
            return !env.isTaskActive() && !isRunningAction
        }
    }

    // `onChanged(dismiss:downloadedLocalID:)` reloads the grid; `dismiss: true` also closes the viewer
    // (delete/upload jump back to the grid; download stays so the user keeps viewing the now-saved item).
    // `downloadedLocalID` (download only) lets the still-open viewer flip the acted item to on-device so
    // it stops offering Download.
    func run(_ kind: MediaBrowserActionKind, item: MediaBrowserItem, source: MediaBrowserSource, from presenter: UIViewController, onChanged: @escaping (_ dismiss: Bool, _ downloadedLocalID: String?) -> Void) {
        guard canRun(kind) else {
            // The action sheet was built when this action was runnable, but a task started (or another action
            // began) while it was open — tell the user instead of silently no-op'ing. (.share is always runnable.)
            presentError(String(localized: "mediaBrowser.action.taskInProgress"), on: presenter)
            return
        }
        switch kind {
        case .share:
            Task { await self.share(item, source: source, from: presenter) }
        case .download:
            runGated { await self.download(item, source: source, from: presenter, onChanged: onChanged) }
        case .deleteLocal:
            runGated { await self.deleteLocal(item, source: source, from: presenter, onChanged: onChanged) }
        case .upload:
            runGated { await self.upload(item, from: presenter, onChanged: onChanged) }
        case .deleteRemote:
            runGated { await self.deleteRemote(item, from: presenter, onChanged: onChanged) }
        }
    }

    // Serializes network/write actions; the flag re-enables them (canRun) when the action finishes.
    private func runGated(_ body: @escaping () async -> Void) {
        isRunningAction = true
        Task { await body(); self.isRunningAction = false }
    }

    // A completing action must not present on / dismiss a viewer the user already closed.
    private func isAlive(_ presenter: UIViewController) -> Bool { presenter.viewIfLoaded?.window != nil }

    // MARK: - Share

    private func share(_ item: MediaBrowserItem, source: MediaBrowserSource, from presenter: UIViewController) async {
        // Double-tap guard: a second concurrent share would materialize a second temp original whose
        // sheet UIKit refuses to present — its completion (the only cleanup path) would never fire.
        guard !isSharing else { return }
        isSharing = true
        defer { isSharing = false }
        let items = await source.shareItems(for: item)
        guard isAlive(presenter), presenter.presentedViewController == nil else { cleanupTempShareItems(items); return }
        guard !items.isEmpty else { presentError(String(localized: "mediaBrowser.action.error"), on: presenter); return }
        let sheet = UIActivityViewController(activityItems: items, applicationActivities: nil)
        // A shared item may be a downloaded remote original in tmp/ — delete it once the sheet is done.
        sheet.completionWithItemsHandler = { [weak self] _, _, _, _ in self?.cleanupTempShareItems(items) }
        if let pop = sheet.popoverPresentationController {   // iPad requires an anchor
            pop.sourceView = presenter.view
            pop.sourceRect = CGRect(x: presenter.view.bounds.midX, y: presenter.view.bounds.midY, width: 0, height: 0)
            pop.permittedArrowDirections = []
        }
        presenter.present(sheet, animated: true)
    }

    // Delete only file URLs we created in tmp/ (remote originals); leaves PHAsset-managed local URLs alone.
    private func cleanupTempShareItems(_ items: [Any]) {
        let tmp = FileManager.default.temporaryDirectory.standardizedFileURL.path
        for case let url as URL in items where url.isFileURL && url.standardizedFileURL.path.hasPrefix(tmp) {
            try? FileManager.default.removeItem(at: url)
        }
    }

    // MARK: - Download (save a remote-only item into the Photos library)

    private func download(_ item: MediaBrowserItem, source: MediaBrowserSource, from presenter: UIViewController, onChanged: @escaping (Bool, String?) -> Void) async {
        let status = await env.photoLibraryService.requestAuthorization()
        guard status == .authorized || status == .limited else {
            presentError(String(localized: "mediaBrowser.action.noPhotoAccess"), on: presenter); return
        }
        guard let fingerprint = item.fingerprint else {
            presentError(String(localized: "mediaBrowser.action.error"), on: presenter); return
        }
        guard let profile = env.appSession.activeProfile, let password = env.appSession.activePassword else {
            presentError(String(localized: "mediaBrowser.action.notConnected"), on: presenter); return
        }
        // Incomplete remote record: importing it can only recover the resolvable subset — a NEW asset with a
        // different fingerprint that will re-upload as its own record. Get informed consent before importing.
        if item.isIncomplete {
            let confirmed = await confirmDestructive(
                title: String(localized: "mediaBrowser.action.incompleteDownload.confirmTitle"),
                message: String(localized: "mediaBrowser.action.incompleteDownload.confirmMessage"),
                confirmTitle: String(localized: "mediaBrowser.action.incompleteDownload.create"),
                confirmStyle: .default,
                on: presenter
            )
            guard confirmed else { return }
        }
        // App-wide mutex (scope): a download is a task too, so it's mutually exclusive with a backup/maintenance.
        await withExecutionLease(on: presenter) {
            let hud = HUD.show(String(localized: "mediaBrowser.action.saving"), on: presenter)
            // Restore through RestoreService (each resource hash-verified). For a complete asset this is the
            // full set; for an incomplete one it's the resolvable subset (consent already given) → a new asset
            // with a different fingerprint. writeHashIndex records the actually-imported instances honestly.
            let instances = await self.manifestInstances(for: fingerprint, month: item.remoteMonth)
            guard !instances.isEmpty else { hud.dismiss(); self.presentError(String(localized: "mediaBrowser.action.error"), on: presenter); return }
            let repo = self.env.hashIndexRepository
            do {
                let restored = try await self.env.restoreService.restoreItems(
                    items: [RestoreService.RestoreItemDescriptor(instances: instances, identity: fingerprint)],
                    profile: profile,
                    password: password,
                    onItemCompleted: { _, _, restoredItem in
                        guard let restoredItem else { return }
                        // The asset is already imported into Photos here. A hash-index write failure must NOT
                        // abort the restore or surface as a download failure — that would leave an unindexed
                        // duplicate the user re-imports on a retry. Log and continue; the local hash-index
                        // rebuild re-derives the row from the imported asset.
                        do {
                            try await Task.detached(priority: .utility) {
                                try repo.writeHashIndex(
                                    assetLocalIdentifier: restoredItem.asset.localIdentifier,
                                    remoteAssetFingerprint: restoredItem.identity,
                                    instances: restoredItem.asset.importedInstances
                                )
                            }.value
                        } catch {
                            actionLog.error("download: hash-index write failed for \(restoredItem.asset.localIdentifier, privacy: .public) (asset imported; index self-heals on next rebuild): \(String(describing: error), privacy: .public)")
                        }
                    }
                )
                guard let localID = restored.first?.asset.localIdentifier else {
                    hud.dismiss(); self.presentError(String(localized: "mediaBrowser.action.error"), on: presenter); return
                }
                self.env.presenceIndex.invalidate()
                hud.dismiss()
                // A complete download flips the viewed item to on-device (it IS the same asset). An incomplete
                // one imported a different asset (F′), so DON'T flip the remote record's badge — let the grid
                // reload re-derive (a new local item appears; the remote record stays as-is).
                onChanged(false, item.isIncomplete ? nil : localID)
                if self.isAlive(presenter) { HUD.flash(String(localized: "mediaBrowser.action.saved"), on: presenter) }
            } catch {
                hud.dismiss()
                self.presentError(String(localized: "mediaBrowser.action.error"), on: presenter)
            }
        }
    }

    // All manifest resource instances (every role) for a fingerprint, from the shared snapshot — this item's
    // month first (a grouping-TZ twin shares the fingerprint under another month). Off-main.
    private func manifestInstances(for fingerprint: Data, month: LibraryMonthKey?) async -> [RemoteAssetResourceInstance] {
        let coordinator = env.backupCoordinator
        return await withCancellableDetachedValue(priority: .userInitiated) {
            Self.resolveInstances(from: coordinator.currentRemoteSnapshotState(since: nil), fingerprint: fingerprint, preferredMonth: month)
        }
    }

    // Pure resolution of one fingerprint's RESOLVABLE instances from an already-materialized snapshot, preferring
    // `preferredMonth` (a grouping-TZ twin shares the fingerprint across two months). Full set for a complete
    // asset, available subset for an incomplete one (the caller took the user's consent). Factored out so a batch
    // can materialize the snapshot ONCE and resolve every item against it instead of copying it per item.
    private nonisolated static func resolveInstances(from state: RemoteLibrarySnapshotState, fingerprint: Data, preferredMonth: LibraryMonthKey?) -> [RemoteAssetResourceInstance] {
        let ordered = [state.monthDeltas.first { $0.month == preferredMonth }].compactMap { $0 }
            + state.monthDeltas.filter { $0.month != preferredMonth }
        for delta in ordered {
            let links = delta.assetResourceLinks.filter { $0.assetFingerprint == fingerprint }
            guard !links.isEmpty else { continue }
            let byHash = Dictionary(delta.resources.map { ($0.contentHash, $0) }, uniquingKeysWith: { first, _ in first })
            let instances = links.compactMap { link -> RemoteAssetResourceInstance? in
                guard let r = byHash[link.resourceHash] else { return nil }
                return RemoteAssetResourceInstance(role: link.role, slot: link.slot, resourceHash: r.contentHash, fileName: r.fileName, fileSize: r.fileSize, remoteRelativePath: r.remoteRelativePath, creationDateMs: r.creationDateMs)
            }
            guard !instances.isEmpty else { continue }
            return instances
        }
        return []
    }

    // MARK: - Delete from device (PhotoKit shows its own system confirmation)

    private func deleteLocal(_ item: MediaBrowserItem, source: MediaBrowserSource, from presenter: UIViewController, onChanged: @escaping (Bool, String?) -> Void) async {
        guard let localID = item.localIdentifier else { return }
        // App-wide mutex (scope): a local delete mutates the library + hash index, so exclude a concurrent backup.
        await withExecutionLease(on: presenter) {
            let assets = PHAsset.fetchAssets(withLocalIdentifiers: [localID], options: nil)
            guard assets.count > 0 else {
                // A fetch miss under limited access is ambiguous — the asset may merely be excluded from the
                // selection, not gone. Purging + reporting success would flip a still-existing photo to
                // remote-only and invite a duplicate re-import.
                guard self.env.photoLibraryService.authorizationStatus() == .authorized else {
                    self.presentError(String(localized: "mediaBrowser.action.noPhotoAccess"), on: presenter)
                    return
                }
                // Genuinely gone from the library: still purge the stale hash-index row so presence reverts
                // to remote-only (re-offering Download) instead of forever reporting `.both`.
                try? self.env.hashIndexRepository.deleteIndexEntries(assetIDs: [localID])
                self.env.presenceIndex.invalidate()
                onChanged(true, nil)
                return
            }
            do {
                try await self.performLibraryChange { PHAssetChangeRequest.deleteAssets(assets) }
                // The local hash index isn't purged on PHAsset deletion — drop the row now so presence doesn't
                // keep reporting `.both`, and invalidate the browser's fp→localID map.
                try? self.env.hashIndexRepository.deleteIndexEntries(assetIDs: [localID])
                self.env.presenceIndex.invalidate()
                onChanged(true, nil)
            } catch {
                // The system delete sheet handles cancellation; only surface genuine failures.
                let ns = error as NSError
                if !(ns.domain == PHPhotosErrorDomain && ns.code == PHPhotosError.userCancelled.rawValue) {
                    self.presentError(String(localized: "mediaBrowser.action.error"), on: presenter)
                }
            }
        }
    }

    // MARK: - Upload (back up an on-device-only item to the connected remote)

    private func upload(_ item: MediaBrowserItem, from presenter: UIViewController, onChanged: @escaping (Bool, String?) -> Void) async {
        guard let localID = item.localIdentifier else { return }
        guard let profile = env.appSession.activeProfile, let password = env.appSession.activePassword else {
            presentError(String(localized: "mediaBrowser.action.notConnected"), on: presenter); return
        }
        // App-wide mutex (scope): mutually exclusive with a Home backup/maintenance; nil ⇒ another task holds it.
        await withExecutionLease(on: presenter) {
            let hud = HUD.show(String(localized: "mediaBrowser.action.uploading"), on: presenter)
            do {
                _ = try await self.env.backupCoordinator.backupAssets(
                    [localID], profile: profile, password: password,
                    iCloudPhotoBackupMode: self.env.iCloudPhotoBackupMode(),
                    monthGroupingTimeZone: self.env.monthGroupingTimeZone()
                )
                hud.dismiss()
                // Result counts can't tell "backed up" from "skipped": a `resources_reused` skip DID back the
                // asset up, while an iCloud-only / asset-gone skip did NOT. Check presence directly — the asset
                // is done iff its hash-index fingerprint now appears in the remote snapshot.
                if await self.isBackedUp(localID) {
                    // The upload added the asset to the remote — announce it so Home (and any other presence index)
                    // reconciles its rows/counts, not just this browser (Home defers until the execution lease releases).
                    NotificationCenter.default.post(name: .RemoteLibrarySnapshotDidChange, object: nil)
                    onChanged(true, nil)   // now on the remote → reload grid + return
                } else {
                    self.presentError(String(localized: "mediaBrowser.action.uploadSkipped"), on: presenter)
                }
            } catch {
                hud.dismiss()
                self.presentError(String(localized: "mediaBrowser.action.error"), on: presenter)
            }
        }
    }

    // Runs `body` while holding the app-wide execution mutex; if another task already holds it, `body` never
    // runs and the user sees "task in progress". The single door for the browser's execution-holding actions.
    private func withExecutionLease(on presenter: UIViewController, _ body: () async -> Void) async {
        guard await env.appRuntimeFlags.withExecutionLease(body) != nil else {
            presentError(String(localized: "mediaBrowser.action.taskInProgress"), on: presenter)
            return
        }
    }

    // True once the asset is represented on the remote. The upload changed the snapshot, so rebuild the
    // presence index and ask it — the single source everyone else reads too.
    private func isBackedUp(_ localID: String) async -> Bool {
        env.presenceIndex.invalidate()
        await env.presenceIndex.refresh()
        let repo = env.hashIndexRepository
        let fp: Data? = await withCancellableDetachedValue(priority: .userInitiated) {
            (try? repo.fetchAssetFingerprintRecords(assetIDs: [localID]))?[localID]?.fingerprint
        }
        guard let fp else { return false }
        // "Backed up" = complete/restorable, not merely present in the manifest — a partial upload isn't done.
        return env.presenceIndex.isBackedUp(fp)
    }

    // MARK: - Delete from backup (irreversible; requires confirmation)

    private func deleteRemote(_ item: MediaBrowserItem, from presenter: UIViewController, onChanged: @escaping (Bool, String?) -> Void) async {
        guard let fingerprint = item.fingerprint, let month = item.remoteMonth else {
            presentError(String(localized: "mediaBrowser.action.error"), on: presenter); return
        }
        guard let profile = env.appSession.activeProfile, let password = env.appSession.activePassword else {
            presentError(String(localized: "mediaBrowser.action.notConnected"), on: presenter); return
        }
        let confirmed = await confirmDestructive(
            title: String(localized: "mediaBrowser.action.deleteRemote.confirmTitle"),
            message: String(localized: "mediaBrowser.action.deleteRemote.confirmMessage"),
            confirmTitle: String(localized: "mediaBrowser.action.deleteRemote"),
            on: presenter
        )
        guard confirmed else { return }
        // The confirmation dialog may have been up while presence changed (the asset was deleted elsewhere / a
        // sync dropped it). Re-check it's still on the remote before the irreversible delete; if it's already
        // gone, just reconcile the views.
        guard env.presenceIndex.isOnRemote(fingerprint) else {
            onChanged(true, nil)
            return
        }
        // App-wide mutex (scope): don't mutate the manifest while a backup/maintenance run holds the execution.
        await withExecutionLease(on: presenter) {
            let hud = HUD.show(String(localized: "mediaBrowser.action.deletingRemote"), on: presenter)
            do {
                try await self.env.backupCoordinator.deleteRemoteAsset(profile: profile, password: password, month: month, assetFingerprint: fingerprint)
                // The asset left the remote — rebuild presence so local/merged views drop `.both`/stale actions.
                self.env.presenceIndex.invalidate()
                hud.dismiss()
                onChanged(true, nil)
            } catch {
                hud.dismiss()
                self.presentError(String(localized: "mediaBrowser.action.error"), on: presenter)
            }
        }
    }

    // MARK: - Batch / multi-target operations (grid multi-select + viewer "delete all")

    // Delete-all for a single viewer item (removes it from every place it lives). Same core as the batch delete.
    // `onChanged` receives whether a remote delete failed, so the viewer can stay open (keeping the error visible)
    // instead of dismissing over it.
    func runDeleteAll(_ item: MediaBrowserItem, from presenter: UIViewController, onChanged: @escaping (_ hadFailures: Bool) -> Void) {
        guard canRun(.deleteRemote) else {
            presentError(String(localized: "mediaBrowser.action.taskInProgress"), on: presenter); return
        }
        runGated { await self.performDeletion(items: [item], from: presenter, onChanged: onChanged) }
    }

    // Grid multi-select entry point. Availability is decided by BatchActionResolver before this is called.
    func runBatch(_ action: BatchAction, items: [MediaBrowserItem], from presenter: UIViewController, onChanged: @escaping () -> Void) {
        let gate: MediaBrowserActionKind = action == .download ? .download : (action == .upload ? .upload : .deleteRemote)
        guard canRun(gate) else {
            presentError(String(localized: "mediaBrowser.action.taskInProgress"), on: presenter); return
        }
        switch action {
        case .upload: runGated { await self.batchUpload(items, from: presenter, onChanged: onChanged) }
        case .download: runGated { await self.batchDownload(items, from: presenter, onChanged: onChanged) }
        case .delete: runGated { await self.performDeletion(items: items, from: presenter, onChanged: { _ in onChanged() }) }
        }
    }

    // Delete `items` from EVERY place they live: on-device ones from the Photos library (one change + hash-index
    // purge), on-remote ones from the backup (looped — no batch delete API). A backup removal is involved → confirm
    // first (device-only deletes are confirmed by PhotoKit's own system sheet). Device is deleted first so a cancel
    // at the system sheet leaves the backup intact.
    private func performDeletion(items: [MediaBrowserItem], from presenter: UIViewController, onChanged: @escaping (_ hadFailures: Bool) -> Void) async {
        let deviceItems = items.filter(\.isDeviceDeletable)
        let remoteItems = items.filter(\.isRemoteDeletable)
        guard !deviceItems.isEmpty || !remoteItems.isEmpty else { return }

        if !remoteItems.isEmpty {
            let confirmed = await confirmBatchDelete(deviceCount: deviceItems.count, remoteCount: remoteItems.count, on: presenter)
            guard confirmed else { return }
        }

        await withExecutionLease(on: presenter) {
            // The remote loop's per-item deleteRemoteAsset each posts a snapshot change; suspend the reactive
            // presence rebuild so it happens once (on resume) instead of ~N times.
            self.env.presenceIndex.suspendUpstreamRefresh()
            defer { self.env.presenceIndex.resumeUpstreamRefresh() }
            if !deviceItems.isEmpty {
                let localIDs = deviceItems.compactMap(\.localIdentifier)
                let assets = PHAsset.fetchAssets(withLocalIdentifiers: localIDs, options: nil)
                if assets.count > 0 {
                    do {
                        try await self.performLibraryChange { PHAssetChangeRequest.deleteAssets(assets) }
                    } catch {
                        let ns = error as NSError
                        // User backed out at the system delete sheet — abort before the irreversible backup delete.
                        if ns.domain == PHPhotosErrorDomain && ns.code == PHPhotosError.userCancelled.rawValue { return }
                        self.presentError(String(localized: "mediaBrowser.action.error"), on: presenter)
                        return
                    }
                }
                // Under limited access an unfetched asset may still exist (excluded from the selection), not
                // be gone — purging its row would flip it to remote-only and invite a duplicate re-import.
                var purgeIDs = localIDs
                if self.env.photoLibraryService.authorizationStatus() != .authorized {
                    var fetched = Set<String>()
                    assets.enumerateObjects { asset, _, _ in fetched.insert(asset.localIdentifier) }
                    purgeIDs = localIDs.filter { fetched.contains($0) }
                    if purgeIDs.count < localIDs.count {
                        self.presentError(String(localized: "mediaBrowser.action.noPhotoAccess"), on: presenter)
                    }
                }
                try? self.env.hashIndexRepository.deleteIndexEntries(assetIDs: purgeIDs)
            }

            // Any device deletions above already happened; still fall through to the final reconcile so the grid
            // reflects them even if the remote leg can't run (disconnected) or some deletes fail.
            var remoteDeleteFailed = false
            if !remoteItems.isEmpty {
                if let profile = self.env.appSession.activeProfile, let password = self.env.appSession.activePassword {
                    let hud = HUD.show(self.deletingProgressText(0, remoteItems.count), on: presenter)
                    var completed = 0
                    var failed = 0
                    for item in remoteItems {
                        if Task.isCancelled { break }
                        guard let fp = item.fingerprint, let month = item.remoteMonth else { continue }
                        if self.env.presenceIndex.isOnRemote(fp) {
                            do {
                                try await self.env.backupCoordinator.deleteRemoteAsset(profile: profile, password: password, month: month, assetFingerprint: fp)
                            } catch {
                                failed += 1
                                actionLog.error("batch deleteRemote failed for \(fp.hexString, privacy: .public): \(String(describing: error), privacy: .public)")
                            }
                        }
                        completed += 1
                        hud.update(self.deletingProgressText(completed, remoteItems.count))
                    }
                    hud.dismiss()
                    NotificationCenter.default.post(name: .RemoteLibrarySnapshotDidChange, object: nil)
                    // Surface a partial failure (the single-item delete does too) so the user isn't left thinking
                    // every backup item was removed when some remote deletes errored.
                    if failed > 0 {
                        remoteDeleteFailed = true
                        self.presentError(String.localizedStringWithFormat(String(localized: "mediaBrowser.batch.delete.failed"), failed), on: presenter)
                    }
                } else {
                    remoteDeleteFailed = true
                    self.presentError(String(localized: "mediaBrowser.action.notConnected"), on: presenter)
                }
            }

            self.env.presenceIndex.invalidate()
            onChanged(remoteDeleteFailed)
        }
    }

    private func batchUpload(_ items: [MediaBrowserItem], from presenter: UIViewController, onChanged: @escaping () -> Void) async {
        let localIDs = items.compactMap(\.localIdentifier)
        guard !localIDs.isEmpty else { return }
        guard let profile = env.appSession.activeProfile, let password = env.appSession.activePassword else {
            presentError(String(localized: "mediaBrowser.action.notConnected"), on: presenter); return
        }
        await withExecutionLease(on: presenter) {
            // backupAssets posts a snapshot change per committed month; suspend the reactive rebuild (the success
            // check below does its own explicit refresh, which suspension does not block).
            self.env.presenceIndex.suspendUpstreamRefresh()
            defer { self.env.presenceIndex.resumeUpstreamRefresh() }
            let hud = HUD.show(String.localizedStringWithFormat(String(localized: "mediaBrowser.batch.uploading"), localIDs.count), on: presenter)
            do {
                _ = try await self.env.backupCoordinator.backupAssets(
                    Set(localIDs), profile: profile, password: password,
                    iCloudPhotoBackupMode: self.env.iCloudPhotoBackupMode(),
                    monthGroupingTimeZone: self.env.monthGroupingTimeZone()
                )
                hud.dismiss()
                let backedUp = await self.backedUpCount(localIDs)
                if backedUp == localIDs.count {
                    NotificationCenter.default.post(name: .RemoteLibrarySnapshotDidChange, object: nil)
                    onChanged()
                    if self.isAlive(presenter) {
                        HUD.flash(String.localizedStringWithFormat(String(localized: "mediaBrowser.batch.uploaded"), backedUp), on: presenter)
                    }
                } else if backedUp > 0 {
                    NotificationCenter.default.post(name: .RemoteLibrarySnapshotDidChange, object: nil)
                    onChanged()
                    // Skipped items are NOT on the backup — an alert, not a success flash.
                    self.presentError(
                        String.localizedStringWithFormat(String(localized: "mediaBrowser.batch.uploadPartial"), backedUp, localIDs.count - backedUp),
                        on: presenter
                    )
                } else {
                    self.presentError(String(localized: "mediaBrowser.action.uploadSkipped"), on: presenter)
                }
            } catch {
                hud.dismiss()
                self.presentError(String(localized: "mediaBrowser.action.error"), on: presenter)
            }
        }
    }

    // Count of the uploaded local IDs now represented on the remote. Rebuilds presence once, then queries it.
    private func backedUpCount(_ localIDs: [String]) async -> Int {
        env.presenceIndex.invalidate()
        await env.presenceIndex.refresh()
        let repo = env.hashIndexRepository
        let fingerprints: [String: Data] = await withCancellableDetachedValue(priority: .userInitiated) {
            let records = (try? repo.fetchAssetFingerprintRecords(assetIDs: Set(localIDs))) ?? [:]
            return records.compactMapValues { $0.fingerprint }
        }
        return localIDs.filter { id in
            guard let fp = fingerprints[id] else { return false }
            return env.presenceIndex.isBackedUp(fp)
        }.count
    }

    private func batchDownload(_ items: [MediaBrowserItem], from presenter: UIViewController, onChanged: @escaping () -> Void) async {
        let status = await env.photoLibraryService.requestAuthorization()
        guard status == .authorized || status == .limited else {
            presentError(String(localized: "mediaBrowser.action.noPhotoAccess"), on: presenter); return
        }
        guard let profile = env.appSession.activeProfile, let password = env.appSession.activePassword else {
            presentError(String(localized: "mediaBrowser.action.notConnected"), on: presenter); return
        }
        // Dedup by fingerprint: a grouping-TZ boundary photo is intentionally re-uploaded under two months, so two
        // same-fingerprint remote twins can both be selected — restoring both would import the identical asset twice.
        var seenFingerprints = Set<Data>()
        let downloadable = items.filter { item in
            guard item.presence == .remoteOnly, let fp = item.fingerprint else { return false }
            return seenFingerprints.insert(fp).inserted
        }
        guard !downloadable.isEmpty else { return }

        // Incomplete records import only their resolvable subset → new, differently-fingerprinted assets. One
        // upfront 3-way consent for the whole batch, mirroring Home's whole-month restore prompt.
        var policy: IncompleteDownloadPolicy = .createNewAsset
        let incompleteCount = downloadable.filter(\.isIncomplete).count
        if incompleteCount > 0 {
            guard let chosen = await confirmIncompleteBatch(count: incompleteCount, on: presenter) else { return }
            policy = chosen
        }
        let toRestore = policy == .skip ? downloadable.filter { !$0.isIncomplete } : downloadable
        guard !toRestore.isEmpty else { return }

        await withExecutionLease(on: presenter) {
            let hud = HUD.show(self.downloadingProgressText(0, toRestore.count), on: presenter)
            // Materialize the remote snapshot ONCE and resolve every item against it — not one full-snapshot copy
            // per item.
            let coordinator = self.env.backupCoordinator
            let descriptors: [RestoreService.RestoreItemDescriptor] = await withCancellableDetachedValue(priority: .userInitiated) {
                let state = coordinator.currentRemoteSnapshotState(since: nil)
                var out: [RestoreService.RestoreItemDescriptor] = []
                out.reserveCapacity(toRestore.count)
                for item in toRestore {
                    guard let fp = item.fingerprint else { continue }
                    let instances = Self.resolveInstances(from: state, fingerprint: fp, preferredMonth: item.remoteMonth)
                    guard !instances.isEmpty else { continue }
                    out.append(RestoreService.RestoreItemDescriptor(instances: instances, identity: fp))
                }
                return out
            }
            guard !descriptors.isEmpty else {
                hud.dismiss(); self.presentError(String(localized: "mediaBrowser.action.error"), on: presenter); return
            }
            let repo = self.env.hashIndexRepository
            let total = descriptors.count
            let savedCount = Counter()
            do {
                _ = try await self.env.restoreService.restoreItems(
                    items: descriptors, profile: profile, password: password,
                    onItemCompleted: { index, _, restoredItem in
                        await MainActor.run { hud.update(self.downloadingProgressText(index, total)) }
                        guard let restoredItem else { return }
                        savedCount.increment()
                        // As in the single download: a hash-index write failure must not fail the restore (the
                        // asset is already imported; the index self-heals on the next rebuild).
                        do {
                            try await Task.detached(priority: .utility) {
                                try repo.writeHashIndex(
                                    assetLocalIdentifier: restoredItem.asset.localIdentifier,
                                    remoteAssetFingerprint: restoredItem.identity,
                                    instances: restoredItem.asset.importedInstances
                                )
                            }.value
                        } catch {
                            actionLog.error("batch download: hash-index write failed for \(restoredItem.asset.localIdentifier, privacy: .public): \(String(describing: error), privacy: .public)")
                        }
                    }
                )
                self.env.presenceIndex.invalidate()
                hud.dismiss()
                // No .RemoteLibrarySnapshotDidChange here: a download adds LOCAL assets and changes no remote facts
                // (matches the single-download path). Grid refresh comes via onChanged; presence via invalidate().
                onChanged()
                if self.isAlive(presenter) { HUD.flash(String(localized: "mediaBrowser.action.saved"), on: presenter) }
            } catch {
                // restoreItems is fail-fast: items before the failing one are already imported and indexed.
                // Reconcile the grid (their tiles must flip, the stale selection must clear) and report the
                // partial count — a bare "error" would read as nothing saved and invite a duplicating retry.
                self.env.presenceIndex.invalidate()
                hud.dismiss()
                let saved = savedCount.current
                if saved > 0 {
                    self.presentError(
                        String.localizedStringWithFormat(String(localized: "mediaBrowser.batch.download.partial"), saved, total),
                        on: presenter
                    )
                } else {
                    self.presentError(String(localized: "mediaBrowser.action.error"), on: presenter)
                }
                onChanged()
            }
        }
    }

    private func confirmBatchDelete(deviceCount: Int, remoteCount: Int, on presenter: UIViewController) async -> Bool {
        var lines: [String] = []
        if remoteCount > 0 { lines.append(String.localizedStringWithFormat(String(localized: "mediaBrowser.batch.delete.remoteLine"), remoteCount)) }
        if deviceCount > 0 { lines.append(String.localizedStringWithFormat(String(localized: "mediaBrowser.batch.delete.deviceLine"), deviceCount)) }
        return await confirmDestructive(
            title: String(localized: "mediaBrowser.batch.delete.confirmTitle"),
            message: lines.joined(separator: "\n"),
            confirmTitle: String(localized: "mediaBrowser.action.delete"),
            on: presenter
        )
    }

    private func confirmIncompleteBatch(count: Int, on presenter: UIViewController) async -> IncompleteDownloadPolicy? {
        await withCheckedContinuation { (continuation: CheckedContinuation<IncompleteDownloadPolicy?, Never>) in
            let once = ActionOnce()
            let resume: (IncompleteDownloadPolicy?) -> Void = { value in if once.take() { continuation.resume(returning: value) } }
            let token = DeinitObserver { resume(nil) }
            // Reuse Home's whole-month restore consent strings (same semantics, fully plural-localized in all locales).
            let message = String.localizedStringWithFormat(String(localized: "home.incompleteDownload.message"), count)
            let alert = UIAlertController(title: String(localized: "home.incompleteDownload.title"), message: message, preferredStyle: .alert)
            alert.addAction(UIAlertAction(title: String(localized: "home.incompleteDownload.createAll"), style: .default) { _ in _ = token; resume(.createNewAsset) })
            alert.addAction(UIAlertAction(title: String(localized: "home.incompleteDownload.skip"), style: .default) { _ in _ = token; resume(.skip) })
            alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel) { _ in _ = token; resume(nil) })
            guard isAlive(presenter) else { resume(nil); return }
            presenter.present(alert, animated: true)
        }
    }

    private func downloadingProgressText(_ done: Int, _ total: Int) -> String {
        String.localizedStringWithFormat(String(localized: "mediaBrowser.batch.downloading"), done, total)
    }

    private func deletingProgressText(_ done: Int, _ total: Int) -> String {
        String.localizedStringWithFormat(String(localized: "mediaBrowser.batch.deleting"), done, total)
    }

    private func confirmDestructive(title: String, message: String, confirmTitle: String, confirmStyle: UIAlertAction.Style = .destructive, on presenter: UIViewController) async -> Bool {
        await withCheckedContinuation { (continuation: CheckedContinuation<Bool, Never>) in
            // Resume exactly once. If the alert is torn down without a button tap (the presenter/browser was
            // dismissed while it was up), the token captured by its actions deinits and resolves to `false` —
            // otherwise the action never returns and `isRunningAction` would stay stuck forever.
            let once = ActionOnce()
            let resume: (Bool) -> Void = { value in if once.take() { continuation.resume(returning: value) } }
            let token = DeinitObserver { resume(false) }
            let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
            alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel) { _ in _ = token; resume(false) })
            alert.addAction(UIAlertAction(title: confirmTitle, style: confirmStyle) { _ in _ = token; resume(true) })
            guard isAlive(presenter) else { resume(false); return }
            presenter.present(alert, animated: true)
        }
    }

    // MARK: - Helpers

    private func performLibraryChange(_ changes: @escaping () -> Void) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            PHPhotoLibrary.shared().performChanges(changes) { success, error in
                if let error { continuation.resume(throwing: error) }
                else if success { continuation.resume() }
                else { continuation.resume(throwing: CocoaError(.fileWriteUnknown)) }
            }
        }
    }

    private func presentError(_ message: String, on presenter: UIViewController) {
        guard isAlive(presenter) else { return }
        let alert = UIAlertController(title: nil, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        presenter.present(alert, animated: true)
    }
}

// One-shot latch so a continuation is resumed exactly once. Main-actor confined (only touched from the
// runner and UIKit callbacks on the main thread).
@MainActor
private final class ActionOnce {
    private var done = false
    func take() -> Bool { if done { return false }; done = true; return true }
}

// Thread-safe tally for progress callbacks that hop threads.
private final class Counter: @unchecked Sendable {
    private let lock = NSLock()
    private var value = 0
    func increment() { lock.withLock { value += 1 } }
    var current: Int { lock.withLock { value } }
}

// Runs its closure on deallocation — lets a confirmation continuation resolve when its alert is torn down
// without a button tap. `@unchecked Sendable` only because `deinit` isn't statically main-actor-provable;
// the alert that owns it is a UIKit object, so release (and thus deinit) happens on the main thread.
private final class DeinitObserver: @unchecked Sendable {
    private let onDeinit: () -> Void
    init(_ onDeinit: @escaping () -> Void) { self.onDeinit = onDeinit }
    deinit { onDeinit() }
}

// Minimal transient overlay for action progress/result (the browser is full-screen, so a plain HUD fits).
@MainActor
final class HUD {
    private let container = UIView()
    private let label = UILabel()   // stored so batch progress can update it in place

    @discardableResult
    static func show(_ text: String, symbol: String? = nil, on presenter: UIViewController) -> HUD {
        let hud = HUD()
        let blur = UIVisualEffectView(effect: UIBlurEffect(style: .systemThickMaterialDark))
        blur.layer.cornerRadius = 12
        blur.clipsToBounds = true
        let leading: UIView
        if let symbol {   // a result HUD shows an icon instead of a perpetual spinner
            let icon = UIImageView(image: UIImage(systemName: symbol))
            icon.tintColor = .white
            icon.contentMode = .scaleAspectFit
            leading = icon
        } else {
            let spinner = UIActivityIndicatorView(style: .medium)
            spinner.color = .white
            spinner.startAnimating()
            leading = spinner
        }
        let label = hud.label
        label.text = text
        label.textColor = .white
        label.font = .preferredFont(forTextStyle: .subheadline)
        let stack = UIStackView(arrangedSubviews: [leading, label])
        stack.axis = .horizontal
        stack.spacing = 10
        stack.alignment = .center
        stack.translatesAutoresizingMaskIntoConstraints = false
        blur.contentView.addSubview(stack)
        hud.container.addSubview(blur)
        blur.translatesAutoresizingMaskIntoConstraints = false
        hud.container.translatesAutoresizingMaskIntoConstraints = false
        presenter.view.addSubview(hud.container)
        NSLayoutConstraint.activate([
            hud.container.centerXAnchor.constraint(equalTo: presenter.view.centerXAnchor),
            hud.container.centerYAnchor.constraint(equalTo: presenter.view.centerYAnchor),
            blur.topAnchor.constraint(equalTo: hud.container.topAnchor),
            blur.bottomAnchor.constraint(equalTo: hud.container.bottomAnchor),
            blur.leadingAnchor.constraint(equalTo: hud.container.leadingAnchor),
            blur.trailingAnchor.constraint(equalTo: hud.container.trailingAnchor),
            stack.topAnchor.constraint(equalTo: blur.contentView.topAnchor, constant: 14),
            stack.bottomAnchor.constraint(equalTo: blur.contentView.bottomAnchor, constant: -14),
            stack.leadingAnchor.constraint(equalTo: blur.contentView.leadingAnchor, constant: 18),
            stack.trailingAnchor.constraint(equalTo: blur.contentView.trailingAnchor, constant: -18),
        ])
        return hud
    }

    static func flash(_ text: String, on presenter: UIViewController) {
        let hud = show(text, symbol: "checkmark.circle.fill", on: presenter)
        Task { @MainActor in
            try? await Task.sleep(nanoseconds: 1_200_000_000)
            hud.dismiss()
        }
    }

    func update(_ text: String) {
        label.text = text
    }

    func dismiss() {
        container.removeFromSuperview()
    }
}
