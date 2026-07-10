import Photos
import UIKit
import os.log

private let actionLog = Logger(subsystem: "com.zizicici.watermelon", category: "MediaBrowserActions")

struct MediaBrowserShareMaterializationTracker: Sendable {
    private var nextToken: UInt64 = 0
    private(set) var activeToken: UInt64?

    mutating func begin() -> UInt64? {
        guard activeToken == nil else { return nil }
        nextToken &+= 1
        activeToken = nextToken
        return nextToken
    }

    mutating func finish(_ token: UInt64) -> Bool {
        guard activeToken == token else { return false }
        activeToken = nil
        return true
    }

    mutating func cancel(_ token: UInt64? = nil) -> Bool {
        guard let activeToken, token == nil || token == activeToken else { return false }
        self.activeToken = nil
        return true
    }
}

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
    private var shareTracker = MediaBrowserShareMaterializationTracker()
    private var shareTask: Task<Void, Never>?
    private var shareBackstopTask: Task<Void, Never>?
    private static let shareMaterializationTimeoutNanoseconds: UInt64 = 5 * 60 * 1_000_000_000

    // True while a browser-initiated action/batch is mid-flight (distinct from a Home task via `isTaskActive`).
    // Lets the grid block a mode switch that would strand a running batch's HUD over unrelated content.
    var isActionRunning: Bool { isRunningAction }

    // A live remote session is a hard prerequisite for upload/download; without one the action can only end in
    // a "not connected" error. Availability (viewer bar + batch bar) hides those actions when disconnected so
    // the UI never advertises a remote operation with no destination — the local browser stays usable offline.
    var isRemoteReachable: Bool { env.appSession.activeProfile != nil && env.appSession.activePassword != nil }

    init(env: Environment) {
        self.env = env
    }

    deinit {
        shareTask?.cancel()
        shareBackstopTask?.cancel()
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
            beginShare(item, source: source, from: presenter)
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

    private func beginShare(_ item: MediaBrowserItem, source: MediaBrowserSource, from presenter: UIViewController) {
        guard let token = shareTracker.begin() else { return }
        shareTask = Task { @MainActor [weak self, weak presenter] in
            let items = await source.shareItems(for: item)
            guard let self else {
                Self.cleanupTempShareItems(items)
                return
            }
            self.completeShare(token: token, items: items, presenter: presenter)
        }
        shareBackstopTask = Task { @MainActor [weak self, weak presenter] in
            do {
                try await Task.sleep(nanoseconds: Self.shareMaterializationTimeoutNanoseconds)
            } catch {
                return
            }
            guard let self, self.shareTracker.cancel(token) else { return }
            let task = self.shareTask
            self.shareTask = nil
            self.shareBackstopTask = nil
            task?.cancel()
            if let presenter {
                self.presentError(String(localized: "mediaBrowser.action.error"), on: presenter)
            }
        }
    }

    private func completeShare(token: UInt64, items: [Any], presenter: UIViewController?) {
        guard shareTracker.finish(token) else {
            Self.cleanupTempShareItems(items)
            return
        }
        shareBackstopTask?.cancel()
        shareBackstopTask = nil
        shareTask = nil
        guard let presenter, isAlive(presenter), presenter.presentedViewController == nil else {
            Self.cleanupTempShareItems(items)
            return
        }
        guard !items.isEmpty else {
            presentError(String(localized: "mediaBrowser.action.error"), on: presenter)
            return
        }
        let sheet = UIActivityViewController(activityItems: items, applicationActivities: nil)
        // A shared item may be a downloaded remote original in tmp/ — delete it once the sheet is done.
        sheet.completionWithItemsHandler = { _, _, _, _ in Self.cleanupTempShareItems(items) }
        if let pop = sheet.popoverPresentationController {   // iPad requires an anchor
            pop.sourceView = presenter.view
            pop.sourceRect = CGRect(x: presenter.view.bounds.midX, y: presenter.view.bounds.midY, width: 0, height: 0)
            pop.permittedArrowDirections = []
        }
        presenter.present(sheet, animated: true)
    }

    func cancelShareMaterialization() {
        guard shareTracker.cancel() else { return }
        let task = shareTask
        shareTask = nil
        shareBackstopTask?.cancel()
        shareBackstopTask = nil
        task?.cancel()
    }

    // Delete only file URLs we created in tmp/ (remote originals); leaves PHAsset-managed local URLs alone.
    private static func cleanupTempShareItems(_ items: [Any]) {
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
        // Resolve the resolvable subset from the LIVE snapshot and decide consent on its FRESH completeness:
        // `item.isIncomplete` was captured at projection and can lag a mid-session record degradation (a linked
        // resource dropped from the backup while the viewer stayed open), which would otherwise import a partial
        // subset — a NEW asset with a different fingerprint — without the consent this gate exists to surface.
        // Mirrors deleteLocal/deleteRemote, which re-check presence live at the act instead of trusting metadata.
        let resolved = await manifestInstances(for: fingerprint, month: item.remoteMonth, expectedProfileKey: RemoteIndexSyncService.remoteProfileKey(profile))
        guard let resolved, !resolved.instances.isEmpty else {
            presentError(String(localized: "mediaBrowser.action.error"), on: presenter); return
        }
        if resolved.isIncomplete {
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
            let instances = resolved.instances
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
                onChanged(false, resolved.isIncomplete ? nil : localID)
                if self.isAlive(presenter) { HUD.flash(String(localized: "mediaBrowser.action.saved"), on: presenter) }
            } catch {
                hud.dismiss()
                self.presentError(String(localized: "mediaBrowser.action.error"), on: presenter)
            }
        }
    }

    // All manifest resource instances (every role) for a fingerprint, from the shared snapshot — this item's
    // month first (a grouping-TZ twin shares the fingerprint under another month). Off-main. The snapshot is
    // read live (fresh) but must belong to `expectedProfileKey`: a profile switch racing the download can
    // repopulate the cache for another node, and resolving that node's paths against the captured creds would
    // fetch the wrong bytes (caught later by hash verification, but only after a spurious failure).
    private func manifestInstances(for fingerprint: Data, month: LibraryMonthKey?, expectedProfileKey: String) async -> (instances: [RemoteAssetResourceInstance], isIncomplete: Bool)? {
        let coordinator = env.backupCoordinator
        return await withCancellableDetachedValue(priority: .userInitiated) {
            let state = coordinator.currentRemoteSnapshotState(since: nil)
            if let ownerKey = state.profileKey, ownerKey != expectedProfileKey { return nil }
            return Self.resolveInstances(from: state, fingerprint: fingerprint, preferredMonth: month)
        }
    }

    // Pure resolution of one fingerprint's RESOLVABLE instances from an already-materialized snapshot, preferring
    // `preferredMonth` (a grouping-TZ twin shares the fingerprint across two months). Full set for a complete
    // asset, available subset for an incomplete one (the caller took the user's consent). Also reports whether the
    // CHOSEN record is incomplete, so consent can be decided against the live snapshot rather than projection-time
    // item metadata (which can lag a mid-session degradation). Factored out so a batch can materialize the snapshot
    // ONCE and resolve every item against it instead of copying it per item.
    // Pure core of `resolveInstances`; `internal` (not `private`) so the freshness-of-completeness contract is
    // directly pinnable by tests — consent must read the LIVE record's completeness, not projection-time metadata.
    nonisolated static func resolveInstances(from state: RemoteLibrarySnapshotState, fingerprint: Data, preferredMonth: LibraryMonthKey?) -> (instances: [RemoteAssetResourceInstance], isIncomplete: Bool) {
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
            // Skip a month that resolves only config/metadata (e.g. adjustmentData) for this fingerprint so a stale
            // preferred month can't halt the search before a same-fingerprint month with restorable media — mirrors
            // the browser/presence `containsRealMedia` rule.
            guard ResourceRole.containsRealMedia(instances.map(\.role)) else { continue }
            let isIncomplete = MonthManifestStore.isAssetIncomplete(links: links, isResourceAvailable: { byHash[$0] != nil }, assetFingerprint: fingerprint)
            return (instances, isIncomplete)
        }
        return ([], false)
    }

    // The remote-only items a batch download should restore. Same-fingerprint twins (a grouping-TZ re-upload
    // spans two months) normally denote the identical asset, so restoring both would import it twice — but two
    // INCOMPLETE twins can each recover a DIFFERENT media side (photo-only in one month, paired-video-only in the
    // other), and dropping either loses a side the user selected. So keep the minimal set of twins that covers all
    // recoverable sides: process COMPLETE-first, then richer (more sides) first, then a stable id (so the choice
    // never depends on the Set-ordered selection), and keep a twin only when it restores a side not already
    // covered by a kept same-fingerprint twin. Identical twins still collapse; complementary twins both survive.
    // Pure + factored out so the rule is unit-testable.
    nonisolated static func dedupedForDownload(_ items: [MediaBrowserItem]) -> [MediaBrowserItem] {
        func hasPhoto(_ item: MediaBrowserItem) -> Bool { item.photoRemoteRelativePath != nil }
        func hasVideo(_ item: MediaBrowserItem) -> Bool { item.videoRemoteRelativePath != nil }
        func restorableSideCount(_ item: MediaBrowserItem) -> Int { (hasPhoto(item) ? 1 : 0) + (hasVideo(item) ? 1 : 0) }
        var coveredPhoto = Set<Data>()
        var coveredVideo = Set<Data>()
        return items
            .filter { $0.presence == .remoteOnly && $0.fingerprint != nil }
            .sorted { lhs, rhs in
                if lhs.isIncomplete != rhs.isIncomplete { return !lhs.isIncomplete }
                let l = restorableSideCount(lhs), r = restorableSideCount(rhs)
                if l != r { return l > r }
                return lhs.id < rhs.id
            }
            .filter { item in
                guard let fp = item.fingerprint else { return false }
                // Richer-first order guarantees a both-sides twin claims coverage before a redundant single-side
                // twin, so the single-side one is dropped (no double import) — while a genuinely complementary
                // twin still adds its uncovered side and survives.
                let addsPhoto = hasPhoto(item) && !coveredPhoto.contains(fp)
                let addsVideo = hasVideo(item) && !coveredVideo.contains(fp)
                guard addsPhoto || addsVideo else { return false }
                if hasPhoto(item) { coveredPhoto.insert(fp) }
                if hasVideo(item) { coveredVideo.insert(fp) }
                return true
            }
    }

    // `dedupedForDownload` picks representatives from selection-time item metadata, but descriptors resolve from a
    // FRESH snapshot — which can diverge if a sync/delete mutates the remote between selection and resolution. Two
    // kept complementary twins can then resolve to instances that share a resource (identical, subset/superset, or
    // partial overlap), and restoring both would import that resource twice. Merge same-identity descriptors that
    // SHARE any resource into one (importing each resource once); keep genuinely disjoint complementary sides
    // separate (the two-asset complementary restore). Resource identity is `role|slot|hash`, NOT hash alone — a
    // legacy no-hash manifest leaves `resourceHash` empty, so a hash-only key would collapse distinct roles and
    // drop a complementary side (RestoreService avoids the same empty-hash trap). Pure + unit-testable.
    nonisolated static func dedupeResolvedDescriptors(_ descriptors: [RestoreService.RestoreItemDescriptor]) -> [RestoreService.RestoreItemDescriptor] {
        struct Cluster { var identity: Data; var ids: Set<String>; var instances: [RemoteAssetResourceInstance] }
        var clusters: [Cluster] = []
        for descriptor in descriptors {
            let ids = Set(descriptor.instances.map(\.id))
            let overlapping = clusters.indices.filter { clusters[$0].identity == descriptor.identity && !clusters[$0].ids.isDisjoint(with: ids) }
            if let keep = overlapping.first {
                clusters[keep].ids.formUnion(ids)
                clusters[keep].instances += descriptor.instances
                for idx in overlapping.dropFirst().reversed() {   // a descriptor bridging two clusters folds them together
                    clusters[keep].ids.formUnion(clusters[idx].ids)
                    clusters[keep].instances += clusters[idx].instances
                    clusters.remove(at: idx)
                }
            } else {
                clusters.append(Cluster(identity: descriptor.identity, ids: ids, instances: descriptor.instances))
            }
        }
        return clusters.map { cluster in
            var seen = Set<String>()
            let instances = cluster.instances.filter { seen.insert($0.id).inserted }
            return RestoreService.RestoreItemDescriptor(instances: instances, identity: cluster.identity)
        }
    }

    // MARK: - Delete from device (PhotoKit shows its own system confirmation)

    private func deleteLocal(_ item: MediaBrowserItem, source: MediaBrowserSource, from presenter: UIViewController, onChanged: @escaping (Bool, String?) -> Void) async {
        guard let localID = item.localIdentifier else { return }
        // `.both` justifies this act by "the backup holds these bytes", but a partial-but-has-media record
        // carries the badge too — the device copy is then the only complete instance, and deleting it loses
        // the unresolvable side permanently. Same consent the download path requires for this record state.
        // Fingerprint-level (not item.isIncomplete): a complete grouping-TZ twin record makes the delete safe.
        if item.presence == .both, let fingerprint = item.fingerprint, !env.presenceIndex.hasCompleteBackup(fingerprint) {
            let confirmed = await confirmDestructive(
                title: String(localized: "mediaBrowser.action.incompleteDownload.confirmTitle"),
                message: String(localized: "mediaBrowser.action.incompleteDeleteLocal.confirmMessage"),
                confirmTitle: String(localized: "mediaBrowser.action.deleteLocal"),
                on: presenter
            )
            guard confirmed else { return }
        }
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
            // `.both` justified Delete-from-Device by "the backup holds these bytes" — re-prove that at the
            // act: a Photos edit after projection leaves an open viewer's handle stale (bound handles are
            // never revalidated in-session), and deleting would destroy the only copy of the edit.
            if item.presence == .both, let fingerprint = item.fingerprint {
                let presenceIndex = self.env.presenceIndex
                let current: Data? = await withCancellableDetachedValue(priority: .userInitiated) {
                    presenceIndex.currentFingerprints(forAssetIDs: [localID])[localID]
                }
                guard current == fingerprint else {
                    self.presentError(String(localized: "mediaBrowser.action.error"), on: presenter)
                    onChanged(false, nil)
                    return
                }
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
                let backedUp = await self.isBackedUp(localID)
                if backedUp == nil || !self.sessionStillMatches(profile) || !self.env.presenceIndex.isRemotePresenceAuthoritative {
                    // Session switched mid-upload, a mid-switch reload owns the shared snapshot (rebuild answered
                    // non-authoritatively), or no post-upload presence build could commit: the captured profile's
                    // backup can't be confirmed. Reload instead of misreporting it — the positive branch must NOT
                    // trust an `isBackedUp` that a profile switch repointed at another node (mirrors the batch guard).
                    onChanged(true, nil)
                } else if backedUp == true {
                    // The upload added the asset to the remote — announce it so Home (and any other presence index)
                    // reconciles its rows/counts, not just this browser (Home defers until the execution lease releases).
                    NotificationCenter.default.post(name: .RemoteLibrarySnapshotDidChange, object: nil)
                    onChanged(true, nil)   // now on the remote → reload grid + return
                } else {
                    self.presentError(String(localized: "mediaBrowser.action.uploadSkipped"), on: presenter)
                }
            } catch {
                hud.dismiss()
                // backupAssets may have committed month flushes into the shared cache before failing (the
                // upload pipeline itself posts nothing) — announce them so Home's remote view isn't left stale.
                NotificationCenter.default.post(name: .RemoteLibrarySnapshotDidChange, object: nil)
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
    // presence index and ask it — the single source everyone else reads too. Current-bytes rows only: a
    // stale row's pre-edit fingerprint may already be on the remote, which would report a skipped upload of
    // the CURRENT bytes as backed up. nil = no post-upload build could commit → bail, no verdict.
    private func isBackedUp(_ localID: String) async -> Bool? {
        guard await refreshPresenceForVerdict() else { return nil }
        let presenceIndex = env.presenceIndex
        let fp: Data? = await withCancellableDetachedValue(priority: .userInitiated) {
            presenceIndex.currentFingerprints(forAssetIDs: [localID])[localID]
        }
        guard let fp else { return false }
        // "Backed up" = complete/restorable, not merely present in the manifest — a partial upload isn't done.
        return env.presenceIndex.isBackedUp(fp)
    }

    // A refresh commit is dropped when an unrelated mutation (e.g. a connect's reload sync) invalidates
    // mid-build — the committed sets/flag would then still describe the PRE-upload build and misreport the
    // verdict as skipped. Retry so the verdict reads a build that postdates the upload; false = bail.
    private func refreshPresenceForVerdict() async -> Bool {
        env.presenceIndex.invalidate()
        for _ in 0..<3 {
            if await env.presenceIndex.refresh() { return true }
        }
        return false
    }

    // True while the active session still matches the profile an action captured at start. It goes false once a
    // profile switch completes mid-action (connect doesn't hold the execution lease), which repoints the shared,
    // profile-gated presence index at the new profile — so any presence-based success/skip check made after an
    // await must bail rather than answer for the wrong node.
    private func sessionStillMatches(_ profile: ServerProfileRecord) -> Bool {
        env.appSession.activeProfile?.id == profile.id
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
        // A profile switch may have completed while the confirmation was up (connect doesn't hold the execution
        // lease), repointing the presence index at another node. Don't delete against an ambiguous session or
        // falsely reconcile the item as gone; the user can retry once reconnected to its profile.
        guard sessionStillMatches(profile) else {
            presentError(String(localized: "mediaBrowser.action.notConnected"), on: presenter); return
        }
        // The confirmation dialog may have been up while presence changed (the asset was deleted elsewhere / a
        // sync dropped it). Re-check it's still on the remote before the irreversible delete; if it's already
        // gone, just reconcile the views. Checked against the LIVE cache — the committed presence set can lag
        // a mid-flight in-place sync, and treating that as "already gone" would report an un-executed delete
        // as done. `.unknown` (a mid-switch reset re-tagged the cache while this session is still active) is
        // not a confirmed absence — fail visibly rather than reconcile an un-executed delete as done.
        switch await env.presenceIndex.remoteLivePresence(fingerprint) {
        case .absent:
            onChanged(true, nil)
            return
        case .unknown:
            presentError(String(localized: "mediaBrowser.action.notConnected"), on: presenter)
            return
        case .present:
            break
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
    // `onChanged` receives whether a remote delete failed (so the viewer can stay open, keeping the error visible)
    // plus the local IDs whose device copies committed — the kept-open viewer must reproject those remote-only
    // instead of re-offering Delete Local for an asset this very action already removed.
    func runDeleteAll(_ item: MediaBrowserItem, from presenter: UIViewController, onChanged: @escaping (_ hadFailures: Bool, _ deletedDeviceIDs: Set<String>) -> Void) {
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
        case .delete: runGated { await self.performDeletion(items: items, from: presenter, onChanged: { _, _ in onChanged() }) }
        }
    }

    // Delete `items` from EVERY place they live: on-device ones from the Photos library (one change + hash-index
    // purge), on-remote ones from the backup (looped — no batch delete API). A backup removal is involved → confirm
    // first (device-only deletes are confirmed by PhotoKit's own system sheet). Device is deleted first so a cancel
    // at the system sheet leaves the backup intact.
    private func performDeletion(items: [MediaBrowserItem], from presenter: UIViewController, onChanged: @escaping (_ hadFailures: Bool, _ deletedDeviceIDs: Set<String>) -> Void) async {
        let deviceItems = items.filter(\.isDeviceDeletable)
        let remoteItems = items.filter(\.isRemoteDeletable)
        guard !deviceItems.isEmpty || !remoteItems.isEmpty else { return }

        // Capture the target credentials before any await: a profile switch mid-operation (during the
        // confirmation dialog or the device-delete sheet) must not redirect the remote delete to a different
        // node. Mirrors the single-item deleteRemote, which captures before its confirmation.
        let profile = env.appSession.activeProfile
        let password = env.appSession.activePassword

        if !remoteItems.isEmpty {
            let confirmed = await confirmBatchDelete(deviceCount: deviceItems.count, remoteCount: remoteItems.count, on: presenter)
            guard confirmed else { return }
        }

        // Device copies deleted while their backup is RETAINED (not remote-deleted in this batch) but
        // incomplete: same only-complete-copy consent as the single-item gate. Delete-everywhere items are
        // exempt (total removal is the stated intent), so this fires only for device-only batches (Local tab),
        // which the remote-leg confirmation above never covers.
        let incompleteRetained = Self.incompleteRetainedDeviceDeletes(items) { self.env.presenceIndex.hasCompleteBackup($0) }
        if incompleteRetained > 0 {
            let confirmed = await confirmDestructive(
                title: String(localized: "mediaBrowser.action.incompleteDownload.confirmTitle"),
                message: String.localizedStringWithFormat(String(localized: "mediaBrowser.batch.deleteLocal.incompleteConfirm"), incompleteRetained),
                confirmTitle: String(localized: "mediaBrowser.action.delete"),
                on: presenter
            )
            guard confirmed else { return }
        }

        await withExecutionLease(on: presenter) {
            // The remote loop's per-item deleteRemoteAsset each posts a snapshot change; suspend the reactive
            // presence rebuild so it happens once (on resume) instead of ~N times.
            self.env.presenceIndex.suspendUpstreamRefresh()
            defer { self.env.presenceIndex.resumeUpstreamRefresh() }
            // `.both` device copies a limited-access fetch couldn't reach — their backup must be preserved (below).
            var unresolvedDeviceIDs = Set<String>()
            // Device copies this action removed (or confirmed gone) — reported to the caller so a kept-open
            // viewer reprojects them even when the remote leg fails.
            var deletedDeviceIDs = Set<String>()
            if !deviceItems.isEmpty {
                let localIDs = deviceItems.compactMap(\.localIdentifier)
                let assets = PHAsset.fetchAssets(withLocalIdentifiers: localIDs, options: nil)
                var fetched = Set<String>()
                assets.enumerateObjects { asset, _, _ in fetched.insert(asset.localIdentifier) }
                if assets.count > 0 {
                    // Same act gate as the single-item deleteLocal: a RETAINED `.both` device delete is justified
                    // by "the backup holds these bytes" — re-prove each claim at the act, since the batch defers
                    // grid reloads and a mid-confirmation Photos edit never reprojects before the delete.
                    let claims = Self.retainedDeviceDeleteClaims(deviceItems, fetchedIDs: fetched)
                    if !claims.isEmpty {
                        let presenceIndex = self.env.presenceIndex
                        let current: [String: Data] = await withCancellableDetachedValue(priority: .userInitiated) {
                            presenceIndex.currentFingerprints(forAssetIDs: claims.keys)
                        }
                        guard claims.allSatisfy({ current[$0.key] == $0.value }) else {
                            self.presentError(String(localized: "mediaBrowser.action.error"), on: presenter)
                            // hadFailures: a delete-all viewer must stay open with the error; the reload reprojects.
                            onChanged(true, [])
                            return
                        }
                    }
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
                    purgeIDs = localIDs.filter { fetched.contains($0) }
                    unresolvedDeviceIDs = Set(localIDs.filter { !fetched.contains($0) })
                    if purgeIDs.count < localIDs.count {
                        self.presentError(String(localized: "mediaBrowser.action.noPhotoAccess"), on: presenter)
                    }
                }
                try? self.env.hashIndexRepository.deleteIndexEntries(assetIDs: purgeIDs)
                deletedDeviceIDs = Set(purgeIDs)
            }

            // Any device deletions above already happened; still fall through to the final reconcile so the grid
            // reflects them even if the remote leg can't run (disconnected) or some deletes fail.
            var remoteDeleteFailed = false
            if !remoteItems.isEmpty {
                if let profile, let password {
                    let hud = HUD.show(self.deletingProgressText(0, remoteItems.count), on: presenter)
                    var completed = 0
                    var failed = 0
                    var skippedForAccess = 0
                    for item in remoteItems {
                        if Task.isCancelled { break }
                        guard let fp = item.fingerprint, let month = item.remoteMonth else { continue }
                        // A `.both` item whose device copy couldn't be deleted (excluded from the limited Photos
                        // selection) keeps its backup — mirror the single deleteLocal abort so a limited-access
                        // miss never strips the only reachable copy while the device copy still exists.
                        if let localID = item.localIdentifier, unresolvedDeviceIDs.contains(localID) {
                            skippedForAccess += 1
                            completed += 1
                            hud.update(self.deletingProgressText(completed, remoteItems.count))
                            continue
                        }
                        // A profile switch completed mid-operation — the isOnRemote gate now answers for another
                        // node; don't delete against an ambiguous session, report it as not removed.
                        guard self.sessionStillMatches(profile) else {
                            failed += 1
                            completed += 1
                            hud.update(self.deletingProgressText(completed, remoteItems.count))
                            continue
                        }
                        // Live-cache check: set-absence from a mid-sync stale presence build must not skip a
                        // confirmed delete and count it as done. `.unknown` (mid-switch re-tagged cache) is
                        // not a confirmed absence — count the item failed instead of silently done.
                        switch await self.env.presenceIndex.remoteLivePresence(fp) {
                        case .present:
                            do {
                                try await self.env.backupCoordinator.deleteRemoteAsset(profile: profile, password: password, month: month, assetFingerprint: fp)
                            } catch {
                                failed += 1
                                actionLog.error("batch deleteRemote failed for \(fp.hexString, privacy: .public): \(String(describing: error), privacy: .public)")
                            }
                        case .absent:
                            break
                        case .unknown:
                            failed += 1
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
                    } else if skippedForAccess > 0 {
                        // Backups preserved for device copies we couldn't delete under limited access — keep the
                        // viewer open (noPhotoAccess was already surfaced) instead of dismissing as fully done.
                        remoteDeleteFailed = true
                    }
                } else {
                    remoteDeleteFailed = true
                    self.presentError(String(localized: "mediaBrowser.action.notConnected"), on: presenter)
                }
            }

            self.env.presenceIndex.invalidate()
            onChanged(remoteDeleteFailed, deletedDeviceIDs)
        }
    }

    // The `.both` claims a batch's device leg must re-prove before deleting (the single-item act gate's rule):
    // retained device deletes (not remote-deletable here) keyed by localIdentifier, valued by the fingerprint
    // the backup is trusted to hold. Delete-everywhere items are exempt (total removal is the stated intent);
    // unfetched IDs are never deleted by the change request, so they are not gated. Pure + unit-testable.
    nonisolated static func retainedDeviceDeleteClaims(_ items: [MediaBrowserItem], fetchedIDs: Set<String>) -> [String: Data] {
        var claims: [String: Data] = [:]
        for item in items {
            guard let id = item.localIdentifier, fetchedIDs.contains(id),
                  item.presence == .both, !item.isRemoteDeletable, let fp = item.fingerprint else { continue }
            claims[id] = fp
        }
        return claims
    }

    // Device deletes in `items` whose backup survives this batch (the item itself is not remote-deletable
    // here) yet has no complete remote record — the copies whose loss the batch consent must surface.
    // Pure + factored out so the exemption rule (delete-everywhere items don't count) is unit-testable.
    nonisolated static func incompleteRetainedDeviceDeletes(_ items: [MediaBrowserItem], hasCompleteBackup: (Data) -> Bool) -> Int {
        items.filter { item in
            guard item.isDeviceDeletable, !item.isRemoteDeletable, item.presence == .both, let fp = item.fingerprint else { return false }
            return !hasCompleteBackup(fp)
        }.count
    }

    private func batchUpload(_ items: [MediaBrowserItem], from presenter: UIViewController, onChanged: @escaping () -> Void) async {
        let localIDs = items.compactMap(\.localIdentifier)
        guard !localIDs.isEmpty else { return }
        guard let profile = env.appSession.activeProfile, let password = env.appSession.activePassword else {
            presentError(String(localized: "mediaBrowser.action.notConnected"), on: presenter); return
        }
        await withExecutionLease(on: presenter) {
            // Coalesce reactive presence rebuilds from posts landing mid-batch (lease lifecycle, the posts
            // below); the success check's explicit refresh is unaffected by suspension.
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
                guard let backedUp = await self.backedUpCount(localIDs),
                      self.sessionStillMatches(profile), self.env.presenceIndex.isRemotePresenceAuthoritative else {
                    // Session switched mid-upload, a mid-switch reload owns the shared snapshot (rebuild
                    // answered non-authoritatively), or no post-upload presence build could commit; reload
                    // without a misleading skipped/partial report for the captured profile's upload.
                    onChanged()
                    return
                }
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
                // Months committed by incremental flush before the failure are already in the shared cache; the
                // upload pipeline posts nothing, so announce them here — else Home stays stale until the next sync.
                NotificationCenter.default.post(name: .RemoteLibrarySnapshotDidChange, object: nil)
                self.presentError(String(localized: "mediaBrowser.action.error"), on: presenter)
            }
        }
    }

    // Count of the uploaded local IDs now represented on the remote. Rebuilds presence once, then queries
    // it. Same current-bytes and bail contracts as the single-item isBackedUp.
    private func backedUpCount(_ localIDs: [String]) async -> Int? {
        guard await refreshPresenceForVerdict() else { return nil }
        let presenceIndex = env.presenceIndex
        let fingerprints: [String: Data] = await withCancellableDetachedValue(priority: .userInitiated) {
            presenceIndex.currentFingerprints(forAssetIDs: localIDs)
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
        let downloadable = Self.dedupedForDownload(items)
        guard !downloadable.isEmpty else { return }

        let expectedProfileKey = RemoteIndexSyncService.remoteProfileKey(profile)
        let coordinator = self.env.backupCoordinator
        // Resolve each kept record's instances + FRESH completeness from the LIVE snapshot before consent:
        // `item.isIncomplete` came from selection-time projection and can lag a mid-session degradation, so a
        // partial subset would import (a new asset) without the consent the count exists to surface. Same root
        // cause as the single-item download consent. Fresh snapshot must belong to the captured profile — a switch
        // racing the batch must not resolve another node's paths against these creds (see manifestInstances).
        let resolved: [(fingerprint: Data, instances: [RemoteAssetResourceInstance], isIncomplete: Bool)] = await withCancellableDetachedValue(priority: .userInitiated) {
            let state = coordinator.currentRemoteSnapshotState(since: nil)
            if let ownerKey = state.profileKey, ownerKey != expectedProfileKey { return [] }
            var out: [(fingerprint: Data, instances: [RemoteAssetResourceInstance], isIncomplete: Bool)] = []
            for item in downloadable {
                guard let fp = item.fingerprint else { continue }
                let r = Self.resolveInstances(from: state, fingerprint: fp, preferredMonth: item.remoteMonth)
                guard !r.instances.isEmpty else { continue }
                out.append((fp, r.instances, r.isIncomplete))
            }
            return out
        }

        // Fresh resolution found nothing restorable for the whole selection (a profile switch repointed the shared
        // snapshot at another node, or every selected record degraded/was deleted since selection). The single-item
        // path surfaces the same empty/foreign resolution as an error; a batch must not silently no-op and leave the
        // stale selection live (the grid reprojects on its next reactive reload).
        guard !resolved.isEmpty else {
            presentError(String(localized: "mediaBrowser.action.error"), on: presenter); return
        }

        // Incomplete records import only their resolvable subset → new, differently-fingerprinted assets. One
        // upfront 3-way consent for the whole batch, mirroring Home's whole-month restore prompt.
        var policy: IncompleteDownloadPolicy = .createNewAsset
        let incompleteCount = resolved.filter(\.isIncomplete).count
        if incompleteCount > 0 {
            guard let chosen = await confirmIncompleteBatch(count: incompleteCount, on: presenter) else { return }
            policy = chosen
        }
        let toRestore = policy == .skip ? resolved.filter { !$0.isIncomplete } : resolved
        // Empty only when the user chose Skip with no complete record left — a user choice, not a stale read.
        guard !toRestore.isEmpty else { return }

        await withExecutionLease(on: presenter) {
            let hud = HUD.show(self.downloadingProgressText(0, toRestore.count), on: presenter)
            // Selection was deduped from stale item metadata; collapse any descriptors that the fresh snapshot
            // resolved to the same (or a subsumed) resource set so a snapshot change can't import one asset twice.
            let descriptors = Self.dedupeResolvedDescriptors(toRestore.map { RestoreService.RestoreItemDescriptor(instances: $0.instances, identity: $0.fingerprint) })
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
        // An earlier alert may still be up (e.g. the limited-access report while a batch's remote leg runs);
        // UIKit refuses a second present on the same presenter, silently dropping this report. Present from
        // the topmost controller instead, deferring one runloop while a present/dismiss is mid-transition.
        var host = presenter
        while let presented = host.presentedViewController {
            if presented.isBeingDismissed || presented.isBeingPresented {
                DispatchQueue.main.async { [weak self] in self?.presentError(message, on: presenter) }
                return
            }
            host = presented
        }
        let alert = UIAlertController(title: nil, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        host.present(alert, animated: true)
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
