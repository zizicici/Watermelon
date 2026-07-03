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
        let storageClientFactory: StorageClientFactory
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
        let items = await source.shareItems(for: item)
        guard isAlive(presenter) else { cleanupTempShareItems(items); return }
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
        return await withCancellableDetachedValue(priority: .userInitiated) { () -> [RemoteAssetResourceInstance] in
            let state = coordinator.currentRemoteSnapshotState(since: nil)
            let ordered = [state.monthDeltas.first { $0.month == month }].compactMap { $0 }
                + state.monthDeltas.filter { $0.month != month }
            for delta in ordered {
                let links = delta.assetResourceLinks.filter { $0.assetFingerprint == fingerprint }
                guard !links.isEmpty else { continue }
                let byHash = Dictionary(delta.resources.map { ($0.contentHash, $0) }, uniquingKeysWith: { first, _ in first })
                // Return the RESOLVABLE instances: the full set for a complete asset, the available subset for an
                // incomplete one. The caller confirms with the user before importing an incomplete asset — the
                // import creates a new, differently-fingerprinted asset (writeHashIndex records that honestly).
                let instances = links.compactMap { link -> RemoteAssetResourceInstance? in
                    guard let r = byHash[link.resourceHash] else { return nil }
                    return RemoteAssetResourceInstance(role: link.role, slot: link.slot, resourceHash: r.contentHash, fileName: r.fileName, fileSize: r.fileSize, remoteRelativePath: r.remoteRelativePath, creationDateMs: r.creationDateMs)
                }
                guard !instances.isEmpty else { continue }
                return instances
            }
            return []
        }
    }

    // MARK: - Delete from device (PhotoKit shows its own system confirmation)

    private func deleteLocal(_ item: MediaBrowserItem, source: MediaBrowserSource, from presenter: UIViewController, onChanged: @escaping (Bool, String?) -> Void) async {
        guard let localID = item.localIdentifier else { return }
        // App-wide mutex (scope): a local delete mutates the library + hash index, so exclude a concurrent backup.
        await withExecutionLease(on: presenter) {
            let assets = PHAsset.fetchAssets(withLocalIdentifiers: [localID], options: nil)
            guard assets.count > 0 else {
                // Already gone from the library: still purge the stale hash-index row so presence reverts to
                // remote-only (re-offering Download) instead of forever reporting `.both`.
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
        let label = UILabel()
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

    func dismiss() {
        container.removeFromSuperview()
    }
}
