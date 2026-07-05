import Foundation

// Single source of truth for "is this asset on device / on the remote / both" across the media browser.
// Owns the two facts every presence decision needs — the fingerprint→localIdentifier reverse map (built
// from the local hash index) and the set of fingerprints present on the connected remote (from the shared
// snapshot, gated on the owning profile) — so no source, viewer, or action re-derives them independently.
//
// One instance per browser session, shared by every source + RemoteThumbnailService + the action runner.
// NSLock-guarded (not an actor) so `localIdentifier(for:)` stays a synchronous read for thumbnail rendering.
final class LibraryPresenceIndex: @unchecked Sendable {
    private let hashIndexRepository: ContentHashIndexRepository
    private let coordinator: BackupCoordinator
    // Read live: the connected remote's profile key (nil = disconnected). Presence is relative to it, so a
    // stale / other-profile snapshot must never mark on-device assets as `.both`.
    private let profileKey: () -> String?

    private let lock = NSLock()
    private var localIDByFingerprint: [Data: String] = [:]
    // Two notions: `remoteFingerprints` = present in a remote manifest (raw, gates "exists / can delete");
    // `backedUpFingerprints` = present AND has real media on the remote — a partial record that still resolves
    // a photo/video counts (its local twin is genuinely backed up), a config-only / phantom record does not
    // (its local twin keeps offering Upload). Gates the presence badge and the merged-tab dedup.
    private var remoteFingerprints: Set<Data> = []
    private var backedUpFingerprints: Set<Data> = []
    private var hasBuilt = false
    private var builtProfileKey: String?
    // Bumped by every invalidate(). A refresh() captures it before its off-lock build and only commits if
    // it is unchanged — so an invalidate() that lands mid-build isn't lost to the stale result overwriting it.
    private var generation = 0
    private var refreshScheduled = false
    // While > 0, upstream signals only mark stale (no reactive rebuild); a rebuild is coalesced to one on resume.
    // Lets a batch that emits many snapshot posts (an N-item remote delete) rebuild once instead of ~N times.
    private var suspendDepth = 0
    private var refreshPendingWhileSuspended = false

    // This index is the ONE place that knows which upstream events can change presence, so UI consumers observe
    // only `.LibraryPresenceDidChange` (posted by refresh) instead of subscribing to a growing set of proxies.
    //   · RemoteLibrarySnapshotDidChange   — remote facts changed, posted AFTER the cache is updated (race-free).
    //   · ExecutionLifecycleDidChange      — a foreground execution ended; the local hash index may have changed.
    //   · BackgroundBackupRunMarkerDidChange — a background run may have changed local fingerprints without a
    //     remote re-sync (e.g. a download), which the snapshot signal wouldn't cover.
    private var observerTokens: [NSObjectProtocol] = []

    init(hashIndexRepository: ContentHashIndexRepository, coordinator: BackupCoordinator, profileKey: @escaping () -> String?) {
        self.hashIndexRepository = hashIndexRepository
        self.coordinator = coordinator
        self.profileKey = profileKey
        for name in [Notification.Name.RemoteLibrarySnapshotDidChange, .ExecutionLifecycleDidChange, .BackgroundBackupRunMarkerDidChange] {
            let token = NotificationCenter.default.addObserver(forName: name, object: nil, queue: nil) { [weak self] _ in
                self?.upstreamStateChanged()
            }
            observerTokens.append(token)
        }
    }

    deinit {
        observerTokens.forEach { NotificationCenter.default.removeObserver($0) }
    }

    // An upstream signal may have changed presence — mark stale and schedule a single rebuild. Bursts (a sync
    // committing many months, or execution start+end) collapse to one refresh via `refreshScheduled`.
    private func upstreamStateChanged() {
        invalidate()
        let shouldSchedule = lock.withLock { () -> Bool in
            // Suspended (inside a batch): defer to a single rebuild on resume instead of one per post.
            if suspendDepth > 0 { refreshPendingWhileSuspended = true; return false }
            guard !refreshScheduled else { return false }
            refreshScheduled = true
            return true
        }
        guard shouldSchedule else { return }
        Task { [weak self] in
            guard let self else { return }
            lock.withLock { self.refreshScheduled = false }
            await self.refresh()
        }
    }

    // Bracket a batch that emits many upstream posts (e.g. an N-item remote delete): while suspended, those posts
    // only mark presence stale; resume performs at most one rebuild if any landed. Balanced calls; safe to nest.
    // Explicit `refresh()` (e.g. the upload success check) is unaffected — only the reactive path is deferred.
    func suspendUpstreamRefresh() {
        lock.withLock { suspendDepth += 1 }
    }

    func resumeUpstreamRefresh() {
        let shouldRefresh = lock.withLock { () -> Bool in
            if suspendDepth > 0 { suspendDepth -= 1 }
            guard suspendDepth == 0, refreshPendingWhileSuspended else { return false }
            refreshPendingWhileSuspended = false
            return true
        }
        guard shouldRefresh else { return }
        Task { [weak self] in await self?.refresh() }
    }

    // Rebuilds both facts off the main thread. Idempotent while the owning profile is unchanged; a profile
    // A→B switch (or first call) forces a rebuild even without an explicit invalidate. Posts on change.
    func refresh() async {
        let currentKey = profileKey()
        let startGeneration: Int? = lock.withLock {
            if hasBuilt && builtProfileKey == currentKey { return nil }
            return generation
        }
        guard let startGeneration else { return }
        let hashIndexRepository = hashIndexRepository
        let coordinator = coordinator
        let built = await withCancellableDetachedValue(priority: .userInitiated) { () -> (map: [Data: String], remote: Set<Data>, backedUp: Set<Data>) in
            let map = (try? hashIndexRepository.fetchLocalIdentifiersByFingerprint()) ?? [:]
            let state = coordinator.currentRemoteSnapshotState(since: nil)
            // Reject a foreign profile's snapshot (profile-switch window): no remote context ⇒ empty set.
            var remote = Set<Data>()
            var backedUp = Set<Data>()
            if state.profileKey == nil || state.profileKey == currentKey {
                for delta in state.monthDeltas {
                    let availableHashes = Set(delta.resources.map { $0.contentHash })
                    let linksByFingerprint = Dictionary(grouping: delta.assetResourceLinks, by: { $0.assetFingerprint })
                    for asset in delta.assets {
                        remote.insert(asset.assetFingerprint)
                        // Backed up = the record has real media on the remote (a partial-but-has-media record
                        // counts). A config-only / phantom record does not, so its local twin keeps offering Upload.
                        let links = linksByFingerprint[asset.assetFingerprint] ?? []
                        if MonthManifestStore.hasBackedUpMedia(links: links, isResourceAvailable: { availableHashes.contains($0) }) {
                            backedUp.insert(asset.assetFingerprint)
                        }
                    }
                }
            }
            return (map, remote, backedUp)
        }
        let committed: Bool = lock.withLock {
            // Drop a now-stale result rather than let it overwrite fresher state: either an invalidate() /
            // another refresh landed mid-build (generation moved), or the profile switched under us since the
            // build captured `currentKey` (a slow A build must not clobber a committed B build). Leaving
            // hasBuilt = false makes the next refresh() rebuild from the current state.
            guard generation == startGeneration, profileKey() == currentKey else { return false }
            localIDByFingerprint = built.map
            remoteFingerprints = built.remote
            backedUpFingerprints = built.backedUp
            builtProfileKey = currentKey
            hasBuilt = true
            return true
        }
        if committed { NotificationCenter.default.post(name: .LibraryPresenceDidChange, object: self) }
    }

    // Forces the next refresh() to rebuild — call after a library change (download / delete) mutates state.
    func invalidate() {
        lock.withLock {
            hasBuilt = false
            generation &+= 1
        }
    }

    func localIdentifier(for fingerprint: Data) -> String? {
        lock.withLock { localIDByFingerprint[fingerprint] }
    }

    // Present in a remote manifest (raw) — the asset EXISTS remotely (even if incomplete). Gates "can delete
    // from backup" and viewer/More visibility.
    func isOnRemote(_ fingerprint: Data) -> Bool {
        lock.withLock { remoteFingerprints.contains(fingerprint) }
    }

    // Present AND has real media on the remote — a partial-but-has-media record still counts as backed up.
    // Gates the presence badge and whether a local copy still needs Upload. A config-only / phantom record
    // is on the remote (isOnRemote) but NOT backed up.
    func isBackedUp(_ fingerprint: Data) -> Bool {
        lock.withLock { backedUpFingerprints.contains(fingerprint) }
    }
}
