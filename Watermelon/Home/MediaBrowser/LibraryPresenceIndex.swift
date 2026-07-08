import Foundation
import Photos

// Single source of truth for "is this asset on device / on the remote / both" across the media browser.
// Owns the two facts every presence decision needs — the fingerprint→localIdentifier reverse map (built
// from the local hash index) and the set of fingerprints present on the connected remote (from the shared
// snapshot, gated on the owning profile) — so no source, viewer, or action re-derives them independently.
//
// One instance per browser session, shared by every source + RemoteThumbnailService + the action runner.
// NSLock-guarded (not an actor) so the fingerprint→handle reads stay synchronous for thumbnail rendering.
// Handles leave this class only through the current-bytes validators — the raw map is not exposed, so a
// stale hash row (asset edited after backup) can never bind a device handle to its pre-edit fingerprint.
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
    // Fingerprints with at least one COMPLETE remote record (every linked resource available, fingerprint
    // matches). backedUp minus this = partial-but-has-media backups, where the device copy is the only
    // complete instance — Delete-from-Device asks for consent before destroying it.
    private var completeFingerprints: Set<Data> = []
    // True only when the committed remote/backed-up sets were built from a snapshot owned by (or nil for) the
    // active profile. False during an A→B switch — the shared cache is tagged for the incoming profile while
    // this one is still active — where an empty remote set means "unknown", not "not backed up".
    private var remotePresenceAuthoritative = false
    private var hasBuilt = false
    private var builtProfileKey: String?
    // Snapshot-cache revision the committed sets were built from — lets consumers detect that the live cache
    // has moved since (an in-place sync mutates it month-by-month and posts only once at the end).
    private var builtRevision: UInt64?
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
    //   · LocalIndexChangePublisher — a local index build / duplicate cleanup mutated fingerprints without any
    //     execution lease or snapshot signal (it only reaches Home otherwise).
    private var observerTokens: [NSObjectProtocol] = []
    private let localIndexChangePublisher: LocalIndexChangePublisher?
    private var indexChangeObserverID: UUID?

    init(
        hashIndexRepository: ContentHashIndexRepository,
        coordinator: BackupCoordinator,
        profileKey: @escaping () -> String?,
        localIndexChangePublisher: LocalIndexChangePublisher? = nil
    ) {
        self.hashIndexRepository = hashIndexRepository
        self.coordinator = coordinator
        self.profileKey = profileKey
        self.localIndexChangePublisher = localIndexChangePublisher
        for name in [Notification.Name.RemoteLibrarySnapshotDidChange, .ExecutionLifecycleDidChange, .BackgroundBackupRunMarkerDidChange] {
            let token = NotificationCenter.default.addObserver(forName: name, object: nil, queue: nil) { [weak self] _ in
                self?.upstreamStateChanged()
            }
            observerTokens.append(token)
        }
        indexChangeObserverID = localIndexChangePublisher?.addObserver { [weak self] _ in
            self?.upstreamStateChanged()
        }
    }

    deinit {
        observerTokens.forEach { NotificationCenter.default.removeObserver($0) }
        if let indexChangeObserverID { localIndexChangePublisher?.removeObserver(indexChangeObserverID) }
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
    // Returns whether the committed state now reflects a build at least as fresh as this call's start —
    // false means the commit was dropped (a mutation landed mid-build) and the committed sets/flag still
    // describe an OLDER build; a one-shot verdict (upload success) must not read them as current.
    @discardableResult
    func refresh() async -> Bool {
        let currentKey = profileKey()
        let startGeneration: Int? = lock.withLock {
            if hasBuilt && builtProfileKey == currentKey { return nil }
            return generation
        }
        guard let startGeneration else { return true }
        let hashIndexRepository = hashIndexRepository
        let coordinator = coordinator
        let built = await withCancellableDetachedValue(priority: .userInitiated) { () -> (map: [Data: String], remote: Set<Data>, backedUp: Set<Data>, complete: Set<Data>, authoritative: Bool, revision: UInt64) in
            let map = (try? hashIndexRepository.fetchLocalIdentifiersByFingerprint()) ?? [:]
            let state = coordinator.currentRemoteSnapshotState(since: nil)
            // Reject a foreign profile's snapshot (profile-switch window): no remote context ⇒ empty set. Record
            // whether the snapshot was authoritative for the active profile so remote-write readiness (Upload)
            // can suppress during the switch window instead of reading the empty set as "not backed up".
            let authoritative = state.profileKey == nil || state.profileKey == currentKey
            var remote = Set<Data>()
            var backedUp = Set<Data>()
            var complete = Set<Data>()
            if authoritative {
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
                        // Fingerprint-level: ANY complete record (a grouping-TZ twin month counts) means the
                        // backup fully holds these bytes.
                        if !MonthManifestStore.isAssetIncomplete(links: links, isResourceAvailable: { availableHashes.contains($0) }, assetFingerprint: asset.assetFingerprint) {
                            complete.insert(asset.assetFingerprint)
                        }
                    }
                }
            }
            return (map, remote, backedUp, complete, authoritative, state.revision)
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
            completeFingerprints = built.complete
            remotePresenceAuthoritative = built.authoritative
            builtProfileKey = currentKey
            builtRevision = built.revision
            hasBuilt = true
            return true
        }
        if committed { NotificationCenter.default.post(name: .LibraryPresenceDidChange, object: self) }
        return committed
    }

    // Forces the next refresh() to rebuild — call after a library change (download / delete) mutates state.
    func invalidate() {
        lock.withLock {
            hasBuilt = false
            generation &+= 1
        }
    }

    // Home's staleness rule: a hash row older than the asset's modificationDate no longer describes the
    // current bytes (edited after backup).
    static func isRowCurrent(recordUpdatedAt: Date, assetModificationDate: Date?) -> Bool {
        guard let assetModificationDate else { return true }
        return assetModificationDate <= recordUpdatedAt
    }

    // Local-render authority: the handle is returned only while its hash row still maps to this fingerprint
    // AND describes the asset's current bytes — an edited-after-backup asset must never be rendered (L1) or
    // published (shared L2 sidecar) as this fingerprint. Off-main only (single-row SQL + PHAsset fetch).
    func localIdentifierForCurrentBytes(_ fingerprint: Data) -> String? {
        localIdentifiersForCurrentBytes([fingerprint])[fingerprint]
    }

    // Batch form for projection builds that bind many handles per load (one chunked row fetch + one PHAsset
    // fetch over just the map-hit IDs). Same drop rules as the single-item helper. Off-main only.
    // The reverse map keeps one arbitrary row per fingerprint, so an older stale row can shadow a current
    // one (download-back of an edited-after-backup asset; a limited-access-excluded twin) — before dropping
    // a failed candidate, fall back to the fingerprint's other rows and bind any current one.
    func localIdentifiersForCurrentBytes(_ fingerprints: some Sequence<Data>) -> [Data: String] {
        let mapHits: [Data: String] = lock.withLock {
            var hits: [Data: String] = [:]
            for fingerprint in fingerprints where hits[fingerprint] == nil {
                if let localID = localIDByFingerprint[fingerprint] { hits[fingerprint] = localID }
            }
            return hits
        }
        guard !mapHits.isEmpty else { return [:] }
        var known = currentFingerprints(forAssetIDs: mapHits.values)
        let failed = mapHits.filter { known[$0.value] != $0.key }.map(\.key)
        var alternatives: [Data: [String]] = [:]
        if !failed.isEmpty {
            alternatives = (try? hashIndexRepository.fetchAssetIDsByFingerprints(Set(failed))) ?? [:]
            let alternativeIDs = Set(alternatives.values.joined()).subtracting(mapHits.values)
            if !alternativeIDs.isEmpty {
                known.merge(currentFingerprints(forAssetIDs: alternativeIDs)) { first, _ in first }
            }
        }
        return Self.selectCurrentHandles(mapHits: mapHits, alternativesByFingerprint: alternatives, currentFingerprintsByAssetID: known)
    }

    // Pure core of the batch validator's selection (fetches injected) so the shadowed-row rule is pinnable:
    // the map candidate wins while its row still fingerprints the current bytes; otherwise the first current
    // alternative row binds; else the fingerprint drops.
    static func selectCurrentHandles(
        mapHits: [Data: String],
        alternativesByFingerprint: [Data: [String]],
        currentFingerprintsByAssetID: [String: Data]
    ) -> [Data: String] {
        var result: [Data: String] = [:]
        for (fingerprint, candidate) in mapHits {
            if currentFingerprintsByAssetID[candidate] == fingerprint {
                result[fingerprint] = candidate
            } else if let alternative = alternativesByFingerprint[fingerprint]?.first(where: { currentFingerprintsByAssetID[$0] == fingerprint }) {
                result[fingerprint] = alternative
            }
        }
        return result
    }

    // Current-bytes fingerprints for device assets, keyed by localIdentifier: a row older than the asset's
    // edit is dropped, as is an unfetchable asset (deleted, or outside a limited-access selection) — neither
    // proves the current bytes match the row's fingerprint. Off-main only.
    func currentFingerprints(forAssetIDs assetIDs: some Collection<String>) -> [String: Data] {
        guard !assetIDs.isEmpty else { return [:] }
        let ids = Set(assetIDs)
        let records = (try? hashIndexRepository.fetchAssetFingerprintRecords(assetIDs: ids)) ?? [:]
        var modificationDateByID: [String: Date?] = [:]
        PHAsset.fetchAssets(withLocalIdentifiers: Array(ids), options: nil).enumerateObjects { asset, _, _ in
            modificationDateByID[asset.localIdentifier] = asset.modificationDate
        }
        return Self.currentFingerprints(records: records, modificationDateByAssetID: modificationDateByID)
    }

    // Pure core of the current-bytes validators (fetch results injected) so the drop rules are pinnable.
    static func currentFingerprints(
        records: [String: LocalAssetFingerprintRecord],
        modificationDateByAssetID: [String: Date?]
    ) -> [String: Data] {
        var result: [String: Data] = [:]
        for (assetID, record) in records {
            guard let modificationDate = modificationDateByAssetID[assetID] else { continue }
            guard isRowCurrent(recordUpdatedAt: record.updatedAt, assetModificationDate: modificationDate) else { continue }
            result[assetID] = record.fingerprint
        }
        return result
    }

    // Present in a remote manifest (raw) — the asset EXISTS remotely (even if incomplete). Gates "can delete
    // from backup" and viewer/More visibility.
    func isOnRemote(_ fingerprint: Data) -> Bool {
        lock.withLock { remoteFingerprints.contains(fingerprint) }
    }

    // Absence from the committed set can mean "the build predates this month" while an in-place sync mutates
    // the shared cache month-by-month (single post at sync end) — not "gone from the remote". Destructive
    // reconcile-as-gone decisions must confirm absence against the live cache with this instead.
    // `.unknown` = the live cache doesn't answer for the active profile (mid-switch it is reset and re-tagged
    // for the incoming profile while this session is still active) — never a confirmed absence.
    enum RemoteLivePresence {
        case present
        case absent
        case unknown
    }

    func remoteLivePresence(_ fingerprint: Data) async -> RemoteLivePresence {
        if isOnRemote(fingerprint) { return .present }
        let currentKey = profileKey()
        let coordinator = coordinator
        return await withCancellableDetachedValue(priority: .userInitiated) {
            let live = coordinator.snapshotContainsAssetFingerprint(fingerprint)
            return Self.classifyLivePresence(contains: live.contains, liveProfileKey: live.profileKey, currentKey: currentKey)
        }
    }

    // `internal` only so the unknown-vs-absent contract is directly pinnable by tests. An untagged cache
    // (nil key: just reset, not yet re-tagged) never authoritatively describes an active profile.
    static func classifyLivePresence(contains: Bool, liveProfileKey: String?, currentKey: String?) -> RemoteLivePresence {
        guard liveProfileKey == currentKey else { return .unknown }
        return contains ? .present : .absent
    }

    // True when the committed sets were built from the cache's current revision. False = a mutation landed
    // after the build (e.g. a mid-flight in-place sync): set-absence is then "unknown", not "gone".
    var isRemotePresenceCurrent: Bool {
        let built: UInt64? = lock.withLock { hasBuilt ? builtRevision : nil }
        guard let built else { return false }
        return built == coordinator.currentSnapshotRevision()
    }

    // Present AND has real media on the remote — a partial-but-has-media record still counts as backed up.
    // Gates the presence badge and whether a local copy still needs Upload. A config-only / phantom record
    // is on the remote (isOnRemote) but NOT backed up.
    func isBackedUp(_ fingerprint: Data) -> Bool {
        lock.withLock { backedUpFingerprints.contains(fingerprint) }
    }

    // At least one remote record for this fingerprint is complete. False for a partial-but-has-media backup
    // (still isBackedUp, badge `.both`) — the device copy is then the only complete instance, so the delete
    // paths ask for consent before removing it. Mirrors the download path's incomplete-record consent.
    func hasCompleteBackup(_ fingerprint: Data) -> Bool {
        lock.withLock { completeFingerprints.contains(fingerprint) }
    }

    // Whether the remote/backed-up sets authoritatively reflect the active profile. False during a profile
    // switch's reload window: the committed build is still the previous profile's while `profileKey()` already
    // answers for the new one, so an item's `.localOnly` projection is UNKNOWN rather than confirmed. Remote-
    // write action readiness (Upload) must not treat that as "safe to upload" — re-checking the built profile
    // against the live one catches the window the stored flag alone would miss.
    var isRemotePresenceAuthoritative: Bool {
        let currentKey = profileKey()
        return lock.withLock { hasBuilt && remotePresenceAuthoritative && builtProfileKey == currentKey }
    }
}
