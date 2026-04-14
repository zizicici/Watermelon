# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

iOS photo backup app ("Watermelon") that backs up `PHAsset` items to remote storage (SMB/WebDAV/external-volume folder). The repository name is `PhotoBackup`; the Xcode target is `Watermelon`.

Build with `Watermelon.xcodeproj`. There is no package manager CLI or test runner — all building and testing is done through Xcode. The project has no automated test suite; regression testing is done manually on device with SMB/WebDAV server and external drive.

## Source Layout

```
Watermelon/
  App/          # AppDelegate, SceneDelegate, AppCoordinator, AppSession, DependencyContainer
  Home/         # HomeViewController, HomeAlbumMatching, HomeLibraryEngines,
                # HomeExecutionCoordinator, HomeExecutionSession, HomeScreenStore,
                # HomeConnectionController, DownloadWorkflowHelper, SelectionActionPanel
  Services/
    Backup/     # BackupCoordinator, BackupRunPreparation, BackupParallelExecutor,
                # BackupMonthScheduler, StorageClientPool,
                # AssetProcessor (+Upload, +Naming), BackupAssetResourcePlanner,
                # MonthManifestStore (+Loading, +Schema),
                # RemoteIndexSyncService, RemoteLibrarySnapshotCache,
                # BackupSessionController, BackupSessionReducer,
                # BackupCancellationController, BackupEventStream,
                # BackupRunModels, BackupRunDriver, BackupResumePlanner
    SMB/        # AMSMB2Client, SMBSetupService, SMBErrorClassifier,
                # SMBClientProtocol (RemoteStorageClientProtocol, RemoteStorageClientError)
    Storage/    # StorageClientFactory, WebDAVClient, LocalVolumeClient,
                # SecurityScopedBookmarkStore
    HashIndex/  # ContentHashIndexRepository
    PhotoLibrary/ Restore/
  UI/
    Auth/       # AddSMBServerLoginViewController, SMBSharePathPickerViewController,
                # AddSMBServerViewController, AddWebDAVStorageViewController,
                # AddExternalStorageViewController, ManageStorageProfilesViewController
    More/       # MoreViewController, SettingOptionsViewController, Settings
    Common/     # FormRowView, UIColor+App
  Data/
    Database/   # DatabaseManager, Records
    Security/   # KeychainService
  Domain/       # BackupDomain, RemoteLibraryDomain, RemotePathBuilder, StorageProfile
```

## Architecture

### App Startup

`SceneDelegate` → `AppCoordinator.start()` → `showHome()` → `HomeViewController`.
No TabBar. Single-screen architecture centered on `HomeViewController`.

### Dependency Injection

Single `DependencyContainer` created at startup holds all top-level singletons:
`DatabaseManager`, `KeychainService`, `AppSession`, `StorageClientFactory`, `PhotoLibraryService`, `ContentHashIndexRepository`, `BackupCoordinator`, `RestoreService`.

`AppSession` holds the active `ServerProfileRecord` and in-memory password (SMB/WebDAV require it; external-volume does not).

### Backup Control Plane

`BackupSessionController` (`@MainActor`) is the unified control plane and UI-facing state aggregator:

1. Handles `start/pause/stop/resume/retry` commands directly.
2. Maintains run token, termination intent, and run task lifecycle.
3. Creates per-run `BackupEventStream` and manages `runTask`/`eventListenerTask`.
4. Processes `BackupEvent` directly from the event stream (no intermediate signal layer).

### Backup Execution Plane

`BackupCoordinator.runBackup(request: BackupRunRequest, eventStream: BackupEventStream)` delegates to two services:

**Phase 1 — Preparation** (`BackupRunPreparationService.prepareRun`):

1. Request photo permission.
2. Build storage client via `StorageClientFactory`, connect, ensure `basePath` exists.
3. `RemoteIndexSyncService.syncIndex` scans month manifest digests and applies deltas to `RemoteLibrarySnapshotCache`.
4. Group `PHAsset` items by month via `BackupMonthScheduler.buildMonthAssetIDsByMonth`.
5. Build month plans sorted by estimated bytes DESC (largest months first).
6. Resolve worker count (SMB/WebDAV: 2, external-volume: 3, max 4) and connection pool size.
7. Return `BackupPreparedRun` with initial client, month plans, worker config, and a client factory closure.

**Phase 2 — Parallel execution** (`BackupParallelExecutor.execute`):

1. Create `StorageClientPool` (actor) with max connections; seed the initial client.
2. Spawn `N` workers in a `TaskGroup`, each pulling months from `MonthWorkQueue` (actor, dynamic pull).
3. Per month per worker: load/create `MonthManifestStore`, process assets in batches of 500.
4. Per-asset processing is delegated to `AssetProcessor`.
5. After each month: flush manifest to remote via atomic temp+move.
6. On error: external-storage errors skip manifest flush and propagate immediately; other fatal errors terminate all workers via the task group.

`BackupRunRequest` carries run-scoped parameters: `profile`, `password`, `onlyAssetLocalIdentifiers` (retry mode), `workerCountOverride`, `onMonthUploaded` (finalizer callback).

### Data Storage

**Local SQLite** (GRDB, `DatabaseManager`), migrations `v7_dev_schema_reset`, `v8_server_profiles_smb_identity`:

- `server_profiles` — saved storage profiles (`storageType`, `connectionParams`, `sortOrder`)
- `sync_state` — key/value store (e.g. `active_server_profile_id`)
- `local_assets` — per-asset fingerprint cache
- `local_asset_resources` — per-resource content hash by `(assetLocalIdentifier, role, slot)`

`server_profiles` uniqueness is SMB-only via partial unique index on `(host, port, shareName, basePath, username, IFNULL(domain, ''))` with `WHERE storageType = 'smb'`.

**Remote monthly manifest** (`MonthManifestStore`), path `/{YYYY}/{MM}/.watermelon_manifest.sqlite`, migration `month_manifest_v3_dev_schema_reset` (dev-phase drop+recreate):

- `resources`
- `assets`
- `asset_resources`

Manifest writes are deferred: `upsertResource/upsertAsset` mark `dirty=true`; `flushToRemote()` uploads at month boundaries and run completion.

Passwords are stored in Keychain (`com.zizicici.watermelon.credentials`), not in local DB.

### Home Page — Data Model

Three-engine architecture in `HomeLibraryEngines.swift`:

- **`HomeLocalIndexEngine`** — in-memory index of all `PHAsset` items, grouped by month. Tracks per-asset fingerprint, content hashes, and `isBackedUp` (fingerprint exists in remote set). Supports incremental updates via `PHPhotoLibraryChangeObserver`.
- **`HomeRemoteIndexEngine`** — in-memory index of remote snapshot data. Applies month-level deltas from `RemoteLibrarySnapshotState`, maintains `remoteFingerprintRefCount` for global fingerprint tracking.
- **`HomeReconcileEngine`** — merges local and remote per-month into `HomeAlbumItem` with `.localOnly` / `.remoteOnly` / `.both` tags. Exposes `matchedCount(for:)` for progress calculation.

`HomeIncrementalDataManager` (`@MainActor`) owns all three engines and provides the public API: `ensureLocalIndexLoaded`, `reloadLocalIndex` (force), `refreshLocalIndex(forAssetIDs:)`, `syncRemoteSnapshot`, `matchedCount(for:)`, `remoteOnlyItems(for:)`.

### Home Page — Matching

`HomeAlbumMatching.mergeItems` matches remote items to local items by content hash. `bestLocalID` ranks candidates by: exact hash-set match > intersection size > `isBackedUp` > creation-date proximity. Remote items are assembled from `assets + asset_resources + resources` relationships.

### Home Page — Execution Mode

`HomeViewController` manages a three-phase execution flow:

1. **Selection phase** — user selects months on left (local) / right (remote) columns. Arrow direction per month: local-only → upload (→), remote-only → download (←), both → sync (↔). `SelectionActionPanel` shows counts and "执行" button.

2. **Upload phase** — a per-execution `BackupSessionController` (fresh instance each session) drives `BackupCoordinator.runBackup` with scoped asset IDs. `handleBackupSnapshot` tracks `startedMonths`/`flushedMonths`/`processedCountByMonth` from backup events.

3. **Download phase** — sequential per-month via `ensureHashIndexAndDownload`:
   - Scoped backup populates local hash index (skips already-backed-up assets quickly).
   - `refreshLocalIndex(forAssetIDs:)` ensures reconciliation reflects newly computed hashes.
   - `processDownloadMonth` downloads `remoteOnlyItems` via `RestoreService`.
   - Per-item: `writeHashIndexForItem` + `refreshLocalIndex` persists progress immediately (survives mid-download stop).

**Progress calculation** (`progressPercent`): reconciliation `matchedCount` is the baseline (always accurate). During upload, `max(sessionPercent, basePercent)` ensures progress never drops. Sync months skip session tracking entirely and use pure reconciliation (updated per-120ms via `syncRemoteDataIfNeeded` in the progress handler). Download months also use pure reconciliation (updated per-item via `refreshLocalIndex`).

**Pause/Stop**: upload phase → `backupSessionController.stopBackup()` (cooperative cancellation). Download phase → `downloadTask.cancel()` + `backupSessionController.stopBackup()` + `Task.checkCancellation` in `RestoreService.restoreItems` loop.

### Storage Clients

Three implementations of `RemoteStorageClientProtocol`:

- **`AMSMB2Client`** — wraps AMSMB2 library (`SMB2Manager`). `@unchecked Sendable` class. Uses `SMBErrorClassifier` for NT_STATUS hex matching (not-found, collision, connection-unavailable) and POSIX error code extraction.
- **`WebDAVClient`** — `actor`. Custom XML parser for PROPFIND/PROPPATCH. Percent-encoded path handling with case normalization. Supports RFC 1123 and Win32 modification date formats.
- **`LocalVolumeClient`** — `actor`. Security-scoped bookmark lifecycle. Fast-path `copyItem` with chunked fallback. `hasLostAccessToExternalVolume` probes root URL to confirm disconnection rather than relying on error code heuristics.

`StorageClientPool` (actor) manages connection pooling: `acquire()` blocks when pool exhausted, `release(reusable:)` recycles or replaces connections, `shutdown()` disconnects all and fails pending waiters.

## Key Rules & Invariants

- Remote scanner is **read-only** — never creates directories or writes manifests during scanning.
- `assetFingerprint` is SHA-256 of sorted `role|slot|hashHex` tokens joined by `\n`.
- Name-collision strategy: files < 5 MiB download+hash compare; files ≥ 5 MiB size heuristic; unresolved conflicts use `_n` suffix.
- Upload retries: max 3 attempts with exponential back-off; collision status triggers immediate rename retry.
- Pause/stop are cooperative cancellation (not force-kill I/O): cancellation controller + task cancellation.
- `MonthManifestStore.loadSeeded` lists the actual remote directory (not just seed resources) to detect orphaned files from prior incomplete flushes.
- Download hash index is written per-item (not batched) so partial downloads survive stop/restart without re-downloading.
- `BackupSessionController` is created fresh per execution session to avoid state leakage between runs.

## Known Gaps

- No automated tests; critical paths rely on manual regression.
- Full backup resume still recomputes pending set by rescanning photo library.
- Manifest flush is per-worker-per-month + run-end; force-kill can still lose unflushed delta.
- WebDAV `RemoteStorageEntry.path` is returned percent-encoded from `list()`/`metadata()`, while SMB and LocalVolume return decoded paths. Callers that feed `entry.path` back into client methods (e.g. `RemoteIndexSyncService`) may double-encode non-ASCII characters on WebDAV.
