# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

iOS photo backup app ("Watermelon") that backs up `PHAsset` items to remote storage (SMB/WebDAV/external-volume folder). The repository name is `PhotoBackup`; the Xcode target is `Watermelon`.

Build with `Watermelon.xcodeproj`. There is no package manager CLI or test runner — all building and testing is done through Xcode. The project has no automated test suite; regression testing is done manually on device with SMB/WebDAV server and external drive.

## Source Layout

```
Watermelon/
  App/          # AppDelegate, SceneDelegate, AppCoordinator, AppSession, DependencyContainer
  Home/         # HomeViewController, HomeAlbumMatching, HomeLibraryEngines
  Services/
    Backup/     # BackupCoordinator, AssetProcessor, BackupAssetResourcePlanner,
                # MonthManifestStore, RemoteIndexSyncService,
                # RemoteManifestIndexScanner, RemoteLibrarySnapshotCache,
                # ContentHashIndexRepository, BackupCancellationController,
                # RemoteNameCollisionResolver
    SMB/        # AMSMB2Client, SMBSetupService
    Storage/    # RemoteStorageClientProtocol, StorageClientFactory,
                # WebDAVClient, LocalVolumeClient, SecurityScopedBookmarkStore
    Discovery/  # SMBDiscoveryService (Bonjour, _smb._tcp)
    PhotoLibrary/ Restore/ Metadata/
  UI/
    Backup/     # BackupSessionController, BackupViewController
    Album/      # AlbumGridCell, AlbumSectionHeaderView
    Auth/ Settings/ Browser/ Common/
  Data/
    Database/   # DatabaseManager, Records
    Security/   # KeychainService
  Domain/       # BackupDomain, RemoteLibraryDomain, RemotePathBuilder
```

## Architecture

### App Startup

`SceneDelegate` → `AppCoordinator.start()` → `showHome()` → `HomeViewController`.
No TabBar. `ServerSelectionViewController` and `SettingsViewController` exist in the codebase but are **not** in the current startup path.

### Dependency Injection

Single `DependencyContainer` created at startup holds all top-level singletons:
`DatabaseManager`, `KeychainService`, `AppSession`, `StorageClientFactory`, `PhotoLibraryService`, `MetadataService`, `BackupCoordinator`, `RestoreService`.

`AppSession` holds the active `ServerProfileRecord` and in-memory password (SMB/WebDAV require it; external-volume does not).

### Backup Control Plane

`BackupSessionController` (`@MainActor`) is the unified control plane and UI-facing state aggregator:

1. Handles `start/pause/stop/resume/retry` commands directly.
2. Maintains run token, termination intent, and run task lifecycle.
3. Creates per-run `BackupEventStream` and manages `runTask`/`eventListenerTask`.
4. Processes `BackupEvent` directly from the event stream (no intermediate signal layer).

### Backup Execution Plane

The execution flow runs inside `BackupCoordinator.runBackup(..., context: BackupRunContext)`:

1. Request photo permission.
2. Build storage client via `StorageClientFactory`, connect, ensure `basePath` exists.
3. `RemoteIndexSyncService.syncIndex` scans month manifest digests and applies deltas to `RemoteLibrarySnapshotCache`.
4. Iterate `PHAsset` sorted by `creationDate ASC`.
5. Per-asset processing is delegated to `AssetProcessor`.
6. Month boundary: flush previous month manifest, load/create new month store.
7. End: flush current month manifest and emit terminal event.

`BackupRunContext` carries run-scoped dependencies:

- `eventSink: BackupEventStream`
- `cancellationController: BackupCancellationController`

`BackupCoordinator` no longer exposes global `eventStream` or `cancelActiveBackup` APIs.

### Data Storage

**Local SQLite** (GRDB, `DatabaseManager`), migrations `v3_dev_reset_schema`, `v4_server_profiles_storage_type`, `v5_server_profiles_sort_order`, `v6_server_profiles_partial_unique_smb`:

- `server_profiles` — saved storage profiles (`storageType`, `connectionParams`, `sortOrder`)
- `sync_state` — key/value store (e.g. `active_server_profile_id`)
- `local_assets` — per-asset fingerprint cache
- `local_asset_resources` — per-resource content hash by `(assetLocalIdentifier, role, slot)`

`server_profiles` uniqueness is SMB-only via partial unique index on `(host, shareName, basePath, username)` with `WHERE storageType = 'smb'`.

**Remote monthly manifest** (`MonthManifestStore`), path `/{YYYY}/{MM}/.watermelon_manifest.sqlite`, migrations `month_manifest_v2_reset_schema` + `month_manifest_v2_schema_baseline` (idempotent baseline, non-destructive):

- `resources`
- `assets`
- `asset_resources`

Manifest writes are deferred: `upsertResource/upsertAsset` mark `dirty=true`; `flushToRemote()` uploads at month boundaries and run completion.

Passwords are stored in Keychain (`com.zizicici.watermelon.credentials`), not in local DB.

### Home Page Matching

`HomeAlbumMatching` merges local index and remote snapshot into `localOnly` / `remoteOnly` / `both` entries, grouped by year-month section. Remote items are assembled strictly from `assets + asset_resources + resources` relationships.

### Remote Thumbnails

`RemoteThumbnailService` downloads full remote files and downsamples locally (actor-based concurrency limiting). Cache key includes `StorageProfile.identityKey` to avoid cross-storage collisions.

## Key Rules & Invariants

- Remote scanner is **read-only** — never creates directories or writes manifests during scanning.
- `assetFingerprint` is SHA-256 of sorted `role|slot|hashHex` tokens joined by `\n`.
- Name-collision strategy: files < 5 MiB download+hash compare; files ≥ 5 MiB size heuristic; unresolved conflicts use `_n` suffix.
- Upload retries: max 3 attempts with exponential back-off; collision status triggers immediate rename retry.
- Pause/stop are cooperative cancellation (not force-kill I/O): cancellation controller + task cancellation.

## Known Gaps

- No automated tests; critical paths rely on manual regression.
- Full backup resume still recomputes pending set by rescanning photo library.
- Manifest flush is month-boundary + run-end; force-kill can still lose unflushed delta.
- `RemoteIndexSyncService` currently has Sendable warnings that should be resolved before Swift 6 strict mode.
