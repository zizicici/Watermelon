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
    Backup/     # BackupExecutor, BackupAssetResourcePlanner, MonthManifestStore,
                # RemoteLibraryScanner, RemoteLibrarySnapshotCache,
                # ContentHashIndexRepository, RemoteNameCollisionResolver
    SMB/        # AMSMB2Client, SMBSetupService
    Storage/    # RemoteStorageClientProtocol, StorageClientFactory,
                # WebDAVClient, LocalVolumeClient, SecurityScopedBookmarkStore
                # RemoteThumbnailService
    Discovery/  # SMBDiscoveryService (Bonjour, _smb._tcp)
    PhotoLibrary/ Restore/ Metadata/
  UI/
    Backup/     # BackupSessionController, BackupStatusViewController,
                # BackupFailedItemsViewController, BackupFailedItemDetailViewController
    Album/      # AlbumGridCell, AlbumSectionHeaderView
    Auth/ Settings/ Browser/ Common/
  Data/
    Database/   # DatabaseManager, Records
    Security/   # KeychainService
  Domain/       # BackupDomain, RemoteLibraryDomain, FingerprintBuilder, RemotePathBuilder
```

## Architecture

### App Startup

`SceneDelegate` → `AppCoordinator.start()` → `showHome()` → `HomeViewController`.
No TabBar. `ServerSelectionViewController` and `SettingsViewController` exist in the codebase but are **not** in the current startup path.

### Dependency Injection

Single `DependencyContainer` created at startup holds all top-level singletons:
`DatabaseManager`, `KeychainService`, `AppSession`, `StorageClientFactory`, `PhotoLibraryService`, `MetadataService`, `BackupExecutor`, `RestoreService`.

`AppSession` holds the active `ServerProfileRecord` and in-memory password (SMB/WebDAV require it; external-volume does not).

### Backup Pipeline

The central flow runs inside `BackupExecutor.runBackup(...)`:

1. Request photo permission.
2. Build storage client via `StorageClientFactory`, connect, ensure `basePath` exists.
3. `RemoteLibraryScanner.scanYearMonthTree` — **read-only** scan of `YYYY/MM/.watermelon_manifest.sqlite` files; populates `RemoteLibrarySnapshotCache`.
4. Iterate `PHAsset` sorted by `creationDate ASC`.
5. Per-asset: `BackupAssetResourcePlanner` assigns role/slot order and computes `assetFingerprint` (SHA-256 of sorted `role|slot|hashHex` tokens).
6. Month boundary: flush previous month's manifest, `loadOrCreate` new month.
7. End: flush current month's manifest.

**Asset failure rule**: if any single resource upload fails, the whole asset is marked failed and nothing is written to `assets`/`asset_resources`.

**Pause/stop**: implemented as `Task.cancel()`; `BackupExecutor` cooperatively checks `Task.isCancelled`. Current network I/O finishes before exiting.

`BackupSessionController` (in `UI/Backup/`) drives the state machine (idle → running → paused/stopped/failed) and aggregates log entries and progress for `BackupStatusViewController`.

### Data Storage

**Local SQLite** (GRDB, `DatabaseManager`), migrations `v3_dev_reset_schema`, `v4_server_profiles_storage_type`, `v5_server_profiles_sort_order`, `v6_server_profiles_partial_unique_smb`:
- `server_profiles` — saved storage profiles (`storageType`, `connectionParams`, `sortOrder`)
- `sync_state` — key/value store (e.g. `active_server_profile_id`)
- `local_assets` — per-asset fingerprint cache
- `local_asset_resources` — per-resource content hash by `(assetLocalIdentifier, role, slot)`

`server_profiles` uniqueness is now SMB-only via partial unique index on `(host, shareName, basePath, username)` with `WHERE storageType = 'smb'`.

**Remote monthly manifest** (`MonthManifestStore`), path `/{YYYY}/{MM}/.watermelon_manifest.sqlite`, migrations `month_manifest_v2_reset_schema` + `month_manifest_v2_schema_baseline` (idempotent baseline, non-destructive):
- `resources` — individual files (keyed by `fileName`, unique on `contentHash`)
- `assets` — logical asset records (keyed by `assetFingerprint`)
- `asset_resources` — join table linking asset ↔ resource with `role`/`slot`

Remote manifest migrations must stay incremental and non-destructive (no reset/drop-recreate for schema upgrades).

Manifest writes are deferred: `upsertResource/upsertAsset` mark `dirty=true`; `flushToRemote()` uploads the sqlite file at month boundaries and at `runBackup` completion.

**Passwords** are stored in Keychain under service `com.zizicici.watermelon.credentials`, not in the database.

### Home Page Matching

`HomeAlbumMatching` (decoupled from the view) merges the local index and remote snapshot into `localOnly` / `remoteOnly` / `both` entries, grouped by year-month section. Remote items are assembled strictly from the three-table manifest relationship.

### Remote Thumbnails

`RemoteThumbnailService` downloads the full remote file and downsamples it locally (actor-based concurrency limiting). Thumbnail cache key uses `StorageProfile.identityKey` to avoid cross-storage collisions.

## Key Rules & Invariants

- Remote scanner is **read-only** — never creates directories or writes manifests during scanning.
- `assetFingerprint` is the SHA-256 of sorted `role|slot|hashHex` tokens joined by `\n`.
- Name-collision strategy: files < 5 MiB → download and compare hash; files ≥ 5 MiB → size heuristic; still colliding → `_n` suffix via `RemoteNameCollisionResolver`.
- Upload retries: max 3 attempts with exponential back-off; `STATUS_OBJECT_NAME_COLLISION` triggers immediate rename retry.

## Known Gaps

- No automated tests; key logic (`BackupExecutor`, collision resolver, flush timing) is untested.
- `SettingsViewController` and `ServerSelectionViewController` are dead code from the current startup path — clarify intent before modifying.
- Full backup always re-scans the entire photo library; no incremental resume after pause.
- Manifest is only flushed at month boundaries and job end — a force-kill mid-month can lose recent progress.
