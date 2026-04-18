# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Watermelon is an iOS photo backup app that reads from `PHAsset` and writes to remote storage (`SMB`, `WebDAV`, or an external-volume folder via security-scoped bookmark).

The app currently centers on a single Home screen plus a More/settings page. Home is no longer a fat view controller; the runtime flow is split across `HomeViewController`, `HomeScreenStore`, `HomeConnectionController`, `HomeExecutionCoordinator`, and `HomeIncrementalDataManager`.

Build with `Watermelon.xcodeproj`. There is no automated test suite in the repo; critical-path validation is still manual.

## Source Layout

```text
Watermelon/
  App/          # AppDelegate, SceneDelegate, AppCoordinator, AppSession, DependencyContainer
  Home/         # HomeViewController, HomeScreenStore, HomeScreenState,
                # HomeConnectionController, HomeExecutionCoordinator,
                # HomeExecutionSession, HomeExecutionDataRefresher,
                # HomeLibraryEngines, HomeAlbumMatching,
                # DownloadWorkflowHelper, SelectionActionPanel
  Services/
    Backup/     # BackupCoordinator, BackupRunPreparation, BackupParallelExecutor,
                # BackupSessionController, BackupSessionAsyncBridge,
                # BackupRunDriver, BackupResumePlanner, BackupMonthScheduler,
                # StorageClientPool, AssetProcessor (+Upload, +Naming),
                # MonthManifestStore (+Loading, +Schema),
                # RemoteIndexSyncService, RemoteLibrarySnapshotCache
    HashIndex/  # ContentHashIndexRepository, LocalHashIndexBuildService
    PhotoLibrary/
    Restore/
    SMB/
    Storage/
  UI/
    Auth/       # storage profile create/edit flows
    More/       # LocalHashIndexManagerViewController, WatermelonMoreDataSource, Settings
    Common/
  Data/
    Database/
    Security/
  Domain/       # backup/storage/remote snapshot domain models
```

## Architecture

### App Startup

`SceneDelegate` -> `AppCoordinator.start()` -> `HomeViewController`.

There is no global TabBar and no root `UINavigationController`; `HomeViewController` is set directly as the window root.

### Dependency Injection

`DependencyContainer` owns the top-level services:

- `DatabaseManager`
- `KeychainService`
- `AppSession`
- `StorageClientFactory`
- `PhotoLibraryService`
- `ContentHashIndexRepository`
- `LocalHashIndexBuildService`
- `BackupCoordinator`
- `RestoreService`

`AppSession` stores the active profile and in-memory session password. SMB/WebDAV require passwords; external volume does not.

### Home Layering

#### `HomeViewController`

UI-only layer for:

- two-column month grid
- top headers and profile menu
- right-side remote overlay (`connecting` / `disconnected`)
- bottom `SelectionActionPanel`
- floating More/settings button

It binds to `HomeScreenStore.onChange` and renders five change kinds:

- `.data`
- `.selection`
- `.execution`
- `.connection`
- `.structural`

#### `HomeScreenStore`

State aggregator for Home. Owns:

- `HomeIncrementalDataManager`
- `HomeConnectionController`
- `HomeExecutionCoordinator`

It maintains:

- `sections`
- `rowLookup`
- `selection`
- derived `connectionState`
- derived `executionState`

It also coalesces refresh work (`reloadLocal`, `syncRemote`, connection/structural notifications) instead of cancelling in-flight refreshes.

#### `HomeConnectionController`

Responsible for:

- loading saved profiles
- auto-connecting the last active profile using `sync_state` + Keychain
- prompting for passwords
- switching/disconnecting profiles
- calling `BackupCoordinator.reloadRemoteIndex(...)`

If a new connection attempt fails and a previous profile is still active, it tries to restore the old remote snapshot.

#### `HomeExecutionCoordinator`

Coordinates one execution session:

1. local hash-index preflight via `LocalHashIndexBuildService`
2. upload via `BackupSessionController` + `BackupSessionAsyncBridge`
3. inline sync-month finalization after upload flush
4. remaining download months
5. pause / resume / stop / missing-connection failure

State for a single execution lives in `HomeExecutionSession` and is exposed as `HomeExecutionState`.

#### `HomeIncrementalDataManager`

Owns the Home data pipeline:

- local photo-library index
- remote snapshot index
- reconciliation engine

It registers as a `PHPhotoLibraryChangeObserver`, applies remote deltas on a processing queue, and scans file sizes on the main actor with `Task.yield()` between months.

### Backup Control Plane

`BackupSessionController` (`@MainActor`) is the upload control plane:

- start / pause / stop / resume
- run lifecycle
- observer snapshots
- month started/completed tracking
- processed/failed counts per month

Home creates a fresh `BackupSessionController` for each execution session.

### Backup Execution Plane

`BackupCoordinator` composes:

- `BackupRunPreparationService`
- `BackupParallelExecutor`
- `RemoteIndexSyncService`

#### Phase 1 — Preparation (`BackupRunPreparationService.prepareRun`)

1. ensure photo authorization
2. create/connect storage client
3. ensure `basePath` exists
4. sync remote manifests into `RemoteLibrarySnapshotCache`
5. optionally build `MonthSeedLookup` when the snapshot is not too large
6. load assets (`full`, `scoped`, or `retry`)
7. group asset IDs by month
8. estimate month bytes from the local hash index
9. resolve worker count / connection pool size

#### Phase 2 — Parallel execution (`BackupParallelExecutor.execute`)

1. create `StorageClientPool`
2. dynamically assign months via `MonthWorkQueue`
3. per month: `MonthManifestStore.loadOrCreate(...)`
4. process assets in batches of 500
5. per asset: `AssetProcessor.process(...)`
6. flush manifest when the month completes
7. run `onMonthUploaded` callback after flush when provided

### Sync-Month Finalization

This is easy to miss from older docs:

- sync months are not always deferred to a single download stage at the end
- after a sync month uploads and flushes successfully, Home can immediately:
  - sync remote data
  - refresh local index
  - download that month’s `remoteOnlyItems`

Pure download months still run after the upload phase finishes.

### Data Storage

#### Local SQLite (`DatabaseManager`)

Migration: `v1_initial`

Tables:

- `server_profiles`
- `sync_state`
- `local_assets`
- `local_asset_resources`

#### Remote Monthly Manifest (`MonthManifestStore`)

Path: `/{YYYY}/{MM}/.watermelon_manifest.sqlite`

Tables:

- `resources`
- `assets`
- `asset_resources`

Migration: `month_manifest_v1_initial`

#### In-Memory Remote Snapshot

Home consumes remote state via:

- `RemoteLibrarySnapshot`
- `RemoteLibrarySnapshotState(revision, isFullSnapshot, monthDeltas)`

The remote snapshot cache is shared and incrementally updated; Home does not rescan the remote storage from scratch on every UI update.

### Storage Clients

Implementations of `RemoteStorageClientProtocol`:

- `AMSMB2Client`
- `WebDAVClient`
- `LocalVolumeClient`

Factory:

- `StorageClientFactory.makeClient(profile:password:)`

## Key Rules & Invariants

- Home selection is disabled when no remote profile is connected or when execution is active.
- Before download/sync execution, local hash-index preflight must succeed sufficiently to avoid duplicate imports.
- `assetFingerprint` is SHA-256 of sorted `role|slot|hashHex` tokens joined by `\n`.
- Sync months may reach `uploadDone` before they are fully complete; they become `completed` only after download finalization.
- Download success writes hash-index entries per item, so completed items survive stop/restart.
- `MonthManifestStore.loadSeeded(...)` lists the actual remote directory to detect orphaned files from an earlier unflushed manifest.
- Worker scheduling is dynamic by month, not static partitioning.

## Known Gaps

- No automated tests; manual regression is still the main safety net.
- Full backup resume still rescans the photo library to rebuild the pending set.
- Manifest flush still has a force-kill window where recent deltas may not be pushed to the remote manifest.
- Download/sync can be blocked by local hash-index preflight when originals are not present on-device (intentional safety tradeoff).
- Home refresh/execution/connection interactions are clearer than before, but still subtle enough that refactors need careful validation.
