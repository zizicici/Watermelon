# Watermelon Photo Backup

**English** · [简体中文](README.zh-CN.md)

`Watermelon` is an iOS app that reads from the Photos library and backs up its assets to remote storage. The current codebase has settled around a single `Home` page, month-level execution plans, and a local hash index as the main pipeline.

## Current Capabilities (per code)

- Storage types: `SMB`, `WebDAV`, `external-volume folder` (via security-scoped bookmark)
- Month-level operations: `upload (local → remote)`, `download (remote → local)`, `sync (bidirectional)`
- Backup modes: `full`, `scoped(assetIDs)`, `retry(assetIDs)`
- Runtime controls: `start / pause / resume / stop / exit execution`
- Upload scheduling: bucketed by month, dynamically claimed by multiple workers
- Remote index: remote manifests are scanned into `RemoteLibrarySnapshotCache`; Home consumes it incrementally via `revision + monthDeltas`
- Local index: `local_assets / local_asset_resources` back the local hash index and size cache
- Download resume: each successful item writes back a hash-index entry and refreshes the local view

## Startup & Main Flow

1. `SceneDelegate` creates `AppCoordinator`.
2. `AppCoordinator.start()` installs `HomeViewController` directly as the window root.
3. `HomeViewController` binds `HomeScreenStore`.
4. `HomeScreenStore.load()` first loads the local photo-library index, then tries to auto-connect the last-active profile.
5. On a successful connection, `BackupCoordinator.reloadRemoteIndex(...)` refreshes the shared remote snapshot.
6. After the user picks months on Home, execution is handed to `HomeExecutionCoordinator`.

## Home Architecture

Home is no longer a fat view controller — it is split into four layers:

1. `HomeViewController`
   Owns the `UICollectionView`, headers, connection menu, right-side overlay, bottom `SelectionActionPanel`, and the More/settings entry.
2. `HomeScreenStore`
   Aggregates Home state: `sections / rowLookup / selection / connectionState / executionState`. Projects internal changes into `.data / .fileSizes / .selection / .execution / .connection / .connectionProgress / .structural`.
3. `HomeConnectionController`
   Loads saved profiles, auto-connects, prompts for passwords, switches / disconnects profiles, and triggers remote-index reloads.
4. `HomeExecutionCoordinator`
   Runs local-index preflight, the upload phase, inline sync-month finalization, the pure-download phase, and pause / resume / stop handling.

## Backup & Download Pipeline

### Upload

1. `HomeExecutionCoordinator` first freezes the execution settings for this run: `upload worker count` and `allow iCloud originals`.
2. If this run includes uploads and `allow iCloud originals` is enabled, a lightweight availability probe runs against the upload scope; as soon as an iCloud-only local asset is detected, this run's upload is forced down to `1` worker.
3. A local-index preflight runs over all involved local assets; the first round is always offline.
4. If this run includes downloads or syncs and the first round still has `unavailableAssetIDs`:
   - `allow iCloud originals` enabled: re-build the index for those assets with network access.
   - Disabled: abort, to avoid producing duplicate resources due to missing local hashes.
5. The upload itself flows through `BackupSessionController` + `BackupSessionAsyncBridge`, which drive `BackupCoordinator.runBackup(...)`.
6. `BackupCoordinator` is split into:
   - `BackupRunPreparationService.prepareRun`
   - `BackupParallelExecutor.execute`
7. `BackupParallelExecutor` uses `MonthWorkQueue` to assign months dynamically. Each worker loads `MonthManifestStore` per month and calls `AssetProcessor.process(...)` per asset.

### Sync Months

- Sync months are **not** simply "upload everything, then download everything".
- Once a sync month's upload flush succeeds, an `onMonthUploaded` callback runs that month's download finalization immediately.
- Pure download months run sequentially after the upload phase finishes.

### Download

1. `DownloadWorkflowHelper` calls `RestoreService.restoreItems(...)`.
2. Each successful item writes a hash-index entry right away.
3. `HomeExecutionDataRefresher` refreshes the local index and remote snapshot so Home progress advances in step.

## Data Storage

### Local SQLite (GRDB)

- `server_profiles`
- `sync_state`
- `local_assets`
- `local_asset_resources`

### Remote Monthly Manifest (SQLite)

Each month directory holds a `/{YYYY}/{MM}/.watermelon_manifest.sqlite` file containing:

- `resources`
- `assets`
- `asset_resources`

## Local Hash Index

`LocalHashIndexBuildService` builds and back-fills the local hash index; `ContentHashIndexRepository` handles reads and writes:

- resource-level `contentHash`
- `assetFingerprint`
- `totalFileSizeBytes`
- coverage / statistics

There is no standalone user entry for hash-index management; building is driven automatically by the per-run preflight in `HomeExecutionCoordinator.prepareLocalIndexIfNeeded()`.

## Development

1. Open `Watermelon.xcodeproj` with Xcode.
2. Select the `Watermelon` scheme.
3. Run on the simulator or a real device.

There is no systematic automated test suite; critical paths are still validated by manual regression on-device.

## Documentation Map

- `AGENTS.md` — canonical project guide for coding agents (includes priority reading order); `CLAUDE.md` is a symlink to it so Claude Code auto-loads it
- `docs/01-Architecture.md` — module layering and dependencies
- `docs/02-BackupCoreV2.md` — backup / download / sync execution details
- `docs/03-DataModel.md` — local DB, remote manifest, and in-memory snapshot schemas
- `docs/04-UIFlow.md` — Home, connection, execution, and More-page flows
- `docs/05-OpenIssues.md` — current risks and technical debt
