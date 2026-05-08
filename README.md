# Watermelon Photo Backup

**English** · [简体中文](README.zh-CN.md)

`Watermelon` is an iOS app that reads from the Photos library and backs up its assets to remote storage. The repo also ships a companion macOS target focused on legacy-data migration. The iOS codebase is settled around a single `Home` page, month-level execution plans, and a local hash index as the main pipeline.

## Repository Layout

- `Watermelon/` — iOS app target source
- `Shared/` — code shared between iOS and macOS (DB, Keychain, storage / SMB clients, manifest store, remote snapshot cache, domain models, execution logging, cross-platform extensions)
- `WatermelonMac/` — macOS target for legacy migration tools and storage-profile management; does **not** run the iOS backup pipeline
- `WatermelonTests/` — XCTest target for Home pure-logic units
- `docs/` — architecture, data model, UI flow, and open issues

> **⚠️ macOS target status**: `WatermelonMac` is **in active testing and has not been released in any form** — no App Store build, no TestFlight build, no signed distribution. Treat it as a development-only artifact: behavior, data layout, and migration paths can change without notice. Do not use it against irreplaceable photo libraries or production remote storage; back up any input data first.

## Current Capabilities (per code)

- Storage types: `SMB`, `WebDAV`, `S3`-compatible object storage, `external-volume folder` (via security-scoped bookmark)
- Month-level operations: `upload (local → remote)`, `download (remote → local)`, `sync (bidirectional)`
- Backup modes: `full`, `scoped(assetIDs)`, `retry(assetIDs)`
- Runtime controls: `start / pause / resume / stop / exit execution`
- Background backup: per-profile, gated by `backgroundBackupEnabled` and Pro entitlement
- Picture-in-Picture progress overlay (Pro)
- Remote-month verification: user-initiated maintenance via `RemoteMaintenanceController`
- Profile reachability: background TCP / HTTP / bookmark probes via `ProfileReachabilityService` flag offline profiles in the destination menu
- Upload scheduling: bucketed by month, dynamically claimed by multiple workers
- Remote index: remote manifests are scanned into `RemoteLibrarySnapshotCache`; Home consumes it incrementally via `revision + monthDeltas`
- Local index: `local_assets / local_asset_resources` back the local hash index and size cache
- Download resume: each successful item writes back a hash-index entry and refreshes the local view

## Startup & Main Flow

1. `SceneDelegate` creates `AppCoordinator`.
2. `AppCoordinator.start()` installs `HomeViewController` directly as the window root.
3. On first launch, `OnboardingViewController` is presented modally over Home.
4. `HomeViewController` binds `HomeScreenStore`.
5. `HomeScreenStore.load()` first loads the local photo-library index, then tries to auto-connect the last-active profile.
6. On a successful connection, `BackupCoordinator.reloadRemoteIndex(...)` refreshes the shared remote snapshot.
7. After the user picks months on Home, execution is handed to `HomeExecutionCoordinator`.

## Home Architecture

Home is no longer a fat view controller — it is split into many focused units:

1. `HomeViewController` — owns the `UICollectionView`, headers, connection menu, side overlays, bottom `SelectionActionPanel`, and the More/settings entry.
2. `HomeScreenStore` — main-actor aggregator. Composes `HomeIncrementalDataManager`, `HomeConnectionController`, `HomeExecutionCoordinator`, `PiPExecutionBridge`, `HomeScopeController`, `HomeScopeNormalizer`, `HomeSectionBuilder`, `HomePhotoAccessGate`, plus lazy `HomeRefreshScheduler` and `HomeSelectionController`. Projects internal changes into `.data / .fileSizes / .selection / .execution / .connection / .connectionProgress / .structural` (with month sets where relevant).
3. `HomeConnectionController` — loads saved profiles, auto-connects, prompts for passwords, switches / disconnects profiles, and triggers remote-index reloads.
4. `HomeIncrementalDataManager` — delegates index mutations and snapshot syncs to `HomeDataProcessingWorker` (which owns `HomeLocalIndexEngine` + `HomeRemoteIndexEngine`) and runs file-size scans through `HomeFileSizeScanCoordinator`.
5. `HomeExecutionCoordinator` — runs local-index preflight, the upload phase, inline sync-month finalization, the pure-download phase, and pause / resume / stop handling.

## Backup & Download Pipeline

### Upload

1. `HomeExecutionCoordinator` first freezes the execution settings for this run: `upload worker count` and `allow iCloud originals`.
2. A local-index preflight runs over all involved local assets; the first round is always offline. The first round also probes cache-hit assets for offline availability so iCloud-recovered assets are caught.
3. If `allow iCloud originals` is enabled and the upload scope (`upload + sync` months) has any `unavailableAssetIDs` after the first round, this run's upload is forced down to `1` worker.
4. If this run includes downloads or syncs and the first round still has `unavailableAssetIDs`:
   - `allow iCloud originals` enabled: re-build the index for those assets with network access (worker = 1).
   - Disabled: abort, to avoid producing duplicate resources due to missing local hashes.
5. The upload itself flows through `BackupSessionController` + `BackupSessionAsyncBridge`, which drive `BackupCoordinator.runBackup(...)`.
6. `BackupCoordinator` composes:
   - `BackupRunPreparationService` (in `BackupRunPreparation.swift`)
   - `BackupParallelExecutor`
   - `RemoteIndexSyncService` (in `Shared/Services/Backup/`)
   - `RemoteFormatCompatibilityService`
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

Migrations: `v1_initial`, `v2_ms_timestamps` (renames `local_assets.modificationDateNs` → `modificationDateMs` and divides existing values by 1_000_000).

Tables:

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

In-execution preflight is owned by `HomeExecutionCoordinator.prepareLocalIndexIfNeeded()`. A separate `LocalIndexBuildCoordinator` (with `LocalIndexChangePublisher`) drives user-initiated builds from the More-page / index UI.

## Tests

`WatermelonTests` covers Home pure-logic units (engines, controllers, schedulers, formatters). Real-device regression on the upload / download / sync paths and on macOS legacy migration is still manual.

## Development

1. Open `Watermelon.xcodeproj` with Xcode.
2. Select the `Watermelon` (iOS) or `WatermelonMac` (macOS — testing only, see warning above) scheme.
3. Run on the simulator or a real device.
4. Run the `WatermelonTests` target for the included unit tests.

## Documentation Map

- `AGENTS.md` — canonical project guide for coding agents (includes priority reading order); `CLAUDE.md` is a symlink to it so Claude Code auto-loads it
- `docs/01-Architecture.md` — module layering and dependencies (covers `Watermelon/`, `Shared/`, `WatermelonMac/`, `WatermelonTests/`)
- `docs/02-BackupCoreV2.md` — backup / download / sync execution details
- `docs/03-DataModel.md` — local DB, remote manifest, and in-memory snapshot schemas
- `docs/04-UIFlow.md` — Home, connection, execution, onboarding, and More-page flows
- `docs/05-OpenIssues.md` — current risks and technical debt
