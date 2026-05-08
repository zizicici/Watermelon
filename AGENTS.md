# AGENTS.md

Briefing for coding agents. Loaded by Claude Code via the `CLAUDE.md` symlink and by Codex through the `AGENTS.md` convention. **Briefing, not reference** — point at `docs/` or read source for anything beyond.

## Project

iOS photo-backup app: reads `PHAsset`, writes to `SMB` / `WebDAV` / `S3`-compatible / external-volume storage. Single Home screen + More page + first-launch onboarding. Build with `Watermelon.xcodeproj`.

`WatermelonMac/` is a separate macOS target for legacy-data migration only — it does **not** run the iOS backup pipeline, has no released build, and shouldn't be pointed at real user data.

`WatermelonTests` (XCTest) covers Home pure-logic units and S3 SigV4. Anything that touches a real photo library or remote is manually regressed.

## Targets

- `Watermelon/` — iOS app (Home UI, BackupCoordinator glue, iOS-only services)
- `Shared/` — code shared with macOS (DB, Keychain, storage / SMB / S3 clients, `MonthManifestStore`, `RemoteIndexSyncService`, `RemoteLibrarySnapshotCache`, domain models, logging)
- `WatermelonMac/` — macOS target (legacy migration, profile management)
- `WatermelonTests/` — XCTest

## Key Files (read in this order for a substantive task)

1. `Watermelon/App/DependencyContainer.swift`
2. `Watermelon/Home/HomeViewController.swift`
3. `Watermelon/Home/HomeScreenStore.swift`
4. `Watermelon/Home/HomeConnectionController.swift`
5. `Watermelon/Home/HomeExecutionCoordinator.swift`
6. `Watermelon/Home/HomeIncrementalDataManager.swift` + `HomeDataProcessingWorker.swift` + `HomeLocalIndexEngine.swift` + `HomeRemoteIndexEngine.swift`
7. `Watermelon/Services/HashIndex/LocalHashIndexBuildService.swift`
8. `Watermelon/Services/Backup/BackupSessionController.swift` + `BackupCoordinator.swift` + `BackupRunPreparation.swift` + `BackupParallelExecutor.swift` + `AssetProcessor.swift`
9. `Shared/Services/Backup/RemoteIndexSyncService.swift` + `MonthManifestStore.swift`
10. `Watermelon/Services/Restore/RestoreService.swift`

## Architecture (only what filenames don't already tell you)

**Home is composed, not monolithic.** `HomeScreenStore` (main-actor) aggregates focused controllers and projects state via seven `HomeChangeKind` cases (`.data` / `.fileSizes` / `.execution` carry month sets; `.selection` / `.connection` / `.connectionProgress` / `.structural` don't). Index mutations run on `HomeDataProcessingWorker`'s serial queue — never call `PHAsset` fetches outside it.

**Storage clients live behind one protocol.** `RemoteStorageClientProtocol` (in `Shared/Services/SMB/SMBClientProtocol.swift`) is implemented by `AMSMB2Client`, `WebDAVClient`, `LocalVolumeClient`, `S3Client`. Construct via `StorageClientFactory.makeClient(profile:password:)`. `ProfileReachabilityService` background-probes saved profiles for offline marking in the destination menu.

## Invariants Worth Memorising

- Home selection is disabled when not connected, photo access missing, execution active, scope reloading, or remote maintenance running.
- Local hash-index preflight runs before any download / sync execution. First round is always offline; iCloud-only assets get a network-allowed second pass only when `allow iCloud originals` is enabled (otherwise the run aborts).
- `assetFingerprint` = SHA-256 of sorted `role|slot|hashHex` tokens joined by `\n`. It is the dedup key everywhere.
- Sync months reach `uploadDone` after upload flush, then `completed` only after `BackupParallelExecutor`'s `onMonthUploaded` finishes the inline download. **Don't treat `uploadDone` as "month done".**
- Successful downloads write a hash-index entry immediately, so they survive stop / restart.
- `MonthManifestStore.loadSeeded(...)` lists the actual remote directory to detect orphans from an unflushed manifest.
- Worker scheduling is dynamic by month. `iCloud originals enabled` + any iCloud-only asset in upload scope forces upload to 1 worker.
- `S3Client.setModificationDate` is a no-op but `shouldSetModificationDate` still returns `true` to keep the upload path uniform.

## Code Style

- **Comments**: default to none. When you do write one, keep it to a single short line and capture **why** (a non-obvious constraint, invariant, or workaround), never **what**. No multi-paragraph docstrings, no "added for X", no narration of the diff.

## Doc Map

- `docs/01-Architecture.md` — full module layering, every helper's role
- `docs/02-BackupCoreV2.md` — preflight / upload / sync / download details, constants, retry rules
- `docs/03-DataModel.md` — SQLite schemas, `connectionParams` payloads, in-memory snapshot types
- `docs/04-UIFlow.md` — Home, menus, selection, execution states, More page
- `docs/05-OpenIssues.md` — known gaps and ordering for follow-up work
