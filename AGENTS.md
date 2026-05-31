# AGENTS.md

Briefing for coding agents. Loaded by Claude Code via the `CLAUDE.md` symlink and by Codex through the `AGENTS.md` convention. **Briefing, not reference** — point at `docs/` or read source for anything beyond.

## Project

iOS photo-backup app: reads `PHAsset`, writes to `SMB` / `WebDAV` / `S3`-compatible / `SFTP` / external-volume storage. Single Home screen + More page + first-launch onboarding. Build with `Watermelon.xcodeproj`.

`WatermelonMac/` is a separate macOS target for legacy-data migration only — it does **not** run the iOS backup pipeline, has no released build, and shouldn't be pointed at real user data.

`WatermelonTests` (XCTest) covers Home pure-logic units, storage/S3/SFTP shapes, Repo V2 materialize / flush / bootstrap / migration / retention, `RemoteIndexSyncService`, `RepoVerifyMonthService`, restore/download boundaries, and backup state reducers/planners. Real PhotoKit + real remote end-to-end paths are still manually regressed.

## Targets

- `Watermelon/` — iOS app (Home UI, BackupCoordinator glue, iOS-only services)
- `Shared/` — code shared with macOS (DB, Keychain, SMB / WebDAV / S3 / SFTP / external-volume clients, storage capability traits, Repo V2 runtime / format / migration / retention services, `MonthManifestStore`, `RemoteIndexSyncService`, `RemoteLibrarySnapshotCache`, domain models, logging)
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
8. `Watermelon/Services/Backup/BackupSessionController.swift` + `BackupCoordinator.swift` + `BackupRunPreparation.swift` + `BackupParallelExecutor.swift` + `AssetProcessor.swift` + `BackgroundBackupRunner.swift`
9. `Shared/Services/Backup/BackupMonthStore.swift` + `V2MonthSession.swift` + `V2MonthIndexes.swift` + `V2MonthCommitFlusher.swift` + `MonthManifestStore.swift`
10. `Shared/Services/Repo/BackupV2RuntimeBuilder.swift` + `BackupV2RepoOpenService.swift` + `RemoteFormatCompatibilityService` in `Shared/Services/Backup/RemoteFormatCompatibility.swift`
11. `Shared/Services/Repo/RepoBootstrap.swift` + `RepoMaterializer.swift` + `CommitLogWriter.swift` + `SnapshotWriter.swift` + `V1MigrationService.swift`
12. `Shared/Services/Repo/RepoCompactionService.swift` + `RepoCheckpointService.swift` + `RepoSnapshotDeletePreflightService.swift` + `RepoSnapshotDeleteExecutor.swift` + `RepoRetentionDeletePreflightService.swift` + `RepoRetentionDeleteExecutor.swift`
13. `Shared/Services/Backup/RemoteIndexSyncService.swift` + `Shared/Services/Repo/RepoVerifyMonthService.swift`
14. `Watermelon/Services/Backup/RemoteMaintenanceController.swift`
15. `Watermelon/Services/Restore/RestoreService.swift`

## Architecture (only what filenames don't already tell you)

**Home is composed, not monolithic.** `HomeScreenStore` (main-actor) aggregates focused controllers and projects state via seven `HomeChangeKind` cases (`.data` / `.fileSizes` / `.execution` carry month sets; `.selection` / `.connection` / `.connectionProgress` / `.structural` don't). Index mutations run on `HomeDataProcessingWorker`'s serial queue — never call `PHAsset` fetches outside it.

**Storage clients live behind one protocol.** `RemoteStorageClientProtocol` (in `Shared/Services/SMB/SMBClientProtocol.swift`) is implemented by `AMSMB2Client`, `WebDAVClient`, `LocalVolumeClient`, `S3Client`, `SFTPClient`. Construct via `StorageClientFactory.makeClient(profile:password:)`. `ProfileReachabilityService` background-probes saved profiles for offline marking in the destination menu.

**Repo V2 is the current write format.** Fresh and migrated remotes use `.watermelon/` V2 metadata: per-asset commit jsonl plus materialized snapshots. V1 per-month sqlite manifests remain only for compatibility, migration, verify helpers, and old-repo reads. Runtime opening goes through format inspection and V2 open planners; foreground runs may migrate V1, background runs do not.

**Remote maintenance is explicit and conservative.** `RemoteMaintenanceController` drives user-triggered month verification and blocks Home selection while active. V2 compaction (checkpoint, commit GC, snapshot GC) runs via `RepoCompactionService` during startup maintenance, guarded by covered-max materialization, clean outcome gates, and post-delete verification.

## Invariants Worth Memorising

- Home selection is disabled when not connected, photo access missing, execution active, scope reloading, or remote maintenance running.
- Local hash-index preflight runs before any download / sync execution. First round is always offline; download / sync abort on iCloud-only assets unless `allow iCloud originals` enables a network second pass. Upload-only with that setting off skips preflight and marks network-required resources skipped during upload.
- `assetFingerprint` = SHA-256 of sorted `role|slot|hashHex` tokens joined by `\n`. It is the dedup key everywhere.
- V2 commit log + snapshot is the main write path. V1 `.watermelon_manifest.sqlite` is legacy compatibility / migration state, not the V2 month source of truth.
- Remote format inspection is fail-closed for unsupported, damaged, or foreground-migration-required states; do not silently fall back to V1 behavior on V2 uncertainty.
- V2 row-writing asset results commit before publish / local hash-index write. Batch flush is commit-only; partial durable commit errors surface via `MonthDurableCommitPartial`.
- Checkpoint, commit GC, and snapshot GC are conservative maintenance only. Non-clean outcomes, migration markers, or failed verification should skip deletion rather than force cleanup.
- `BackgroundBackupRunner` opens V2 runtime with migration disabled; V1 / V2-with-V1-residue profiles are skipped with logs until a foreground run migrates them.
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
- `docs/06-RepoV2.md` — V2 remote format, migration, materialization, compaction, and historical hardening notes
