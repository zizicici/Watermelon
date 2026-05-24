# 06 — Repo V2

V1（per-month manifest sqlite）→ V2（commit log + snapshot）格式重构。`~/.claude/plans/wobbly-launching-stroustrup.md` 是历史设计记录；当前事实以本文件的“现状 / 当前实现摘要”和源码为准。

## Boundary Invariants

- Required wire numeric fields reject missing, boolean, fractional, negative-for-unsigned, and overflow values through their owning decoder/store. Optional wire numeric fields may be absent per the owning schema; malformed present values must not coerce into a valid number (boolean/fractional/overflow must surface as absent or rejected, never as 0/1/truncated).
- Bootstrap and migration advisory timestamps (`VersionManifest.created_at_ms`, `MigrationMarker.started_at_ms` / `last_step_at_ms`) are optional metadata, not repo identity, seq, clock, or materialization authority fields.
- Snapshot materialization is idempotent for unchanged remote metadata.
- Snapshot baseline plus uncovered commits must materialize to the same state as replaying all valid commits from genesis for the same repo; equality includes assets, resources, asset-resource links, deleted keys/stamps, observed seq by writer, and observed clock.
- Presence/freshness handles fail closed: transport/probe uncertainty cannot be published as fresh authoritative absence.
- Identity source resolution converges only when authoritative sources agree; own current claim can repair wipe-and-reuse missing DB exact row, foreign claim cannot.
- Metadata create outcome `verification == .verifiedLocalBytes` means remote bytes matched the local payload during the gate's verification window.
- `MonthManifestStore.loadSeeded(...)` must surface V1 seeded-manifest orphans by listing the actual remote directory; this is retained for current app behavior, not V2 materializer semantics.
- Concurrent state allocation must not produce duplicate same-writer seq values or emit clocks below accepted remote observation.

## 现状

V2 cutover 已完成。新 / 旧客户端在仓库上的行为：

| 仓库状态 / inspect 结果 | V2 客户端（本次起） | 1.2.0+ V1 客户端 | pre-1.2.0 客户端 |
|---|---|---|---|
| `.fresh` | `RepoBootstrap` 建 identity claims / finalization / `repo.json` read-cache + `version.json`，进入 V2 写入路径 | 看到 `.watermelon/` → throw `remoteFormatUnsupported` | 无 sentinel 检测，会把目录当 fresh 用，与 V2 desync |
| `.v1` | 前台自动同步迁移，随后 V2 写入 | 正常 V1 行为（在迁移完成前） | 同上 |
| `.v2(formatVersion:)` | `RepoMaterializer` 物化，写入路径用 V2 commit log + snapshot | throw `remoteFormatUnsupported(minAppVersion)` | desync |
| `.v2WithV1Manifests` | 需要前台继续迁移；后台直接跳过并写 execution log | throw `remoteFormatUnsupported` | desync |
| `.v2WithPendingMigrationCleanup` | build runtime 时做 cleanup-only，然后按 V2 运行 | throw `remoteFormatUnsupported` | desync |
| `.unsupported` 或 inspect 抛 `damagedV2Repo` | fail-closed，向用户展示兼容性 / 损坏诊断 | throw `remoteFormatUnsupported` | 未定义 |

后台备份（`BackgroundBackupRunner`）不会自动迁移 V1：检测到 V1 仓库直接结束 + execution log 提示用户在前台运行。

## 当前实现摘要

当前 V2 由四组能力组成：

1. **身份与版本**：`RepoBootstrap`、`IdentityClaimStore`、`VersionManifestStore`、`RepoIdentityAuthority` 共同收敛 canonical `repoID`，`repo.json` 是 read-cache，`version.json` 是格式版本 sentinel。
2. **写入与读取**：`CommitLogWriter` 写 per-asset / tombstone jsonl commit，`SnapshotWriter` 写月级 materialized snapshot，`RepoMaterializer` 以 accepted snapshot baseline + uncovered commits 物化当前状态。
3. **迁移与兼容**：`RemoteFormatCompatibilityService.inspectRemoteFormat(...)` 路由 fresh / V1 / V2 / pending-cleanup / unsupported；前台可执行 V1→V2 migration，后台只记录需要前台迁移。
4. **维护与 retention**：`RepoCheckpointService` 按 `RepoCompactionPolicy` 写 per-month checkpoint snapshot；`RepoRetentionBarrierService` 发布 `.watermelon/retention/` manifest；`RepoRetentionCommitDeleteExecutor` 在 barrier、liveness、legacy grace 和 post-delete verification 都允许时保守删除 commit 前缀。

Retention manifest 的文件名和 JSON wire schema 见 `docs/03-DataModel.md` §7；这里不重复字段级定义。

## 当前组件清单（Stage 1 后续演进）

本节源自 Stage 1 落地清单，但已经把后续 Iter / Unit 的当前结果合并进去；下方 “Iter N” 小节是历史变更记录，不应反向覆盖本节和源码里的当前事实。

### 远端协议层

- `Shared/Services/SMB/SMBClientProtocol.swift` — 新增 `atomicCreate(localURL:remotePath:respectTaskCancellation:)` + `AtomicCreateResult` 枚举（`created` / `alreadyExists` / `bestEffortRetry`）
- 默认实现：`exists() + upload()`（race-prone 兜底）
- 各 client 专用实现：
  - `LocalVolumeClient` — POSIX `O_EXCL`（强原子）
  - `S3Client` — `If-None-Match: *` PUT（强原子，单段；多段回退到 exists+multipart）
  - `WebDAVClient` — `If-None-Match: *` PUT（强弱取决于服务端支持）
  - `SFTPClient` — Citadel `SFTPOpenFileFlags.forceCreate`（即 SSH_FXF_EXCL）
  - `AMSMB2Client` — 走默认（exists+upload；Citadel 不暴露 SMB2 FILE_CREATE）

### 本地数据库

- `v3_repo_local_state` migration
- 新表 `repo_state(profileID, repoID, writerID, lastClock, lastSeq, migrationCompleted)`
- `server_profiles.writerID TEXT`
- `RepoStateRecord` GRDB record

### 远端布局 + 编排（`Shared/Services/Repo/`）

- `RepoLayout` — 路径常量 + filename 编解码（snapshot/commit/liveness）；snapshot/commit 用完整 writerID UUID（GC 安全）；数据文件名需要 writer 后缀时使用 `writerIDShort` / run prefix 约定
- `RepoIdentity` — writerID/repoID/runID 生成 + `repo_state` 行 lazy ensure
- `LamportClock` / `PersistedLamportClock`
- `SeqAllocator` — 单调本地 seq + 冷启动远端 max 对齐；persist 在 upload 之前
- `Models/CoveredRanges` — `[writerID: [(low, high)]]` 范围结构（`contains` / `merge` / `superset(of:)` / `merging`）
- `Models/CommitOp` / `SnapshotRow` / `IntegrityCheck` / `RepoSnapshotState`
- `Models/CommitOpMapper` / `SnapshotRowMapper` — `JSONSerialization` 手映射
- `CommitLogWriter` — jsonl；exclusive 后端直写并处理 phantom `.alreadyExists` 校验，overwrite-prone 后端走 `MetadataCreateGate.createWithStagingFallback(... requireExclusiveMove)`，调用方在 `.alreadyExists` 后重新分配 seq 重试
- `CommitLogReader` — 流式解析 + sha256+rowCount 校验。`RepoMaterializer` 对单条 corrupt commit 走 log + skip；commit reader 自身仍 throw
- `SnapshotWriter` — jsonl；通过 `MetadataCreateGate` 写最终 snapshot path，covered 由 `V2MonthSnapshotFlusher` 传入；`.bestEffortRetry` 可接受并依赖 commit log 重建
- `SnapshotReader` — 流式解析
- `RepoMaterializer` — 2 LIST + 并发 GET + replay；filter 用 `CoveredRanges.contains(writerID, seq)` 处理 gap；排序 `(clock, writerID, seq, opSeq)`；单条 commit 解码失败时 skip 并打 warn 不中断整次 materialize
- `LivenessTracker` — 30s 心跳 / 5min stale；LocalVolume = no-op；由 `BackupV2RuntimeBuilder` 启动、`BackupV2RuntimeServices.shutdown()` 在 run 收尾时停止
- `RepoBootstrap` — fresh 远端先通过 identity claims / `repo-identity.json` finalization 选出 canonical `repoID`，`repo.json` 是 read-cache；`VersionManifestStore` 写 `version.json`
- `V1MigrationService` — marker FSM 驱动 full migration：先确保 version 发布，phase 1 写 legacy-import commit + snapshot 并隔离不可迁移 residue，phase 2 写本地 `migrationCompleted=1`，phase 3 删除 / quarantine 旧 manifest 并 verify final state；pending-cleanup 仓库可走 cleanup-only
- `RepoCompactionPlanner` / `RepoCheckpointService` / `RepoRetentionBarrierService` / `RepoRetentionCommitDeleteExecutor` — 当前 per-month checkpoint + retention barrier + commit 前缀删除维护路径
- `BackupV2RuntimeServices` — 一个 run 期间 V2 服务的容器；`shutdown()` 关闭 `LivenessTracker` 心跳
- `BackupV2RuntimeBuilder` — 共享 build 入口（前台 / 后台两种调用）；`RepoIdentityAuthority.resolve()` 会收集 remote `repo.json`、既有 V2 commit/snapshot data 里的 repoID、以及与当前远端兼容的本地 `repo_state`，建议值按 `remote ?? data ?? stored ?? UUID()` 选择，发布前要求各来源一致，确保 SeqAllocator/Lamport 状态在 session 间稳定

### 兼容性

- `RemoteFormatCompatibilityService.inspectRemoteFormat(...)` — 6 态 (`.fresh / .v1 / .v2(formatVersion:) / .v2WithV1Manifests(formatVersion:) / .v2WithPendingMigrationCleanup(formatVersion:ownerWriterID:) / .unsupported(minAppVersion:)`)；损坏 V2 元数据以 `BackupCompatibilityError.damagedV2Repo` 抛出
- `verify(...)` 老接口保留：throw `remoteFormatUnsupported`，给 1.2.0+ V1 客户端继续 lock-out

### 写入路径 cutover

- `BackupRunPreparation`
  - `prepareRun` 不再调 `verify()`，改用 `inspectRemoteFormat` 路由
  - `.fresh` → `RepoBootstrap.initializeFreshRepo`
  - `.v1` → `V1MigrationService` 全 3 phase（`migrationCompleted=1` 后跳过）
  - `.v2` → 加载 `repoID` 继续
  - `.unsupported` → throw（保持兼容性提示）
  - 构造 `BackupV2RuntimeServices` 并随 `BackupPreparedRun` 透传
  - `reloadRemoteIndex` 改用 `inspectRemoteFormat`（不再因 `.watermelon/` 存在而 throw）
- `BackupParallelExecutor` worker 通过 `BackupMonthStore` 分流：V2 走 `V2MonthSession.loadOrCreate(...)`，V1 才走 `MonthManifestStore.loadOrCreate(...)`
- `V2MonthSession` 内：
  - `V2MonthIndexes` 持有 in-memory resources / assets / links / pending asset+tombstone 集合
  - `flushToRemote` 先写 commit jsonl，再写 snapshot jsonl，不再上传 V1 sqlite manifest
  - snapshot 写失败时通过 `FlushError.snapshotWriteFailed(committedAssets:committedTombstones:)` 把已落 commit 的 delta 带回调用方；只有最终且非 paused 的月 flush 会映射为 Home 的 durable-upload/snapshot-deferred warning
- Unit 8 后，row-writing asset 在 result 返回前写 per-asset commit；`BackupParallelExecutor` 与 `BackgroundBackupRunner` 同步采用「每 10 个非 failed 结果 flush 一次」（`flushInterval = 10`）的 snapshot cadence；月末仍有兜底 flush
- `AssetProcessor+Upload` `client.upload` → `client.atomicCreate`；V2 且 `client.dataPathOverwriteRisk == .perKey` 时强制使用 writerID / runID 后缀候选，避免多 writer 同名覆盖
- `BackgroundBackupRunner` 加 V1 gate：
  - `BackupV2RuntimeBuilder.build(allowMigration: false)`
  - `requiresForegroundMigration` → 跳过 profile + execution log
  - `unsupportedRemoteFormat` → 标记失败 + log
  - 否则得到 `v2Services` 并传给 `V2MonthSession.loadOrCreate`
- `RemoteIndexSyncService.syncIndex` 检测 `.watermelon/version.json`；V2 仓库走 `RepoMaterializer.materialize()` 物化 `RemoteLibrarySnapshotCache`，V1 仓库继续走原来的 manifest digest 扫描路径

### Domain

- `RemoteManifestResource.fileName` → `physicalRemotePath`（语义为完整路径 `<year>/<month>/<leaf>`）
- 派生属性 `logicalName`（leaf）替代之前直接 `.fileName` 用法
- `RemoteAssetResourceLink` 加 `logicalName: String`
- 新增 `ResourceCryptoMetadata`（占位，E2EE 留 Stage 2）
- 新增 `RemoteAssetResource` join 视图
- `RemoteManifestResource` 不再暴露旧 `remoteRelativePath` 计算属性；restore-facing 的 `RemoteAssetResourceInstance` 仍保留 `remoteRelativePath` / `alternateRemoteRelativePaths` 作为下载候选路径

### i18n

新增 6 条 xcstrings 全 14 locales 翻译：
- `backup.repo.bootstrapped`
- `backup.repo.migrationStarted`
- `backup.repo.migrationCompleted`
- `backup.auto.log.profileNeedsForegroundMigration`
- `backup.auto.log.profileFormatUnsupported`
- `backup.auto.log.profileFormatInspectFailed`

### 测试

- 代表性覆盖：`CoveredRangesTests`、`RepoLayoutTests`、`RepoStateRecordTests`、`CommitOpMapperTests`、`SnapshotRowMapperTests`、`IntegrityCheckTests`、`CommitLogParseTests`、`RepoMaterializerRoundTripTests`、`RepoMaterializerReadRaceTests`、`V2FlushTests`、`BootstrapStateMachineTests`、`ConcurrentBootstrapRaceTests`、`RepoVerifyMonthServiceTests`、`RemoteIndexFormatRouteDecisionTests`、`RepoCheckpointServiceTests`、`RepoCheckpointBarrierHookTests`、`RepoRetentionDeletePreflightTests`、`RepoRetentionCommitDeleteExecutorTests`、`RetentionDeletionSafetyGateTests`、`StorageCapabilityMatrixTests`
- 不在文档里硬编码总测试数；以当前 `WatermelonTests/` 为准

### verifyMonth 6 类报告 + 物理文件撞名 + snapshot covered

- `Models/VerifyMonthReport.swift` 定义 6 类 `VerifyMonthReportKind`（phantomAsset / partiallyMissing / allResourcesGone / metadataOnlyLeft / fingerprintMismatch / verificationIncomplete）；`allowsCleanup` 只标识 phantomAsset / allResourcesGone / metadataOnlyLeft 可写 tombstone
- `RepoVerifyMonthService` 对 V2 仓库 materialize → 列举月份目录 → 比对 → 产生 `VerifyMonthReport`；`verificationIncomplete` 来自 content-trust 预算耗尽或 probe 不确定，不会被当成 missing；`applyTombstones` 会重新 materialize + 重新 probe，确认仍 cleanup-eligible 后才写 `tombstoneAsset` commit
- `BackupRunPreparation.verifyMonth` 通过 `inspectRemoteFormat` 路由：V2 / pending-cleanup 走 `verifyMonthV2`，V2+V1 residue 要求前台迁移，V1 保持原 reconcile。带 profile 的 V2 verify 会在结束前 `syncIndex(expectV2:localRepoID:)` 刷新 remote snapshot cache；只有存在 cleanup candidates 时才用 `maintenanceStartupMode: .disabled(.verifyMonthTombstoneApply)` 临时打开 V2 runtime 并尝试 tombstone
- 物理文件撞名后缀：V2 且 `dataPathOverwriteRisk == .perKey` 时用 writerID / runID 后缀候选；V1 和 `.none` 风险后端保持原行为
- `V2MonthSnapshotFlusher` 根据 commit flush 结果维护 covered ranges 并传给 `SnapshotWriter`；writer 本身只负责持久化和校验

## 历史变更记录

以下 Iter 小节记录当时发现的问题和修补顺序。出现“当时 / 曾 / 已删除 / 当前”这类措辞时，以“现状 / 当前实现摘要”和源码为准。

### Iter 3 加固

- `RepoBootstrap.loadRepoIDStrict` / `loadVersionManifestStrict`：明确区分「文件不存在」（→ `.absent`）与「下载/解析失败」（→ throw），避免短网络抖动被误判为 `.unsupported` 或绕过 repoIdentityMismatch 检测
- 半 bootstrap 自愈早期策略：`marker exists + version.json absent` 曾按 `.fresh` 重跑 idempotent bootstrap；后续 Iter 4 / 当前实现又补了 V1 manifests、V2 数据目录和 migration marker 判定
- V1 迁移先建立 canonical repoID：`BackupV2RuntimeBuilder.build` 在 V1 路径里 phase1 之前 `ensureRepoJSON`，若与本地 repoID 冲突直接抛 `repoIdentityMismatch`，避免 phase1 用 stale id 写出永远不会被 replay 的 commit
- `RepoMaterializer.materialize(expectedRepoID:)` / `materializeMonth(...)` 接受可选 repoID 过滤；BackupV2RuntimeBuilder 冷启动 / RemoteIndexSyncService.syncIndexV2 / V2MonthSession / RepoVerifyMonthService 都会传入。脏迁移残留的 foreign-id commit 不会再被 replay 进 state
- `RepoMaterializer` 单 month snapshot 损坏 fallback：按 lamport 降序遍历该 month 的 snapshot，corrupt 的跳过取下一个；全损坏时回退到 empty + commit replay
- BackupV2RuntimeServices 持有专用 `metadataClient`：commit / snapshot / liveness 不再与 worker upload 抢占同一 connection；shutdown 时 disconnect
- `BackupCompatibilityError` 新增 `.repoIdentityMismatch` / `.requiresForegroundMigration`（i18n 14 locales），`prepareV2Runtime` / `verifyMonthV2` 把 `BackupV2RuntimeBuildError` 映射成它们，UI 不再看到 enum 名 + hex UUID
- Unit 8 后，V2 row-writing asset 在 result 返回前已写 per-asset commit；`RemoteIndexSyncService.committedAssetFingerprints()` 直接来自 durable committed view，只保留 physical-missing subtraction
- `CommitLogWriter.alreadyExists` retry：V2 commit flush 在 alreadyExists 后 re-allocate seq 重写最多 4 次，覆盖本地 lastSeq drift / 多端写同 seq 文件名场景
- `CommitLogWriter` 在 `bestEffortRetry` 路径上做 download + sha256 verify：非原子后端（SMB exists+upload）写完做一次回读校验，不一致时按 alreadyExists 处理触发上层 retry
- `RepoVerifyMonthService` 物理路径 multi-path：以 `pathsByHash[hash]` 集合做 OR 命中，多 writer 同 hash 不同 leaf 不再被误判 missing
- 14 locale 修正 `backup.auto.log.profileFormatInspectFailed` 文案：从「按旧模式继续」改为「跳过本次运行」（实际行为是 `.skipped`，不会回退 V1）
- `IntegrityAccumulator` / `CommitLogWriter` / `SnapshotWriter` 写盘改 guard let utf8，避免静默 nil 写空文件
- `BackupV2Constants.batchFlushInterval = 10` 公共常量，BackupParallelExecutor / BackgroundBackupRunner 共用
- `DatabaseManager.deleteServerProfile` 显式 `DELETE FROM repo_state WHERE profileID=?`，避免删 profile 后留下孤儿 repo_state 行

### Iter 4 加固

- V1 迁移中断自愈：`inspectRemoteFormat` 在 `marker exists + version absent` 时再 LIST 一遍 V1 manifests,有则返回 `.v1`(让 phase1+phase2+phase3 完整跑完),没有再走 `.fresh`。否则之前会被误判 fresh,V1 manifests 永远 stuck
- V2 path 也走 `ensureRepoJSON`:`BackupV2RuntimeBuilder.build` 在 `.v2` 分支不再 lazy 读 repo.json,而是用 local DB 的 repoID 调 ensureRepoJSON 落盘。version 写成但 repo.json 写失败的半成品状态会被这次重写补齐;否则后续每次 session 都生成新 UUID
- `SnapshotHeader` 加 `repoID` 字段(legacy 空字符串 backward-compat),materializer 同时按 repoID 过滤 snapshot 与 commit;之前只过滤 commit,foreign-repo snapshot 仍会污染 state/covered
- 当时的 `makeSeedFromV2State` bridge 需要 dedup multi-path same hash：V2 允许同 hash 多 physical path(多 writer 撞名),但 V1 sqlite UNIQUE(contentHash) 不允许。该 bridge 已被 Iter 7 删除；当前 multi-path 由 `V2MonthSession` 原生保留
- `processWithLocalCache` 的 `resources_reused_cached` 分支和 full upload 一样走 `commitPendingAssetToRemote(ignoreCancellation: false)`，commit 成功后才 publish asset / 写本地 hash-index
- BackupParallelExecutor / BackgroundBackupRunner 把 batch flush 触发条件从 `result.status == .success` 放宽到 `result.status != .failed`,让 cached-reuse `.skipped` 路径(monthStore 已 dirty)也参与 10-asset snapshot cadence
- `RepoBootstrap.ensureRepoJSON` 在 `.bestEffortRetry` 分支也回读 remote repo.json:非原子后端(SMB exists+upload TOCTOU)两个设备并发 bootstrap 时,各自都以为 created,read-back 后用对方真正落盘的 id 收敛
- `AssetProcessor+Upload` `.bestEffortRetry` 路径加 remote size verify:size 不一致 → 视为 race → 触发 collision rename retry,避免 SMB 并发同名上传互相覆盖
- `V1MigrationService.scanV1Months` 子目录 list 不再 `try?`:transient list 失败 surface 出去,phase1 不会漏迁某个月,phase3 不会把那个 manifest 误删
- `BackgroundBackupRunner` 修连接泄漏:metadataClient 创建/连接失败时也 disconnect 已连的 primary client
- `RepoMaterializer` snapshot 同 lamport tiebreak 加 `(writerID, runIDPrefix)` desc:之前 `Array.sort` 不保证 stable,session 间选不同 baseline 引起 replay 量波动
- 删除已废弃的 `resolveExistingRepoID` helper(被新版本 V2 path 取代)

### Iter 5 加固

- Materializer 内层 snapshot 循环把 repoID + 文件名 vs header 检查推到「与 corrupt 等价」位置:foreign-repo / 错位文件名走 next candidate,而不是把整月 baseline 置空。同样的 filename-vs-header 校验也加到 commit replay
- `RepoBootstrap.ensureVersionJSON` 在 `.alreadyExists` / `.bestEffortRetry` 后回读 version.json,format 高于本地 → 抛 `VersionConflict.higherFormatVersion`,builder 映射到 `unsupportedRemoteFormat` 错误
- `RepoBootstrap.ensureRepoJSON` 在 `.alreadyExists` / `.bestEffortRetry` + strict load `.absent` 时 throw `BootstrapError.ioFailure`,不再悄悄返回本地 suggested(防止 split brain)
- `SnapshotReader.listSnapshotFilenames` 与 CommitLogReader 一致:仅对「目录不存在」吞掉,其它错误 surface
- `RepoVerifyMonthService.applyTombstones` 加 alreadyExists retry(4 次,与 V2 commit flush 对齐),并发 cleanup 不再直接失败
- `verifyMonthV2` / `BackgroundBackupRunner` 把 `BackupV2RuntimeBuildError.repoIdentityMismatch` 映射为可读错误(前者抛 `BackupCompatibilityError.repoIdentityMismatch`,后者 append 新 i18n 文案 `backup.auto.log.profileRepoIdentityMismatch`,14 locales)
- `AssetProcessor+Upload` `.bestEffortRetry` 路径在 size 匹配时再做 content-hash 校验:同 size 不同内容的并发上传不会再被认作成功(避免把本地 hash 绑定到对方写入的 remote 文件)
- 当时为 `MonthManifestStore` bridge 加过 `v2KnownLogicalNamesByHash` fallback，避免 dedup 后误 tombstone；该 field 已随 Iter 7 的 V2MonthSession 迁移从 store 移除
- `RemoteAssetResourceInstance` 加 `alternateRemoteRelativePaths`,`HomeAlbumMatching` 收集多 path 候选(lex-min 作为主,其余作为 alt),`RestoreService` 下载失败时按 alt 路径 fallback
- prepareRun 双 materialize 优化:`BackupV2RuntimeBuilder` 把 cold-start materialize 输出存入 `BackupV2RuntimeServices.initialMaterializeOutput`(actor box,one-shot consume),`syncIndex(preMaterialized:)` 接受预热结果;V2 path 启动时少跑一次全仓 LIST + replay
- `RepoMaterializer` 顶部加 repoID-empty policy invariant 注释:snapshot 接受空(legacy),commit 拒绝空,两边历史不同步时一起更新

### Iter 7 架构调整

**目标:消除 6 轮 review 暴露的 4 类根因 + 补足端到端测试。** 见 `~/.claude/plans/snazzy-imagining-rivest.md`。

**根因消除:**

- (A) V1 sqlite schema 污染 V2:`V2MonthSession`(in-memory,不带 sqlite)替代 `MonthManifestStore` 的 V2 路径,`v2KnownLogicalNamesByHash` / `makeSeedFromV2State` dedup / `writeV2Snapshot` 多 path 重发 / `loadV2Materialized` 全部删除。multi-path 在 V2MonthSession 里是天然的(`pathsByHash` / `resourcesByPath`)
- (B) optimistic-cache + deferred V2 commit:当时留 Stage 2；Unit 8 已改为 per-asset commit；当前又补了保守 checkpoint / retention，剩余未来项转为物理数据文件 GC、snapshot GC、repair UI 与 retention 可观测性
- (C) bootstrap 状态机:文档化决策表 + Phase A 的 `BootstrapStateMachineTests` 锁定 16 个半成品状态的 inspect 输出。schema 不重构(向后兼容代价大于收益)
- (D) `try?` / silent catch:Phase B audit 把 `SnapshotReader.list` / `CommitLogReader.list` 改成「list throw + metadata 也 throw → 传播 list 错误」(metadata 探测失败不再吞)
- (E) 缺端到端测试:Phase A 加 `InMemoryRemoteStorageClient` actor + 6 个测试套(round-trip / V1 migration / bootstrap / restore fallback / V2 flush / concurrent bootstrap)。不要在文档中硬编码总测试数

**Bootstrap 状态机决策表(Phase D 文档化):**

| basePath dir | `.watermelon/` | `repo.json` | `version.json` | V1 manifests | inspect 输出 | 备注 |
|---|---|---|---|---|---|---|
| absent / empty | - | - | - | - | `.fresh` | 全新仓库 |
| 存在 | absent | - | - | absent | `.fresh` | 旧目录,无 V2 痕迹 |
| 存在 | absent | - | - | present | `.v1` | 旧 V1 仓库待迁移 |
| 存在 | present | absent | absent | absent | `.fresh` | 半成品 bootstrap → 幂等重跑 |
| 存在 | present | absent | absent | present | `.v1` | V1 phase1 后 / phase2 前 → 续 migration |
| 存在 | present | present | absent | absent | `.fresh` | 半成品(repo 后 / version 前)→ 重跑补 version |
| 存在 | present | present | absent | present | `.v1` | V1 phase2 中段 → 续 migration |
| 存在 | present | present | present(v=2) | - | `.v2(2)` | 正常 V2 |
| 存在 | present | present | present(v≠2) | - | `.unsupported` | 未来格式 |
| 任意 | - | - | version 读失败(transport) | - | throw | 不再静默降级为 `.unsupported` |

锁在 `WatermelonTests/BootstrapStateMachineTests.swift`。

上表是 Iter 7 时的核心决策表；当前实现还会检查 V2 commit/snapshot 数据目录与 migration marker：有 V2 数据但缺可读 `version.json` 时 fail-closed 为 `damagedV2Repo`，V2 + V1 residue 映射为 `.v2WithV1Manifests`，只剩 cleanup marker 时映射为 `.v2WithPendingMigrationCleanup`。

**Phase C 删除的代码:**

- `MonthManifestStore.flushV2` / `writeV2Snapshot` / `loadV2Materialized` 这条 V2 sqlite bridge 已移除，V2 写入改由 `V2MonthSession` + flusher 承担
- `MonthManifestStore.pendingV2AssetFingerprints` / `pendingV2TombstoneFingerprints` / `materializedCovered` / `sessionWrittenCovered` / `v2KnownLogicalNamesByHash` 已从 store 移除；pending 集合存在于 `V2MonthIndexes`
- `MonthManifestStore` init 的 `v2Services` / `materializedCovered` / `v2KnownLogicalNamesByHash` 参数
- `MonthManifestStore.loadOrCreate` / `loadSeeded` 的 v2-相关参数
- `MonthManifestStore+Loading.loadV2Materialized` / `makeSeedFromV2State`
- `applyDeletions` 的 V2 mirror 分支

**新增:**

- `Shared/Services/Backup/BackupMonthStore.swift` — protocol(MonthManifestStore + V2MonthSession 都 conform)
- `Shared/Services/Backup/V2MonthSession.swift` — V2-native in-memory session
- `WatermelonTests/Support/InMemoryRemoteStorageClient.swift` — `RemoteStorageClientProtocol` actor fake
- `WatermelonTests/RepoMaterializerRoundTripTests.swift`
- `WatermelonTests/V2FlushTests.swift`（round-trip flush + tombstone delta 等）
- `WatermelonTests/V1MigrationServiceTests.swift`
- `WatermelonTests/BootstrapStateMachineTests.swift`
- `WatermelonTests/RestoreServiceFallbackTests.swift`
- `WatermelonTests/ConcurrentBootstrapRaceTests.swift`

**Per-asset commit 已落地**：旧 batch commit / optimistic subtraction 机制已删除。当前每个 row-writing asset 写 commit；batch / final flush 写 snapshot cadence。commit 前缀 retention 已有保守实现，剩余边界见 `docs/05-OpenIssues.md` §12。

### Iter 6 加固

- `AssetProcessor+Upload.detectRemoteContentRace` 把 metadata/download/hash 失败语义反转为「假设有 race → 触发 collision rename」。原先「无法验证」被当成「没有 race」会让本地 hash 绑到对方写入的远端字节
- 当时的 `MonthManifestStore.writeV2Snapshot` 曾补写 primary + 所有备用 logical name；该 V2 sqlite bridge 已被 Iter 7 删除，当前 snapshot 行由 `V2MonthSnapshotFlusher` 从 `V2MonthIndexes` 输出
- `BackupV2RuntimeBuilder.build` 的 `.v2` 分支也调用 `bootstrap.ensureSubdirectories()`:WebDAV/SMB/SFTP 上的半初始化 V2 repo 不再因子目录缺失硬失败
- `SnapshotReader.listSnapshotFilenames` / `CommitLogReader.listCommitFilenames` 用 `client.metadata` 做 backend-agnostic 的 not-found 探测,WebDAV 404 / SFTP/SMB 协议特有码不再被当成硬失败
- `RepoBootstrap.verifyVersionCompatible` 严格化:read 失败 / parse 失败 / format 不等于本地 → 抛 `VersionConflict`(分别 `unreadable` / `mismatchedFormatVersion`),builder 把后者映射成 `unsupportedRemoteFormat`
- `RemoteIndexSyncService.syncIndexV2` / `BackupRunPreparation.verifyMonthV2`:V2 repo 的 `.absent` repo.json 不再降级到 `expectedRepoID = nil`,直接抛错(broken identity state,backup-flow 才能修)
- `RepoMaterializer` commit 收集阶段从 filename 推进 `observedSeqByWriter`,corrupt / 错名 commit 也把 seq 计入 cold-start max,allocator 不会再撞名循环耗尽 4 次重试
- `RepoStateAuthority` 统一 `repo_state` counter 边界：`lastSeq` 以 `maxPersistableSeq = UInt64(Int64.max)` 为上限，因为 SQLite `INTEGER` 是 signed storage；负数 `lastSeq` 读入按 `0` 修复，same-writer remote seq 只有在不超过该上限时才推进本 writer allocator，越界 observation 不持久化/不进入 actor-local state，foreign-writer seq 不参与本地 allocator；remote clock 仍通过 `PersistedLamportClock.observe` 的既有 ceiling/repair 语义采纳，不经 `RepoStateAuthority` 包装
- `BackupParallelExecutor` / `BackgroundBackupRunner` 的 flush helper 同时消费 `BackupMonthFlushDelta.committedAssetFingerprints` 和 `BackupMonthFlushDelta.committedTombstoneFingerprints`:若防御性 flush 提交了遗留 pending ops，就用 `unsortedSnapshot()` publish 整个月到 `RepoCommittedView`
- `SnapshotRowMapper.decodeHeader` 严格 covered 解析:bad pair / 类型错误 → 抛 `SnapshotWireError.malformed`,不再静默降级为空 covered 让坏 snapshot 当 baseline
- `LocalVolumeClient.atomicCreate` 修 cleanup gap:source 打开失败也清空 destination;destination close 错误 surface(外接盘 unmount/写满有时只在 close 时报)
- `SeqAllocator` 在 write transaction 内读取 DB high-water 后写 signed-safe `lastSeq`，不再用 signed `lastSeq < ?` 比较 high-bit 值；`PersistedLamportClock.persist` 仍用 conditional advance 保护 clock high-water
- `RestoreService` 三个补丁:`fileName` 走 `RemotePathBuilder.sanitizeFilename`(防路径穿越);失败路径加 `removeItem(at: tempURL)`(漏 cleanup);保留 disconnect fire-and-forget(动 API 边界,留给 Stage 2)
- `CommitOpMapper` / `SnapshotRowMapper` 所有 hex → Data 解码加 `!fp.isEmpty` 守卫:`Data(hexString: "")` 返回 `Data()` 不是 nil,空 fingerprint commit/snapshot 不会再被接受

### `BackupResumePlanner` 切 `RemoteViewHandle`

- `makePlan` 接受 `BackupResumeDedupMode`：V1 用 `completedAssetIDs`，V2 用 `RemoteViewHandle`
- V2 模式用 `RemoteViewHandle.resumeCoverage.safeToSkipAssetFingerprintsByMonth` 去重；该 handle 从 durable per-asset commits 投影 safe-to-skip 覆盖，扣掉 physical-missing resources，并把存在同月 strict-subset survivor 的 superseding fingerprint 留给 AssetProcessor healing
- retry / scoped / full 共用这条 fingerprint 覆盖过滤路径

### Iter 9 加固：`.perKey` 数据路径走 writerID / runID 后缀

- SMB / WebDAV / S3 这类 `dataPathOverwriteRisk == .perKey` 的后端可能让多 writer 同名资源互相覆盖；S3 小文件即使 atomic create 是 exclusive，也仍按 per-key 风险处理数据文件名
- V2 写入路径在 `prepareUpload` 阶段只要 `monthStore.v2Services?.writerID != nil` 且后端为 `.perKey`，就强制走 writerID / runID 后缀候选（`RepoLayout.writerIDShort` + run prefix）
- `.none` 风险后端（LocalVolume / SFTP）和 V1（无 `v2Services`）保持原行为，不强制新后缀

### Iter 10 当前实现：checkpoint / retention / maintenance

- `.watermelon/retention/` 已加入 V2 metadata 布局；`RetentionManifest` 记录 checkpoint snapshot、checkpoint SHA、covered ranges、delete prefix、observed seq high、policy 和 liveness gate，字段级 wire schema 见 `docs/03-DataModel.md` §7
- `RepoCompactionPolicy.default` 当前阈值：`checkpointCommitThreshold = 5000`、`checkpointByteThreshold = 16 MiB`、`retentionStalenessThresholdSeconds = 24h`、`legacyClientGraceSeconds = 7d`、`snapshotFallbackKeepCount = 2`；policy 也保留 `minimumCheckpointIntervalSeconds = 6h` 字段，但当前 checkpoint 推荐逻辑只消费 commit 数和 bytes
- `V2MonthSession.flushToRemote` 在 dirty 清空后运行 `RepoCheckpointBarrierHook`：低于阈值时也尝试删除已有 barrier 覆盖的 commit 前缀；达到阈值时先写 checkpoint snapshot，确认 materialize 接受后发布 retention barrier，再跑删除
- `V2MonthCommitFlusher` / `V2MonthSnapshotFlusher` 通过 `V2RetentionBarrierRefresh` 做 barrier-aware refresh，确保本 session 的 commit / snapshot basis 不丢失已发布 barrier 的覆盖约束
- `RepoRetentionDeletePreflightService` fail-closed：版本、repoID、migration marker、barrier set、checkpoint readback、accepted snapshot、observed seq、candidate header、planner cross-check、liveness gate 任一不满足都会阻止删除
- `RepoRetentionCommitDeleteExecutor` 删除后用 `RepoRetentionPostDeleteVerifier` 做 retention-equivalence 校验；verification failed / inconclusive 都不会被当成普通成功
- 正常 V2 runtime build 会启动 maintenance；`verifyMonth` 为应用 tombstone 临时打开 runtime 时显式禁用 startup maintenance
- `RepoMaintenanceRuntimeBuilder` 在 enabled 模式下启动 liveness heartbeat（LocalVolume 为 no-op），并只在后端支持 `supportsLivenessSafeRenewal` 且 peer view complete 时跑 orphan metadata sweep；只有支持安全 renewal 的后端会在 heartbeat 中公布 `RetentionPeerCapability`。`RepoMaintenanceStartupRunner` 也只在 enabled 模式下对足够老的 retention manifest 继续跑 commit 前缀删除
- 当前 capability 事实：LocalVolume / S3 / WebDAV 支持 `supportsLivenessSafeRenewal`；SMB / SFTP 不支持，因此不公布 barrier-aware retention capability，也不跑 orphan metadata sweep。commit 前缀删除仍由 preflight、liveness gate、legacy grace 与 post-delete verification 共同决定

当前仍未实现 / 延后项：

1. **物理数据文件 GC**：跨 writer 安全的版本（plan §0 / §6.5 明确不做）；commit 前缀 retention 已有保守实现
2. **gcOrphan op + 物理文件 refcount + 「从备份删除 asset」UI**
3. **`reportMissing` / `reportTampered` / `commitCorruption` op**
4. **跨 writer snapshot GC**（grace + liveness gate + 联合覆盖判定）
5. **WebDAV `If-None-Match` probe + 缓存**：当前直传 `*`，服务端不支持时回 412 当作 alreadyExists（错误归类正确，语义略有偏差）
6. **持久化本地 V2 materialized cache**（cold start 进入毫秒级）
7. **月级 materialize 并发深化**
8. **E2EE**（modes: plain / e2ee-content-visible / e2ee-private）
9. **audit / repair 完整 UI**（兜底 pre-1.2.0 客户端误用造成的 desync）
10. **全局 / 跨月 compaction snapshot**（当前只有 per-month checkpoint + retention barrier）
11. **`writers/<writerID>.json` 元数据**
12. **「合并历史 writer」UI**（处理 Keychain 丢失场景）
13. **`verifyMonth` `partiallyMissing` 类提供 re-upload 动作**

## 关键假设 / 限制

1. `min_app_version "2.0.0"` 是 `RepoLayout.minAppVersionPlaceholder`；发布时换实际下一版本号
2. 升级**不可逆**：phase 2 翻转后已升级客户端永远无法回退到 V1
3. Unit 8 已消除 V2 deferred batch commit 窗口；batch / final flush 只负责 snapshot cadence。当前 checkpoint / retention 只保守删除被 accepted checkpoint + barrier + liveness + verification 共同保护的 commit 前缀
4. **多端并发**：V2 best-effort，不保证零冲突（特别是 WebDAV）；plan §12 已声明
5. **pre-1.2.0 V1 客户端**：协议无 sentinel 锁出，依赖用户主动升级所有设备
