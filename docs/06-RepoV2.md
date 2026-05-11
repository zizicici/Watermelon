# 06 — Repo V2

V1（per-month manifest sqlite）→ V2（commit log + snapshot）格式重构。完整设计见 `~/.claude/plans/wobbly-launching-stroustrup.md`。

## 现状

V2 cutover 已完成。新 / 旧客户端在仓库上的行为：

| 仓库状态 | V2 客户端（本次起） | 1.2.0+ V1 客户端 | pre-1.2.0 客户端 |
|---|---|---|---|
| fresh（首次连接） | `RepoBootstrap` 写 `.watermelon/repo.json` + `version.json`，进入 V2 写入路径 | 看到 `.watermelon/` → throw `remoteFormatUnsupported` | 无 sentinel 检测，会把目录当 fresh 用，与 V2 desync |
| 含 V1 manifest 的仓库 | 自动同步迁移（phase 1+2+3），随后 V2 写入 | 正常 V1 行为（在迁移完成前） | 同上 |
| V2 仓库 | 直接 `RepoMaterializer` 物化，写入路径用 V2 commit log | throw `remoteFormatUnsupported(minAppVersion)` | desync |

后台备份（`BackgroundBackupRunner`）不会自动迁移 V1：检测到 V1 仓库直接结束 + execution log 提示用户在前台运行。

## Stage 1 落地清单

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

- `v3_repo_state` migration
- 新表 `repo_state(profileID, repoID, writerID, lastClock, lastSeq, migrationCompleted)`
- `server_profiles.writerID TEXT`
- `RepoStateRecord` GRDB record

### 远端布局 + 编排（`Shared/Services/Repo/`）

- `RepoLayout` — 路径常量 + filename 编解码（snapshot/commit/liveness）；snapshot/commit 用完整 writerID UUID（GC 安全）；物理文件撞名后缀仍用 `_N`（不参与 GC 决策）
- `RepoIdentity` — writerID/repoID/runID 生成 + `repo_state` 行 lazy ensure
- `LamportClock` / `PersistedLamportClock`
- `SeqAllocator` — 单调本地 seq + 冷启动远端 max 对齐；persist 在 upload 之前
- `Models/CoveredRanges` — `[writerID: [(low, high)]]` 范围结构（`contains` / `merge` / `superset(of:)` / `merging`）
- `Models/CommitOp` / `SnapshotRow` / `IntegrityCheck` / `RepoSnapshotState`
- `Models/CommitOpMapper` / `SnapshotRowMapper` — `JSONSerialization` 手映射
- `CommitLogWriter` — jsonl + `atomicCreate` 直传，无 verify roundtrip
- `CommitLogReader` — 流式解析 + sha256+rowCount 校验。`RepoMaterializer` 对单条 corrupt commit 走 log + skip；commit reader 自身仍 throw
- `SnapshotWriter` — jsonl + `atomicCreate(.tmp)` → GET 验证 → move；own-superseded GC 钩子（删除前读 header 二次校验完整 writerID）；LIST 失败 fallback 到空 covered 时打 warn log
- `SnapshotReader` — 流式解析
- `RepoMaterializer` — 2 LIST + 并发 GET + replay；filter 用 `CoveredRanges.contains(writerID, seq)` 处理 gap；排序 `(clock, writerID, seq, opSeq)`；单条 commit 解码失败时 skip 并打 warn 不中断整次 materialize
- `LivenessTracker` — 30s 心跳 / 5min stale；LocalVolume = no-op；由 `BackupV2RuntimeBuilder` 启动、`BackupV2RuntimeServices.shutdown()` 在 run 收尾时停止
- `RepoBootstrap` — fresh 远端写 `.watermelon/repo.json` + `version.json`；冲突时不 stomp，read existing repo.json 复用其 id
- `V1MigrationService` — 3-phase migration：phase 1 写 legacy-import commit + snapshot；phase 2 写 `.watermelon/repo.json` + `version.json` + 设 `migrationCompleted=1`（repo.json 必须落盘，否则后续会话 loadRepoID 会拿到 nil → 重新生成 UUID → SeqAllocator 撞名）；phase 3 删旧 manifests
- `BackupV2RuntimeServices` — 一个 run 期间 V2 服务的容器；`shutdown()` 关闭 `LivenessTracker` 心跳
- `BackupV2RuntimeBuilder` — 共享 build 入口（前台 / 后台两种调用）；resolveRepoID 永远先在本地 DB 中按 profileID 查 `repo_state`，再 fallback 到 `loadRepoID()`，最后 fallback 到 `UUID()`，确保 SeqAllocator/Lamport 状态在 session 间稳定

### 兼容性

- `RemoteFormatCompatibilityService.inspectRemoteFormat(...)` — 4 态 (`.fresh / .v1 / .v2(formatVersion:) / .unsupported(minAppVersion:)`)
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
- `BackupParallelExecutor` worker 调 `MonthManifestStore.loadOrCreate(v2Services:)` 让 store 知道 V2 上下文
- `MonthManifestStore` 内：
  - `flushToRemote` 在 `v2Services` 存在时走 `flushV2`，写 commit jsonl + snapshot jsonl，不再上传 V1 sqlite manifest
  - `pendingV2AssetFingerprints` / `pendingV2TombstoneFingerprints` 跟踪本次 batch 新加 / 删除
  - `upsertAsset` 时把 fingerprint 入 pending（subset 替换转 tombstone）
  - `applyDeletions`（inline reconcile / cleanupMissingResources 走的路径）也把删除的 fingerprint 入 tombstone pending，避免 V2 commit 漏掉 inline 删除
- `BackupParallelExecutor` 与 `BackgroundBackupRunner` 同步采用「每 10 个成功上传 flush 一次」（`flushInterval = 10`）的 batch 提交节奏；月末仍有兜底 flush
- `AssetProcessor+Upload` `client.upload` → `client.atomicCreate`；`alreadyExists` → 触发 collision rename 重试（沿用现有 `RemoteFileNaming.resolveNextAvailableName` 数字后缀，未切到 `~wid6[-N]`）
- `BackgroundBackupRunner` 加 V1 gate：
  - `BackupV2RuntimeBuilder.build(allowMigration: false)`
  - `requiresForegroundMigration` → 跳过 profile + execution log
  - `unsupportedRemoteFormat` → 标记失败 + log
  - 否则得到 `v2Services` 并传给 `MonthManifestStore.loadOrCreate`
- `RemoteIndexSyncService.syncIndex` 检测 `.watermelon/version.json`；V2 仓库走 `RepoMaterializer.materialize()` 物化 `RemoteLibrarySnapshotCache`，V1 仓库继续走原来的 manifest digest 扫描路径

### Domain

- `RemoteManifestResource.fileName` → `physicalRemotePath`（语义为完整路径 `<year>/<month>/<leaf>`）
- 派生属性 `logicalName`（leaf）替代之前直接 `.fileName` 用法
- `RemoteAssetResourceLink` 加 `logicalName: String`
- 新增 `ResourceCryptoMetadata`（占位，E2EE 留 Stage 2）
- 新增 `RemoteAssetResource` join 视图
- 旧 `remoteRelativePath` 计算属性删除，所有 caller 改 `physicalRemotePath`

### i18n

新增 6 条 xcstrings 全 14 locales 翻译：
- `backup.repo.bootstrapped`
- `backup.repo.migrationStarted`
- `backup.repo.migrationCompleted`
- `backup.auto.log.profileNeedsForegroundMigration`
- `backup.auto.log.profileFormatUnsupported`
- `backup.auto.log.profileFormatInspectFailed`

### 测试

- 新增：`CoveredRangesTests`、`RepoLayoutTests`、`LamportClockTests`、`RepoStateRecordTests`、`CommitOpMapperTests`、`SnapshotRowMapperTests`、`IntegrityCheckTests`、`CommitLogParseTests`
- 总计 206 tests pass，无回归

### verifyMonth 5 类报告 + 物理文件撞名 + snapshot covered

- `Models/VerifyMonthReport.swift` 定义 5 类 `VerifyMonthReportKind`（phantomAsset / partiallyMissing / allResourcesGone / metadataOnlyLeft / fingerprintMismatch）；`allowsCleanup` 标识其中 3 类可写 tombstone
- `RepoVerifyMonthService` 对 V2 仓库 materialize → 列举月份目录 → 比对 → 产生 `VerifyMonthReport`；`applyTombstones` 把 cleanup-eligible 项写成 `tombstoneAsset` commit
- `BackupRunPreparation.verifyMonth` 检测 `.watermelon/version.json` 是否存在，存在走 `verifyMonthV2`（含 `applyTombstones` + 重建 snapshot cache），否则保持原 V1 reconcile
- 物理文件撞名后缀：当 v2Services 存在时用 `~<wid6>` / `~<wid6>-N`，否则保留 `_N`（兼容 V1 / 迁移路径）
- `SnapshotWriter` 写盘前 LIST `commits/` filter 本 month + 本 writerID 的 seq，合并相邻段为 ranges 加入 `header.covered`，cold-start materialize 不再因「snapshot 只覆盖单 seq」做多余 replay

### Iter 3 加固

- `RepoBootstrap.loadRepoIDStrict` / `loadVersionManifestStrict`：明确区分「文件不存在」（→ `.absent`）与「下载/解析失败」（→ throw），避免短网络抖动被误判为 `.unsupported` 或绕过 repoIdentityMismatch 检测
- 半 bootstrap 自愈：`inspectRemoteFormat` 在 `marker exists + version.json absent` 时返回 `.fresh`，让 caller 重跑 idempotent bootstrap 补齐 version.json
- V1 迁移先建立 canonical repoID：`BackupV2RuntimeBuilder.build` 在 V1 路径里 phase1 之前 `ensureRepoJSON`，若与本地 repoID 冲突直接抛 `repoIdentityMismatch`，避免 phase1 用 stale id 写出永远不会被 replay 的 commit
- `RepoMaterializer.materialize(expectedRepoID:)` / `materializeMonth(...)` 接受可选 repoID 过滤；BackupV2RuntimeBuilder 冷启动 / RemoteIndexSyncService.syncIndexV2 / MonthManifestStore.loadV2Materialized / RepoVerifyMonthService 都会传入。脏迁移残留的 foreign-id commit 不会再被 replay 进 state
- `RepoMaterializer` 单 month snapshot 损坏 fallback：按 lamport 降序遍历该 month 的 snapshot，corrupt 的跳过取下一个；全损坏时回退到 empty + commit replay
- BackupV2RuntimeServices 持有专用 `metadataClient`：commit / snapshot / liveness 不再与 worker upload 抢占同一 connection；shutdown 时 disconnect
- `BackupCompatibilityError` 新增 `.repoIdentityMismatch` / `.requiresForegroundMigration`（i18n 14 locales），`prepareV2Runtime` / `verifyMonthV2` 把 `BackupV2RuntimeBuildError` 映射成它们，UI 不再看到 enum 名 + hex UUID
- `RemoteIndexSyncService.committedAssetFingerprints()` 减去 `uncommittedV2Fingerprints`，`MonthManifestStore.flushToRemote` 返回 `FlushDelta`，BackupParallelExecutor / BackgroundBackupRunner flush 成功后 `markCommittedV2(delta)`：resume planner 不再跳过「物理文件已上传但 V2 commit 未 flush」的 asset
- `CommitLogWriter.alreadyExists` retry：flushV2 在 alreadyExists 后 re-allocate seq 重写最多 4 次，覆盖本地 lastSeq drift / 多端写同 seq 文件名场景
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
- `makeSeedFromV2State` dedup multi-path same hash:V2 允许同 hash 多 physical path(多 writer 撞名),但 V1 sqlite UNIQUE(contentHash) 不允许;按 lex-min 物理路径选一份保留,verify/HomeUI 仍走 path-set 多路径命中
- `processWithLocalCache` 的 `resources_reused_cached` 分支补 `markUncommittedV2`:之前只有 full upload 路径标记,cached-reuse 写完 asset+cache 但漏 mark,在 flushV2 落盘前 pause/resume 会让 resume planner 跳过这条 fingerprint
- BackupParallelExecutor / BackgroundBackupRunner 把 batch flush 触发条件从 `result.status == .success` 放宽到 `result.status != .failed`,让 cached-reuse `.skipped` 路径(monthStore 已 dirty)也参与 10-asset batch 提交节奏
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
- `RepoVerifyMonthService.applyTombstones` 加 alreadyExists retry(4 次,与 flushV2 对齐),并发 cleanup 不再直接失败
- `verifyMonthV2` / `BackgroundBackupRunner` 把 `BackupV2RuntimeBuildError.repoIdentityMismatch` 映射为可读错误(前者抛 `BackupCompatibilityError.repoIdentityMismatch`,后者 append 新 i18n 文案 `backup.auto.log.profileRepoIdentityMismatch`,14 locales)
- `AssetProcessor+Upload` `.bestEffortRetry` 路径在 size 匹配时再做 content-hash 校验:同 size 不同内容的并发上传不会再被认作成功(避免把本地 hash 绑定到对方写入的 remote 文件)
- `MonthManifestStore` 加 `v2KnownLogicalNamesByHash` field,`reconcileWithRemoteListing` 在 chosen path 缺失时 fallback 检查同 hash 的其它 logicalName,任意一份在 remote 即视为存在(避免多 path 信息 dedup 后被错误 tombstone)
- `RemoteAssetResourceInstance` 加 `alternateRemoteRelativePaths`,`HomeAlbumMatching` 收集多 path 候选(lex-min 作为主,其余作为 alt),`RestoreService` 下载失败时按 alt 路径 fallback
- prepareRun 双 materialize 优化:`BackupV2RuntimeBuilder` 把 cold-start materialize 输出存入 `BackupV2RuntimeServices.initialMaterializeOutput`(actor box,one-shot consume),`syncIndex(preMaterialized:)` 接受预热结果;V2 path 启动时少跑一次全仓 LIST + replay
- `RepoMaterializer` 顶部加 repoID-empty policy invariant 注释:snapshot 接受空(legacy),commit 拒绝空,两边历史不同步时一起更新

### Iter 7 架构调整

**目标:消除 6 轮 review 暴露的 4 类根因 + 补足端到端测试。** 见 `~/.claude/plans/snazzy-imagining-rivest.md`。

**根因消除:**

- (A) V1 sqlite schema 污染 V2:`V2MonthSession`(in-memory,不带 sqlite)替代 `MonthManifestStore` 的 V2 路径,`v2KnownLogicalNamesByHash` / `makeSeedFromV2State` dedup / `writeV2Snapshot` 多 path 重发 / `loadV2Materialized` 全部删除。multi-path 在 V2MonthSession 里是天然的(`pathsByHash` / `resourcesByPath`)
- (B) optimistic-cache + deferred V2 commit:留 Stage 2 与 compaction 一起做(per-asset commit 目前会让 commit 文件爆炸)
- (C) bootstrap 状态机:文档化决策表 + Phase A 的 `BootstrapStateMachineTests` 锁定 16 个半成品状态的 inspect 输出。schema 不重构(向后兼容代价大于收益)
- (D) `try?` / silent catch:Phase B audit 把 `SnapshotReader.list` / `CommitLogReader.list` 改成「list throw + metadata 也 throw → 传播 list 错误」(metadata 探测失败不再吞)
- (E) 缺端到端测试:Phase A 加 `InMemoryRemoteStorageClient` actor + 6 个测试套(round-trip / V1 migration / bootstrap / restore fallback / V2 flush / concurrent bootstrap),236 tests 全绿

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

锁在 `WatermelonTests/BootstrapStateMachineTests.swift` 9 个测试里。

**Phase C 删除的代码:**

- `MonthManifestStore.flushV2` / `writeV2Snapshot` / `overlaySeedCrypto` (实际还在,V1 path 也用,留着)
- `MonthManifestStore.pendingV2AssetFingerprints` / `pendingV2TombstoneFingerprints` / `materializedCovered` / `sessionWrittenCovered` / `v2KnownLogicalNamesByHash`
- `MonthManifestStore` init 的 `v2Services` / `materializedCovered` / `v2KnownLogicalNamesByHash` 参数
- `MonthManifestStore.loadOrCreate` / `loadSeeded` 的 v2-相关参数
- `MonthManifestStore+Loading.loadV2Materialized` / `makeSeedFromV2State`
- `applyDeletions` 的 V2 mirror 分支

**新增:**

- `Shared/Services/Backup/BackupMonthStore.swift` — protocol(MonthManifestStore + V2MonthSession 都 conform)
- `Shared/Services/Backup/V2MonthSession.swift` — V2-native in-memory session
- `WatermelonTests/Support/InMemoryRemoteStorageClient.swift` — `RemoteStorageClientProtocol` actor fake
- `WatermelonTests/RepoMaterializerRoundTripTests.swift` — 9 tests
- `WatermelonTests/V2FlushTests.swift` — 2 tests(round-trip flush + tombstone delta)
- `WatermelonTests/V1MigrationServiceTests.swift` — 4 tests
- `WatermelonTests/BootstrapStateMachineTests.swift` — 9 tests
- `WatermelonTests/RestoreServiceFallbackTests.swift` — 3 tests
- `WatermelonTests/ConcurrentBootstrapRaceTests.swift` — 3 tests

**Per-asset commit 仍延后**(plan §11):batch flush + `pendingV2*` + `markCommittedV2` 机制保留,直到 Stage 2 compaction 落地。详见 `docs/05-OpenIssues.md` §13。

### Iter 6 加固

- `AssetProcessor+Upload.detectRemoteContentRace` 把 metadata/download/hash 失败语义反转为「假设有 race → 触发 collision rename」。原先「无法验证」被当成「没有 race」会让本地 hash 绑到对方写入的远端字节
- `MonthManifestStore.writeV2Snapshot` 每个 hash 输出 primary + 所有备用 logical name 的 SnapshotResourceRow:之前只写 deduped path,加上 `covered` 覆盖旧 commits → 下一次 materialize 看不到 alt 路径,数据丢失
- `BackupV2RuntimeBuilder.build` 的 `.v2` 分支也调用 `bootstrap.ensureSubdirectories()`:WebDAV/SMB/SFTP 上的半初始化 V2 repo 不再因子目录缺失硬失败
- `SnapshotReader.listSnapshotFilenames` / `CommitLogReader.listCommitFilenames` 用 `client.metadata` 做 backend-agnostic 的 not-found 探测,WebDAV 404 / SFTP/SMB 协议特有码不再被当成硬失败
- `RepoBootstrap.verifyVersionCompatible` 严格化:read 失败 / parse 失败 / format 不等于本地 → 抛 `VersionConflict`(分别 `unreadable` / `mismatchedFormatVersion`),builder 把后者映射成 `unsupportedRemoteFormat`
- `RemoteIndexSyncService.syncIndexV2` / `BackupRunPreparation.verifyMonthV2`:V2 repo 的 `.absent` repo.json 不再降级到 `expectedRepoID = nil`,直接抛错(broken identity state,backup-flow 才能修)
- `RepoMaterializer` commit 收集阶段从 filename 推进 `observedSeqByWriter`,corrupt / 错名 commit 也把 seq 计入 cold-start max,allocator 不会再撞名循环耗尽 4 次重试
- `BackupParallelExecutor` / `BackgroundBackupRunner` flushDelta 处理同时消费 `committedV2AssetFingerprints` 和 `committedV2TombstoneFingerprints`:tombstone 也从 uncommittedV2 集合中移除,不再残留到下次全量 materialize
- `SnapshotRowMapper.decodeHeader` 严格 covered 解析:bad pair / 类型错误 → 抛 `SnapshotWireError.malformed`,不再静默降级为空 covered 让坏 snapshot 当 baseline
- `LocalVolumeClient.atomicCreate` 修 cleanup gap:source 打开失败也清空 destination;destination close 错误 surface(外接盘 unmount/写满有时只在 close 时报)
- `SeqAllocator.persist` / `PersistedLamportClock.persist` 改 conditional UPDATE(`AND lastSeq < ?` / `AND lastClock < ?`):跨进程并发(BG runner + foreground)的回退场景下不再覆盖更高的持久化值
- `RestoreService` 三个补丁:`fileName` 走 `RemotePathBuilder.sanitizeFilename`(防路径穿越);失败路径加 `removeItem(at: tempURL)`(漏 cleanup);保留 disconnect fire-and-forget(动 API 边界,留给 Stage 2)
- `CommitOpMapper` / `SnapshotRowMapper` 所有 hex → Data 解码加 `!fp.isEmpty` 守卫:`Data(hexString: "")` 返回 `Data()` 不是 nil,空 fingerprint commit/snapshot 不会再被接受

### `BackupResumePlanner` 切 `RepoMaterializer.MonthState`

- `makePlan` 接受可选 `materializedMonthState: RepoSnapshotState?`
- 当 state 提供且 `ContentHashIndexRepository` 注入：用 `fetchAssetFingerprintRecords(assetIDs:)` 把待办 assetID 映射到 fingerprint，命中 `state.months[*].assets.keys` 的视为已备份，提前从 pending 集剔除
- 三种 mode（retry / scoped / full）共用此过滤路径

### Iter 9 加固：`.overwritePossible` 资源永远走 writerID 后缀

- SMB / S3-multipart 这类后端 `atomicCreateGuarantee == .overwritePossible`，写入会沉默覆盖另一台正在并发上传同名文件的 writer
- V2 写入路径在 `prepareUpload` 提前查 `client.atomicCreateGuarantee(forFileSize:remotePath:)`；当结果 `== .overwritePossible` 且 `monthStore.v2Services?.writerID != nil` 时强制将文件重命名为 `<base>~<wid6>.<ext>`（沿用 `RepoLayout.writerIDShort` 既有约定）
- 原本只在「本地 manifest 已记录到同名」时才会触发后缀重命名；强制路径多花的成本是：单 writer 模式也会看到后缀文件名，但不会出现两个 writer 同名互覆的窗口
- `.exclusive`（local volume / SFTP）和 V1（无 `v2Services`）保持原行为，不会出现新后缀

仅以下纯 Stage 2 项目未在本次范围：

1. **真消除强杀窗口**：per-asset commit 或本地 `resource_intents` pending state（plan §11）
2. **commit / 物理文件 GC**：跨 writer 安全的版本（plan §0 / §6.5 明确不做）
3. **gcOrphan op + 物理文件 refcount + 「从备份删除 asset」UI**
4. **`reportMissing` / `reportTampered` / `commitCorruption` op**
5. **跨 writer snapshot GC**（grace + liveness gate + 联合覆盖判定）
6. **WebDAV `If-None-Match` probe + 缓存**：当前直传 `*`，服务端不支持时回 412 当作 alreadyExists（错误归类正确，语义略有偏差）
7. **持久化本地 V2 materialized cache**（cold start 进入毫秒级）
8. **月级 materialize 并发深化**
9. **E2EE**（modes: plain / e2ee-content-visible / e2ee-private）
10. **audit / repair 完整 UI**（兜底 pre-1.2.0 客户端误用造成的 desync）
11. **全局 compaction snapshot（Delta Lake 风格）+ checkpoint**
12. **`writers/<writerID>.json` 元数据**
13. **「合并历史 writer」UI**（处理 Keychain 丢失场景）
14. **`verifyMonth` `partiallyMissing` 类提供 re-upload 动作**

## 关键假设 / 限制

1. `min_app_version "2.0.0"` 是 `RepoLayout.minAppVersionPlaceholder`；发布时换实际下一版本号
2. 升级**不可逆**：phase 2 翻转后已升级客户端永远无法回退到 V1
3. 强杀窗口 ~9 asset；真消除留 Stage 2
4. **多端并发**：V2 best-effort，不保证零冲突（特别是 WebDAV）；plan §12 已声明
5. **pre-1.2.0 V1 客户端**：协议无 sentinel 锁出，依赖用户主动升级所有设备
