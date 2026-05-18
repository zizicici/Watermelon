# 备份核心流程（当前实现）

## 1. 两条执行主线

当前代码里有两层执行链路：

1. **通用上传链路**
   `BackupSessionController` / `BackupCoordinator` / `AssetProcessor`
   控制器 + 准备 + 并行执行 + 单 asset 处理
2. **首页执行链路**
   `HomeExecutionCoordinator` 在通用上传链路外，再拼上本地索引预检查、同步月份内联下载和纯下载月份执行

通用上传链路同时还被 `BackgroundBackupRunner`（后台备份）使用——它通过 `DependencyContainer.makeForBackgroundTask()` 拉起一份独立依赖，复用 `BackupCoordinator.runBackup(...)`。

## 2. 首页执行入口

调用链：

1. `HomeViewController.executeTapped()`
2. `HomeScreenStore.startExecution(backup:download:complement:)`
3. `HomeExecutionCoordinator.enter(...)`
4. `HomeExecutionCoordinator.startExecution()`

执行会话由 `HomeExecutionSession` 管理，按月保存 `MonthPlan`：

- `needsUpload`
- `needsDownload`
- `phase`
- `failedItemCount`
- `failureMessage`

`MonthPlan.Phase` 当前枚举：`pending / uploading / uploadPaused / uploadDone / downloading / downloadPaused / completed / partiallyFailed / failed`。

进入执行时，`HomeExecutionCoordinator` 还会冻结一份本次执行配置快照：

1. `上传并发`
2. `允许访问 iCloud 原件`

这份快照会贯穿 preflight / upload / resume，全程不再重新读取设置。

## 3. 执行前的本地索引预检查

执行一开始，`HomeExecutionCoordinator.prepareLocalIndexIfNeeded()` 只做一步本地 hash 索引预检查：

`LocalHashIndexBuildService.buildIndex(for:assetIDs, workerCount: 2, allowNetworkAccess: false)`

目的：

1. 让本次涉及的本地 asset 尽量都具备资源级 hash
2. 让后续 reconcile、下载去重、同步判定有稳定基线

关键规则：

1. 第一轮始终离线执行，因此 iCloud-only 资源会被标记为 `unavailable`。cache-hit 资产也会顺带做一次轻量离线可用性探测，避免曾经建过索引、之后被系统回收到 iCloud 的资产漏检。
2. 第一轮结束后，如果启用了 `允许访问 iCloud 原件` 且 **上传范围** (`upload + sync` 月份) 内存在 `unavailableAssetIDs`，本次 upload 会强制降为 `1` 个 worker——这一决定直接从第一轮结果推导。
3. 如果本次 **只上传**，即使有少量本地索引仍不完整，也允许继续执行。
4. 如果本次包含 **下载或同步**，且第一轮存在 `unavailableAssetIDs`：
   - 启用 `允许访问 iCloud 原件`：只对这些 `unavailableAssetIDs` 再跑一次 `buildIndex(... allowNetworkAccess: true)`，worker 固定为 `1`
   - 未启用：直接停止执行，并提示去设置启用该选项，或先在系统相册把原件下载到本机
5. 如果联网补索引后仍有 `failedAssetIDs / unavailableAssetIDs`，则继续停止执行。

`LocalIndexBuildCoordinator`（位于 `Watermelon/Services/HashIndex/`）是用户主动触发索引重建时使用的另一条入口（在更多页 / 索引页），与执行态预检查互不复用 worker。

## 4. 上传主流程

上传实际通过 `BackupSessionAsyncBridge.runUpload(...)` 进入通用备份链路：

1. `BackupSessionController.startBackupWhenReady(...)`
2. `BackupSessionController.startBackup(...)`
3. `BackupRunDriver`
4. `BackupCoordinator.runBackup(request:eventStream:)`

### `BackupRunPreparationService.prepareRun`（`BackupRunPreparation.swift`）

准备阶段顺序：

1. 校验或申请相册权限
2. 创建并连接远端 client
3. `BackupV2RuntimeBuilder.build(...)` 确保 `basePath`，并用 `inspectRemoteFormat` 路由 fresh / V1 / V2 / unsupported
4. fresh 仓库 bootstrap V2 identity（claims / finalization / `repo.json` read-cache）+ `version.json`；V1 仓库在前台同步迁移；V2 仓库建立 `BackupV2RuntimeServices`
5. V2 路径额外打开专用 metadata client，并把 cold-start materialize 输出缓存到 `initialMaterializeOutput`
6. `RemoteIndexSyncService.syncIndex(...)` 写入 `RemoteLibrarySnapshotCache`：V2 走 materialize（可消费预热结果），V1 继续扫 per-month manifest
7. 读取待处理资产
   - `full`：全图库，按创建时间升序
   - `retry / scoped`：按指定 ID 获取，再排序
8. 按月份构建 `monthAssetIDsByMonth`
9. 从本地 hash 索引读取 `totalFileSizeBytes`，估算每个月体积
10. 按 “预计字节数 → 数量 → 月份键” 顺序构建 `MonthPlan / MonthWorkItem`
11. 决定 worker 数与连接池大小

如果 `RemoteIndexSyncService.syncIndex` 抛出非 “连接不可用” 类错误：当前 fresh / V1-migrated / V2 路径都会带 `BackupV2RuntimeServices` 并 fail-closed；没有 V2 runtime 的旧 fallback 才会降级为 warning 并继续执行。

### worker 数

默认规则：

1. `SMB / WebDAV / S3 / SFTP = 2`
2. `externalVolume = 3`
3. 用户可在设置里手动覆盖 `1...4`
4. 启用 `允许访问 iCloud 原件` 时，不会直接永远单 worker；只有离线预检查在上传范围 (`upload + sync` 月份) 内产出 `unavailableAssetIDs`（包含 cache-hit 但已被系统回收到 iCloud 的资产）时，才会把本次 upload 强制改为 `1`
5. 最终还会再按月份数裁剪
6. SFTP 的每个 worker 都会起一条独立的 SSH 连接 + SFTP subsystem。多 worker = 多 TCP/握手，受服务端 `MaxStartups` / `MaxSessions` 限制；遇到紧配置的 sshd 需要回落到 worker = 1

连接池大小 `connectionPoolSize` 由 `BackupMonthScheduler.resolveConnectionPoolSize(...)` 推导：SMB / WebDAV / S3 / SFTP 默认裁到 `min(workerCount, 2)`，用户手动覆盖 worker 时给到 `workerCount`；externalVolume 始终给 `workerCount`。

## 5. 并行执行面

`BackupParallelExecutor.execute(...)` 的核心步骤：

1. 创建 `StorageClientPool`（`Shared/Services/Backup/`）
2. 用初始已连接 client 预热连接池
3. 用 `MonthWorkQueue`（actor，定义在 `Watermelon/Services/Backup/BackupMonthScheduler.swift`）动态分发月份
4. 每个 worker：
   - 领取一个月份
   - V2：`V2MonthSession.loadOrCreate(...)` materialize 单月、列真实月份目录，并构建 in-memory indexes
   - V1：`MonthManifestStore.loadOrCreate(...)` 装载 legacy sqlite manifest
   - 以 `500` 个 asset 为一批处理
   - 批量读取本地 hash cache
   - 逐 asset 调 `AssetProcessor.process(...)`
5. 每处理 10 个非 failed 结果，就对当前 `BackupMonthStore` 调一次 batch `flushToRemote(...)`；月末仍兜底 flush
6. V2 flush 的 `FlushDelta` 会清理 `RemoteIndexSyncService` 的 uncommitted fingerprints；snapshot 写失败但 commit 已落盘时也会记录已 durable 的 fingerprints
7. 最终 flush 成功后若提供了 `onMonthUploaded`，会先执行月级收尾；收尾 `.success` 才发出 `.monthChanged(.completed)`，`.downloadIncomplete` 会发出对应状态，`.failed` 走 fatal failure

## 6. 单 asset 处理

`AssetProcessor`（核心类在 `AssetProcessor.swift`，命名细节在 `+Naming`，上传策略在 `+Upload`）的关键规则：

1. 先基于 `LocalHashIndexBuildService` / `ContentHashIndexRepository` 的结果尝试本地 cache 快速命中（`processWithLocalCache`）
2. 未命中时，按 `BackupAssetResourcePlanner`（`Shared/Services/Backup/`）选择资源并分配 `role/slot`
3. 将资源导出到临时文件并计算 `SHA-256`
4. 生成 `assetFingerprint`（`role|slot|hashHex` token 排序、`\n` 连接、再 SHA-256）
5. 对每个资源执行上传或跳过：
   - 若未启用 `允许访问 iCloud 原件` 且导出遇到 `networkAccessRequired`：整条 asset 记为 `skipped`
   - manifest 中已有同 hash：直接跳过
   - 同名冲突：
     - 小于 `5 MiB`（`smallFileThresholdBytes = 5 * 1024 * 1024`）：优先下载远端文件比 hash
     - 其他情况：走尺寸/重命名策略
   - 上传重试：默认上限 `3` 次；当 `client.shouldLimitUploadRetries(for:)` 命中（如 WebDAV 永久错误）会被压到 `2`
6. 所有资源成功后才写：
   - 月 manifest 的 `assets / asset_resources / resources`
   - 本地 `local_assets / local_asset_resources`
   - 远端快照缓存增量

## 7. 同步月份的内联下载

这是当前实现最容易被旧文档误导的地方。

同步月份不是统一等到上传阶段结束后再下载，而是：

1. `HomeExecutionCoordinator.makeUploadMonthFinalizer()` 把回调传给 `BackupCoordinator`
2. 某个 sync 月份 flush 完成后，worker 会调用 `onMonthUploaded(month)`
3. 回调内部执行：
   - 标记该月上传已完成（`uploadDone`）
   - 立刻 `syncRemoteDataAndWait()`（驱动 `BackupCoordinator` 重新拉取最新远端快照）
   - 刷新该月相关本地索引
   - 取出该月 `remoteOnlyItems`
   - 交给 `DownloadWorkflowHelper.downloadItems(...)`
4. 下载成功后再把该月标记为 `completed`

效果：

1. sync 月份能更早完成闭环
2. 恢复执行时，已经做完上传但下载被暂停的 sync 月份可以继续补完

## 8. 剩余下载阶段

上传阶段结束后，`HomeExecutionCoordinator.runDownloadPhase()` 只处理仍未终态的下载月份：

1. 先同步一次远端快照
2. 如该月存在本地 asset，则刷新本地索引
3. 读取 `remoteOnlyItems(month)`
4. `DownloadWorkflowHelper.downloadItems(...)`
5. 每个 item 成功后：
   - `writeHashIndex(...)`
   - 触发 `refreshLocalIndexAndNotify([assetID])`

下载阶段的取消粒度仍是 item 级，因为 `RestoreService.restoreItems(...)` 在 item 循环开头检查 `Task.checkCancellation()`。

## 9. 进度与 UI 映射

### 上传阶段

1. `BackupSessionAsyncBridge` 把 started/completed months 和 `processedCountByMonth` 回传给 `HomeExecutionSession`
2. `HomeExecutionCoordinator` 以 `2s` 节流触发远端同步
3. 首页箭头百分比取：
   - `sessionPercent`
   - 基线进度（`HomeIncrementalDataManager.matchedCount(for:)` / `HomeLocalIndexEngine` 月级 `backedUpCount`，按 fingerprint 匹配本地和远端）
   - 二者中的较大值，保证不回退

### 下载阶段

1. 纯下载和 sync 月份下载阶段都以 `matchedCount`（本地月聚合中的 `backedUpCount`）为准
2. 每个 item 成功后立即刷新本地索引，因此百分比会逐步前进

## 10. 暂停 / 恢复 / 停止

### 暂停

1. 上传阶段：
   - 取消执行 task
   - 请求 `backupBridge.requestPause()`
   - 已完成上传但未完成下载的 sync 月份会记录待恢复 asset IDs
2. 下载阶段：
   - 取消下载 task
   - 标记 `downloadPaused`

### 恢复

1. `HomeExecutionSession.resume()` 恢复到对应阶段
2. 已完成的月份不会重新执行
3. 已上传未下载完成的 sync 月份会继续下载收尾
4. resume 会沿用该 run 启动时冻结的 `上传并发 / 允许访问 iCloud 原件` 配置

### 停止

1. 上传阶段：发送 stop intent，并等待运行中的备份链路自行收束
2. 下载阶段：取消下载 task，然后退出执行态

## 11. metadata flush 语义

V2 当前主路径：

1. `V2MonthSession` 在内存里维护当月 resources / assets / links / tombstones
2. `flushToRemote(...)` 先通过 `V2MonthCommitFlusher` 写 commit jsonl，再通过 `V2MonthSnapshotFlusher` 写 snapshot jsonl
3. 每 10 个非 failed asset 触发一次 batch flush，月末或 pause/connection-loss 收束时再兜底 flush
4. commit 已落盘但 snapshot 写失败时，错误会携带 `committedAssets / committedTombstones`，调用方必须先清理对应 uncommitted fingerprints 再继续传播错误
5. `V2MonthSession.loadOrCreate(...)` 会列出真实月份目录，避免“文件已存在但 V2 metadata 未 flush”的 orphan 造成重名冲突

V1 兼容路径仍由 `MonthManifestStore` 维护 sqlite manifest：本地 sqlite 改动先落临时文件，月末 `flushToRemote(...)` 上传 `.watermelon_manifest.sqlite`。`MonthManifestStore` 实现拆为三段：核心入口在 `MonthManifestStore.swift`，初始化 / seed 流程在 `+Loading.swift`，schema / 迁移在 `+Schema.swift`（`month_manifest_v1_initial`）。

## 11.5 远端资源 presence 语义（统一类型）

`RemoteResourcePresence`（`Shared/Services/Backup/RemoteResourcePresence.swift`）是 V2 worker session、sync overlay、verify probe 三处对"这个远端资源能不能信"的统一答复：

- `.hashVerified` —— LIST + size + SHA 全过
- `.listedSizeMatched` —— LIST + size 匹配，未做 SHA 验证（可作为候选，不可作为内容相等断言）
- `.missing` —— LIST 缺该路径或 size 不匹配
- `.inconclusive(reason)` —— `.neverProbed` / `.verifyBudgetExhausted` / `.probeFailure` 三种暂不能定论

每月的 presence map 用 `RemoteMonthPresenceMap`（path-keyed value type）：

- `V2MonthIndexes` / `V2MonthSession` 持有 path-keyed map；`findByFileName` / `anyPresentPath` 通过 `isUsableCandidate(_:)` 接受 size-matched 或 hash-verified 候选；`upsertResource` 写新字节后把路径标成 `.hashVerified`
- `RemoteIndexSyncService` 的 overlay probe 输出按 hash 聚合（`[Data: RemoteResourcePresence]`）；`OverlayMonthProbe` 的 `missingHashes` / `inconclusiveHashes` 都是从这份 map derived，每月 freshness 由调用方按 policy（`.preserveFallback` / `.failClosedWhenMissingFallback`）在 probe 输出之外判定
- `RepoVerifyMonthService` 的 `PresenceSnapshot` 也用同一份 map；`hasInconclusiveResource(in:)` 判定 verify 是否完整

调用约定：
- `isHashVerified(_:)` —— 只认 SHA 过的；任何对内容字节安全敏感的判断走这条
- `isUsableCandidate(_:)` —— size 匹配或 SHA 过都接受；upload preflight / dedup 走这条
- `isMissing(_:)` —— 只认 `.missing`；inconclusive **不算** missing（避免 tombstone 错杀未探测字节）

## 12. 远端维护（用户主动触发）

`RemoteMaintenanceController`（`Watermelon/Services/Backup/`）是 “验证远端” 入口：

1. 通过 `BackupCoordinator.verifyAllMonths(...)` 检查所有月份的 manifest 与实际远端文件一致性
2. 暴露 `isVerifying / progress / lastError` 给 More 页 / 诊断页
3. 校验运行期间会通过 `Notification.Name` 把 Home 的 `isMaintenanceBlocked` 拉为 `true`，从而让 `isSelectable` 与 `startExecution` 都被阻塞

它不参与执行链路，但与执行链路互斥。

## 13. 关键常量

1. 本地索引离线预检查 worker：`2`
2. iCloud recovery 预检查 worker：`1`
3. Home 侧远端同步节流：`2s`
4. V2 batch flush 间隔：`10` 个非 failed asset（`BackupV2Constants.batchFlushInterval`）
5. 并行执行的 PHAsset 批大小：`500`
6. 小文件碰撞校验阈值：`5 * 1024 * 1024`（`smallFileThresholdBytes`）
7. 上传最大重试次数：`3`（`client.shouldLimitUploadRetries(for:)` 命中时降为 `2`）
