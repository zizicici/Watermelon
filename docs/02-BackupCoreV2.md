# 备份核心流程（当前实现）

## 1. 两条执行主线

当前代码里有两层执行链路：

1. **通用上传链路**
   `BackupSessionController` / `BackupCoordinator` / `AssetProcessor`
2. **首页执行链路**
   `HomeExecutionCoordinator` 在通用上传链路外，再拼上本地索引预检查、同步月份内联下载和纯下载月份执行

## 2. 首页执行入口

调用链：

1. `HomeViewController.executeTapped()`
2. `HomeScreenStore.startExecution(upload:download:sync:)`
3. `HomeExecutionCoordinator.enter(upload:download:sync:)`
4. `HomeExecutionCoordinator.startExecution()`

执行会话由 `HomeExecutionSession` 管理，按月保存 `MonthPlan`：

- `needsUpload`
- `needsDownload`
- `phase`
- `failedItemCount`
- `failureMessage`

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

## 4. 上传主流程

上传实际通过 `BackupSessionAsyncBridge.runUpload(...)` 进入通用备份链路：

1. `BackupSessionController.startBackupWhenReady(...)`
2. `BackupSessionController.startBackup(...)`
3. `BackupRunDriver`
4. `BackupCoordinator.runBackup(request:eventStream:)`

### `BackupRunPreparationService.prepareRun`

准备阶段顺序：

1. 校验或申请相册权限
2. 创建并连接远端 client
3. 保证 `basePath` 存在
4. `RemoteIndexSyncService.syncIndex(...)` 扫描远端 manifest
5. 如果快照条目数不大于 `120_000`，构建 `MonthSeedLookup`
6. 读取待处理资产
   - `full`：全图库，按创建时间升序
   - `retry/scoped`：按指定 ID 获取，再按创建时间排序
7. 按月份构建 `monthAssetIDsByMonth`
8. 从本地 hash 索引读取 `totalFileSizeBytes`，估算每个月体积
9. 按“预计字节数优先、数量次之、月份次之”构建 `MonthWorkItem`
10. 决定 worker 数与连接池大小

### worker 数

默认规则：

1. `SMB / WebDAV = 2`
2. `externalVolume = 3`
3. 用户可在设置里手动覆盖 `1...4`
4. 启用 `允许访问 iCloud 原件` 时，不会直接永远单 worker；只有离线预检查在上传范围 (`upload + sync` 月份) 内产出 `unavailableAssetIDs`（包含 cache-hit 但已被系统回收到 iCloud 的资产）时，才会把本次 upload 强制改为 `1`
5. 最终还会再按月份数裁剪

## 5. 并行执行面

`BackupParallelExecutor.execute(...)` 的核心步骤：

1. 创建 `StorageClientPool`
2. 用初始已连接 client 预热连接池
3. 用 `MonthWorkQueue` 动态分发月份
4. 每个 worker：
   - 领取一个月份
   - `MonthManifestStore.loadOrCreate(...)`
   - 以 `500` 个 asset 为一批处理
   - 批量读取本地 hash cache
   - 逐 asset 调 `AssetProcessor.process(...)`
5. 月份结束后 `flushToRemote(...)`
6. flush 成功后发出 `.monthChanged(.completed)`
7. 若提供了 `onMonthUploaded`，则在该月份 flush 完成后执行月级收尾

## 6. 单 asset 处理

`AssetProcessor.process(...)` 的关键规则：

1. 先基于 `LocalHashIndexBuildService` / `ContentHashIndexRepository` 的结果尝试本地 cache 快速命中
2. 未命中时，按 `BackupAssetResourcePlanner` 选择资源并分配 `role/slot`
3. 将资源导出到临时文件并计算 `SHA-256`
4. 生成 `assetFingerprint`
5. 对每个资源执行上传或跳过：
   - 若未启用 `允许访问 iCloud 原件` 且导出遇到 `networkAccessRequired`：整条 asset 记为 `skipped`
   - manifest 中已有同 hash：直接跳过
   - 同名冲突：
     - 小于 `5 MiB`：优先下载远端文件比 hash
     - 其他情况：走尺寸/重命名策略
   - 上传失败最多重试 `3` 次
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
   - 标记该月上传已完成
   - 立刻 `syncRemoteDataAndWait()`
   - 刷新该月相关本地索引
   - 取出该月 `remoteOnlyItems`
   - 交给 `DownloadWorkflowHelper.downloadItems(...)`
4. 下载成功后再把该月标记为 `downloadCompleted`

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
   的较大值，保证不回退

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

## 11. manifest flush 语义

1. `MonthManifestStore` 本地 sqlite 改动先落本地临时文件
2. 月份结束时 `flushToRemote(...)`
3. 如果远端存储已不可用：
   - 记录日志
   - 跳过 flush
   - 将错误向上抛出
4. `loadSeeded(...)` 会额外列出真实远端目录，避免“文件已存在但 manifest 未记账”造成的重名冲突

## 12. 关键常量

1. 本地索引离线预检查 worker：`2`
2. iCloud recovery 预检查 worker：`1`
4. Home 侧远端同步节流：`2s`
5. month seed 内存阈值：`120_000` 条目
6. 并行执行的 PHAsset 批大小：`500`
7. 小文件碰撞校验阈值：`5 * 1024 * 1024`
8. 上传最大重试次数：`3`
