# Watermelon Photo Backup

`Watermelon` 是一个以 iOS 相册为数据源、将资源备份到远端存储的应用。当前代码已经围绕 `Home` 单页、月级执行计划和本地 hash 索引形成一条比较完整的主链路。

## 当前能力（按代码）

- 存储类型：`SMB`、`WebDAV`、`外接存储目录（security-scoped bookmark）`
- 月份级操作：`上传（本地→远端）`、`下载（远端→本地）`、`同步（双向）`
- 备份模式：`full`、`scoped(assetIDs)`、`retry(assetIDs)`
- 运行控制：`开始 / 暂停 / 继续 / 停止 / 退出执行`
- 上传调度：按月份分桶后由多个 worker 动态领取任务
- 远端索引：远端 manifest 扫描后写入 `RemoteLibrarySnapshotCache`，Home 侧按 `revision + monthDeltas` 增量消费
- 本地索引：`local_assets / local_asset_resources` 作为本地 hash 索引与体积缓存
- 下载续传：下载成功后逐 item 写回 hash 索引并刷新本地视图状态

## 启动与主流程

1. `SceneDelegate` 创建 `AppCoordinator`
2. `AppCoordinator.start()` 直接把 `HomeViewController` 设为 root
3. `HomeViewController` 绑定 `HomeScreenStore`
4. `HomeScreenStore.load()` 先加载本地图库索引，再尝试自动连接上次激活的远端存储
5. 远端连接成功后由 `BackupCoordinator.reloadRemoteIndex(...)` 刷新共享远端快照
6. 用户在首页按月选择后，执行计划交给 `HomeExecutionCoordinator`

## 首页架构

Home 现在不是“胖 VC”，而是四层分工：

1. `HomeViewController`
   负责 UICollectionView、header、连接菜单、右侧 overlay、底部 `SelectionActionPanel` 和更多页入口。
2. `HomeScreenStore`
   聚合首页状态，拥有 `sections / rowLookup / selection / connectionState / executionState`，并把内部变化统一映射为 `.data / .selection / .execution / .connection / .structural`。
3. `HomeConnectionController`
   负责 profile 列表、自动连接、密码提示、切换连接、断开连接，以及连接时触发远端索引重载。
4. `HomeExecutionCoordinator`
   负责本地索引预检查、上传阶段、同步月份内联下载、纯下载月份执行、暂停/恢复/停止。

## 备份与下载主链路

### 上传

1. `HomeExecutionCoordinator` 会先冻结一次执行设置，包括 `上传并发` 和 `允许访问 iCloud 原件`。
2. 若本次包含上传且启用了 `允许访问 iCloud 原件`，会先对上传范围做轻量 availability probe；一旦检测到仅存于 iCloud 的本地资源，本次 upload 会自动降为 `1` 个 worker。
3. 随后会对本次涉及的本地 asset 做本地索引预检查；第一轮始终离线建索引。
4. 若本次包含下载或同步，且第一轮仍有 `unavailableAssetIDs`：
   - 启用 `允许访问 iCloud 原件`：只对这些资产再做一次联网补索引
   - 未启用：直接终止，避免因为缺少本地 hash 而生成重复资源
5. 上传实际通过 `BackupSessionController` + `BackupSessionAsyncBridge` 驱动 `BackupCoordinator.runBackup(...)`。
6. `BackupCoordinator` 再分成：
   - `BackupRunPreparationService.prepareRun`
   - `BackupParallelExecutor.execute`
7. `BackupParallelExecutor` 用 `MonthWorkQueue` 动态分发月份，worker 按月加载 `MonthManifestStore`，逐 asset 调用 `AssetProcessor.process(...)`。

### 同步月份

- 同步月份不是简单地“全部上传完再统一下载”。
- 每个同步月份在上传 flush 完成后，会立即通过 `onMonthUploaded` 回调进入该月份的下载收尾。
- 剩余纯下载月份则在上传阶段结束后顺序执行。

### 下载

1. `DownloadWorkflowHelper` 调 `RestoreService.restoreItems(...)`
2. 每个 item 下载成功后立刻写本地 hash 索引
3. `HomeExecutionDataRefresher` 刷新本地索引与远端快照，使首页进度立即前进

## 数据存储

### 本地 SQLite（GRDB）

- `server_profiles`
- `sync_state`
- `local_assets`
- `local_asset_resources`

### 远端月 manifest（SQLite）

每个月目录维护一个 `/{YYYY}/{MM}/.watermelon_manifest.sqlite`，包含：

- `resources`
- `assets`
- `asset_resources`

## 本地 Hash 索引

`LocalHashIndexBuildService` 负责批量建立或补齐本地 hash 索引，`ContentHashIndexRepository` 负责读写：

- 资源级 `contentHash`
- `assetFingerprint`
- `totalFileSizeBytes`
- 覆盖率与统计信息

UI 入口在更多页的 `LocalHashIndexManagerViewController`，支持：

- `创建`
- `更新`
- `暂停 / 停止`
- `重置`
- 可选移除本地已不存在条目

## 开发说明

1. 使用 Xcode 打开 `Watermelon.xcodeproj`
2. 选择 `Watermelon` scheme
3. 在模拟器或真机运行

当前没有成体系自动化测试；核心链路仍以真机手工回归为主。

## 文档导航

- `docs/00-LLM-HANDOVER.md`：快速接手说明
- `docs/01-Architecture.md`：模块分层与依赖关系
- `docs/02-BackupCoreV2.md`：备份 / 下载 / 同步执行细节
- `docs/03-DataModel.md`：本地数据库、远端 manifest 与内存快照结构
- `docs/04-UIFlow.md`：首页、连接、执行与更多页流程
- `docs/05-OpenIssues.md`：当前风险点与技术债
