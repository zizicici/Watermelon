# Watermelon Photo Backup

[English](README.md) · **简体中文**

`Watermelon` 是一个以 iOS 相册为数据源、将资源备份到远端存储的应用。仓库还附带一个 macOS target，目前主要用于遗留数据迁移。iOS 端代码已经围绕 `Home` 单页、月级执行计划和本地 hash 索引形成一条比较完整的主链路。

## 仓库结构

- `Watermelon/`：iOS app target 源码
- `Shared/`：iOS 与 macOS 共享源码（数据库、Keychain、存储 / SMB / S3 / SFTP 客户端、`MonthManifestStore`、`RemoteLibrarySnapshotCache`、领域模型、执行日志、跨端扩展）
- `WatermelonMac/`：macOS target，遗留迁移工具与 profile 管理；**不**驱动 iOS 备份链路
- `WatermelonTests/`：XCTest 目标，覆盖 Home 纯逻辑单元
- `docs/`：架构、数据模型、UI 流程、技术债

> **⚠️ macOS 端状态**：`WatermelonMac` **目前仍在测试阶段，尚未发布任何版本** —— 没有 App Store、没有 TestFlight、也没有任何对外的签名分发。它属于开发期产物，行为、数据布局、迁移路径都可能在没有通知的情况下变化。请不要拿它处理重要相册或生产环境的远端存储；如果一定要试用，先把输入数据备份好。

## 当前能力（按代码）

- 存储类型：`SMB`、`WebDAV`、`S3` 兼容对象存储、`SFTP`、`外接存储目录（security-scoped bookmark）`
- 月份级操作：`上传（本地→远端）`、`下载（远端→本地）`、`同步（双向）`
- 备份模式：`full`、`scoped(assetIDs)`、`retry(assetIDs)`
- 运行控制：`开始 / 暂停 / 继续 / 停止 / 退出执行`
- 后台备份：按 profile 配置 `backgroundBackupEnabled`，需要 Pro
- 画中画进度（Pro）
- 远端校验：用户主动触发，由 `RemoteMaintenanceController` 跑
- Profile 可达性：`ProfileReachabilityService` 在后台对已保存 profile 做轻量探测（SMB/TCP、WebDAV/S3 HTTP HEAD、外接存储 bookmark），把 `离线` 标记同步到首页菜单
- 上传调度：按月份分桶后由多个 worker 动态领取任务
- 远端索引：远端 manifest 扫描后写入 `RemoteLibrarySnapshotCache`，Home 侧按 `revision + monthDeltas` 增量消费
- 本地索引：`local_assets / local_asset_resources` 作为本地 hash 索引与体积缓存
- 下载续传：下载成功后逐 item 写回 hash 索引并刷新本地视图状态

## 启动与主流程

1. `SceneDelegate` 创建 `AppCoordinator`
2. `AppCoordinator.start()` 直接把 `HomeViewController` 设为 root
3. 首次启动时，`OnboardingViewController` 会以模态页方式叠在首页上引导
4. `HomeViewController` 绑定 `HomeScreenStore`
5. `HomeScreenStore.load()` 先加载本地图库索引，再尝试自动连接上次激活的远端存储
6. 远端连接成功后由 `BackupCoordinator.reloadRemoteIndex(...)` 刷新共享远端快照
7. 用户在首页按月选择后，执行计划交给 `HomeExecutionCoordinator`

## 首页架构

Home 现在不是“胖 VC”，而是被拆成多个职责明确的小组件：

1. `HomeViewController` — 负责 UICollectionView、header、连接菜单、左右 overlay、底部 `SelectionActionPanel` 和更多页入口。
2. `HomeScreenStore` — 主 actor 状态聚合器；持有 `HomeIncrementalDataManager`、`HomeConnectionController`、`HomeExecutionCoordinator`、`PiPExecutionBridge`、`HomeScopeController`、`HomeScopeNormalizer`、`HomeSectionBuilder`、`HomePhotoAccessGate`，以及 lazy 构造的 `HomeRefreshScheduler`、`HomeSelectionController`；把内部变化映射为 `.data / .fileSizes / .selection / .execution / .connection / .connectionProgress / .structural`，并在相关 case 中携带 `Set<LibraryMonthKey>`。
3. `HomeConnectionController` — 负责 profile 列表、自动连接、密码提示、切换 / 断开连接，以及连接时触发远端索引重载。
4. `HomeIncrementalDataManager` — 把索引变更与快照同步交给 `HomeDataProcessingWorker`（持有 `HomeLocalIndexEngine` + `HomeRemoteIndexEngine`），文件大小扫描交给 `HomeFileSizeScanCoordinator`。
5. `HomeExecutionCoordinator` — 负责本地索引预检查、上传阶段、同步月份内联下载、纯下载月份执行、暂停 / 恢复 / 停止。

## 备份与下载主链路

### 上传

1. `HomeExecutionCoordinator` 会先冻结一次执行设置，包括 `上传并发` 和 `允许访问 iCloud 原件`。
2. 对本次涉及的本地 asset 做本地索引预检查；第一轮始终离线建索引，并对 cache-hit 资产做轻量可用性探测，识别已被回收到 iCloud 的资产。
3. 若启用了 `允许访问 iCloud 原件` 且上传范围 (`upload + sync` 月份) 内仍有 `unavailableAssetIDs`，本次 upload 强制降为 `1` 个 worker。
4. 若本次包含下载或同步，且第一轮仍有 `unavailableAssetIDs`：
   - 启用 `允许访问 iCloud 原件`：只对这些资产再做一次联网补索引（worker = 1）
   - 未启用：直接终止，避免因为缺少本地 hash 而生成重复资源
5. 上传实际通过 `BackupSessionController` + `BackupSessionAsyncBridge` 驱动 `BackupCoordinator.runBackup(...)`。
6. `BackupCoordinator` 聚合：
   - `BackupRunPreparationService`（位于 `BackupRunPreparation.swift`）
   - `BackupParallelExecutor`
   - `RemoteIndexSyncService`（位于 `Shared/Services/Backup/`）
   - `RepoFormatRouter` / `LiteRepoGateway`（位于 `Shared/Services/Repo/` 与 `Watermelon/Services/Backup/`）
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

迁移：`v1_initial`、`v2_ms_timestamps`（把 `local_assets.modificationDateNs` 重命名为 `modificationDateMs` 并把已有值除以 1_000_000）、`v3_writer_id`（给 `server_profiles` 加 `writerID` 列，供 Repo V2 写锁使用）。

表：

- `server_profiles`
- `sync_state`
- `local_assets`
- `local_asset_resources`

### 远端月 manifest（SQLite）

两种布局下 manifest 都含相同的 `resources` / `assets` / `asset_resources` 表（`MonthManifestStore.ManifestLayout`），仅文件位置不同：

- `.v1`（当前生产）：`/{YYYY}/{MM}/.watermelon_manifest.sqlite`
- `.lite`（Repo V2）：`/.watermelon/months/{YYYY-MM}.sqlite`

## 本地 Hash 索引

`LocalHashIndexBuildService` 负责批量建立或补齐本地 hash 索引，`ContentHashIndexRepository` 负责读写：

- 资源级 `contentHash`
- `assetFingerprint`
- `totalFileSizeBytes`
- 覆盖率与统计信息

执行态预检查由 `HomeExecutionCoordinator.prepareLocalIndexIfNeeded()` 自动触发；用户主动触发的索引重建（更多页 / 索引页）走 `LocalIndexBuildCoordinator`，配合 `LocalIndexChangePublisher` 把索引变更广播给 Home / 索引页。

## 自动化测试

`WatermelonTests` 覆盖 Home 纯逻辑单元（引擎、controllers、scheduler、formatter）。涉及真实相册或远端的链路（执行协调、备份执行、下载、连接切换、macOS 迁移）仍以真机回归为主。

## 开发说明

1. 使用 Xcode 打开 `Watermelon.xcodeproj`
2. 选择 `Watermelon`（iOS）或 `WatermelonMac`（macOS，仅用于测试，详见上方提示）scheme
3. 在模拟器或真机运行
4. 运行 `WatermelonTests` 跑现有单测

## 文档导航

- `AGENTS.md`：面向所有 coding agent 的项目主指引（含优先阅读顺序）；`CLAUDE.md` 是指向它的符号链接，让 Claude Code 照常 auto-load
- `docs/01-Architecture.md`：模块分层与依赖关系（含 `Watermelon/` / `Shared/` / `WatermelonMac/` / `WatermelonTests/`）
- `docs/02-BackupCoreV2.md`：备份 / 下载 / 同步执行细节
- `docs/03-DataModel.md`：本地数据库、远端 manifest 与内存快照结构
- `docs/04-UIFlow.md`：首页、连接、执行、Onboarding 与更多页流程
- `docs/05-OpenIssues.md`：当前风险点与技术债
