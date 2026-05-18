# 架构与关键组件（当前）

## 0. 顶层目录结构

仓库现已拆成多个 target / 共享源码：

1. `Watermelon/`：iOS app target 源码（Home、`BackupCoordinator` 黏合层、PiP、Auth/More/Onboarding 等 iOS 专属代码）
2. `Shared/`：iOS + macOS 共享源码（数据库、Keychain、存储/SMB 客户端、`MonthManifestStore`、`RemoteIndexSyncService`、`RemoteLibrarySnapshotCache`、`StorageClientPool`、执行日志、`Domain/` 模型、跨端扩展）
3. `WatermelonMac/`：macOS app target，目前以遗留数据迁移工具与 profile 管理 UI 为主，**不**驱动 iOS 备份链路
4. `WatermelonTests/`：XCTest 目标，覆盖 Home 纯逻辑单元

下文除非显式标注路径，默认描述 iOS app 的运行时分层。

## 1. App 入口

1. `SceneDelegate` 创建 `AppCoordinator`
2. `AppCoordinator.start()` 直接把 `HomeViewController` 设为 `window.rootViewController`
3. 当前不是 TabBar，也不是全局 `UINavigationController` 根结构
4. 首次启动通过 `CompletionGate.hasCompleted` 判断后，会以 `.pageSheet` 形式 present `OnboardingViewController`（自包一层 `UINavigationController`）

说明：

1. 首页上的更多页会按当前上下文 `push`，若没有导航栈则自己包一层 `UINavigationController` 再 present。

## 2. DependencyContainer

`DependencyContainer` 当前提供：

1. `databaseManager` (`DatabaseManager`)
2. `keychainService` (`KeychainService`)
3. `appSession` (`AppSession`)
4. `storageClientFactory` (`StorageClientFactory`)
5. `photoLibraryService` (`PhotoLibraryService`)
6. `hashIndexRepository` (`ContentHashIndexRepository`)
7. `localHashIndexBuildService` (`LocalHashIndexBuildService`)
8. `restoredAssetFingerprintVerifier` (`RestoredAssetFingerprintVerifier`)
9. `localIndexChangePublisher` (`LocalIndexChangePublisher`)
10. `localIndexBuildCoordinator` (`LocalIndexBuildCoordinator`)
11. `backupCoordinator` (`BackupCoordinator`)
12. `restoreService` (`RestoreService`)
13. `appRuntimeFlags` (`AppRuntimeFlags`)
14. `remoteMaintenanceController` (`RemoteMaintenanceController`)
15. `profileReachabilityService` (`ProfileReachabilityService`)

说明：

1. `AppSession` 保存当前激活 profile 和会话内密码。SMB / WebDAV / S3 需要密码（S3 把 secret access key 落 Keychain）；SFTP 把 `SFTPCredentialBlob`（password 或 PEM + 可选 passphrase）的 JSON 落 Keychain，并在 `connectionParams` 里钉住主机指纹（`StorageProfile.supportsPasswordPrompt = false` —— 单字段密码弹窗装不下，凭证缺失只能进编辑页重填）；外接存储不需要。
2. `LocalHashIndexBuildService` 直接被 `HomeExecutionCoordinator` 用作执行前预检查工具。
3. `RestoredAssetFingerprintVerifier` 被 `DownloadWorkflowHelper` 用于下载后重建并验证本地 durable fingerprint binding，避免只因写入相册成功就把 item 视为可去重。
4. `LocalIndexBuildCoordinator` 封装更多页 / 索引 UI 触发的非执行态索引构建，并叠加权限检查与进度通知。
5. `LocalIndexChangePublisher` 把索引相关变更广播给 Home / 索引页，避免轮询。
6. `RemoteMaintenanceController` 负责用户主动触发的远端月份校验任务；它在校验期间会把 Home 的 `isSelectable` 拉为 `false`。
7. `ProfileReachabilityService` 在后台周期性探测已保存 profile（SMB / SFTP 走 TCP、WebDAV / S3 走 HTTP HEAD、外接存储走 security-scoped bookmark resolve），结果以 `unknown / reachable / unreachable` 形式暴露给 Home，供右侧菜单标记 “离线”。`DependencyContainer` 在初始化时立刻 `start()` 它，并保留 NWPathMonitor 与前台进入通知触发的 force-sweep 能力。
8. `DependencyContainer.makeForBackgroundTask()` 会构造一份独立的依赖给 `BackgroundBackupRunner` 使用。

## 3. Home 模块

### `HomeViewController`

职责：

1. 构建 UI：左右两栏 header（背景 + MarqueeLabel + toggle 按钮 + 菜单 overlay）、月份列表（`UICollectionViewCompositionalLayout`）、底部 `SelectionActionPanel`、左右两侧 overlay、右下角更多按钮 (FAB)
2. 配置 diffable data source 和按年 section 的 supplementary views
3. 只做渲染与交互分发，不直接承载首页业务状态
4. 监听 `HomeScreenStore.onChange`，按 `HomeChangeKind` 七种 case 分别走 `renderDataChange / renderFileSizeChange / renderSelectionChange / renderExecutionChange / renderConnectionChange / updateRemoteOverlay / renderStructuralChange`；其中 `.data / .fileSizes / .execution` 都携带 `Set<LibraryMonthKey>`，保证首页在小变动时只重配特定月份

### `HomeScreenStore`

职责：

1. 聚合 Home 子组件：`HomeIncrementalDataManager`、`HomeConnectionController`、`HomeExecutionCoordinator`、`PiPExecutionBridge`、`HomeScopeController`、`HomeScopeNormalizer`、`HomeSectionBuilder`、`HomePhotoAccessGate`，以及 lazy 构造的 `HomeRefreshScheduler`、`HomeSelectionController`
2. 维护首页渲染所需状态：`sections`、`rowLookup`、`selection`、`localPhotoAccessState`、`localLibraryScope`、`isReloadingScope`
3. 对外暴露 `connectionState` 与 `executionState`，以及综合判断 `isSelectable`（要求已连接、已授权相册、不在执行态、不在 scope 重载、不在远端 maintenance）
4. 把内部状态变化统一映射为 `HomeChangeKind`
5. 串行合并 `reloadLocal / syncRemote / notifyConnection / notifyStructural` 等刷新任务（由 `HomeRefreshScheduler` 执行）
6. 订阅 `LocalIndexChangePublisher` 与 `RemoteMaintenanceController`，把索引构建 / 校验事件转成 `.structural` 通知
7. 在 `load / reloadProfiles / connect / disconnect` 等 profile 集合或 active profile 变化处把最新数据推给 `ProfileReachabilityService`；其 `onChange` 回调会被合并成一次 `.notifyConnection` 刷新，让目标菜单重新渲染 `离线` 标记

### `HomeConnectionController`

职责：

1. 加载已保存远端 profile
2. 从 `sync_state.active_server_profile_id` 和 Keychain 自动恢复连接
3. 处理密码输入、切换 profile、断开连接
4. 调用 `BackupCoordinator.reloadRemoteIndex(...)` 重建共享远端快照
5. 连接失败时，如旧连接仍然有效，则尝试恢复旧连接对应的远端索引

### `HomeIncrementalDataManager`

数据管线门面，把重活分给两位协作者：

1. `HomeDataProcessingWorker`：在串行处理队列上跑索引变更与远端快照同步，持有 `HomeLocalIndexEngine` 与 `HomeRemoteIndexEngine`
2. `HomeFileSizeScanCoordinator`：在主 actor 上跑文件大小扫描，按月 `Task.yield()`，启动全量扫描与 `PHChange` 触发的 rescan 共用一个 refcount 控制 size snapshot 释放

对外提供：

1. `monthRow / allMonthRows / localAssetIDs / remoteOnlyItems / matchedCount`
2. `remoteOnlyItems(for:)` 已改为 `async`：单次处理队列 hop 内同时抓远端原始 delta（经 `BackupCoordinator.remoteMonthRawData(for:)`）、用 `HomeAlbumMatching.buildRemoteItems` 构建 `RemoteAlbumItem`、并 snapshot 本月本地 fingerprint 集合，queue 外只做差集过滤 + 创建时间升序排序，全程不再走 `PHAsset` / `local_asset_resources` fetch
3. 注册为 `PHPhotoLibraryChangeObserver`，并把 `RemoteLibrarySnapshotState` 增量写入 `HomeRemoteIndexEngine`

### `HomeLocalIndexEngine` / `HomeRemoteIndexEngine`

旧 `HomeLibraryEngines.swift` 已拆为两个独立文件，由 `HomeDataProcessingWorker` 持有。reconcile 仍然是 **按月聚合 + 按需构建** 的派生数据：

1. `HomeLocalIndexEngine`
   - 不持有 `PHAsset` 或每 asset 的 `LocalState`，只保存：月份分组、`assetID → month / mediaKind` 查表、`assetID → fingerprint` 镜像（与 `local_assets.assetFingerprint` 同步）、每月 `MonthAggregate`（assetCount / photoCount / videoCount / backedUpCount）、每月体积缓存
   - `backedUpCount` 在 `recomputeAggregates` 中按月与 `HomeRemoteIndexEngine` 月级 fingerprint 集对比得出（用 fingerprint 去重，以防多本地指向同一远端）
2. `HomeRemoteIndexEngine`
   - 消费 `RemoteLibrarySnapshotState`，只保留每月的 fingerprint 集合、`HomeMonthSummary`、当前 `snapshotRevision`
   - 不再缓存完整的 `remoteItemsByMonth` 或 fingerprint 引用计数；原始月 delta 需要时走 `BackupCoordinator.remoteMonthRawData(for:)` 实时取

月级 `matchedCount` 直接等同于 `HomeLocalIndexEngine` 的 `backedUpCount`；`remoteOnlyItems(for:)` 是一次 `async` 调用，纯 fingerprint-set 差集派生。

`HomeAlbumMatching.swift` 现已收窄为远端资源构造工具（`buildRemoteItems` + 代表资源选择），不再承担本地/远端合并；旧的 `mergeItems / LocalAlbumItem / HomeAlbumItem / ItemSourceTag` 均已移除。

### Home 模块的辅助控制器

以下文件都集中在 `Watermelon/Home/`，均为状态管理类小组件，没有 UI：

1. `HomeRefreshScheduler` — 把 reloadLocal / syncRemote / notifyConnection / notifyStructural 合批，避免连续触发刷新
2. `HomeScopeController` — 维护当前本地图库 scope（全部 / 指定相册），并标记 scope 重载中
3. `HomeScopeNormalizer` — 校验已选相册 ID 是否仍然存在、是否需要弹提示
4. `HomeSelectionController` — 月份选择状态（toggle、年级、双侧全选、连接变化清空）
5. `HomeFileSizeScanCoordinator` — 文件大小扫描的 task / 引用计数管理
6. `HomeDataProcessingWorker` — 串行处理队列 worker，持有两套引擎
7. `HomeSectionBuilder` — 年聚合、构建 `sections` 和 `rowLookup`
8. `HomeMenuFactory` — 构建左右 header 的 UIMenu（相册切换、profile 切换、设置等）
9. `HomePhotoAccessGate` — 缓存 / 监听 PhotoKit 授权状态
10. `HomeHeaderSummaryFormatter` — 把行集合聚合为 header 文本
11. `HomeLocalLibraryScope` — scope 类型定义
12. `PhotoKitChangeProvider` / `PhotoLibraryAbstraction` — `PHAsset` ↔ `LibraryAssetSnapshot` 转换层和跨 target 的纯类型

辅助 UI：

1. `LocalAlbumPickerViewController` — 多选相册作为 scope
2. `LocalAlbumDetailViewController` / `LocalAlbumGridSupport` — 单相册网格预览
3. `LocalIndexViewController` — 索引状态 / 重建入口
4. `DuplicatesViewController` — fingerprint 重复组浏览
5. `FocusModeViewController` — 执行态全屏遮罩
6. `ExecutionLogHistoryViewController` / `ExecutionLogEntryCell` — 历史日志列表
7. `HomeExecutionLogViewController` — 当前会话日志查看

### `HomeExecutionCoordinator`

职责：

1. 建立一次执行会话 `HomeExecutionSession`
2. 在执行前冻结 `上传并发` 与 `允许访问 iCloud 原件`
3. 调用 `LocalHashIndexBuildService.buildIndex(... allowNetworkAccess: false)` 做离线本地索引预检查；cache-hit 资产顺带做一次轻量可用性探测，以识别已被回收到 iCloud 的资产
4. 上传阶段通过 `BackupSessionAsyncBridge` 驱动 `BackupSessionController`
5. 同步月份在上传 flush 后通过 `onMonthUploaded` 内联进入下载收尾
6. 纯下载月份在上传阶段结束后顺序执行
7. 处理暂停、恢复、停止、连接丢失和失败告警

### `HomeExecutionSession`

职责：

1. 保存 `monthPlans`
2. 跟踪 `uploadMonths / downloadMonths / syncMonths`
3. 管理执行阶段 `ExecutionPhase`（`uploading / uploadPaused / downloading / downloadPaused / completed / failed(message)`）
4. 汇总 `assetCountByMonth / processedCountByMonth`
5. 维护上传阶段 remote sync 节流时间

## 4. 备份控制面

### `BackupSessionController`

职责：

1. 管理 `idle / running / paused / stopped / failed / completed`
2. 接收 `start / pause / stop / resume`
3. 通过 `BackupRunDriver` 启动或终止真正的备份任务
4. 汇总月度 started/completed、processed/failed 计数
5. 对外暴露 observer 友好的 `Snapshot`

说明：

1. 首页不会复用旧 controller；每次执行都会新建一份 `BackupSessionController`。
2. `BackupSessionAsyncBridge` 把控制器适配为 `async/await` 接口，给 `HomeExecutionCoordinator` 使用。
3. `BackupSessionReducer` 是控制器内部的状态机推导器。
4. `BackupResumePlanner` 负责从历史 monthPlan / manifest 推导可恢复的执行计划。

## 5. 备份执行面

### `BackupCoordinator`

聚合：

1. `BackupRunPreparationService`（位于 `BackupRunPreparation.swift`）
2. `BackupParallelExecutor`
3. `RemoteIndexSyncService`（位于 `Shared/Services/Backup/`）
4. `RemoteFormatCompatibilityService`（远端格式兼容性校验）

并对外暴露便捷接口：`reloadRemoteIndex(...)`、`remoteMonthRawData(for:)`、`currentRemoteSnapshotState(since:)`。

### `BackupRunPreparationService`（`BackupRunPreparation.swift`）

`prepareRun` 顺序：

1. 请求/校验相册权限
2. 创建并连接远端 client
3. `BackupV2RuntimeBuilder.build(...)` 通过 `RemoteFormatCompatibilityService.inspectRemoteFormat(...)` 路由 fresh / V1 / V2 / unsupported；前台允许 V1→V2 迁移，后台不允许
4. V2 路径建立专用 metadata client，并把 cold-start materialize 结果放进 `BackupV2RuntimeServices.initialMaterializeOutput`
5. `RemoteIndexSyncService.syncIndex(...)` 同步远端快照：V2 走 materialize（可消费预热结果），V1 继续扫 per-month manifest
6. 按 `full / scoped / retry` 加载资产并分组到 `monthAssetIDsByMonth`
7. 从本地 hash 索引读取每个 asset 的 `totalFileSizeBytes`，估算每月体积
8. 按 “预计字节数 → 数量 → 月份键” 顺序构建 `MonthPlan / MonthWorkItem`
9. 决定 worker 数
10. 决定连接池大小

### `BackupParallelExecutor`

职责：

1. 创建 `StorageClientPool`，并用预连接 client 预热
2. 用 `MonthWorkQueue`（定义在 `BackupMonthScheduler.swift` 中的 actor）动态分发月份
3. 每个 worker 逐月打开月份状态：V2 用 `V2MonthSession.loadOrCreate(...)`，V1 用 `MonthManifestStore.loadOrCreate(...)`
4. 分批读取 PHAsset（每批 500）
5. 调 `AssetProcessor.process(...)` 执行单 asset 上传
6. 每 10 个非 failed asset result 对当前 `BackupMonthStore` 做一次 batch flush；月末仍兜底 flush
7. V2 asset result 返回前已写 durable commit；每 10 个非 failed asset 的 flush 主要写 snapshot，防御性 commit delta 会先 publish 月快照；最终 flush 后先执行 `onMonthUploaded` 月级收尾，再按结果进入 `completed` / `downloadIncomplete` / fatal failure

### `AssetProcessor`（`AssetProcessor.swift` + `+Naming` + `+Upload`）

职责：

1. 规划资产资源（role/slot），通过 `BackupAssetResourcePlanner`
2. 优先复用本地 hash cache
3. 导出到临时文件并计算 SHA-256 digest
4. 检查 hash 已存在 / 文件名碰撞
5. 上传远端资源（最大重试 3 次）
6. 写本地 hash 索引
7. 写 `BackupMonthStore`（V2 session 或 V1 manifest store）
8. 增量更新共享远端快照缓存

### `BackupMonthStore` / `V2MonthSession` / `MonthManifestStore`（`Shared/Services/Backup/`）

文件：`BackupMonthStore.swift`、`V2MonthSession.swift`、`V2MonthIndexes.swift`、`V2MonthCommitFlusher.swift`、`V2MonthSnapshotFlusher.swift`、`MonthManifestStore.swift` + `+Loading.swift` + `+Schema.swift`

职责：

1. `BackupMonthStore` 是上传路径的统一协议，供 `AssetProcessor` 同时写 V2 session 与 V1 manifest store
2. `V2MonthSession` 是当前 V2 写入路径：单月 materialize + 真实目录 listing → in-memory indexes → commit jsonl + snapshot jsonl flush
3. `MonthManifestStore` 是 V1 兼容路径：管理月级 sqlite `resources / assets / asset_resources`，支持旧仓库读取、迁移与 V1 verify
4. 两条路径都会把真实远端目录文件集合叠加进月份状态，避免“文件已上传但 metadata 未 flush”的 orphan 造成重名冲突

### `RemoteIndexSyncService` / `RemoteLibrarySnapshotCache`（`Shared/Services/Backup/`）

1. `RemoteIndexSyncService` 先 inspect 远端格式：V2 走 `RepoMaterializer.materialize(expectedRepoID:)`，V1 扫描 `YYYY/MM/.watermelon_manifest.sqlite` 摘要并按 changed/removed months 增量下载
2. V2 同步还会刷新 physical-presence overlay；Home-facing committed fingerprints 只扣掉物理文件缺失的资源，per-asset commit 已移除 uncommitted-cache 层
3. `RemoteLibrarySnapshotCache` 维护内存态完整快照与 `revision`，向 Home 暴露 `currentState(since:)` / `monthRawData(for:)` 等接口

### `RemoteMaintenanceController`（`Watermelon/Services/Backup/`）

用户主动触发的远端月份校验控制器：

1. 通过 `BackupCoordinator.verifyAllMonths(...)` 跑跨月校验
2. 维护 `isVerifying / progress / lastError`
3. 校验运行时通过通知让 Home 把 `isSelectable` 关掉，避免与执行态冲突

### 其他执行辅助

1. `BackupMonthScheduler` — `MonthWorkQueue` 等动态调度结构
2. `BackupRunDriver` — 真正驱动 `runBackup(...)` 的胶水
3. `BackgroundBackupRunner` — 后台备份任务入口（与 `DependencyContainer.makeForBackgroundTask()` 配合）
4. `BackupCancellationController` / `BackupEvent` / `BackupEventStream` / `BackupScopeModels` / `StorageClientPool` / `RemoteFileNaming` / `RemoteFormatCompatibility` 均位于 `Shared/Services/Backup/`

## 6. 存储抽象

统一协议：`RemoteStorageClientProtocol`（声明在 `Shared/Services/SMB/SMBClientProtocol.swift`）

核心接口：

1. `connect / disconnect`
2. `storageCapacity`
3. `list / metadata / exists`
4. `upload / download / move / copy / delete / createDirectory`
5. `setModificationDate`
6. `atomicCreate / moveIfAbsent` — 原子写入对，与 `MetadataCreateGate` 配合做 commit/snapshot/repo.json/version.json 的安全落盘

能力位（ADT，调用方按值分支，不查 backend 类型）：

- `atomicCreateGuarantee(forFileSize:remotePath:)` —— 直接 `atomicCreate` 返回 `.exclusive` 还是 `.overwritePossible`。S3 在 multipart 阈值以下是 `.exclusive`（`If-None-Match: *` PUT），multipart 是 `.overwritePossible`
- `moveIfAbsentGuarantee` —— `moveIfAbsent` 默认能力。LocalVolume / SMB / S3-conditional 是 `.exclusive`；WebDAV / SFTP 是 `.overwritePossible`
- `supportsExclusiveMoveIfAbsent(forDestinationPath:)` async —— runtime probe，覆盖静态 `moveIfAbsentGuarantee`。S3 forward 到 `conditionalCopyIfAbsentSupported()`（首次跑探测、actor 缓存）；WebDAV forward 到 `Overwrite: F` 探测；其它 backend 默认 `(moveIfAbsentGuarantee == .exclusive)`
- `dataPathOverwriteRisk` —— `.perKey`（S3 / WebDAV / SMB）vs `.none`（LocalVolume / SFTP），上传路径决定是否强加 `~widN` 后缀
- `supportsLivenessSafeOverwriteUpload` —— `client.upload` 到既有路径是否可作为 liveness 心跳续期（既不丢旧又不留空）。LocalVolume / S3 为 `true`；SMB / WebDAV / SFTP 为 `false`。默认保守为 `false`，不耦合 `dataPathOverwriteRisk`。与 `supportsLivenessSafeOverwriteMove` 共同决定派生的 `supportsLivenessSafeRenewal`，`LivenessTracker` Phase D 直接消费 upload 原子
- `supportsLivenessSafeOverwriteMove` —— `client.move` 到既有路径是否在目的地做原子替换（同伴永不观察到路径缺失）。LocalVolume（POSIX `rename(2)`）/ S3（CopyObject + DeleteObject，目的地按对象原子替换）/ WebDAV（RFC 4918 `MOVE` + `Overwrite: T`，主流实现走 `rename(2)`）为 `true`；SMB（libsmb2 `smb2_rename_async` 硬编码 `replace_if_exist=0`）/ SFTP（v3 `rename` 拒绝既有目的地）为 `false`。无默认 —— 协议强制每个后端显式声明
- `supportsLivenessSafeRenewal` —— 派生自 `supportsLivenessSafeOverwriteUpload || supportsLivenessSafeOverwriteMove`：至少一条续期路径安全则为 `true`。SMB 与 SFTP 两路均不安全，因此为 `false`；为 `false` 时 `BackupV2RuntimeBuilder` 必须跳过 orphan sweep（心跳一旦失活，同伴会把我们的 writerID 判为陈旧，进而删除正在写入的 staging）。`BackupV2RuntimeBuilder` 消费
- `readAfterWriteGraceSeconds` —— 共享 read-after-write 容忍预算，被 `MetadataCreateGate.metadataReadAfterWriteDeadline` 与 `LivenessTracker.snapshotPeerStatuses` 共同消费。S3 / WebDAV `30`（R2 / MinIO / B2 / CDN 反代场景）；其他后端 `0`
- `backendNameCaseSensitivity` —— `.caseSensitive` / `.caseInsensitive` / `.unknown`，`presenceKey(for:)` extension 折叠
- `concurrencyMode` —— `.concurrent` vs `.serialOnly`（SMB / SFTP），`SerialOperationsClient` 包裹

新增能力位必须按其实际承载的语义命名（原语 vs coordination 安全），避免出现"原语命名 + coordination 取值"的错位。

`MetadataCreateGate.createWithStagingFallback` 消费 `atomicCreateGuarantee` / `moveIfAbsentGuarantee` / `dataPathOverwriteRisk` / `readAfterWriteGraceSeconds`：先看 `atomicCreateGuarantee`（`.exclusive` → 直接 atomicCreate；`.overwritePossible` → UUID staging + 验证 + move），再看 `moveIfAbsentGuarantee` + runtime probe 决定 finalization 路径（exclusive moveIfAbsent vs `bestEffortCopyIfAbsent` 兜底 vs 抛 `nonExclusiveFinalization`）。liveness 能力原子和派生 composite 不在 gate 路径上，由 `LivenessTracker` / `BackupV2RuntimeBuilder` 各自消费。

协议扩展默认提供 `shouldSetModificationDate / shouldLimitUploadRetries / directReadURL / disconnectSafely / supportsExclusiveMoveIfAbsent`（默认 `moveIfAbsentGuarantee == .exclusive`）。

当前实现：

1. `AMSMB2Client`（`Shared/Services/SMB/`）
2. `WebDAVClient`（`Shared/Services/Storage/`）
3. `LocalVolumeClient`（`Shared/Services/Storage/`）
4. `S3Client`（`Shared/Services/Storage/`）— actor，使用纯 Swift 的 SigV4 签名；支持 multipart upload（默认 8 MiB part、4 路并发、上限 10000 part）、server-side copy；`setModificationDate` 是空实现（对象存储无法修改 mtime），但仍然返回 `shouldSetModificationDate = true` 以便和其他客户端共用上传分支
5. `SFTPClient`（`Shared/Services/Storage/`）— actor，基于 Citadel 0.12.1（其传递依赖 `swift-nio-ssh` 来自 `Wellz26/swift-nio-ssh` fork）。两阶段 TOFU：`captureHostKeyFingerprint` 在 host-key validation 阶段 abort 取指纹，凭证不会经过未确认的连接；`verifyBasePathWritable` 用钉住的指纹做真正的连接 + 写探针。每个 worker 一个独立 SSH 会话；密钥支持 ed25519 / RSA（其它类型抛 `SFTPUnsupportedKeyTypeError`）。`list` 调用满 32 次后整体重连一次以释放 Citadel 0.12.1 的服务端句柄泄漏。SFTP v3 没有原生 server-side copy，`copy()` 走本地 download + upload。

创建入口：

1. `StorageClientFactory.makeClient(profile:password:)`（`Shared/Services/Storage/`）

辅助：

1. `WebDAVErrorClassifier` — WebDAV 错误归一化
2. `SecurityScopedBookmarkStore` — 外接存储 bookmark 持久化
3. `SMBSetupService` — SMB 连接 / 凭据准备
4. `S3SigV4Signer` — 纯 Swift 实现的 AWS SigV4 签名
5. `S3ProfileVerifier` — 添加 S3 profile 时的 connect + write-probe 校验
6. `S3ErrorClassifier` — S3 / URLError 到用户面文案 + `isConnectionUnavailable` 谓词的归一化
7. `SFTPErrorClassifier` — Citadel `SFTPError` / `SSHClientError` / `AuthenticationFailed` / `NIOConnectionError` / POSIX domain 到用户面文案的归一化；`SFTPHostKeyMismatchError` 与 `SFTPUnsupportedKeyTypeError` 都走 `LocalizedError` 默认通道。`SFTPClient.verifyBasePathWritable` 在添加 / 编辑 SFTP profile 时做 connect + mkdir + write-probe + delete。

## 7. 数据层

### 本地持久化

1. `DatabaseManager` 使用 GRDB；目前注册了四条迁移：
   - `v1_initial`：建 `server_profiles / sync_state / local_assets / local_asset_resources`
   - `v2_ms_timestamps`：把 `local_assets.modificationDateNs` 重命名为 `modificationDateMs` 并把已有值除以 1_000_000
   - `v3_repo_state`：新增 `server_profiles.writerID` 与 `repo_state(profileID, repoID, writerID, lastClock, lastSeq, migrationCompleted)`
   - `v4_selection_version`：新增 `local_assets.selectionVersion / resourceSignature`，用于 hash cache 资源选择规则失效判断

本地主要表：

1. `server_profiles`
2. `sync_state`
3. `local_assets`
4. `local_asset_resources`
5. `repo_state`

### 远端持久化

1. V2 主路径：`.watermelon/version.json`、identity / repo metadata、`commits/`、`snapshots/`、`liveness/`、`migrations/`
2. V1 兼容路径：每个月一个 `.watermelon_manifest.sqlite`，`MonthManifestStore` 迁移名 `month_manifest_v1_initial`

### 会话态

1. `AppSession` 保存当前激活 profile 与密码
2. 密码持久化在 Keychain，不进入 SQLite

## 8. 日志

`Shared/Services/Logging/`：

1. `ExecutionLogEntry` — 单条日志数据结构
2. `ExecutionLogFileStore` — 持久化到磁盘
3. `ExecutionLogSessionInfo` — 一次执行会话的元信息
4. `ExecutionLogSessionWriter` — 写入 / 滚动 session 日志
5. `ExecutionLogPalette` — 渲染颜色

`Watermelon/Home/` 下的 `HomeExecutionLogViewController`、`ExecutionLogHistoryViewController`、`ExecutionLogEntryCell` 是消费端。

## 9. 更多页 / 设置

入口：

1. 首页右下角悬浮 `ellipsis` 按钮

自定义段落（`WatermelonMoreDataSource`）：

1. `通用` → 系统语言入口
2. `远端存储` → `管理存储`
3. `备份` → `上传并发` / `允许访问 iCloud 原件`
4. `后台备份` → 后台备份入口（Pro）与后台节点计数入口
5. `画中画进度` → `画中画进度`（Pro），开启后再露出 `画中画提示音`
6. `诊断` → `执行日志历史`（DEBUG 构建额外露出 `Test Crash`）

再叠加 MoreKit 自带的 `membership / contact / appjun / about` 段落。

## 10. 自动化测试

`WatermelonTests/` 覆盖 Home 纯逻辑、Repo/V2 元数据、存储能力和部分 restore/download 边界：

1. `EngineTests` / `RemoteIndexEngineTests` — 本地 / 远端索引引擎
2. `WorkerTests` — `HomeDataProcessingWorker`
3. `RefreshSchedulerTests` — `HomeRefreshScheduler`
4. `ScopeControllerTests` / `ScopeNormalizerTests` — 本地图库 scope
5. `SelectionControllerTests` — 月份选择
6. `SectionBuilderTests` — 年/月分组
7. `HeaderSummaryFormatterTests` — header 汇总格式化
8. `RemoteFileNamingTests` — 远端命名
9. `S3SigV4SignerTests` — 用 AWS 官方测试向量验证 SigV4 canonical request / signature
10. `S3ClientTests` — S3 client 的 request 构造（multipart 分片、key 编码、retry 分类）
11. `SFTPCredentialBlobTests` — `SFTPCredentialBlob` 与 `SFTPConnectionParams` 的 JSON round-trip
12. `SFTPErrorClassifierTests` — `SFTPErrorClassifier.isConnectionUnavailable` 表驱动覆盖（Citadel / NIO 类型未链接到测试 target，POSIX domain 与本地错误类型为主）
13. `RepoMaterializerRoundTripTests` / `V2FlushTests` / `BackupV2RuntimeBuilderTests` / `BootstrapStateMachineTests` — V2 repo materialize、batch flush、bootstrap / migration 状态机
14. `RepoVerifyMonthServiceTests` / `RemoteIndexSyncServiceTests` / `RemoteResourcePresenceTests` / `StorageCapabilityMatrixTests` — verify、remote index、presence overlay、backend capability contract
15. `RestoreServiceFallbackTests` / `RestoredAssetFingerprintVerifierTests` — 下载 fallback 与 durable fingerprint 校验
16. `TestSupport.swift` — 共享 fixture（确定性日期、样例记录）

仍主要依赖真机回归：`HomeExecutionCoordinator` 到真实 PhotoKit / 真实远端的端到端执行、连接切换、暂停恢复、sync 月份内联下载、外接存储拔出、`ProfileReachabilityService` 网络探测。

## 11. macOS Target（`WatermelonMac/`）

定位：当前主要是 **遗留数据迁移工具 + 远端 profile 管理**，不运行 iOS 备份链路。

入口：

1. `WatermelonMacApp.swift` / `RootView.swift` / `AppDelegate.swift` / `MacDependencyContainer.swift`

主要模块：

1. `Services/Legacy/`：旧库扫描、`DHashComputer`、`PerceptualHashCache`、`LegacyMigrationPlanner` / `Executor`、`MediaTimestampReader` 等遗留导入流水线
2. `UI/Migration/`：遗留导入相关 SwiftUI 视图
3. `UI/Profiles/`：profile 列表、添加 SMB / WebDAV / S3 / 本地、SMB 发现 / share picker（macOS 端目前未提供 SFTP 添加界面，只能在 iOS 端创建后通过共享数据库读取）
4. `UI/Common/`：通用 SwiftUI 组件（密码 prompt、字符串 trim 等）

它共享 `Shared/` 里的存储客户端、Keychain 与领域模型，但不持有 `BackupCoordinator`。
