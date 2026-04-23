# 架构与关键组件（当前）

## 1. App 入口

1. `SceneDelegate` 创建 `AppCoordinator`
2. `AppCoordinator.start()` 直接把 `HomeViewController` 设为 `window.rootViewController`
3. 当前不是 TabBar，也不是全局 `UINavigationController` 根结构

说明：

1. 首页上的更多页会按当前上下文 `push`，若没有导航栈则自己包一层 `UINavigationController` 再 present。

## 2. DependencyContainer

`DependencyContainer` 当前提供：

1. `DatabaseManager`
2. `KeychainService`
3. `AppSession`
4. `StorageClientFactory`
5. `PhotoLibraryService`
6. `ContentHashIndexRepository`
7. `LocalHashIndexBuildService`
8. `BackupCoordinator`
9. `RestoreService`

说明：

1. `AppSession` 保存当前激活 profile 和会话内密码。
2. `LocalHashIndexBuildService` 既供更多页索引管理器使用，也供首页执行前的本地索引预检查使用。

## 3. Home 模块

### `HomeViewController`

职责：

1. 构建 UI：顶部本地/远端 header、月份列表、底部 `SelectionActionPanel`、右侧远端 overlay、右下角更多按钮
2. 配置 diffable data source 和按年 section 的 supplementary views
3. 只做渲染与交互分发，不直接承载首页业务状态

### `HomeScreenStore`

职责：

1. 聚合 `HomeIncrementalDataManager`、`HomeConnectionController`、`HomeExecutionCoordinator`
2. 维护首页渲染所需状态：`sections`、`rowLookup`、`selection`
3. 对外暴露 `connectionState` 与 `executionState`
4. 把内部状态变化统一映射为 `HomeChangeKind`
5. 串行合并 `reloadLocal / syncRemote / notifyConnection / notifyStructural` 刷新任务

### `HomeConnectionController`

职责：

1. 加载已保存远端 profile
2. 从 `sync_state.active_server_profile_id` 和 Keychain 自动恢复连接
3. 处理密码输入、切换 profile、断开连接
4. 调用 `BackupCoordinator.reloadRemoteIndex(...)` 重建共享远端快照
5. 连接失败时，如旧连接仍然有效，则尝试恢复旧连接对应的远端索引

### `HomeIncrementalDataManager`

职责：

1. 维护本地图库索引、远端快照索引；**不再**缓存整库的 reconcile 结果，也不再做三元 (`.localOnly / .remoteOnly / .both`) 合并，按月匹配以月聚合的 `backedUpCount` 和 on-demand 构建的 `remoteOnlyItems` 取代
2. 对外提供 `monthRow / allMonthRows / localAssetIDs / remoteOnlyItems / matchedCount`（`remoteOnlyItems(for:)` 已改为 `async`）
3. 注册 `PHPhotoLibraryChangeObserver`
4. 在处理队列上执行索引重建、远端增量同步；`remoteOnlyItems` 在单次处理队列 hop 里完成远端原始 delta 抓取、`HomeAlbumMatching.buildRemoteItems` 构建远端 items、以及本月本地 fingerprint 集合 snapshot，queue 外只做 fingerprint-set 差集过滤和按创建时间升序排序；全程不再走 `PHAsset` / `local_asset_resources` fetch
5. 在主 actor 上调度文件大小扫描，并通过 `Task.yield()` 分摊大图库扫描成本；启动全量扫描与 `PHChange` 触发的增量 rescan 分两条 Task 各自跑（rescan 用 pending 集合合批），共用一个 refcount 避免 size snapshot 被提前释放

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
3. 管理执行阶段 `ExecutionPhase`
4. 汇总 `assetCountByMonth / processedCountByMonth`
5. 维护上传阶段 remote sync 节流时间

## 4. Home 数据引擎

`HomeLibraryEngines.swift` 中的两个引擎被 `HomeIncrementalDataManager` 封装。此前的 `HomeReconcileEngine` 已被移除，reconcile 变成 **按月聚合 + 按需构建** 的派生数据：

1. `HomeLocalIndexEngine`
   - 不再持有 `PHAsset` 或每 asset 的 `LocalState`，只保存：月份分组、`assetID → month / mediaKind` 查表、`assetID → fingerprint` 镜像（与 `local_assets.assetFingerprint` 同步）、每月 `MonthAggregate`（assetCount / photoCount / videoCount / backedUpCount）、每月体积缓存
   - `backedUpCount` 在 `recomputeAggregates` 中按月与 `HomeRemoteIndexEngine` 月级 fingerprint 集对比得出（用 fingerprint 去重，以防多本地指向同一远端）
2. `HomeRemoteIndexEngine`
   - 消费 `RemoteLibrarySnapshotState`，只保留每月的 fingerprint 集合和 `HomeMonthSummary`（在 `resolveMonth` 里按 `buildRemoteItems` 的相同资源可解析规则计算 assetCount / photoCount / videoCount / totalSize）
   - 不再缓存完整的 `remoteItemsByMonth` 或 fingerprint 引用计数；原始月 delta 需要时走 `RemoteLibrarySnapshotCache.monthRawData(for:)` 实时取

月级 `matchedCount` 直接等同于 `HomeLocalIndexEngine` 的 `backedUpCount`；`remoteOnlyItems(for:)` 是一次 `async` 调用，纯 fingerprint-set 差集派生——在单次处理队列 hop 内抓齐远端 items 与本地月 fingerprint 集合，queue 外只做差集过滤 + 排序，已不再构造 `LocalAlbumItem` / `HomeAlbumItem` 或触发 `PHAsset` fetch。

`HomeAlbumMatching.swift` 现已收窄为远端资源构造工具（`buildRemoteItems` + 代表资源选择），不再承担本地/远端合并；旧的 `mergeItems / LocalAlbumItem / HomeAlbumItem / ItemSourceTag` 均已移除。

## 5. 备份控制面

### `BackupSessionController`

职责：

1. 管理 `idle / running / paused / stopped / failed / completed`
2. 接收 `start / pause / stop / resume`
3. 通过 `BackupRunDriver` 启动或终止真正的备份任务
4. 汇总月度 started/completed、processed/failed 计数
5. 对外暴露 observer 友好的 `Snapshot`

说明：

1. 首页不会复用旧 controller；每次执行都会新建一份 `BackupSessionController`。

## 6. 备份执行面

### `BackupCoordinator`

职责：

1. 聚合 `BackupRunPreparationService`
2. 聚合 `BackupParallelExecutor`
3. 聚合 `RemoteIndexSyncService`

### `BackupRunPreparationService`

职责：

1. 请求相册权限
2. 创建并连接远端 client
3. 保证 `basePath` 存在
4. 扫描远端 manifest，构建 `RemoteLibrarySnapshot`
5. 在快照较小时构建 `MonthSeedLookup`
6. 按月份生成 `MonthWorkItem`
7. 从本地 hash 索引估算每个月体积
8. 决定 worker 数和连接池大小

### `BackupParallelExecutor`

职责：

1. 创建 `StorageClientPool`
2. 用 `MonthWorkQueue` 动态调度月份
3. 每个 worker 逐月加载 `MonthManifestStore`
4. 分批读取 `PHAsset` 与本地 hash cache
5. 调 `AssetProcessor.process(...)` 执行单 asset 上传
6. 月份结束后 flush manifest
7. 在 flush 完成后执行 `onMonthUploaded` 月级收尾

### `AssetProcessor`

职责：

1. 规划资产资源（role/slot）
2. 优先复用本地 hash cache
3. 导出到临时文件并计算 digest
4. 检查 hash 已存在 / 文件名碰撞
5. 上传远端资源
6. 写本地 hash 索引
7. 写月 manifest
8. 增量更新共享远端快照缓存

### `MonthManifestStore`

职责：

1. 管理月级 sqlite：`resources / assets / asset_resources`
2. 支持直接下载现有 manifest 或从 snapshot seed 初始化
3. 记录真实远端目录文件集合，检测“文件已上传但 manifest 未 flush”的孤儿文件
4. 通过临时文件 + move 原子刷新远端 manifest

### `RemoteIndexSyncService`

职责：

1. 扫描远端 `YYYY/MM/.watermelon_manifest.sqlite` 摘要
2. 对比上次 digest，找出 changed / removed months
3. 仅重新下载变化月份的 manifest
4. 写入 `RemoteLibrarySnapshotCache`
5. 向 Home 暴露 `currentState(since:)`

## 7. 存储抽象

统一协议：`RemoteStorageClientProtocol`

核心接口：

1. `connect / disconnect`
2. `list / metadata / exists`
3. `upload / download / move / delete / createDirectory`
4. `setModificationDate`
5. `storageCapacity`

当前实现：

1. `AMSMB2Client`
2. `WebDAVClient`
3. `LocalVolumeClient`

创建入口：

1. `StorageClientFactory.makeClient(profile:password:)`

## 8. 数据层

### 本地持久化

1. `DatabaseManager` 使用 GRDB，迁移：`v1_initial`

本地主要表：

1. `server_profiles`
2. `sync_state`
3. `local_assets`
4. `local_asset_resources`

### 远端持久化

1. 每个月一个 `.watermelon_manifest.sqlite`
2. `MonthManifestStore` 迁移名：`month_manifest_v1_initial`

### 会话态

1. `AppSession` 保存当前激活 profile 与密码
2. 密码持久化在 Keychain，不进入 SQLite

## 9. 更多页 / 设置

入口：

1. 首页右下角悬浮 `ellipsis` 按钮

自定义段落（`WatermelonMoreDataSource`）：

1. 通用：系统语言入口
2. 远端存储：`管理存储`
3. 备份：`上传并发` / `允许访问 iCloud 原件` / `后台备份`（Pro） / `画中画进度`（Pro）
4. 诊断：`执行日志历史`（`ExecutionLogHistoryViewController`）

再叠加 MoreKit 自带的 `membership / contact / appjun / about` 段落。
