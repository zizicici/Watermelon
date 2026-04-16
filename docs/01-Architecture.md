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
2. `LocalHashIndexBuildService` 既供更多页索引管理器使用，也供首页执行前预检查使用。

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

1. 维护本地图库索引、远端快照索引、reconcile 结果
2. 对外提供 `monthRow / allMonthRows / localAssetIDs / remoteOnlyItems / matchedCount`
3. 注册 `PHPhotoLibraryChangeObserver`
4. 在处理队列上执行索引重建、远端增量同步与 reconcile
5. 在主 actor 上调度文件大小扫描，并通过 `Task.yield()` 分摊大图库扫描成本

### `HomeExecutionCoordinator`

职责：

1. 建立一次执行会话 `HomeExecutionSession`
2. 在执行前调用 `LocalHashIndexBuildService` 做本地索引预检查
3. 上传阶段通过 `BackupSessionAsyncBridge` 驱动 `BackupSessionController`
4. 同步月份在上传 flush 后通过 `onMonthUploaded` 内联进入下载收尾
5. 纯下载月份在上传阶段结束后顺序执行
6. 处理暂停、恢复、停止、连接丢失和失败告警

### `HomeExecutionSession`

职责：

1. 保存 `monthPlans`
2. 跟踪 `uploadMonths / downloadMonths / syncMonths`
3. 管理执行阶段 `ExecutionPhase`
4. 汇总 `assetCountByMonth / processedCountByMonth`
5. 维护上传阶段 remote sync 节流时间

## 4. Home 数据引擎

`HomeLibraryEngines.swift` 中的三层模型仍然存在，但已被 `HomeIncrementalDataManager` 封装：

1. `HomeLocalIndexEngine`
   - 本地 `PHAsset` 索引
   - 维护月份分组、fingerprint、hash、是否已备份、媒体类型、体积缓存
2. `HomeRemoteIndexEngine`
   - 消费 `RemoteLibrarySnapshotState`
   - 维护远端月份项、fingerprint 引用计数、snapshot revision
3. `HomeReconcileEngine`
   - 生成 `.localOnly / .remoteOnly / .both`
   - 计算每个月的 `matchedCount`

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

1. `DatabaseManager` 使用 GRDB，当前迁移包含：
2. `v7_dev_schema_reset`
3. `v8_server_profiles_smb_identity`

本地主要表：

1. `server_profiles`
2. `sync_state`
3. `local_assets`
4. `local_asset_resources`

### 远端持久化

1. 每个月一个 `.watermelon_manifest.sqlite`
2. `MonthManifestStore` 迁移名：`month_manifest_v3_dev_schema_reset`

### 会话态

1. `AppSession` 保存当前激活 profile 与密码
2. 密码持久化在 Keychain，不进入 SQLite

## 9. 更多页 / 设置

入口：

1. 首页右下角悬浮 `ellipsis` 按钮

内容：

1. `管理存储`
2. `本地 Hash 索引`
3. `上传并发`
4. 系统语言入口与 MoreKit 通用页内容
