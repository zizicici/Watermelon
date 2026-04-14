# 架构与关键组件（当前）

## 1. App 入口

1. `SceneDelegate` -> `AppCoordinator.start()`
2. `AppCoordinator` 当前只路由到 `NewHomeViewController`
3. 非 TabBar 结构，根是单 `UINavigationController`

## 2. DependencyContainer

`DependencyContainer` 提供：

1. `DatabaseManager`
2. `KeychainService`
3. `AppSession`
4. `StorageClientFactory`
5. `PhotoLibraryService`
6. `ContentHashIndexRepository`
7. `BackupCoordinator`
8. `RestoreService`

说明：

1. `AppSession` 保存当前激活的 profile 与内存态密码。
2. SMB/WebDAV 需要密码；外接存储不需要密码（bookmark 在 `connectionParams`）。

## 3. 存储协议层

统一接口：`RemoteStorageClientProtocol`

核心方法：

1. `connect / disconnect`
2. `storageCapacity`
3. `list / metadata / exists`
4. `upload / download / move / delete / createDirectory`
5. `setModificationDate`

实现：

1. `AMSMB2Client`（SMB）
2. `WebDAVClient`（WebDAV）
3. `LocalVolumeClient`（外接存储目录）

创建入口：

1. `StorageClientFactory.makeClient(profile:password:)`

## 4. 备份控制面

### `BackupSessionController`（`@MainActor`）

职责：

1. 管理 UI 状态（idle/running/paused/stopped/failed/completed）
2. 聚合日志、进度、失败项、最近处理项
3. 管理范围选择状态 `BackupScopeSelection`
4. 对外暴露观察快照 `Snapshot`
5. 管理单 run 生命周期（run token、runTask/eventListenerTask、termination intent）
6. 为每次 run 创建独立 `BackupEventStream`，直接处理 `BackupEvent`
7. 处理控制命令（start/pause/stop/resume）与协作取消

## 5. 备份执行面

### `BackupCoordinator`

职责：

1. 权限检查与连接建立
2. `RemoteIndexSyncService.syncIndex` 远端索引同步
3. 加载本地 hash 缓存
4. 按月份分桶、排序并并发调度 worker
5. 通过 `StorageClientPool` 管理并发连接
6. 聚合进度与阶段耗时日志

### `AssetProcessor`

职责：

1. 单 Asset 资源导出 + hash
2. 本地缓存命中快速跳过
3. 资源上传与碰名处理
4. 写月 manifest 与本地 hash 索引
5. 推送远端快照缓存增量

### `MonthManifestStore`

职责：

1. 管理月级 sqlite（resources/assets/asset_resources）
2. 维护内存索引与远端文件名集合
3. flush 到远端（临时文件 + move/replace）

### `RemoteIndexSyncService`

职责：

1. 直接扫描远端年/月目录的 manifest 摘要（大小、修改时间）
2. 比较上次 digest，增量替换/移除月份
3. 驱动 `RemoteLibrarySnapshotCache` 更新

实现说明：

1. 当前是 `final class`，内部可变状态下沉到私有 actor `MutableState`。

## 6. Home UI 层

### NewHomeViewController

1. 左右双栏布局：左侧"本地相册"、右侧"远端存储"（下拉菜单切换连接）
2. 按年-月 section 展示，每月一行左右各一个 cell
3. 支持月份多选，箭头方向：只选本地→上传(→)、只选远端→下载(←)、两侧都选→同步(↔)
4. 箭头旁显示进度百分比（基于 reconciliation `matchedCount`）

### HomeLibraryEngines

三层数据引擎（`HomeIncrementalDataManager` 聚合）：
1. `HomeLocalIndexEngine`：本地 PHAsset 索引，按月分组，跟踪 fingerprint/hash/isBackedUp
2. `HomeRemoteIndexEngine`：远端快照索引，应用月级 delta，维护全局 fingerprint 引用计数
3. `HomeReconcileEngine`：合并本地/远端为 both/localOnly/remoteOnly，暴露 `matchedCount(for:)`

### 执行模式

1. **上传阶段**：每次执行新建 `BackupSessionController`，驱动 `BackupCoordinator.runBackup`
2. **下载阶段**：逐月 `ensureHashIndexAndDownload`（先跑 scoped backup 填充 hash → 刷新 local index → 下载 remoteOnly）
3. 下载逐 item 写入 hash index（`writeHashIndexForItem` + `refreshLocalIndex`），中断后可续
4. 进度：上传用 `max(sessionPercent, reconciliation baseline)`；下载和同步用纯 reconciliation
5. 停止：上传 → `stopBackup()`；下载 → cancel Task + `stopBackup()` + `Task.checkCancellation`
