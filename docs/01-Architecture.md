# 架构与关键组件（当前）

## 1. App 入口

1. `SceneDelegate` -> `AppCoordinator.start()`
2. `AppCoordinator` 当前只路由到 `HomeViewController`
3. 非 TabBar 结构，根是单 `UINavigationController`

## 2. DependencyContainer

`DependencyContainer` 提供：

1. `DatabaseManager`
2. `KeychainService`
3. `AppSession`
4. `StorageClientFactory`
5. `PhotoLibraryService`
6. `MetadataService`
7. `ContentHashIndexRepositoryProtocol`（实际实现 `ContentHashIndexRepository`）
8. `BackupCoordinatorProtocol`（实际实现 `BackupCoordinator`）
9. `RestoreService`

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

### `BackupRunCommandActor`

职责：

1. 接收 `startRun / resumeRun / requestPause / requestStop`
2. 管理单 run 生命周期（run token、active task、intent）
3. 为每次 run 创建独立 `BackupEventStream`
4. 监听事件并回传 `BackupEngineSignal` 给 `BackupSessionController`
5. 处理“等待前一 run 清理完成后再启动新 run”的串行化逻辑

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

1. 基于 `RemoteManifestIndexScanner` 扫描摘要
2. 比较上次 digest，增量替换/移除月份
3. 驱动 `RemoteLibrarySnapshotCache` 更新

实现说明：

1. 当前是 `final class`，内部可变状态下沉到私有 actor `MutableState`。

## 6. Home / More / Backup UI 层

### Home

1. 连接菜单：当前连接 + 添加存储 + 更多
2. 内容区：本地/远端匹配结果按年月 section
3. 运行中节流刷新远端 section；run 结束后触发一次全量刷新

### More

1. `远端存储`：进入 `ManageStorageProfilesViewController`
2. `本地数据`：进入 `LocalHashIndexManagerViewController`
3. `备份`：设置上传并发（automatic/1/2/3/4）

### Backup

1. 顶部范围卡片 + 调整按钮
2. 状态卡片支持多 worker 切换查看
3. 列表过滤（全部/成功/失败/跳过/日志）
