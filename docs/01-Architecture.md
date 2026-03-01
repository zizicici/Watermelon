# 架构与关键组件

## 1. 应用入口

入口：`SceneDelegate -> AppCoordinator.start()`。

当前只有一个主入口路由：

1. `showHome()` -> `HomeViewController`

不使用 TabBar。

## 2. 依赖注入（DependencyContainer）

`DependencyContainer` 当前持有：

1. `DatabaseManager`
2. `KeychainService`
3. `AppSession`
4. `StorageClientFactory`
5. `PhotoLibraryService`
6. `MetadataService`
7. `BackupCoordinator`
8. `RestoreService`

`AppSession` 保存当前活跃 `ServerProfileRecord` 和内存态密码（SMB/WebDAV 需要，外接存储为空串）。

## 3. Services 分工

### Storage / 连接协议层

1. `RemoteStorageClientProtocol`：统一远端文件系统接口（SMB/WebDAV/外接存储共用）。
2. `StorageClientFactory`：按 `ServerProfileRecord.storageType` 构建具体 client。
3. `AMSMB2Client`：SMB client 实现。
4. `WebDAVClient`：WebDAV client 实现。
5. `LocalVolumeClient`：外接存储目录实现（security-scoped bookmark）。
6. `SMBSetupService`：添加/编辑 SMB 时枚举 share 与路径。
7. `SMBDiscoveryService`：Bonjour 发现 `_smb._tcp`。
8. `RemoteThumbnailService`：远端文件下载 + 本地下采样（actor 限流）。

### Backup

1. `BackupCoordinator`：无全局运行状态的备份执行器（按 Asset 主循环）。
2. `BackupCancellationController`：run 级别取消控制器。
3. `AssetProcessor`：单 Asset 资源导出、hash、上传、重试、manifest 写入。
4. `BackupAssetResourcePlanner`：资源排序、role/slot 分配、assetFingerprint 计算。
5. `MonthManifestStore`：月 manifest sqlite 读写 + flush。
6. `RemoteManifestIndexScanner`：只读扫描 `YYYY/MM` 下 manifest 摘要。
7. `RemoteIndexSyncService`：按月摘要增量刷新远端快照。
8. `RemoteLibrarySnapshotCache`：内存快照缓存。
9. `ContentHashIndexRepository`：本地 `local_assets` / `local_asset_resources` 读写。
10. `RemoteNameCollisionResolver`：文件名冲突 `_n` 递增。

### Photo / Restore / Metadata

1. `PhotoLibraryService`：照片权限、`PHAsset` 查询、`PHAssetResource` 导出。
2. `RestoreService`：远端文件下载后写回系统相册。
3. `MetadataService`：文件元信息辅助。

## 4. Backup 控制面与执行面

### 控制面（UI/Backup）

1. `BackupEngineActor`（定义在 `BackupRunCommandActor.swift`）：
2. 唯一负责 `start/pause/stop/resume` 命令编排。
3. 每次 run 独立创建 `BackupEventStream + BackupCancellationController`。
4. 维护 run token，避免跨 run 事件污染。
5. `BackupSessionController`：仅负责 UI 状态聚合与展示，不直接驱动底层执行细节。

### 执行面（Services/Backup）

1. `BackupCoordinator.runBackup(..., context: BackupRunContext)` 接收 run 级上下文执行备份。
2. `BackupRunContext` 包含 `eventSink` 与 `cancellationController`。
3. `BackupCoordinator` 不再暴露全局 `eventStream` / `cancelActiveBackup()`。

## 5. UI 结构

### 主页面

1. `HomeViewController`：统一展示本地与远端匹配结果。
2. 远端匹配算法在 `HomeAlbumMatching`（与视图解耦）。
3. `AlbumGridCell`、`AlbumSectionHeaderView` 为 Home 网格子组件。

### 备份状态

1. `BackupSessionController`：状态机、日志/进度聚合、失败项重试入口。
2. `BackupViewController`：开始/暂停/停止、结果筛选、日志展示。

### 存储配置与管理

1. `AddSMBServerLoginViewController`
2. `SMBSharePathPickerViewController`
3. `AddSMBServerViewController`
4. `AddWebDAVStorageViewController`
5. `AddExternalStorageViewController`
6. `ManageStorageProfilesViewController`

这些链路由 Home 右上角连接菜单触发。

## 6. 当前主链路文件建议

后续改动优先集中：

1. `Watermelon/Home/HomeViewController.swift`
2. `Watermelon/UI/Backup/BackupRunCommandActor.swift`
3. `Watermelon/UI/Backup/BackupSessionController.swift`
4. `Watermelon/Services/Backup/BackupCoordinator.swift`
5. `Watermelon/Services/Backup/AssetProcessor.swift`
6. `Watermelon/Services/Backup/MonthManifestStore.swift`
7. `Watermelon/Services/Backup/RemoteIndexSyncService.swift`
