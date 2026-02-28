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
7. `BackupExecutor`
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

1. `BackupExecutor`：备份编排器（按 Asset 主循环）。
2. `BackupAssetResourcePlanner`：资源排序、role/slot 分配、assetFingerprint 计算。
3. `MonthManifestStore`：月 manifest sqlite 读写 + flush。
4. `RemoteLibraryScanner`：只读扫描 `YYYY/MM` 下 manifest，生成快照。
5. `RemoteLibrarySnapshotCache`：内存快照缓存 + 线程安全 upsert。
6. `ContentHashIndexRepository`：本地 `local_assets` / `local_asset_resources` 读写。
7. `RemoteNameCollisionResolver`：文件名冲突 `_n` 递增。

### Photo / Restore / Metadata

1. `PhotoLibraryService`：照片权限、`PHAsset` 查询、`PHAssetResource` 导出。
2. `RestoreService`：远端文件下载后写回系统相册。
3. `MetadataService`：文件元信息辅助。

## 4. UI 结构

### 主页面

1. `HomeViewController`：统一展示本地与远端匹配结果。
2. 远端匹配算法在 `HomeAlbumMatching`（与视图解耦）。
3. `AlbumGridCell`、`AlbumSectionHeaderView` 为 Home 网格子组件。

### 备份状态

1. `BackupSessionController`：备份状态机与日志/进度聚合。
2. `BackupStatusViewController`：开始/暂停/停止、结果筛选、日志展示。
3. `BackupFailedItemsViewController` 与 `BackupFailedItemDetailViewController`：失败项重试入口。

### 存储配置与管理

1. `AddSMBServerLoginViewController`
2. `SMBSharePathPickerViewController`
3. `AddSMBServerViewController`
4. `AddWebDAVStorageViewController`
5. `AddExternalStorageViewController`
6. `ManageStorageProfilesViewController`

这些链路由 Home 右上角连接菜单触发。

## 5. 当前主链路文件建议

后续改动优先集中：

1. `Watermelon/Home/HomeViewController.swift`
2. `Watermelon/Home/HomeAlbumMatching.swift`
3. `Watermelon/UI/Backup/BackupSessionController.swift`
4. `Watermelon/Services/Backup/BackupExecutor.swift`
5. `Watermelon/Services/Backup/MonthManifestStore.swift`
6. `Watermelon/Services/Backup/RemoteLibraryScanner.swift`
