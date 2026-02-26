# 架构与关键组件

## 1. 应用入口与导航

入口：`SceneDelegate -> AppCoordinator.start()`

`AppCoordinator` 的路由：

1. `showLogin(allowAutoLogin: true)` -> `ServerSelectionViewController`
2. 登录完成 -> `showMainAlbum()` -> `AlbumViewController`
3. Album 左上角设置 -> `showSettings()` -> `SettingsViewController`

根控制器是单个 `UINavigationController`，不再使用 TabBar。

## 2. 依赖注入（DependencyContainer）

`DependencyContainer` 统一持有：

1. `DatabaseManager`
2. `KeychainService`
3. `AppSession`
4. `PhotoLibraryService`
5. `MetadataService`
6. `ManifestSyncService`（当前主链路几乎不依赖）
7. `BackupExecutor`
8. `RestoreService`

`AppSession` 仅保存当前活跃 `ServerProfileRecord` 和 `password`（内存态）。

## 3. Services 分工

### SMB

1. `AMSMB2Client`：`SMBClientProtocol` 实现（connect/list/upload/download/move/delete 等）。
2. `SMBSetupService`：添加服务器流程里列 Share、列目录。
3. `SMBDiscoveryService`：Bonjour 发现 `_smb._tcp`。
4. `SMBRemoteImageDataProvider`：Kingfisher 数据源。
5. `RemoteThumbnailService`：远端图片下载+下采样（actor + 限流 3）。

### Backup

1. `BackupExecutor`：主备份循环、去重、冲突处理、上传、progress/log 回调。
2. `MonthManifestStore`：月级 manifest 本地 sqlite 读写 + 上传到 SMB。
3. `RemoteLibraryScanner`：扫描远端 `YYYY/MM`，汇总快照。
4. `ContentHashIndexRepository`：本地 hash 索引 UPSERT/查询。
5. `RemoteNameCollisionResolver`：文件名 `_n` 递增。

### Photo / Restore / Metadata

1. `PhotoLibraryService`：权限、PHAsset 查询、PHAssetResource 原始导出。
2. `RestoreService`：从远端下载并写回系统相册（支持资源组导回）。
3. `MetadataService`：文件大小/像素/UTI。

## 4. UI 层结构

### Auth

1. `ServerSelectionViewController`：发现+已保存服务器列表，自动登录。
2. `AddSMBServerLoginViewController`：输入 host/user/pass/domain。
3. `SMBSharePathPickerViewController`：选择 Share 和路径。
4. `AddSMBServerViewController`：确认并保存 Profile + Keychain。

### Album & Backup

1. `AlbumViewController`：本地/远端分段、网格、筛选、导回、刷新。
2. `BackupSessionController`：UI 侧状态机（idle/running/paused/stopped/failed/completed）。
3. `BackupStatusViewController`：开始/暂停/停止 + 处理列表 + 日志。

### Settings

1. `SettingsViewController`：权限状态、切服、远端索引重载、本地 hash 清理。

## 5. 当前“主链路文件”建议

后续变更优先集中在这批文件：

1. `Watermelon/Services/Backup/BackupExecutor.swift`
2. `Watermelon/Services/Backup/MonthManifestStore.swift`
3. `Watermelon/Services/Backup/RemoteLibraryScanner.swift`
4. `Watermelon/UI/Album/AlbumViewController.swift`
5. `Watermelon/UI/Backup/BackupSessionController.swift`
6. `Watermelon/UI/Backup/BackupStatusViewController.swift`
