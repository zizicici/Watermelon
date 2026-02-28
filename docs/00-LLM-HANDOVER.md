# PhotoBackup / Watermelon 接手总览

## 1. 当前状态（一句话）

当前主线是“单个 Home 页面 + 备份状态页 + 多存储连接管理（SMB/外接存储）”，备份按 Asset 计数，远端月 manifest 使用三表关系模型（resources/assets/asset_resources）。

## 2. 优先阅读文件

1. `Watermelon/App/AppCoordinator.swift`
2. `Watermelon/Home/HomeViewController.swift`
3. `Watermelon/Home/HomeAlbumMatching.swift`
4. `Watermelon/UI/Backup/BackupSessionController.swift`
5. `Watermelon/Services/Backup/BackupExecutor.swift`
6. `Watermelon/Services/Backup/MonthManifestStore.swift`
7. `Watermelon/Services/Backup/RemoteLibraryScanner.swift`

构建入口优先用 `Watermelon.xcodeproj`。

## 3. 运行时主流程

1. `AppCoordinator.start()` 直接 `showHome()`。
2. Home 右上角是连接按钮（当前连接/添加存储/管理存储）。
3. 添加存储支持 SMB 和外接存储目录。
4. 管理页支持删除、排序、编辑连接参数；点击连接项会直接进入参数编辑页（名称在参数页内编辑）。
5. 连接成功后会先 `reloadRemoteIndex`，拿到远端快照并缓存。
6. 底部工具栏右侧“备份”打开 `BackupStatusViewController`。
7. 备份状态由 `BackupSessionController` 统一驱动（start/pause/stop/retry）。
8. Home 页面根据本地索引 + 远端快照进行本地/远端匹配显示。

## 4. 备份链路要点

1. `BackupExecutor.runBackup` 按 Asset 遍历，不再按单资源计总进度。
2. 资源排序/role+slot 分配/assetFingerprint 由 `BackupAssetResourcePlanner` 负责。
3. 月切换时 flush 上月 manifest，结束时 flush 当前月。
4. 若 Asset 有任意资源失败，则该 Asset 记失败，不写入 `assets`/`asset_resources`。
5. 资源重名时继续走历史 `_n` 冲突规避策略。
6. 远端扫描是只读，不再在扫描时创建目录或写 manifest。

## 5. 数据存储（当前真实）

本地数据库（`DatabaseManager`）核心表：

1. `server_profiles`
2. `sync_state`
3. `local_assets`
4. `local_asset_resources`

`server_profiles` 关键字段包含 `storageType`、`connectionParams`、`sortOrder`，用于多存储类型连接管理。

远端每月 manifest（`.watermelon_manifest.sqlite`）核心表：

1. `resources`
2. `assets`
3. `asset_resources`

## 6. 已清理/已下线链路

1. `ManifestSyncService` 已删除。
2. `BackupViewController` 已删除。
3. `BackupExecutor` 里快照缓存锁逻辑已抽离到 `RemoteLibrarySnapshotCache`。

## 7. 仍在仓库但不在主入口的页面

1. `ServerSelectionViewController`
2. `SettingsViewController`

目前 App 启动不会进入它们。
