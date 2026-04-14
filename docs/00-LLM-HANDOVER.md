# PhotoBackup / Watermelon 接手总览（按当前代码）

## 1. 一句话现状

项目当前主线是：`Home` 单页 + `More` 配置页；备份支持 `SMB / WebDAV / 外接存储`，并已实现按月份分桶的多 worker 上传。

## 2. 先看哪些文件

1. `Watermelon/App/DependencyContainer.swift`
2. `Watermelon/Home/HomeViewController.swift`
3. `Watermelon/Home/HomeLibraryEngines.swift`
4. `Watermelon/Home/HomeAlbumMatching.swift`
5. `Watermelon/Services/Backup/BackupSessionController.swift`
6. `Watermelon/Services/Backup/BackupCoordinator.swift`
7. `Watermelon/Services/Backup/AssetProcessor.swift`
8. `Watermelon/Services/Backup/MonthManifestStore.swift`
9. `Watermelon/Services/Backup/RemoteIndexSyncService.swift`
10. `Watermelon/Services/Restore/RestoreService.swift`

## 3. 主流程（运行时）

1. `AppCoordinator.start()` 直接进入 `HomeViewController`。
2. Home 顶部左右分栏：左侧”本地相册”、右侧”远端存储”（下拉菜单切换连接）。
3. 连接成功时先执行 `backupCoordinator.reloadRemoteIndex(...)`，再刷新 Home 数据。
4. 用户选中月份后，底部面板显示备份/下载/同步计数，点”执行”进入三阶段执行模式。
5. `BackupSessionController`（每次执行新建实例）负责 UI 状态聚合与备份命令处理，调用 `BackupCoordinator.runBackup(request:eventStream:)`。
6. `BackupCoordinator` 执行权限检查、远端索引同步、按月并发调度、月 manifest flush 与收尾。
7. 下载阶段由 `RestoreService.restoreItems` 执行，逐 item 写入 hash index 确保中断后可续。

## 4. 备份链路关键点

1. 备份模式：`full` / `scoped(assetIDs)` / `retry(assetIDs)`。
2. 执行前会加载本地 hash 索引（`local_assets/local_asset_resources`）用于跳过与估算。
3. 全量或范围资产会按“月份”分桶，worker 动态领取月份任务（非静态切片）。
4. 存储连接使用 `StorageClientPool`（网络协议连接池上限为 2，本地盘按 worker 数）。
5. 单 Asset 由 `AssetProcessor` 处理：导出+hash、碰名处理、上传、写 manifest、写本地索引、更新远端快照缓存。
6. `MonthManifestStore.flushToRemote` 失败抛出异常，默认终止 run。
7. 暂停/停止是协作取消，不会强杀正在进行的单次 I/O。

## 5. 数据存储（真实）

本地 GRDB：

1. `server_profiles`
2. `sync_state`
3. `local_assets`
4. `local_asset_resources`

远端（每月目录）：

1. `resources`
2. `assets`
3. `asset_resources`

文件路径：`/{YYYY}/{MM}/.watermelon_manifest.sqlite`

## 6. UI 入口要点

1. Home 是左右双栏（本地 / 远端），按年-月 section 展示，支持月份选择。
2. 选中月份后底部弹出 `SelectionActionPanel`，显示备份(→)/下载(←)/同步(↔)计数。
3. 点”执行”进入执行模式：上传阶段 → 下载阶段 → 完成。支持暂停/停止。
4. 箭头百分比基于 reconciliation `matchedCount`，上传阶段额外叠加 session 实时进度。

## 7. 当前已替代/已下线

1. 旧 `BackupExecutor` 已由 `BackupSessionController + BackupCoordinator + AssetProcessor` 替代。
2. 旧 `BackupRunCommandActor` 已合并入 `BackupSessionController`（@MainActor），不再存在独立 actor。
