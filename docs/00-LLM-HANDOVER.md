# PhotoBackup / Watermelon 接手总览（按当前代码）

## 1. 一句话现状

项目当前主线是：`Home` 单页 + `BackupView` 备份状态页 + `More` 配置页；备份支持 `SMB / WebDAV / 外接存储`，并已实现按月份分桶的多 worker 上传。

## 2. 先看哪些文件

1. `Watermelon/App/DependencyContainer.swift`
2. `Watermelon/Home/HomeViewController.swift`
3. `Watermelon/Services/Backup/BackupSessionController.swift`
4. `Watermelon/Services/Backup/BackupCoordinator.swift`
6. `Watermelon/Services/Backup/AssetProcessor.swift`
7. `Watermelon/Services/Backup/MonthManifestStore.swift`
8. `Watermelon/Services/Backup/RemoteIndexSyncService.swift`

## 3. 主流程（运行时）

1. `AppCoordinator.start()` 直接进入 `HomeViewController`。
2. Home 右上角连接菜单：当前连接、添加存储、更多。
3. 连接成功时先执行 `backupCoordinator.reloadRemoteIndex(...)`，再刷新 Home 数据。
4. 点工具栏“备份”打开 `BackupViewController`（sheet）。
5. `BackupSessionController` 负责 UI 状态聚合与备份命令处理（开始/暂停/停止/恢复），为每次 run 创建独立 `BackupEventStream`，调用 `BackupCoordinator.runBackup(request:eventStream:)`。
6. `BackupCoordinator` 执行权限检查、远端索引同步、按月并发调度、月 manifest flush 与收尾。

## 4. 备份链路关键点

1. 备份模式：`full` / `scoped(assetIDs)` / `retry(assetIDs)`。
2. 执行前会加载本地 hash 索引（`local_assets/local_asset_resources`）用于跳过与估算。
3. 全量或范围资产会按“月份”分桶，worker 动态领取月份任务（非静态切片）。
4. 存储连接使用 `StorageClientPool`（网络协议连接池上限为 2，本地盘按 worker 数）。
5. 单 Asset 由 `AssetProcessor` 处理：导出+hash、碰名处理、上传、写 manifest、写本地索引、更新远端快照缓存。
6. `MonthManifestStore.flushToRemote` 失败默认中断 run（`ManifestFlushFailurePolicy.failRun`）。
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

1. Home 右上角菜单末项是“更多”（不再直接放“管理存储”）。
2. More 页里包含：
3. `远端存储`（管理存储）
4. `本地数据`（本地 Hash 索引管理）
5. `备份`（上传并发 worker 设置）
6. 备份页支持范围卡片、范围选择器、多 worker 上传状态切换显示。

## 7. 当前已替代/已下线

1. 旧 `BackupExecutor` 已由 `BackupSessionController + BackupCoordinator + AssetProcessor` 替代。
2. 旧 `BackupRunCommandActor` 已合并入 `BackupSessionController`（@MainActor），不再存在独立 actor。
