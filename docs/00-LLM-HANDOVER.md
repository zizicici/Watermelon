# Watermelon 接手总览（按当前代码）

## 1. 一句话现状

项目当前主线是：`Home` 单页 + `More` 配置页；首页已经拆成 `ViewController + Store + ConnectionController + ExecutionCoordinator` 四层，备份与同步依赖本地 hash 索引和远端月 manifest。

## 2. 优先阅读这些文件

1. `Watermelon/App/DependencyContainer.swift`
2. `Watermelon/Home/HomeViewController.swift`
3. `Watermelon/Home/HomeScreenStore.swift`
4. `Watermelon/Home/HomeConnectionController.swift`
5. `Watermelon/Home/HomeExecutionCoordinator.swift`
6. `Watermelon/Home/HomeExecutionSession.swift`
7. `Watermelon/Home/HomeLibraryEngines.swift`
8. `Watermelon/Services/HashIndex/LocalHashIndexBuildService.swift`
9. `Watermelon/Services/Backup/BackupSessionController.swift`
10. `Watermelon/Services/Backup/BackupRunPreparation.swift`
11. `Watermelon/Services/Backup/BackupParallelExecutor.swift`
12. `Watermelon/Services/Backup/AssetProcessor.swift`
13. `Watermelon/Services/Backup/RemoteIndexSyncService.swift`
14. `Watermelon/Services/Restore/RestoreService.swift`

## 3. 运行时主流程

1. `AppCoordinator.start()` 直接把 `HomeViewController` 设为 root。
2. `HomeViewController` 初始化 `HomeScreenStore`，负责 UI 渲染，不再直接持有大块业务状态。
3. `HomeScreenStore.load()` 先跑 `HomeIncrementalDataManager.ensureLocalIndexLoaded()`，随后尝试自动连接上次激活的 profile。
4. 连接成功后，`HomeConnectionController` 调 `BackupCoordinator.reloadRemoteIndex(...)`，共享的 `RemoteLibrarySnapshotCache` 被刷新。
5. 首页月份选择完成后，`HomeExecutionCoordinator.enter(...)` 建立一次新的执行会话。
6. 执行前会先冻结一次运行时设置，然后跑一次离线本地索引预检查。
7. 预检查中，cache-hit 资产仍会做一次轻量离线可用性探测，保证已被回收到 iCloud 的资产也能被识别为 `unavailable`。
8. 第一轮结束后：若启用 `允许访问 iCloud 原件` 且上传范围内存在 `unavailableAssetIDs`，upload 强制降到 `1` 个 worker；download / sync 则对 `unavailableAssetIDs` 再做一次联网补索引。
8. 上传由 `BackupSessionController` 驱动 `BackupCoordinator.runBackup(...)`；下载由 `DownloadWorkflowHelper + RestoreService` 完成。

## 4. Home 当前分层

### `HomeViewController`

- 负责双栏 UICollectionView、顶部 header、连接菜单、右侧 overlay、底部操作面板、更多页 FAB。
- 通过 `store.onChange` 响应五类变化：`.data / .selection / .execution / .connection / .structural`。

### `HomeScreenStore`

- 首页状态中心。
- 聚合 `HomeIncrementalDataManager`、`HomeConnectionController`、`HomeExecutionCoordinator`。
- 维护 `sections / rowLookup / selection / executionState / connectionState`。
- 负责把内部状态变化转成 UI 级别更新。

### `HomeConnectionController`

- 加载已保存 profile
- 从 `sync_state + Keychain` 自动连接
- 处理密码提示、切换连接、断开连接
- 连接时重载远端索引；失败时必要时恢复旧连接的远端快照

### `HomeExecutionCoordinator`

- 管理执行阶段：本地索引预检查 → 上传 → 内联同步下载 / 剩余下载
- 维护暂停、恢复、停止和失败处理
- 利用 `HomeExecutionDataRefresher` 合并远端同步与本地索引刷新

## 5. 关键执行行为

1. 执行开始时会冻结 `上传并发` 和 `允许访问 iCloud 原件`；任务运行中改设置不会立即生效。
2. 若启用了 `允许访问 iCloud 原件` 且上传范围中检测到仅存于 iCloud 的本地资源，本次 upload 会自动降为 `1` 个 worker。
3. 本地 hash 索引预检查第一轮始终离线执行；若本次包含下载或同步且启用了 `允许访问 iCloud 原件`，会只对 `unavailableAssetIDs` 再补一次联网索引。
4. 如果 download / sync 在补索引后仍不完整，则直接停止执行，避免重复图片。
5. 上传阶段按“月份”分桶，worker 动态领取月份，不是静态切片。
6. 同步月份会在该月上传 flush 后立即进入下载收尾，不必等所有上传完成。
7. 下载成功后逐 item 写回本地 hash 索引，因此中断后能自动跳过已完成 item。

## 6. 数据与索引

### 本地 SQLite

- `server_profiles`
- `sync_state`
- `local_assets`
- `local_asset_resources`

### 远端

- 每个月目录维护 `.watermelon_manifest.sqlite`
- schema：`resources / assets / asset_resources`

### 内存快照

- `RemoteLibrarySnapshotCache` 维护共享远端快照
- Home 侧通过 `RemoteLibrarySnapshotState(revision, isFullSnapshot, monthDeltas)` 增量消费

## 7. 当前已淘汰的理解

1. 首页不是 `HomeViewController` 单独承载状态，当前主控层是 `HomeScreenStore`。
2. “上传完再统一下载所有 sync 月份” 已不是实际行为；sync 月份现在按月内联收尾。
3. 根控制器不是 `UINavigationController`；`HomeViewController` 直接作为 root，需要时自行 push / present 其他页面。
