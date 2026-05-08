# 当前风险点 / 技术债（按当前实现）

## 1. 自动化测试只覆盖了纯逻辑层

1. `WatermelonTests` 已覆盖 Home 端的引擎（`HomeLocalIndexEngine` / `HomeRemoteIndexEngine`）、`HomeDataProcessingWorker`、`HomeRefreshScheduler`、`HomeScopeController` / `HomeScopeNormalizer`、`HomeSelectionController`、`HomeSectionBuilder`、`HomeHeaderSummaryFormatter`、`RemoteFileNaming` 等纯逻辑单元。
2. `HomeExecutionCoordinator`、`BackupCoordinator`、`BackupParallelExecutor`、`AssetProcessor`、`RestoreService`、连接切换 / 暂停恢复 / sync 月份内联下载 / 外接存储拔出等真正涉及相册或远端的链路 **仍然没有自动化覆盖**。
3. macOS target 和 `BackgroundBackupRunner` 也都不在测试范围内。
4. 这些链路依旧依赖真机手工回归。

## 2. iCloud-only 资源仍有重复 I/O 成本

1. 当前首页执行已支持 `允许访问 iCloud 原件`：离线预检查可识别 cache-hit 但已被回收到 iCloud 的资产，download / sync 还支持对 `unavailableAssetIDs` 做联网补索引。
2. 但在某些场景，同一条 iCloud-only 本地资源仍可能被重复读取：
   - 预检查对 cache-hit 资产做一次轻量本地可用性判断
   - 联网补索引时完整导出一次原件并计算 hash
   - 上传阶段若远端未命中，可能再次完整导出并上传
3. 对大视频、Live Photo 或蜂窝网络场景，这个额外成本会比较明显。
4. 后续可评估复用 preflight 产物到上传阶段，减少重复完整导出。

## 3. full run 的恢复成本仍高

1. full backup 或其恢复流程，仍需要重新遍历图库并重新计算 pending 集。
2. 大图库下，开始执行和恢复执行都会有明显前置耗时。

## 4. manifest flush 仍存在强杀窗口

1. manifest 主要在“月份完成”与“任务收尾”时 flush。
2. 如果应用在 flush 前被系统强杀，最近一批增量仍可能没写回远端 manifest。
3. `MonthManifestStore.loadSeeded(...)` 已通过列出真实远端目录来规避重名碰撞，但不能消除未 flush 元数据丢失本身。

## 5. 首页状态机复杂度依然不低

当前首页的状态由多层协作完成：

1. `HomeScreenStore`
2. `HomeConnectionController`
3. `HomeExecutionCoordinator` / `HomeExecutionDataRefresher` / `HomeExecutionSession`
4. `HomeIncrementalDataManager` / `HomeDataProcessingWorker`
5. `HomeRefreshScheduler` / `HomeFileSizeScanCoordinator`
6. `HomeScopeController` / `HomeScopeNormalizer` / `HomeSelectionController` / `HomePhotoAccessGate`
7. `RemoteMaintenanceController`（与执行态互斥）

这套分层已经比旧版清晰，但下面这些场景同时发生时仍需谨慎：

1. refresh 合并
2. deferred photo changes 排空
3. 连接失败后的远端快照恢复
4. scope 切换叠加 PHChange、再叠加 maintenance / 执行态

## 6. 大图库文件大小扫描仍有成本

1. 首页会异步扫描每个月本地资源总大小，由 `HomeFileSizeScanCoordinator` 在主 actor 上逐月 `Task.yield()`。
2. 启动全量扫描与 PHChange 增量 rescan 共用 size snapshot refcount，已避免被对方提前释放。
3. 但大图库初次进入仍可能较慢；在文件大小全部补齐前，部分汇总会暂时显示 `-`。

## 7. 并发策略仍是“固定默认 + 手动覆盖”

1. 默认并发：`SMB / WebDAV = 2`、`externalVolume = 3`
2. 用户可手动覆盖到 `1...4`
3. iCloud-only 资产存在时上传会被强制单 worker
4. 目前没有根据带宽、远端 RTT、失败率动态调节 worker 数

## 8. 下载取消粒度仍是 item 级

1. `RestoreService.restoreItems(...)` 在 item 循环边界检查取消。
2. 一个 item 内部若包含多资源（如 Live Photo），中断时仍可能丢掉该 item 的部分临时进度。
3. 不过成功完成的 item 会立即写回 hash 索引，所以下次能跳过整 item。

## 9. macOS Target 的定位仍偏窄

1. `WatermelonMac/` 目前主要承载 “遗留导入 + profile 管理” 功能，并不复用 iOS 备份链路。
2. 它共享 `Shared/` 里的存储 / Keychain / 领域模型，但没有 `BackupCoordinator`，不能在桌面端做实际备份 / 下载。
3. 短期内可视为 “数据迁移工具 + 远端配置工具”；如果要把 Mac 端纳入备份运行时，需要新设计触发与进度反馈层。

## 10. 建议优先级

1. 优先补 `HomeExecutionCoordinator` / `BackupCoordinator` 的中等粒度集成测试，特别是暂停 / 恢复 / stop / 连接丢失。
2. 评估为 full run 持久化 pending 集，减少恢复时重扫。
3. 评估复用 iCloud recovery 结果到上传阶段，降低 iCloud-only 资源的重复 I/O 成本。
4. 评估按失败率和吞吐量自适应调整 worker 数。
5. 决定 macOS target 的最终定位（迁移工具 / 完整备份端 / 仅配置端）。
