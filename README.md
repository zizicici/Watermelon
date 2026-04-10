# Watermelon Photo Backup

仓库名是 `PhotoBackup`，实际 iOS App/Target 名称是 `Watermelon`。

## 项目现状（按当前代码）

`Watermelon` 是一个以 iOS 相册为数据源、将资源备份到远端存储的应用，当前支持：

- 存储类型：`SMB`、`WebDAV`、`外接存储目录（security-scoped bookmark）`
- 操作模式：`上传（本地→远端）`、`下载（远端→本地）`、`同步（双向）`
- 备份模式：`全量`、`范围备份（scoped）`、`失败重试（retry）`
- 运行控制：`开始 / 暂停 / 继续 / 停止`
- 并发上传：按”月份”分桶后由多个 worker 动态领取任务（可在设置中手动覆盖 worker 数）
- 远端索引：按月维护 `.watermelon_manifest.sqlite`，并做增量同步
- 本地索引：维护本地 hash 索引（含资源大小），用于跳过已存在资源和提升二次备份速度
- 下载断点续传：逐 item 写入 hash index，中断后重启自动跳过已下载项

## 启动与主流程

- App 启动入口：`AppCoordinator.start()` -> `NewHomeViewController`
- Home 是左右双栏布局（本地相册 / 远端存储），用户选中月份后执行上传、下载或同步
- 执行三阶段：上传阶段 → 下载阶段 → 完成（支持暂停/停止）
- 进度基于 reconciliation `matchedCount`（content-hash 匹配），百分比单调递增不回退
- 备份控制面：`BackupSessionController`（每次执行新建实例）
- 备份执行面：`BackupCoordinator` + `AssetProcessor` + `MonthManifestStore`
- 下载执行面：`RestoreService`（逐 item 增量持久化 hash index）

## 备份架构（当前实现）

1. `BackupSessionController` 管理运行状态与控制命令（start/pause/stop/resume），同时负责 UI 状态聚合。每次执行创建新实例，避免跨会话状态泄漏。
2. `BackupCoordinator` 接收 `BackupRunRequest`，完成权限检查、远端索引同步、月份级调度与 worker 执行。
3. `AssetProcessor` 处理单个 asset：导出资源、计算 hash、碰名处理、上传、写入月 manifest、本地索引回写。
4. `MonthManifestStore` 在每个月目录维护 manifest 三表（见下文），并在合适时机 flush 到远端。`loadSeeded` 列出实际远端目录以检测孤儿文件。
5. `RemoteIndexSyncService` 扫描远端 manifest，构建快照供首页和备份流程复用。
6. `RestoreService` 下载远端资源到本地相册，支持 `Task.checkCancellation` 粒度的取消。

## 存储抽象

统一通过 `RemoteStorageClientProtocol` 访问远端，核心接口包括：

- `connect / disconnect`
- `list / metadata / exists`
- `upload / download / move / delete / createDirectory`
- `setModificationDate`
- `storageCapacity`

当前实现：

- `AMSMB2Client`
- `WebDAVClient`
- `LocalVolumeClient`

构造入口：`StorageClientFactory.makeClient(profile:password:)`。

## 数据模型

### App 本地数据库（GRDB）

- `server_profiles`：连接配置（含 `storageType` 与 `connectionParams`）
- `sync_state`：通用状态（如 active profile）
- `local_assets`：本地资产指纹与聚合统计
- `local_asset_resources`：本地资产下资源级 hash 与 size

### 远端月 manifest（SQLite）

每个月目录下维护 `.watermelon_manifest.sqlite`，包含：

- `resources`
- `assets`
- `asset_resources`

## 首页月份选择与执行

`NewHomeViewController` 提供月份级操作：

- 左右双栏：本地相册 / 远端存储，按年-月 section 展示
- 月份选择：点击 cell 选中，箭头方向自动判定（→上传 / ←下载 / ↔同步）
- 年级/全局全选 toggle
- 底部面板：显示各方向计数，点"执行"进入执行模式
- 执行模式：显示分类进度（pending/running/completed）、暂停/停止控制
- 进度百分比：reconciliation `matchedCount` 为基线，上传阶段叠加 session 实时进度（取较大值）

## 本地 Hash 索引管理

`LocalHashIndexManagerViewController` 支持：

- 查看索引覆盖与统计
- 创建 / 更新索引
- 运行中暂停 / 停止
- 重置索引
- 可选“移除本地已不存在条目”

## 依赖

Swift Package 依赖：

- `AMSMB2`
- `GRDB`
- `Kingfisher`
- `SnapKit`
- `MarqueeLabel`
- `AppInfo`

## 本地开发

1. 使用 Xcode 打开 `Watermelon.xcodeproj`
2. 选择 `Watermelon` scheme
3. 在模拟器或真机运行

> 注意：项目当前包含大量开发期重构代码与文档，建议结合 `docs/` 一起阅读。

## 文档导航

- `docs/00-LLM-HANDOVER.md`：快速接手说明
- `docs/01-Architecture.md`：模块结构与依赖关系
- `docs/02-BackupCoreV2.md`：备份主流程与关键规则
- `docs/03-DataModel.md`：本地与远端 schema
- `docs/04-UIFlow.md`：当前 UI 交互流
- `docs/05-OpenIssues.md`：现有风险与后续建议
