# 当前风险点 / 技术债（按当前实现）

## 1. 自动化测试只覆盖了纯逻辑层

1. `WatermelonTests` 已覆盖 Home 端的引擎（`HomeLocalIndexEngine` / `HomeRemoteIndexEngine`）、`HomeDataProcessingWorker`、`HomeRefreshScheduler`、`HomeScopeController` / `HomeScopeNormalizer`、`HomeSelectionController`、`HomeSectionBuilder`、`HomeHeaderSummaryFormatter`、`RemoteFileNaming`、`WriteLockService`、`OrphanCleanupLite` 等纯逻辑单元。
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
3. `MonthManifestStore.loadSeeded(...)` 已通过列出真实远端目录规避重名碰撞；`OrphanCleanupLite` 的 repair-first 清理还能从残留 `.tmp` / `.bak` 把月度 manifest 恢复回规范路径。但若强杀发生在 scratch 落盘前，最近一批增量仍可能整体丢失。

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

1. 默认并发：`SMB / WebDAV / S3 / SFTP = 2`、`externalVolume = 3`
2. 用户可手动覆盖到 `1...4`
3. iCloud-only 资产存在时上传会被强制单 worker
4. 目前没有根据带宽、远端 RTT、失败率动态调节 worker 数
5. SFTP 多 worker = 多 SSH 会话；遇 sshd `MaxStartups` / `MaxSessions` 紧配置需要回落到 1，目前没有自动探测

## 8. 下载取消粒度仍是 item 级

1. `RestoreService.restoreItems(...)` 在 item 循环边界检查取消。
2. 一个 item 内部若包含多资源（如 Live Photo），中断时仍可能丢掉该 item 的部分临时进度。
3. 不过成功完成的 item 会立即写回 hash 索引，所以下次能跳过整 item。

## 9. macOS Target 的定位仍偏窄

1. `WatermelonMac/` 目前主要承载 “遗留导入 + profile 管理” 功能，并不复用 iOS 备份链路。
2. 它共享 `Shared/` 里的存储 / Keychain / 领域模型，但没有 `BackupCoordinator`，不能在桌面端做实际备份 / 下载。
3. 短期内可视为 “数据迁移工具 + 远端配置工具”；如果要把 Mac 端纳入备份运行时，需要新设计触发与进度反馈层。
4. macOS 端目前没有 SFTP 添加 / 编辑 UI；只能在 iOS 端创建后通过共享数据库读取。

## 10. SFTP 后端的已知限制

1. **依赖**：Citadel `0.12.1`，传递依赖 `swift-nio-ssh` 来自 `Wellz26/swift-nio-ssh` fork（非 Apple 官方 repo），属于供应链层面的事实声明。
2. **Citadel 0.12.1 目录句柄泄漏**：`listDirectory` 会泄漏服务端 fd。`SFTPClient` 每 32 次 `list` 整体重连一次以释放句柄，重连一次约 200–500 ms。Citadel 升级后应去掉。
3. **两阶段 TOFU 要双重 SSH 握手**：保存 SFTP profile 时先用空凭证连一次取主机指纹再 abort、用户确认后再用钉住的指纹真正连一次走 `verifyBasePathWritable`。Citadel 没有公开 hook 在 host-key 阶段把通道 hand off 给后续 user-auth；除非 vendor 一份 Citadel/`swift-nio-ssh`，无法消除。一次性保存路径的 1–2s 开销，不在 hot path。
4. **私钥类型**：仅 OpenSSH ed25519 / RSA。ECDSA 等其它类型在 `makeAuthenticationMethod` 抛 `SFTPUnsupportedKeyTypeError`（用户面文案带类型名）。
5. **`copy()` 走本地中转**：SFTP v3 没有 server-side copy verb，`SFTPClient.copy` 落本地临时文件再上传；备份热路径不调用，`MonthManifestStore` 的 `.bak` dance 用 `move` + `delete`。

## 11. 既有 ConnectionParams 的 Swift 6 isolation 警告

1. `WatermelonMac` target 配置 `SWIFT_DEFAULT_ACTOR_ISOLATION = MainActor`；`Shared/` 里没标 `nonisolated` 的纯 value type 会被推断成 MainActor。
2. `SFTPConnectionParams` / `SFTPCredentialBlob` / `RemotePathBuilder` 已加 `nonisolated`；遗留的 `ExternalVolumeConnectionParams` / `WebDAVConnectionParams` / `S3ConnectionParams` 还没加，目前是 warning（"main actor-isolated conformance ... cannot be used in nonisolated context; this is an error in the Swift 6 language mode"），未来打开 Swift 6 模式会变 error。
3. 修法是给这三个类型也加 `nonisolated`，与 SFTP 的处理一致；本仓库未跟 SFTP 的改动一起做，避免扩大 PR 范围。

## 12. 建议优先级

1. 优先补 `HomeExecutionCoordinator` / `BackupCoordinator` 的中等粒度集成测试，特别是暂停 / 恢复 / stop / 连接丢失。
2. 评估为 full run 持久化 pending 集，减少恢复时重扫。
3. 评估复用 iCloud recovery 结果到上传阶段，降低 iCloud-only 资源的重复 I/O 成本。
4. 评估按失败率和吞吐量自适应调整 worker 数。
5. 决定 macOS target 的最终定位（迁移工具 / 完整备份端 / 仅配置端）。
6. 给遗留 `ExternalVolume / WebDAV / S3 ConnectionParams` 加 `nonisolated`，关掉 macOS build 的 isolation warning。
7. 关注 Citadel 上游修复目录句柄泄漏，移除 `listReconnectThreshold` 重连。

## 13. 首次抢锁的原子性依赖后端条件写

1. 写锁获取走 `RemoteUploadMode.createIfAbsent` 原子创建：SMB 用 fork `zizicici/AMSMB2` 的 `uploadItem(overwrite:)`、SFTP 用 `.forceCreate`、外接卷用独占 `copyItem`，由文件系统 / 协议层保证原子。
2. S3 / WebDAV 走 `If-None-Match: *` 条件 PUT。若 S3 兼容后端（部分 MinIO / Ceph / 旧实现）忽略该头，并发首次抢锁可能两端都判成功，原子性退化为非原子——这是该后端的能力上限，App 侧无法消除。
