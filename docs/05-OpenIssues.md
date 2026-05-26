# 当前风险点 / 技术债（按当前实现）

## 1. 自动化测试仍缺真实端到端覆盖

1. `WatermelonTests` 已覆盖 Home 端纯逻辑、RemoteFileNaming、S3/SFTP 形状、Repo V2 materialize / flush / bootstrap / migration、RemoteIndexSyncService、RepoVerifyMonthService、storage capability matrix、RestoreService fallback、RestoredAssetFingerprintVerifier 等。
2. 仍缺的是跨真实 PhotoKit + 真实远端的端到端链路：`HomeExecutionCoordinator`、`BackupCoordinator`、`BackupParallelExecutor`、`AssetProcessor` 组合后的连接切换 / 暂停恢复 / sync 月份内联下载 / 外接存储拔出等。
3. macOS target 和 `BackgroundBackupRunner` 的真实调度路径也仍主要依赖手工或系统集成回归。

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

## 4. 强杀窗口与 V2 metadata retention 的剩余边界

V2 远端格式已落地基础设施（commit log + snapshot + materializer + V1→V2 migration 流程，详见 `docs/06-RepoV2.md`）。Unit 8 后，V2 row-writing asset 在 publish asset / 写本地 hash-index 前会先写 per-asset commit；每 10 个非 failed 结果的 flush cadence 仅用于 snapshot。

1. V1 路径（cutover 前）：manifest 在「月份完成」与「任务收尾」时 flush；强杀前一批未 flush 元数据丢失（月级窗口）。
2. V2 路径（cutover 后）：row-writing asset 逐条写 commit；batch / final flush 写 snapshot cadence，不再有 deferred batch commit 窗口。
3. 当前已有 per-month checkpoint / retention barrier / commit 前缀删除：干净 flush 后按 `RepoCompactionPolicy` 写 checkpoint，发布 retention manifest，再由 preflight + liveness gate + post-delete verification 保守删除候选 commit。
4. 剩余边界不是“完全未设计”，而是 retention 仍是 best-effort 维护：遇到 barrier 不完整、liveness view 不完整、active non-self writer、legacy grace 未过、migration marker、post-delete verification 不确定等情况会跳过删除。SMB / SFTP 这类不支持安全 liveness renewal 的后端不会公布 barrier-aware retention capability，也不会跑 orphan metadata sweep；commit 前缀删除仍会经过 liveness gate 和 verification 保守判定。物理数据文件 GC、snapshot GC 和用户可见 repair UI 仍未实现。

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
5. SFTP 多 worker = 多 SSH 会话；V2 cutover 后每个 profile 还会额外开一条专用的 metadata SSH 连接（`BackupV2RuntimeServices.metadataClient`，commit/snapshot/liveness 写入用），因此实际并发连接数 = `worker_count + 1`。遇 sshd `MaxStartups` / `MaxSessions` 紧配置需要回落到 1，目前没有自动探测

## 8. 下载恢复进度仍未持久化到 resource 级

1. `RestoreService.restoreItems(...)` 会在 item、resource 与 hash 读取循环中检查取消。
2. 但一个 item 内部若包含多资源（如 Live Photo），中断时仍没有持久化 resource 级恢复进度，可能丢掉该 item 的部分临时进度。
3. 成功保存到相册后，`DownloadWorkflowHelper` 会通过 `RestoredAssetFingerprintVerifier` 重建并验证 durable fingerprint binding；只有验证成功的 item 才能被后续 reconcile 当作已恢复。

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

## 12. V2 checkpoint / retention compaction 仍是保守维护

当前 V2 row-writing asset 已是 per-asset commit：`AssetProcessor` 在 publish asset / 写本地 hash-index 前调用 `commitPendingAssetToRemote(ignoreCancellation: false)`。每 10 个非 failed 结果的 flush cadence 保留，但职责变为写 snapshot；旧 batch commit / optimistic subtraction 链已删除。

当前实现已加入：

1. `RepoCompactionPlanner` / `RepoCheckpointService`：按 replay commit 数或 bytes 判断是否写 per-month checkpoint snapshot。
2. `RepoRetentionBarrierService` / `RetentionManifestRemoteStore`：为被接受的 checkpoint 发布 retention barrier。
3. `RepoRetentionDeletePreflightService` / `RepoRetentionCommitDeleteExecutor`：只删除 barrier 覆盖、accepted snapshot 覆盖、liveness gate 允许、post-delete materialize 等价的 commit 前缀。
4. `BackupV2RuntimeBuilder → RepoMaintenanceStartupRunner → RepoRetentionStartupMaintenance → RetentionMaintenanceOrchestrator`：启动时扫描足够老的 retention manifest 并尝试继续删除候选。

仍需关注的是实际运行中的跳过率和可观测性：SMB / SFTP 这类缺少安全 liveness renewal 的后端不会公布 barrier-aware retention capability，也不会跑 orphan metadata sweep；commit 前缀删除在没有阻塞 peer 时仍可尝试，但会保守受 liveness gate / legacy grace / verification 约束。删除失败、verification inconclusive、barrier set invalid 等目前主要停留在日志 / 测试覆盖层，没有面向用户的维护视图。

## 13. V2 元数据 atomic-create 在 `.overwritePossible` 后端的残留风险

1. SMB 的 `atomicCreate` 走 exists+upload，无真正的 no-overwrite 原语；WebDAV 会发 `If-None-Match: *`，但因服务端兼容性仍按 `.overwritePossible` 处理；S3 multipart 同样落到 `.overwritePossible`。`CommitLogWriter` 写最终路径用 atomicCreate + readback verify。`RepoBootstrap.ensureVersionJSON` 已切到 pre-check + `MetadataCreateGate` 暂存路径 + 回读校验，但 staged-move 阶段仍可能在极端窗口被对端 overwrite。
2. `CommitLogWriter` 写 `(writerID, seq)` 最终路径用 atomicCreate + post-verify。同一 writer 的并发 run 仍可能两次都通过 verify，后写的覆盖前者；写入路径写 writer-unique 物理文件（`~widN`）也只能压低概率，因为同 writer 在同一秒选到的候选名仍可能相同。真修需要 no-overwrite primitive 或 run-unique（runID + attempt counter）终态文件名 + 一致的 manifest 引用。当前作为残留技术债保留。
3. `RepoBootstrap.ensureRepoJSON` 已从单次 500ms 等待改为 read-stability loop（最长 ~1.5s）；首次写入仍无法用 no-overwrite 原语保证全局唯一 canonical，losing-claim 的 ts 仍留在 claims 目录，未来 winning claim 损坏后 lex-min 可能翻转 canonical。需要 protocol-level 修改（immutable seed / losing-claim adoption），不在本轮范围。
4. `RemoteFileNaming.preferredRemoteFileName` 已通过 `clampLeafToByteBudget` 把最终文件名（含 `~widN` / `-N` / UUID 后缀和扩展名）压回 255 UTF-8 byte 内；输入超长 sanitized stem 会被按 UTF-8 边界截断再加后缀。极端碰撞导致 UUID escape 时仍由同一预算闸控制。
5. Physical-presence overlay / probe 会先按 leaf-name 找候选，再要求 listing size 等于 manifest `fileSize`；size 不一致会被纳入 missing 集合，下次 sync 会真修。被 truncate 但 size 完全没变的损坏（如同尺寸覆盖）仍只能通过深度 verify 抓出。
6. `BackupMonthFinalizationResult.failed` 已携带 `underlyingError`，但后续层对 inline-download underlying error 的分类仍有限；如果要让 `BackupSessionController` 对 connection-unavailable / verify failure / user cancellation 做完全一致的 UI 分类，还需要把 Home `DownloadMonthResult`、Async bridge、BSC reducer 的错误分类协议继续收敛。

## 14. 建议优先级

1. 优先补 `HomeExecutionCoordinator` / `BackupCoordinator` 的中等粒度集成测试，特别是暂停 / 恢复 / stop / 连接丢失。
2. 评估为 full run 持久化 pending 集，减少恢复时重扫。
3. 评估复用 iCloud recovery 结果到上传阶段，降低 iCloud-only 资源的重复 I/O 成本。
4. 评估按失败率和吞吐量自适应调整 worker 数。
5. 决定 macOS target 的最终定位（迁移工具 / 完整备份端 / 仅配置端）。
6. 给遗留 `ExternalVolume / WebDAV / S3 ConnectionParams` 加 `nonisolated`，关掉 macOS build 的 isolation warning。
7. 关注 Citadel 上游修复目录句柄泄漏，移除 `listReconnectThreshold` 重连。
8. 给 V2 retention / checkpoint 维护补用户可见诊断，尤其是 barrier invalid、liveness blocked、verification inconclusive 和按后端能力跳过的原因。
