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

## 14. 手动残留文件清理只覆盖「有 manifest 的月份」

> 代码与用户文案统一用「残留文件 / leftover」（不与既有的元数据清理 `OrphanCleanupLite` 混用 orphan 一词）。入口在节点详情页独立 section「检查残留文件」，走 `LeftoverCleanupViewController` 模态（扫描→评审→删除→汇总，执行中禁止 dismiss + Stop）。

1. 该功能 (`LeftoverFileScanner` + `BackupRunPreparation.scanLeftoverFiles/deleteLeftoverFiles`) 只枚举 `.watermelon/months/<YYYY-MM>.sqlite` 能解析出的月份——即至少 flush 过一次 manifest、可证明归本 App 管理的月份。这是刻意的安全取舍：没有对应 manifest 的 `YYYY/MM` 目录无法证明是本 App 创建的，**绝不**当作我们的来删（`manifestNames == nil ⇒ 整月跳过、删 0`）。
2. **覆盖缺口**：前台备份默认 `incrementalFlushInterval == nil`，manifest 只在月末 flush 一次。一个**从未成功备份过**的全新月份若在首次月末 flush 之前被中断（崩溃 / OOM / 强退 / 断电），会留下满是数据文件、但无 manifest 的 `YYYY/MM` 目录——这种残留对本功能不可见，扫描会报「未发现」。后台备份走增量 flush（每 10 个），manifest 出现较早，缺口更小；月份一旦完整跑过一次即被纳入覆盖。
3. 现状以文案沟通该范围：详情页 section footer 与模态评审页 footer 均说明「仅列出本 App 管理的月份」，空状态文案为「在本 App 管理的月份中未发现残留文件」。彻底覆盖需要枚举无 manifest 的 `YYYY/MM` 数据目录并作为「无法证明归属」的单独类别呈现，属后续工作，不在本次范围。
4. `enumerateManifestMonths` 与 `makeLeftoverManifestNamesProvider`（编码「仅 manifest 月份」与「notFound→nil 跳过 / 传输故障→throw fail-closed」两条安全规则）目前是 `BackupRunPreparation` 私有方法、无单测；`LeftoverFileScanner` 对 provider 返回 nil/throw 的处理已覆盖，但 provider 自身的分类未覆盖。建议后续抽出可测 seam 或加针对 `InMemoryRemoteStorageClient` 的集成测试。

## 15. 缩略图 GC 的存活集用「全部 manifest asset fingerprint」（泄漏、可自愈）

1. 维护期的缩略图垃圾回收（`ThumbnailOrphanScanner`）按 fingerprint 命名扫描 `.watermelon/thumbs/`，删除 fingerprint 不在「存活集」里的 sidecar。存活集由 `BackupRunPreparation.buildLiveFingerprintHexes` 把每月 manifest 的**全部** asset fingerprint（`MonthManifestStore.assetFingerprintHexes()`）并起来，不区分是否「有真实媒体」。
2. 按现在「有媒体才算 backed up」的规则，config-only（只有 adjustmentData）/ phantom / all-media-missing 这类记录并不是真正的备份，但它们的 fingerprint 仍进存活集，会**误保护**同 fingerprint 的缩略图 sidecar → 泄漏（只是文件残留，不是数据损坏）。
3. **会自愈**：`reconcileMonth` / `cleanupMissingResources` 现按 `hasBackedUpMedia` 把这些「没意义」的记录（连同其资源行）剪掉，所以某月一旦再跑一次 verify/reconcile，这些 fingerprint 就从 manifest 消失，下一次缩略图 GC 重建存活集时不再保护它们、孤儿缩略图被删。只是清理有时间滞后（要等「该月被 reconcile」+「下一次 GC」都跑过）。
4. 若要根治：`buildLiveFingerprintHexes` 改成只并入「有媒体」的 fingerprint（对每条 asset 套 `hasBackedUpMedia`），与 browser 显示 / Home 计数 / reconcile 剪枝同一套规则。评估为低优先级，暂不处理。

## 16. 已打开的 viewer 中 `(false,false)` presence 仍折成 `.remoteOnly`（纯展示 stale）

1. `MediaPresence.of(onDevice:onRemote:)`（`MediaBrowserModels.swift`）在两端都为 false 时回落成 `.remoteOnly`。正常 grid 不会产生这种项（没有「既不在设备也不在远端」的入口）。
2. 唯一可达路径：一个**已打开**的 remote viewer，其 remote asset 被别处（另一台设备 / 一次 sync）删掉后，`presenceChanged` 重算（`MediaBrowserViewerViewController.swift:81`）得到 `onDevice=false, onRemote=false`，底部 badge 仍显示「Remote」。
3. 影响仅为**展示层 stale**：该 item 的 More 菜单会被 `isPresent` 隐藏（不再暴露任何破坏性操作），只是底部 badge 文案短暂不对，grid 一旦 reload 即消失。
4. 彻底修需要给 `MediaPresence` 增加第 4 个「已不存在」态并在 viewer 里区分处理，会牵动整个 presence 模型；评估为纯 cosmetic 低优先级，暂不处理。

## 17. 非独立 MOVE 后端的兼容直传模式，及 version.json 的崩溃天花板

1. 部分云 WebDAV 网关（已确认 123pan）的 MOVE **不独立**：`move(temp→final)` 让 temp 与 final 别名到同一 content blob，删掉 moved-from 的 temp 会连带毁掉 final。直接 PUT（含覆盖）在这类后端上是独立/持久的。
2. 后端能力**运行时探测**（`RemoteMoveIndependenceProbe`）:写 A → `move A→B` → 删 A → GET B;B 仍能按字节读回才判独立(GET 权威,PROPFIND 会撒谎);任何故障/歧义一律 fail-safe 判非独立。`RemoteStorageClientProtocol.resolveMoveIsNonIndependent(basePath:)` 默认 false(SMB/S3/SFTP/外接卷天生独立、不探测),`WebDAVClient` 每会话探一次并 memo。好 WebDAV 探到独立后继续走原子 `temp→MOVE`,性能不受影响。
   - **未来优化点(非正确性)**:memo 是 per-`WebDAVClient` 实例的,不是 run/profile 级。并行备份有连接池(WebDAV 默认 2、上限 4),所以一个 run 最多可能探接近 `connectionPoolSize` 次(每次 ~5 个快操作、几个 RTT),而非只探一次。有界、只在 startup、每 run 一次,对慢 WebDAV 略明显但不是媒体吞吐牺牲。若要优化,可把 MOVE 独立性做成 endpoint 键的 run/profile 级共享缓存;评估为低优先级。
3. 非独立后端上,canonical 一律**直传**(跳过 `temp→MOVE→delete`),且崩溃可恢复:月份 direct PUT 在覆盖 canonical 前先落 durable 恢复 scratch(全新月 → `.tmp`(新字节);覆盖 → `.bak`(旧字节)),验证读回成功后才删;`OrphanCleanupLite` 在兼容模式下用 `download scratch → 校验 → PUT canonical → readback` 修复损坏/缺失的月份 canonical(独立 blob,并在 download 与 PUT 之间重证 ownership),**不**用会别名的 server-side move/copy。V1→Lite 迁移的月份也走直传。
4. **version.json 的天花板(不修，仅记录)**:version.json 直传对"进程崩溃卡在 canonical 半写坏与 in-process 处理之间"**没有**恢复。此时会留下 malformed `version.json`,`RepoFormatRouter` 判 terminal `.damaged`(该分支根本不看 version scratch,`.malformedVersion` 恢复又被 `assertCanonicalVersionSafeToReplace` 挡住,所以 scratch 也救不了)。
   - 不修的理由:version.json 是 ~100 字节一次性 PUT(单个 TCP 段;合规服务器不会用不完整 body 提交对象),仅在建库/迁移后各提交一次,半持久化几乎不可能;它无用户数据、完全可重建;关掉它要同时改 router 判定与放松 version 安全门,对近乎不可能的场景不成比例。
   - 已处理的部分:非崩溃的 upload 失败/读回不符已由 `commitByDirectPut` 的 `removeCanonicalIfMalformed` / `removeProvenBadCanonical` 兜掉(只删证明为坏的,valid 或 inconclusive 一律不删)。
   - 手动恢复:删除 `.watermelon/version.json` 后重连即重建。
   - 对比:月份 manifest 不共享此天花板——它的 sqlite 可达 MB 级、跨多个 TCP 段,半持久化有现实可能,且装真实备份账本,所以那里保留恢复 scratch + cleanup 直传修复是成比例的。

5. **兼容模式下 alias 保护与新 direct-PUT scratch 回收(已按字节区分)**:为防止删到 legacy temp→MOVE 的 alias scratch 连累 canonical,非独立后端上 cleanup 只跳过与 canonical **字节相同**的 valid redundant month scratch(alias 一定字节相同),以及 version / migrate scratch;**字节不同**的 redundant scratch 仍回收(alias 不可能字节不同)。这样新代码 direct-PUT 留下的、在 canonical 前进后已字节不同的 stale `.tmp`,会在下次 cleanup 被清掉,不会长期累积。残留只剩「与当前 canonical 字节相同的当前恢复 scratch 自删故障」这一种,且它一旦 canonical 前进就变字节不同、被回收——**自愈、无数据损坏**。
   - 相关数据损坏点已修:`preferredRecoveryCandidate` 在非独立后端不再"单个 `.tmp` 优先",改按 mtime 取最新,避免旧 `.tmp` 被优先恢复而丢掉更新的 `.bak` 账本(见 `OrphanCleanupLiteTests` 的 `旧 .tmp + 新 .bak + invalid canonical → 恢复 .bak` 回归)。
   - 注:`LeftoverFileScanner`「检查残留文件」只扫数据文件与有 manifest 的月份,**不**清 `.watermelon/months/*.tmp/.bak`;这类 scratch 的回收只走 `OrphanCleanupLite`。
