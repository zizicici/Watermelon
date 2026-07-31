# 当前风险点 / 技术债（按当前实现）

## 1. 自动化仍未覆盖真实媒体与远端链路

1. `WatermelonTests` 已覆盖 Home 端的引擎（`HomeLocalIndexEngine` / `HomeRemoteIndexEngine`）、`HomeDataProcessingWorker`、`HomeRefreshScheduler`、`HomeScopeController` / `HomeScopeNormalizer`、`HomeSelectionController`、`HomeSectionBuilder`、`HomeHeaderSummaryFormatter`、`RemoteFileNaming`、`WriteLockService`、`OrphanCleanupLite` 等纯逻辑单元。
2. `HomeExecutionCoordinator`、`BackupParallelExecutor`、`AssetProcessor`、`RestoreService`、连接切换竞态 / 暂停恢复 / sync 月份内联下载 / 外接存储拔出等真正涉及相册或远端的链路 **仍然没有完整自动化覆盖**。Mac 外接文件夹的连接、真实索引加载，以及预置 Lite 仓库后的手动下载校验与执行已贯通真实 `BackupCoordinator`；最终 PhotoKit 导入仍由测试 seam 代替，也不包含物理卷异常。
3. `WatermelonMacTests` 已作为 macOS 宿主测试 target 接入 `WatermelonMac` scheme，220 项测试覆盖退出与窗口生命周期、执行期间防止系统空闲睡眠及终态释放、结果页期间后续任务的退出门禁、媒体浏览器与重复照片删除写入边界的取消门禁、媒体浏览器取消后对已提交删除的状态协调、旧版迁移取消/失败后的部分进度与远端重载门控、远端缩略图按需加载、维护会话归属、排队取消策略和清理的取消/失败终态、仓库维护窗口的目标、generation 与凭据会话归属、迟到停止确认的终态保护与安全停止幂等性、列表式精简引导页、设置实时可用状态、StoreKit 商品与购买生命周期、目的地修改门控、凭据身份规范化、凭据缺失时的编辑恢复、连接编辑字段策略、WebDAV 测试连接基础路径、连接凭据提示的过期尝试与成功后持久化门控、SFTP 编辑期间的实时主机密钥刷新、已连接节点的会话刷新、Home 远端快照的 profile 与 generation 所有权门禁、浏览器初始会话的原子捕获、备份确认后的会话门控、远端预览读取归属与照片授权转换、Help 与 Tools 菜单、Home 授权/远端空状态与缺失侧零计数、媒体浏览器动作矩阵、执行前不完整项目扫描签名、残留清理的 manifest 月份枚举与读取故障分类、恢复量旁路估算的取消与过期结果隔离、同步月份在上传提交后的恢复计划拆分、月份执行状态与进度投影，以及 Pause / Stop / 可恢复故障 / 致命故障的处置矩阵；其中 4 项使用一次性目录：一项贯通 Mac 外接文件夹 profile 保存、security-scoped bookmark、客户端工厂及上传/列举/下载/删除；另一项贯通 `MacRemoteConnectionController`、真实远端索引加载、AppSession 激活与断开清理；第三项预置真实 Lite 仓库，贯通真实索引、批量校验计划和 `MacBackupExecutionController` 手动下载，并验证 Stop 会取消恢复、释放校验会话及 execution lease，随后能再次执行；第四项贯通真实完整校验、验证时间写回、干净仓库残留扫描及维护 execution lease 释放。最终 PhotoKit 导入边界使用记录型替身。另 4 项直接驱动 `MacBackupExecutionController`，覆盖上传和下载阶段的暂停、继续、停止、暂停后会话失效、日志收尾与 execution lease 释放，并验证同步月份的上传已提交后 Resume 只重跑下载。真实上传 core、PhotoKit、物理外接卷和网络后端写入仍不在端到端测试范围内。
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
2. 全局默认可手动指定 `1...4`；节点可覆盖到 `1 / 2 / 3 / 4 / 6 / 8 / 10 / 12 / 16 / 20 / 24`
3. iCloud-only 资产存在时上传会被强制单 worker
4. 目前没有根据带宽、远端 RTT、失败率动态调节 worker 数
5. SFTP 多 worker = 多 SSH 会话；遇 sshd `MaxStartups` / `MaxSessions` 紧配置需要回落到 1，目前没有自动探测
6. 高并发可能触发远端限流，或因网络、CPU、磁盘竞争而降低速度，目前没有自动性能回退
7. 备份前 Lite manifest 下载与上传使用同一并发数；高档位会建立更多数据连接，目前没有按 manifest 大小或连接成本单独自适应

## 8. 下载取消粒度仍是 item 级

1. `RestoreService.restoreItems(...)` 在 item 循环边界检查取消。
2. 一个 item 内部若包含多资源（如 Live Photo），中断时仍可能丢掉该 item 的部分临时进度。
3. 不过成功完成的 item 会立即写回 hash 索引，所以下次能跳过整 item。

## 9. macOS 已进入完整备份端阶段，但仍缺生产回归

1. `WatermelonMac/` 已复用共享备份、恢复、写锁、索引、维护和 OneDrive core，支持月份计划、浏览器单项/批量操作、暂停恢复及全部存储类型。
2. 现阶段主要风险已从“功能缺失”转为“真实环境未覆盖”：Mac 手动执行 controller 的上传、下载与同步提交边界已有中等粒度自动化，外接文件夹连接、真实索引 core 和直接下载校验计划也已贯通；但真实上传 core、PhotoKit、MSAL、发布签名沙盒 bookmark 和网络后端写入的组合仍没有端到端自动化。
3. Mac 不提供计划或后台自动备份；手动任务在主窗口最小化后继续运行，并在实际运行、暂停、恢复或安全停止期间阻止系统因空闲进入睡眠。完成或失败结果仍保留到用户确认，但日志收尾后立即释放 execution lease 和防睡眠活动；关闭最后一个窗口则进入正常退出流程并安全停止仍在活动的任务。
4. Mac 媒体浏览器不持久化原片或缩略图缓存，预览使用内存与临时文件。它没有 iOS 缓存大小设置，这是平台实现差异而非遗漏。
5. OneDrive 发布前必须在 Entra 中注册并验证 `com.zizicici.watermelon-mac` 与 `msauth.com.zizicici.watermelon-mac://auth`，不能以 iOS 真机登录结果替代。
6. iOS 免费版只允许一个存储目标，并通过 MoreKit 的终身会员商品解锁更多节点；MoreKit 当前只声明 iOS 平台。Mac 设置已使用 StoreKit 2 提供同一商品 ID 的查询、购买和恢复入口，但 App Store Connect 尚未确认 Mac bundle 的商品归属与跨平台权益，因此暂不启用单节点门槛。发布前必须用签名构建验证商品可购买、可恢复且与 iOS 权益一致。

## 10. SFTP 后端的已知限制

1. **依赖**：Citadel `0.12.1`，传递依赖 `swift-nio-ssh` 来自 `Wellz26/swift-nio-ssh` fork（非 Apple 官方 repo），属于供应链层面的事实声明。
2. **Citadel 0.12.1 目录句柄泄漏**：`listDirectory` 会泄漏服务端 fd。`SFTPClient` 每 32 次 `list` 整体重连一次以释放句柄，重连一次约 200–500 ms。Citadel 升级后应去掉。
3. **两阶段 TOFU 要双重 SSH 握手**：保存 SFTP profile 时先用空凭证连一次取主机指纹再 abort、用户确认后再用钉住的指纹真正连一次走 `verifyBasePathWritable`。Citadel 没有公开 hook 在 host-key 阶段把通道 hand off 给后续 user-auth；除非 vendor 一份 Citadel/`swift-nio-ssh`，无法消除。一次性保存路径的 1–2s 开销，不在 hot path。
4. **私钥类型**：仅 OpenSSH ed25519 / RSA。ECDSA 等其它类型在 `makeAuthenticationMethod` 抛 `SFTPUnsupportedKeyTypeError`（用户面文案带类型名）。
5. **`copy()` 走本地中转**：SFTP v3 没有 server-side copy verb，`SFTPClient.copy` 落本地临时文件再上传；备份热路径不调用，`MonthManifestStore` 的 `.bak` dance 用 `move` + `delete`。

## 11. macOS Swift 6 迁移边界

1. `WatermelonMac` 已与 iOS 一致配置 `SWIFT_DEFAULT_ACTOR_ISOLATION = nonisolated`，避免把共享备份、数据库和网络层隐式拉到主线程。
2. AppKit 入口和 controller 通过显式 `@MainActor` 或 AppKit 类型继承保持主线程隔离；无状态 value type、锁保护 helper 与 request state 保持非隔离。
3. `WatermelonMac` 与 `WatermelonMacTests` 已切到 Swift 6 language mode；iOS App 与 `WatermelonTests` 暂时保持 Swift 5。Mac clean Debug/Release 构建和 220 项宿主测试通过；iOS Debug 构建及 1658 项测试也通过（0 失败，2 跳过）。
4. Citadel、Foundation/AppKit 回调与 AVFoundation 对象只在明确的锁、actor 或立即快照边界上使用窄范围 `@preconcurrency` / `@unchecked Sendable` 适配；真实相册与远端压力场景仍需发布前回归。

## 12. 建议优先级

1. 继续补 `HomeExecutionCoordinator` / `BackupCoordinator` 的中等粒度集成测试，重点覆盖真实 core 协作与连接中断；Mac controller 的上传、直接下载、sync 月份提交后仅恢复下载及会话丢失已先覆盖。
2. 评估为 full run 持久化 pending 集，减少恢复时重扫。
3. 评估复用 iCloud recovery 结果到上传阶段，降低 iCloud-only 资源的重复 I/O 成本。
4. 评估按失败率和吞吐量自适应调整 worker 数。
5. 对 Mac 的真实 PhotoKit、OneDrive 和全部远端后端执行发布回归。
6. 继续扩展 Mac controller、调度状态与并发边界测试 seam。
7. 关注 Citadel 上游修复目录句柄泄漏，移除 `listReconnectThreshold` 重连。

## 13. 首次抢锁的原子性依赖后端条件写

1. 远端写锁获取走 `RemoteUploadMode.createIfAbsent` 原子创建：SMB 用 fork `zizicici/AMSMB2` 的 `uploadItem(overwrite:)`、SFTP 用 `.forceCreate`，由协议层保证原子。`LocalVolumeClient` 不创建远端锁，只走 app-wide mutex 下的专用本地入口。
2. S3 / WebDAV 走 `If-None-Match: *` 条件 PUT。若 S3 兼容后端（部分 MinIO / Ceph / 旧实现）忽略该头，并发首次抢锁可能两端都判成功，原子性退化为非原子——这是该后端的能力上限，App 侧无法消除。

## 14. 手动残留文件清理只覆盖「有 manifest 的月份」

> 代码与用户文案统一用「残留文件 / leftover」（不与既有的元数据清理 `OrphanCleanupLite` 混用 orphan 一词）。入口在节点详情页独立 section「检查残留文件」，走 `LeftoverCleanupViewController` 模态（扫描→评审→删除→汇总，执行中禁止 dismiss + Stop）。

1. 该功能 (`LeftoverFileScanner` + `BackupRunPreparation.scanLeftoverFiles/deleteLeftoverFiles`) 只枚举 `.watermelon/months/<YYYY-MM>.sqlite` 能解析出的月份——即至少 flush 过一次 manifest、可证明归本 App 管理的月份。这是刻意的安全取舍：没有对应 manifest 的 `YYYY/MM` 目录无法证明是本 App 创建的，**绝不**当作我们的来删（`manifestSnapshots == nil ⇒ 整月跳过、删 0`）。
2. **覆盖缺口**：前台备份默认 `incrementalFlushInterval == nil`，manifest 只在月末 flush 一次。一个**从未成功备份过**的全新月份若在首次月末 flush 之前被中断（崩溃 / OOM / 强退 / 断电），会留下满是数据文件、但无 manifest 的 `YYYY/MM` 目录——这种残留对本功能不可见，扫描会报「未发现」。后台备份走增量 flush（每 10 个），manifest 出现较早，缺口更小；月份一旦完整跑过一次即被纳入覆盖。
3. 现状以文案沟通该范围：详情页 section footer 与模态评审页 footer 均说明「仅列出本 App 管理的月份」，空状态文案为「在本 App 管理的月份中未发现残留文件」。彻底覆盖需要枚举无 manifest 的 `YYYY/MM` 数据目录并作为「无法证明归属」的单独类别呈现，属后续工作，不在本次范围。
4. 月份枚举与 manifest 快照读取已提取为 `LeftoverManifestMaintenance`。5 项 Mac 宿主测试覆盖只接受规范月份文件、月份目录不存在返回空、manifest 不存在返回 nil，以及存储失效时枚举和读取均抛错并 fail-closed；真实远端的故障分类仍需集成回归。
5. 扫描在同一 maintenance lease 内用有界连接池并行处理月份；每份 manifest 只下载一次，同时提取数据文件名、资源 hash 元数据与缩略图 fingerprint。资源元数据按月写入本次维护专用的临时 SQLite catalog，不把全库资源和多份匹配索引常驻内存；评审结束后自动删除。缩略图根目录预扫描后，各 shard 也走同一连接池并行 LIST。UI 分别显示月份、缩略图 shard 和收尾阶段，不把不同单位合并成一个总数。
6. 评审页默认用同一次扫描取得的已知大小、文件名与时间做低成本初判：大小必须明确且相同，再满足「属于同一个 App 冲突文件名家族（原名及无前导零的 `_1`、`_2` 等后缀）」或「远端修改时间与资源创建时间相差不超过 3 秒」之一，才显示为高概率雷同。时间证据只用于会把远端 mtime 写成拍摄时间的后端；候选查询走临时 catalog 的大小 / 名称与大小 / 时间索引，不做全量两两比较，也不额外读取 manifest。
7. 原文件下载与 SHA-256 比较放在评审列表末尾，作为高级可选检查；它可只作用于已勾选的数据文件，算出的 hash 直接查询同一个临时 catalog，精确匹配结果会替代初判结果。删除仍在新的 maintenance lease 下重新验证。

## 15. 非独立 MOVE 后端的兼容直传模式，及 version.json 的崩溃天花板

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
