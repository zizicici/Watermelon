# UI 流程与状态（当前主链路）

## 1. 启动与首页

App 启动后直接进入 `HomeViewController`。

首次启动时（`CompletionGate.hasCompleted == false`），`AppCoordinator` 会以 `.pageSheet` 模态展示 `OnboardingViewController`，内部自带一层 `UINavigationController`，引导完成后回到首页。

### 顶部区域

左右两栏 header：

1. 左侧：`本地相册`
   - 全选 / 取消全选 toggle（本地相册未授权时隐藏）
   - 照片 / 视频 / 体积汇总
   - 长按 / 菜单：可切换 scope（全部 / 指定相册）
2. 右侧：`远端存储`
   - 当前连接名称
   - profile 下拉菜单
   - 全选 / 取消全选 toggle（远端未连接时隐藏）
   - 照片 / 视频 / 体积汇总

右侧连接菜单内容（由 `HomeMenuFactory.buildDestination()` 构建）：

1. `添加存储`：SMB（手动 / Bonjour 发现）、WebDAV、S3、SFTP、外接存储
2. `管理存储` 入口
3. 当前已连接 profile：`当前 Profile 设置` + `断开`
4. 其它已保存 profile 按类型分组（SMB / WebDAV / S3 / SFTP / 外接存储），并以 `name + 显示 URL` 为副标题
5. `ProfileReachabilityService` 标记为 `unreachable` 的 profile 副标题前会带 `离线` 标识

### 左右 overlay

未就绪时，对应一栏会被 overlay 覆盖：

1. 左侧 `本地相册`：未授权时按权限状态显示 `授予访问` 或去设置入口
2. 右侧 `远端存储`：
   - `connecting`：spinner + 当前连接 / 处理文案（如处理月份或扫描索引）
   - `disconnected`：`未连接远端存储` + `选择存储` 按钮
   - `connected`：overlay 隐藏

### 更多页入口

1. 首页右下角有一个悬浮 `ellipsis` 按钮
2. 进入 `MoreViewController`
3. 如果当前没有导航栈，则会自动包一层导航控制器再弹出

## 2. 内容区

1. `UICollectionViewCompositionalLayout`
2. 按年分 section，按月展示 row
3. 每行两个 cell：
   - 左：本地月份
   - 右：远端月份
4. 中间 supplementary badge 显示方向箭头和百分比

箭头方向规则：

1. 只选本地：`→`（上传）
2. 只选远端：`←`（下载）
3. 两边都选：`↔`（同步）

月份 cell 颜色：

1. 1-3 月：绿色
2. 4-6 月：蓝色
3. 7-9 月：琥珀色
4. 10-12 月：红色

## 3. 选择规则

### 可交互条件 (`HomeScreenStore.isSelectable`)

只有在下面五个条件都满足时，月份选择才允许：

1. 已连接远端存储
2. 已授权本地相册访问
3. 当前不在执行态
4. scope 没有正在重载
5. `RemoteMaintenanceController` 没有在跑校验

`isRemoteSelectionAllowed` 还会在 scope 为 “指定相册” 时屏蔽远端侧选择。

### 选择行为

1. 支持单月选择
2. 支持年级 toggle
3. 支持顶部左右全选 toggle
4. 连接状态变化时，已选月份会被清空
5. scope 变更（例如从 “全部” 切到指定相册）也会清空选择

## 4. 底部操作面板（`SelectionActionPanel`）

### 选择态

显示：

1. `备份(→)` 月份数
2. `下载(←)` 月份数
3. `同步(↔)` 月份数
4. `执行` 按钮

分类按钮支持长按菜单查看月份列表。

### 执行态

显示：

1. `备份 / 下载 / 同步` 三类阶段状态
2. `暂停 / 恢复`
3. `停止`
4. 执行结束后显示 `完成`

如果有失败月份，还会带失败汇总菜单。

## 5. 执行前确认

点击 `执行` 后：

1. 统计本次备份 / 下载 / 同步月份数
2. 弹出确认框
3. 用户确认后调用 `store.startExecution(...)`

## 6. 执行阶段

### 6.1 本地索引预检查

执行开始前，首页执行链路会先冻结一次本次任务的设置：

1. `上传并发`
2. `允许访问 iCloud 原件`

然后执行前置检查：

1. 只有在下载 / sync 需要完整本地索引，或 `允许访问 iCloud 原件` 未关闭时，才对本次涉及的本地 asset 做离线 hash 预检查 (`buildIndex(allowNetworkAccess: false)`)，默认 2 个 worker；upload-only 且 iCloud 原件关闭时会跳过
2. 预检查中，cache-hit 资产会额外做一次轻量离线可用性探测：命中 iCloud-only 的话会被标成 `unavailable`，保证已被系统回收的资产能被识别出来
3. 第一轮结束后，若启用了 `允许访问 iCloud 原件` 且 **上传范围** (`upload + sync` 月份) 内存在 `unavailableAssetIDs`，本次 upload 自动改为 `1` 个 worker
4. 如果本次包含下载或同步，且第一轮仍有 `unavailableAssetIDs`：
   - 启用 `允许访问 iCloud 原件`：只对这些资产再做一次联网补索引，worker 固定为 `1`
   - 未启用：直接失败并弹窗
5. 若补索引后仍不完整，也会继续失败并弹窗

### 6.2 上传阶段

1. `HomeExecutionCoordinator` 通过 `BackupSessionController` 驱动通用上传链路
2. 月份进入 `uploading`
3. 处理进度会写入 `processedCountByMonth`
4. 月份 flush 完成后进入：
   - 上传-only 月份：`completed`
   - sync 月份：`uploadDone`
5. 若 V2 commit 已 durable 但 snapshot 写失败，月份记录为 partial warning，同时 upload work 已关闭；paused final flush 不会发这个完成信号

### 6.3 同步月份内联下载

sync 月份在上传 flush 后会立刻做该月下载收尾：

1. 先同步远端快照
2. 刷新该月本地索引
3. 调 `backupCoordinator.verifyMonth(...)` 校验该月远端 metadata / 物理文件；如果校验改动了远端，再同步一次远端快照
4. 只下载 `remoteOnlyItems` 中 `isRestorable == true` 的项；incomplete / fingerprint mismatch / metadata-only 等会被跳过并计入 partial
5. 每个 item 保存到相册后，`RestoredAssetFingerprintVerifier` 会重建并验证 durable fingerprint binding；验证成功才刷新本地索引

所有可恢复 item 下载且 fingerprint 校验通过后，该月变为 `completed`；如果存在跳过的 incomplete 项或 fingerprint 未 durable，则标记为 `downloadIncomplete` / partial，而不是 completed。

### 6.4 纯下载阶段

上传阶段结束后，剩余 `download` 月份会按顺序执行：

1. 同步远端快照
2. 刷新本地索引
3. `verifyMonth(...)`；如果校验产生远端变更，再同步一次远端快照
4. 下载 `remoteOnlyItems` 中可恢复的项，并对每个恢复结果做 durable fingerprint 校验
5. 完全成功后标记该月 `completed`；有不可恢复项或校验失败时标记 partial / `downloadIncomplete`

## 7. Cell 执行态样式

`MonthPlan.Phase` 与视觉对应：

1. `pending` — 正常样式
2. `uploading / downloading` — 正常底色 + spinner
3. `uploadPaused / downloadPaused` — 正常底色 + 暂停标记
4. `uploadDone` — 仍按运行中样式显示，等待 sync 下载完成
5. `completed` — 灰底 + 绿色勾
6. `partiallyFailed` — 运行态底色 + warning 指示
7. `failed` — 失败样式

`partiallyFailed` 只表示已有用户可见 warning；它不保证该月所有 upload/download work 已关闭。

## 8. 进度规则

### 上传

箭头百分比取：

1. `BackupSessionController` 回传的 session 进度
2. `matchedCount` 基线进度（本地月聚合 `backedUpCount`，按 fingerprint 匹配本地和远端）

二者中的较大值，保证百分比单调前进。

### 下载 / 同步下载

1. 直接依赖 `matchedCount`（本地月聚合 `backedUpCount`）
2. 每个 item 下载且 durable fingerprint 校验通过后刷新本地索引，因此进度按已验证 item 推进

## 9. 暂停 / 恢复 / 停止

### 暂停

1. 上传阶段：请求 backup pause
2. 下载阶段：取消下载 task，并把月份状态切为 paused

### 恢复

1. 已完成月份不会重跑
2. sync 月份若已上传但未下载完，会从下载态继续
3. resume 沿用启动时冻结的 `上传并发 / 允许访问 iCloud 原件`

### 停止

1. 弹确认框
2. 停止后退出执行态
3. 用户需要重新选择月份再执行

## 10. 辅助页面

均位于 `Watermelon/Home/` 与 `Watermelon/UI/`：

1. `LocalAlbumPickerViewController` — 把本地图库 scope 切换为指定相册
2. `LocalAlbumDetailViewController` / `LocalAlbumGridSupport` — 单相册网格预览
3. `LocalIndexViewController` — 本地索引状态、覆盖率、`重建索引` 入口（走 `LocalIndexBuildCoordinator`）
4. `DuplicatesViewController` — 按 fingerprint 展示重复资产（依赖本地索引）
5. `FocusModeViewController` — 执行态全屏遮罩，关 idle timer
6. `HomeExecutionLogViewController` / `ExecutionLogHistoryViewController` / `ExecutionLogEntryCell` — 当前 / 历史日志查看
7. `RemoteIncompleteAssetsViewController`（`UI/Auth/`）— 校验出的不完整远端资产明细

## 11. More 页面

入口：

1. 首页右下角 FAB

当前自定义项（`WatermelonMoreDataSource`）：

1. `通用` → 系统语言入口
2. `远端存储` → `管理存储`
3. `备份` →
   - `上传并发`
   - `允许访问 iCloud 原件`
4. `后台备份` → 后台备份入口（Pro）与后台节点计数入口
5. `画中画进度` →
   - `画中画进度`（Pro）
   - 当 PiP 进度处于开启且持有 Pro 时，再露出 `画中画提示音`
5. `诊断` →
   - `执行日志历史`（`ExecutionLogHistoryViewController`）
   - DEBUG 构建额外露出 `Test Crash`

再叠加 MoreKit 自带的 `membership / contact / appjun / about` 段落。

完成一次执行后，若距 DB 创建已满 7 天，会通过 `RatingPromptService.requestReviewIfEligible(in:)` 调用 `AppStore.requestReview(in:)` 请求系统评价框。
