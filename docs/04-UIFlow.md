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
2. 右侧：`节点`
   - 当前连接名称
   - profile 下拉菜单
   - 全选 / 取消全选 toggle（节点未连接时隐藏）
   - 照片 / 视频 / 体积汇总

右侧连接菜单内容（由 `HomeMenuFactory.buildDestination()` 构建）：

1. `新增节点`：SMB（手动 / Bonjour 发现）、WebDAV、S3、SFTP、外接存储
2. `管理节点` 入口
3. 当前已连接 profile：`当前节点设置` + `断开连接`
4. 其它已保存 profile 按类型分组（SMB / WebDAV / S3 / SFTP / 外接存储），并以 `name + 显示 URL` 为副标题
5. `ProfileReachabilityService` 标记为 `unreachable` 的 profile 副标题前会带 `离线 | ` 标识

### 左右 overlay

未就绪时，对应一栏会被 overlay 覆盖：

1. 左侧 `本地相册`：未授权时显示 `授予访问` 按钮
2. 右侧 `节点`：
   - `connecting`：spinner + 进度文案，随 `RemoteSyncProgress.Kind` 变化：
     - `scanningRemoteIndex`：`扫描远端索引...`
     - `remoteIndex`：`处理远端月份 N / M`
     - `repoUpgrade`（V1→Lite 资料库升级）：按 `RepoUpgradePhase` 切文案 —— `copying` `正在升级远端资料库 N / M 个月`、`validating` `正在校验远端资料库 N / M 个月`、`finalizing` `正在提交远端资料库...`、`cleaning` `正在清理旧版残留文件 N / M 个月`（收尾 orphan cleanup 阶段 `total == 0`，回退为 `正在清理旧版残留文件...`）；各计数阶段 `total == 0` 时回退为不带计数的文案
   - `disconnected`：`未连接节点` + `选择存储` 按钮（code key `home.overlay.selectStorage`）
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
3. 两边都选：`↔`（互补）

月份 cell 颜色：

1. 1-3 月：绿色
2. 4-6 月：蓝色
3. 7-9 月：琥珀色
4. 10-12 月：红色

## 3. 选择规则

### 可交互条件 (`HomeScreenStore.isSelectable`)

只有在下面五个条件都满足时，月份选择才允许：

1. 已连接节点
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
3. `互补(↔)` 月份数
4. `执行` 按钮

分类按钮支持长按菜单查看月份列表。

### 执行态

显示：

1. `备份 / 下载 / 互补` 三类阶段状态
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

正式上传前，首页执行链路会先冻结一次本次任务的设置：

1. `上传并发`
2. `允许访问 iCloud 原件`

然后执行前置检查：

1. 对本次涉及的所有本地 asset 做离线 hash 预检查 (`buildIndex(allowNetworkAccess: false)`)，默认 2 个 worker
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

### 6.3 同步月份内联下载

sync 月份在上传 flush 后会立刻做该月下载收尾：

1. 先同步远端快照
2. 刷新该月本地索引
3. 只下载 `remoteOnlyItems`
4. 每个 item 成功后立即写 hash 索引并刷新本地索引

完成后该月变为 `completed`。

### 6.4 纯下载阶段

上传阶段结束后，剩余 `download` 月份会按顺序执行：

1. 同步远端快照
2. 刷新本地索引
3. 下载 `remoteOnlyItems`
4. 完成后标记该月 `completed`

### 6.5 写入锁冲突提示

执行前 Repo 路由若检测到锁冲突，经执行日志 / 弹窗呈现本地化文案：

1. 其它设备正在写入（`lockConflict`）：`另一台设备正在写入此远端备份。`
2. 本机上一次会话仍占用（`ownLockConflict`）：`这台设备暂时还不能安全接管上一次备份。原因：<原因>。请稍后重试。`；能给出重试时间时改为 `这台设备稍后应该就能继续上一次备份。原因：<原因>。请等到 <时间> 之后再试。`。`<原因>` 取自四种：上一次写入锁仍在安全保护窗口内 / 远端锁缺少可靠时间戳 / 远端锁在确认期间发生变化 / 无法确认远端锁的归属。

## 7. Cell 执行态样式

`MonthPlan.Phase` 与视觉对应：

1. `pending` — 正常样式
2. `uploading / downloading` — 正常底色 + spinner
3. `uploadPaused / downloadPaused` — 正常底色 + 暂停标记
4. `uploadDone` — 仍按运行中样式显示，等待 sync 下载完成
5. `completed` — 灰底 + 绿色勾
6. `partiallyFailed` — 运行态底色 + warning 指示
7. `failed` — 失败样式

## 8. 进度规则

### 上传

箭头百分比取：

1. `BackupSessionController` 回传的 session 进度
2. `matchedCount` 基线进度（本地月聚合 `backedUpCount`，按 fingerprint 匹配本地和远端）

二者中的较大值，保证百分比单调前进。

### 下载 / 同步下载

1. 直接依赖 `matchedCount`（本地月聚合 `backedUpCount`）
2. 每个 item 下载成功后立即刷新本地索引，因此进度按 item 推进

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

1. `通用` → `语言`
2. `节点` → `管理节点`
3. `备份` → `上传并发` / `允许访问 iCloud 原件`
4. `备份到电脑` → `限速`（默认开启；关闭需要 Pro；启用时 Browser Link 上传与下载共用 1 MB/s 上限，下次连接生效）
5. `自动备份` → `后台自动备份`（Pro） / `自动备份节点`（已启用 / 可用节点计数）
6. `画中画` → `画中画进度`（Pro）；开启且持有 Pro 时再露出 `声音`
7. `诊断` → `诊断日志`（跳转 `ExecutionLogHistoryViewController`）；DEBUG 构建额外露出 `Test Crash (Debug)`

再叠加 MoreKit 自带的 `membership / contact / appjun / about` 段落。

完成一次执行后，若距 DB 创建已满 7 天，会通过 `RatingPromptService.requestReviewIfEligible(in:)` 调用 `AppStore.requestReview(in:)` 请求系统评价框。
