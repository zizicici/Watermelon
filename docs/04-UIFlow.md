# UI 流程与状态（当前主链路）

## 1. 启动与首页

App 启动后直接进入 `HomeViewController`。首页包含备份与 Drop Mode 两种常驻形态；默认显示备份，切换形态不会销毁或清空 Drop 会话。

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

1. `新增节点`：SMB（手动 / Bonjour 发现）、WebDAV、S3、SFTP、OneDrive、Dropbox、Google Drive、外接存储
2. `管理节点` 入口
3. 当前已连接 profile：`当前节点设置` + `断开连接`
4. 其它已保存 profile 按类型分组（SMB / WebDAV / S3 / SFTP / OneDrive / Dropbox / Google Drive / 外接存储），并以 `name + 显示 URL` 为副标题
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

### Drop Mode 首页形态

1. 备份首页右下角的主题色纸飞机与 More 按钮同为 48×48，首次进入前展示一次性教程，之后由 `HomeViewController` 原地切换到 Drop 形态，不 push 新页面，也不使用 sheet 或 navigation bar
2. Drop 使用与备份首页相同高度、间距和配色的左右双 Header：左侧为“本地相册”，下方“选项”打开四个独立子菜单，每项再选择“启用 / 关闭”；右侧为“文件”，下方“选择文件”直接打开系统文件选择器。旧的右上角 Filter 与文件按钮不再显示，发送时仍合并图片与文件为同一批项目
3. 图片沿用本地媒体网格、详情查看和滑动多选；文件通过系统文件选择器复制到本次 Drop 会话的临时目录，不再提供独立文件页面，由底部 Panel 的文件摘要 Popover 查看和移除
4. Panel 右侧发送入口只显示纸飞机，尺寸和样式与备份首页的开始按钮一致；免费用户超过每次 3 项的限制时按钮不可用且使用灰色样式，Pro 用户不受此限制
5. 图片 Filter 只影响 PhotoKit 资源导出；普通文件按原字节和原文件名投递。两类内容都写入所选节点的 `Inbox`，重名按序号保留，不做备份去重
6. Drop 可以在未授权相册或未连接节点时进入；真正选择目的地及发送时才检查全局执行、维护、连接和本地索引互斥
7. 投递或文件导入期间不能切回备份形态；空闲时使用 Home 共用的形态切换按钮返回备份，已选图片和文件继续保留
8. Drop 未选择内容时不显示底部 Panel；选中内容后，Panel 与首页一样通过屏幕外/贴底约束滑入，内容区和右下角 FAB 跟随其顶边收缩。选择态按图片、视频、文件分别显示图标、数量和预计传输体积，摘要按内容宽度布局并可横向滚动；点击分类会打开由 Navigation Controller 承载的 grouped Popover 列表，条目使用系统 subtitle cell 和动态 grouped 配色；图片、视频显示缩略图，点击进入详情，从右向左滑动可取消选中，右上角“清除”可取消该类全选；运行态的分类计数、日志文本以及 72×36 暂停、继续和停止按钮沿用主页 Panel 样式；成功后保留“执行完毕”和绿色勾，用户点击勾后才清空本次选择并收起 Panel

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

### 媒体浏览器

点击年份标题或月份 cell 的非复选区域会打开 `MediaBrowserGridViewController`。完整图库依次提供本地、合并、远端三种模式；相册详情只提供本地模式。点开项目进入全屏查看器，其底部“信息”入口以默认深色的 sheet 展示文件概览和按 `EXIF / TIFF / GPS / IPTC / HEIF / Apple` 分组的原始元数据；字段和值采用 iOS value-cell 布局，长按任一值可复制。本地项目通过 PhotoKit 读取原始资源；远端项目按需物化照片或视频原件，并沿用浏览器的缓存、完整性校验和临时文件清理规则。

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

1. `并发数`（节点覆盖优先，否则使用全局默认值）
2. `允许访问 iCloud 原件`

然后执行前置检查：

1. 对本次涉及的所有本地 asset 做离线 hash 预检查 (`buildIndex(allowNetworkAccess: false)`)，默认 2 个 worker
2. 预检查中，cache-hit 资产会额外做一次轻量离线可用性探测：命中 iCloud-only 的话会被标成 `unavailable`，保证已被系统回收的资产能被识别出来
3. 如果本次包含下载或同步，且第一轮仍有 `unavailableAssetIDs`：
   - 启用 `允许访问 iCloud 原件`：只对这些资产再做一次联网补索引，worker 固定为 `1`
   - 未启用：直接失败并弹窗
4. 若补索引后仍不完整，也会继续失败并弹窗

预检查不再决定上传并发数——需要联网导出的资产由上传阶段自己识别并放到 iCloud 趟处理。

### 6.2 上传阶段

1. `HomeExecutionCoordinator` 通过 `BackupSessionController` 驱动通用上传链路
2. 月份进入 `uploading`
3. 处理进度会写入 `processedCountByMonth`
4. 上传分本地、iCloud 两趟。本地趟 flush 完成后：
   - 该月还有资源需要联网导出：`localUploadDone`，等第二趟再收尾
   - 否则上传-only 月份进入 `completed`，sync 月份进入 `uploadDone`
5. iCloud worker 领取月份时，该月从 `localUploadDone` 回到 `uploading`；flush 完成后再按第 4 条的后两种结果收尾

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
4. `localUploadDone` — 仍按运行中样式显示，但不转 spinner，表示已提交本地趟、等待 iCloud worker；领取后回到 `uploading`
5. `uploadDone` — 仍按运行中样式显示，等待 sync 下载完成
6. `completed` — 灰底 + 绿色勾
7. `partiallyFailed` — 运行态底色 + warning 指示
8. `failed` — 失败样式

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

1. 上传阶段：请求 backup pause；当前 asset 完成并 flush 后进入 paused
2. 下载阶段：请求 download drain；当前 remote asset 完成导入与索引写回后，把月份状态切为 paused
3. `pausing` 期间禁止 Resume；upload paused 终态的 sync 待恢复范围完成反标后才解除该门禁
4. 若当前 asset 的网络 attempt 已失败，则直接停止恢复重试；若完成的是最后一个剩余 asset，则 UI 直接进入 completed，不短暂落到 paused

### 恢复

1. 已完成月份不会重跑
2. sync 月份若已上传但未下载完，会从下载态继续
3. resume 沿用启动时冻结的 `并发数 / 允许访问 iCloud 原件`

### 停止

1. 弹确认框
2. 当前 asset 完成后退出执行态
3. `pausing` 中点击 Stop 会升级现有 drain intent；当前 asset 不会被硬取消，但尚未进入 asset 的 preflight 会立即取消
4. 用户需要重新选择月份再执行

## 10. 辅助页面

均位于 `Watermelon/Home/` 与 `Watermelon/UI/`：

1. `LocalAlbumPickerViewController` — 把本地图库 scope 切换为指定相册
2. `LocalAlbumDetailViewController` / `LocalAlbumGridSupport` — 单相册网格预览
3. `LocalIndexViewController` — 本地索引状态、覆盖率、`重建索引` 入口（走 `LocalIndexBuildCoordinator`）
4. `DuplicatesViewController` — 按 fingerprint 展示重复资产（依赖本地索引）
5. `FocusModeViewController` — 执行态全屏遮罩，关 idle timer
6. `HomeExecutionLogViewController` / `ExecutionLogHistoryViewController` / `ExecutionLogEntryCell` — 当前 / 历史日志查看
7. `RemoteIncompleteAssetsViewController`（`UI/Auth/`）— 校验出的不完整远端资产明细
8. `LeftoverCleanupViewController`（`UI/Auth/`）— 扫描残留文件、按需下载核对 SHA；经完整本地 / 远端去重后可选择删除，或将照片、视频及 content identifier 唯一配对的 Live Photo 加入远端 manifest

## 11. More 页面

入口：

1. 首页右下角 FAB

当前自定义项（`WatermelonMoreDataSource`）：

1. `通用` → `语言`
2. `节点` → `管理节点`；节点详情可将 `并发数` 设为继承全局默认、显式自动或 `1 / 2 / 3 / 4 / 6 / 8 / 10 / 12 / 16 / 20 / 24`，并提示并发过高可能触发限流或反而降低速度
3. `备份` → `默认并发数` / `允许访问 iCloud 原件`
4. `备份到电脑` → `传输速度`（默认选择 `标准（1 MB/s）`，包括 Pro 用户；`不限速` 需要 Pro；上传与下载共用速度上限，下次连接生效）
5. `自动备份` → `后台自动备份`（Pro） / `自动备份节点`（已启用 / 可用节点计数）
6. `画中画` → `画中画进度`（Pro）；开启且持有 Pro 时再露出 `声音`
7. `诊断` → `诊断日志`（跳转 `ExecutionLogHistoryViewController`）；DEBUG 构建额外露出 `Test Crash (Debug)`

再叠加 MoreKit 自带的 `membership / contact / appjun / about` 段落。

完成一次执行后，若距 DB 创建已满 7 天，会通过 `RatingPromptService.requestReviewIfEligible(in:)` 调用 `AppStore.requestReview(in:)` 请求系统评价框。
