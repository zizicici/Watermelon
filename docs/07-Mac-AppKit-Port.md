# macOS AppKit 版本

## 定位与安全边界

`WatermelonMac` 已从遗留迁移工具演进为完整照片备份端。它保留旧数据迁移入口，同时使用 AppKit 提供 Home、目的地管理、媒体浏览、设置和维护界面。备份格式、fingerprint、manifest、写锁、恢复校验、PhotoKit/hash-index core 与 iOS 共用。

代码级功能覆盖已经接近 iOS 主链路，但真实照片图库和远端后端仍只能手工回归。发布前只可使用可丢弃图库与测试目的地验证写入。

## 已实现能力

- AppKit 生命周期、主菜单、`NSWindowController`、Local/Remote 双栏首页和辅助窗口复用。
- PhotoKit 读写授权、全库或多相册 scope、按月索引、增量变更、照片/视频/大小统计。Mac 首页复用 iOS 的相册范围归一化和 fingerprint freshness 规则：已选相册被删除时静默保留仍存在的相册或回退到整个图库；照片编辑后立即撤销过期的已备份匹配，并在无执行任务时分批重算，执行结束会继续此前排队的校验。
- SMB、WebDAV、S3、SFTP、外接文件夹与 OneDrive 的添加、编辑、凭据保存和连接；远端目的地的新建与编辑统一使用规范化连接身份和 v2 凭据引用，不再因入口不同生成两份 Keychain 记录。Mac 后台探测已保存目的地，并在目标选择菜单标出离线节点。
- SFTP TOFU/host-key 变更确认、OneDrive MSAL 登录、外接文件夹 security-scoped bookmark。
- 本地/远端月份合并；单月、全年与全库选择；上传、下载和双向同步计划。
- 离线优先 hash preflight、可选 iCloud 原片恢复、动态 worker 约束。
- 共享 `BackupCoordinator`、Lite transition、write lock、manifest flush、逐月同步下载和安全 Stop。
- 真正的 Pause/Resume；已提交月份不会重跑。
- 共享 `RestoreService`，包含 size/hash 校验、PhotoKit 导入与即时本地索引写回。
- AppKit 媒体浏览器与 iOS 一致使用“本地 / 全部 / 远端”三个模式，按月份展示完整时间线；从 Home 的年份或月份左右区域进入时会定位对应月份和模式，点击相册主体则进入该相册的本地浏览器。全库查询在后台执行，并随 PhotoKit、当前连接、远端快照和执行生命周期实时刷新。浏览器支持预览、Live Photo、视频、分享和缩放；单项信息面板复用 iOS 的 PhotoKit/远端原件读取、文件概览与 EXIF/TIFF/GPS/IPTC/HEIF/Apple 分组解析，值可按 Mac 原生方式选择复制。
- 浏览器单项与批量备份、恢复和删除沿用 iOS 的动作判定：批量备份只接受全为本地独有的选择，批量下载只接受全为远端独有的选择，批量删除按当前视图删除每个项目实际存在的副本；恢复与删除前重新按当前连接归属解析远端项目，不使用打开窗口时的过期完整性状态。
- 共享远端缩略图 sidecar：新备份生成、浏览器读取、补齐与清除。浏览器只为当前可见项目按需读取远端缩略图，并以最多两条连接复用会话，不再进入页面就预取整库。为当前已连接节点首次开启缩略图时，Mac 会与 iOS 一样立即询问是否补齐既有备份；未连接节点只保存未来备份设置，不弹出无法执行的操作。
- 本地照片索引工具（项目数、总大小、最后更新时间）、精确重复照片工具、执行日志历史与搜索；Home 执行态可用单个日志图标直接定位本次会话，窗口会实时刷新，并仅在当前活动会话被选中时显示状态、传输速度和预计剩余时间，已结束的历史日志不增加冗余状态栏。照片权限不可用时，索引与重复照片窗口显示与 Home 相同的精简授权动作，不把不可读取误报为 `0 / 0` 或无重复项，并在从系统设置返回后自动恢复。日志默认显示全部级别，并支持 Finder 定位和右键删除已结束的单次记录，仍在写入的日志不能删除。
- 仓库校验、紧凑健康概览、不完整项目明细、残留扫描、内容 hash 复核、孤立缩略图扫描和选择后删除；不完整项目的分组、摘要与详情文本由 iOS / Mac 共用。
- 设置窗口使用 AppKit 原生偏好设置工具栏和 `.preference` 窗口样式，分为通用、图片浏览器、Pro 与关于四页；每页以系统 `NSBox`、`NSGridView`、`NSSwitch`、`NSPopUpButton` 和标准按钮组成统一标签列与控件列。通用页复用共享默认并发、iCloud 原片访问和月份分组时区；图片浏览器页打开本地图库或进入逐节点远端缩略图设置；Pro 页使用 StoreKit 2 查询终身商品、购买和恢复权益，并持续监听匹配商品的事务变化；关于页显示版本、iOS App、联系开发者与隐私政策。目的地详情可覆盖单节点并发，继承全局、按协议自动和固定数量的选项模型及标题均由 iOS / Mac 共用。设置与目的地管理在执行期间仍可查看；只有月份分组时区按 iOS 的执行、Home 本地索引加载条件禁止修改，默认并发与 iCloud 模式不会触发无关的照片重建索引。
- 目的地新增、编辑、重命名、删除及逐节点设置统一取得共享 profile mutation lease；执行、远端维护或该节点连接期间，AppKit 控件同步禁用并使用 iOS 共用的任务进行中提示。已打开的 SMB、WebDAV、S3 与 SFTP 表单会实时刷新保存、测试连接和目录选择动作，不保留看似可用但提交后才失败的按钮；Mac 将 S3 写入验证合并在保存流程中，因此该异步验证与最终数据库提交持有同一 lease，避免验证期间启动备份。目的地列表支持原生拖动排序，并与 iOS 一样限制在同一存储类型内。
- 外接文件夹直接使用一次原生 `NSOpenPanel` 完成新增或改目录，名称默认取文件夹名，不复制 iOS 的选择界面。选择后的 bookmark、重复目录检查、稳定位置 token 与更换物理目录时的状态失效由 iOS / Mac 共用 `ExternalStorageProfileSaveWorker`；AppKit 监听系统卷挂载与卸载，活动外接目标所在卷被卸载时立即断开并刷新可达性，其他卷变化只刷新状态。SMB 复用 iOS 的发现、凭据、共享、目录选择顺序及连接上下文失效规则，SFTP 复用共享 TOFU / host-key 变更策略。
- 遗留迁移的凭据输入、远端目录选择、扫描结果和导入进度均为原生 AppKit controller；扫描与写入继续复用原有 `LegacyMigrationPlanner` / `LegacyMigrationExecutor`，没有复制业务分支。
- 本地索引、仓库维护、远端缩略图维护、媒体浏览器操作和遗留迁移在任务运行时都会拦截窗口关闭；选择停止后要等任务真正退出并释放 execution lease 才自动关闭，不会留下不可见的后台写入。
- 首次启动引导、真实 App 图标和语言系统设置入口；原生 Help 菜单只保留重新打开引导、联系支持与隐私政策，复用 iOS 已有邮箱和隐私地址。
- 手动任务完整成功后复用 iOS 的评分资格规则：数据库使用满 7 天才通过 macOS 原生 `AppStore.requestReview(in:)` 请求系统评分；失败、部分完成和跳过不完整项目都不触发。

## UI 约定

- 配色与 iOS 保持同一套 Material Green / Amber / Red 语义。
- 不使用装饰性渐变；普通 AppKit 控件保持系统中性色，主要操作和状态容器使用 Material 语义色。
- iOS 与 Mac 共用 `Shared/Resources/Localizable.xcstrings` 普通字符串表；Mac 专属文案也已补齐现有 14 种语言，不再维护两份重复翻译。快捷指令的 12 个条目单独保留在 iOS-only `Watermelon/AppIntents/AppIntents.xcstrings`。
- 页面只常驻显示当前状态和完成任务所需的信息；实现细节、安全边界和不可逆影响在对应操作的确认框或错误状态中呈现，避免重复 subtitle/footer。
- Home 的 Local/Remote 月份合并、缺失侧零值、选择意图和左右 overlay 与 iOS 共用同一领域状态；远端选择同样只在指定相册范围下禁用，并等待当前连接的远端快照应用完成。中间方向徽标复用 iOS 的匹配进度规则，并在上传或下载时按当前月份继续推进；执行期间在月份行原有选择位置显示上传、下载、暂停、完成和失败状态，不增加说明文字。执行前使用 iOS 相同的月份确认、不完整下载策略和本地 hash-index preflight；完成或失败状态保留到用户点击完成。切换目的地失败时恢复原连接及其远端索引。AppKit 只负责表格、菜单和进度控件。
- Home 没有目的地时只保留“外接存储”和“新增节点”两个动作，不显示解释性标题或“或”；新增节点菜单覆盖 SMB、WebDAV、SFTP、S3 与 OneDrive。已有目的地但未连接时同样只保留连接和新增两个动作。
- 从 Home 新增任一类型的目的地后，会关闭新增窗口、选中新节点并立即连接，与 iOS 的创建后连接语义一致；从 Tools 打开的目的地管理窗口不会自动连接。
- 节点名称只在新建表单和管理页的独立重命名动作中出现；SMB、WebDAV、S3、SFTP 与 OneDrive 的连接参数编辑页不重复显示名称，也不会提供一个保存后无效的输入框。
- 设置和目的地管理遵循 Mac 单窗口习惯，重复菜单操作只激活已有窗口；Home 的“创建后连接”回调只属于当前新增面板，取消新增不会改变管理窗口后续创建节点的行为。
- `--demo-onboarding`、`--demo-settings`、`--demo-profiles`、`--demo-local-profile`、`--demo-smb-profile`、`--demo-webdav-profile`、`--demo-s3-profile`、`--demo-sftp-profile`、`--demo-onedrive-profile`、`--demo-photo-browser`、`--demo-photo-metadata`、`--demo-maintenance`、`--demo-logs`、`--demo-execution-log`、`--demo-local-index`、`--demo-duplicates`、`--demo-album-picker`、`--demo-no-destination`、`--demo-connecting` 和 `--demo-connected` 可用于 DEBUG 视觉回归；三个 Home 状态参数可独立注入合成图库，不需要额外拼接隐藏参数。

## 平台原生差异

1. iOS 的 Backup to Computer / Browser Link、配对流程、快捷指令和画中画进度不移植到 Mac，也不在 Mac 菜单、设置或 Home 暴露入口；App Intents 源码与专属字符串表均保持在 `Watermelon/AppIntents`，不移入 `Shared`。Mac 本身就是完整备份端，并以原生窗口显示进度。共享字符串和底层类型仍服务 iOS target，不代表 Mac 功能。
2. Mac 媒体浏览器只保留内存缩略图和操作所需的临时原件，不建立 iOS 那套持久原片/缩略图磁盘缓存，因此没有缓存大小设置。
3. Mac 不提供计划或后台自动备份，也没有登录项和逐节点自动备份设置；连接会话首次激活及后续刷新都会忽略共享 profile 中仅供 iOS 使用的后台备份标记，同时不改写数据库。手动任务在主窗口最小化后继续运行，并在实际运行、暂停、恢复或安全停止期间阻止系统因空闲进入睡眠；完成或失败结果可继续留在 Home，但日志收尾后立即释放 execution lease 和防睡眠活动。关闭最后一个窗口会进入正常退出流程，仍在活动的任务必须先安全停止。
4. App 级 execution lease 会互斥备份、维护、索引更新、重复照片删除、目的地变更和遗留迁移。

## 发布阻塞项

- Microsoft Entra 必须为 macOS bundle ID `com.zizicici.watermelon-mac` 注册 `msauth.com.zizicici.watermelon-mac://auth`。Mac 的 Info.plist、URL scheme 与 MSAL 2.x bundle 规则已经一致；只有在 Entra 明确注册并完成 Mac 真机登录验证后才能发布 OneDrive。
- iOS 免费版的单存储目标限制依赖 MoreKit 终身会员商品；MoreKit 目前仅支持 iOS。Mac 已提供独立 StoreKit 2 商品查询、购买与恢复入口，但 `com.zizicici.watermelon-mac` 的商品归属和跨平台权益仍须在 App Store Connect 真机验证。验证前不在 Mac 启用单节点门槛，避免商品不可用时形成死路。
- 每种后端都要验证新仓库、V1→Lite、断点续跑、锁竞争、scratch 恢复、Pause/Resume/Stop、掉线与拔盘。
- PhotoKit 要覆盖 full/limited、iCloud-only、大视频、Live Photo、编辑后 fingerprint 变化、批量删除与恢复。
- Release 签名要验证 App Sandbox、Photos entitlement、Keychain access group、外接卷 bookmark 和多语言资源。
- 自动化测试仍以共享纯逻辑为主；Mac 已有独立宿主测试 target 覆盖 AppKit 侧策略，并以可取消上传、下载替身直接驱动手动执行 controller 的暂停、继续、停止、会话失效和同步月份提交边界。外接文件夹已覆盖连接 controller、真实远端索引 core、会话激活和断开清理；预置 Lite 仓库的直接下载还会经过真实批量校验计划和执行 controller，并验证 Stop 取消、校验会话释放及取消后重新执行。仓库维护也已贯通真实完整校验、验证时间写回和干净仓库残留扫描；仅最终 PhotoKit 导入使用测试 seam。真实上传 core、PhotoKit 和网络后端写入仍依赖手工回归。
- `MacLivePhotoKitUploadIntegrationTests` 提供显式启用的本机回归入口，会让 `MacBackupExecutionController` 读取一项真实 PhotoKit 资源，经共享 `BackupCoordinator`、`AssetProcessor` 与 LocalVolume 写入一次性目录，再从 Lite manifest 重新加载验证。该文件默认不编译进日常测试；只允许在可丢弃 Photos 图库中以 `SWIFT_ACTIVE_COMPILATION_CONDITIONS='$(inherited) WATERMELON_LIVE_PHOTOKIT_UPLOAD'` 构建，并设置 `WATERMELON_RUN_LIVE_PHOTOKIT_UPLOAD=1`。可用 `WATERMELON_LIVE_PHOTO_ASSET_ID` 固定测试资源；省略时选最新图片。源图库不被修改，测试目的地结束后自动删除；运行前仍需确保原片已在本机，或在 Mac 设置中允许访问 iCloud 原片。

## 当前验证

- `WatermelonMac` 与 `WatermelonMacTests` 已切到 Swift 6 language mode；iOS App 与 `WatermelonTests` 保持 Swift 5。Mac clean Debug/Release 构建和 220 项 macOS 宿主测试通过；iOS Debug 构建及 1658 项测试也通过（0 失败，2 跳过）。target 默认非隔离，AppKit 入口显式 `MainActor`。
- `WatermelonMac` 源码已无 SwiftUI view 或 `NSHostingController`；遗留迁移窗口完成 AppKit 构建验证。
- 共享代码迁移后的 iOS Simulator Debug 构建通过。
- 共享字符串目录包含 995 个条目；994 个有效本地化条目均覆盖现有 14 种语言，保留的无本地化 `%@` 占位条目来自原 iOS 字符串目录。快捷指令的 12 个条目及完整 14 语种翻译保留在 iOS-only `AppIntents.xcstrings`。Mac 专属条目由 596 个清理到 61 个；新增的两个条目仅对应原生 Help 菜单的联系支持与隐私政策。Home 的 SFTP 主机密钥确认、设置与管理节点菜单继续复用 iOS 文案，文件菜单与文件列表复用 `mediaMetadata.section.file`，连接期间的索引、V1→Lite 升级与清理进度复用 iOS Home 的阶段文案，媒体浏览器与 Home 共用 `home.photoLibrary`，占位符与换行结构校验通过，iOS 和 Mac bundle 均生成同一套 `Localizable.strings`。
- 设置窗口的通用、图片浏览器、Pro 与关于四页均已截图检查，原生工具栏、分组边界、固定标签列和控件列无偏移或裁切；首次引导也已完成英文与简体中文视觉回归。`InfoPlist.xcstrings` 会为 14 种现有语言生成应用名及照片/局域网权限说明。
- 媒体信息面板已通过 `--demo-photo-metadata` 离屏截图检查，分组标题、两列字段和值、长值换行与滚动容器布局正常；共享元数据解析与文件读取测试通过。
- 目的地可达性服务的调度、强制 sweep 合并、节流、后台停止和 profile 变更测试共 5 项通过；iOS 保持不主动启动该服务，Mac Debug 构建验证了 AppKit 菜单接线。
- 不完整远端项目的月份排序、条目排序、摘要指纹和缺失 hash 详情测试共 2 项通过；Mac Debug 构建验证了维护概览、明细 sheet 与验证后刷新接线。
- Home 已截图检查已连接、未配置目的地、已有目的地但未连接、连接中和照片未授权状态；远端快照应用前会保持 Remote overlay，不会露出上一连接的月份，顶部 Remote 选择也会保持禁用，直到当前连接的快照真正应用完成。连接、执行、浏览器和维护回写的每份快照都携带动作所属的 AppSession generation，并同时匹配当前 session profile、用户选中的 profile、实际 connected profile 和快照 `profileKey`；同一节点重连后的旧 generation 回调直接忽略，不会覆盖或清空新投影，当前 generation 内即使数据库 ID 相同，只要 session、选择或连接指向不同远端目标，也会按未连接清空 Remote。8 项所有权策略测试及完整 220 项 Mac 宿主测试通过。本地-only 备份仍可在 Home 投影加载期间开始，下载和互补则等待当前远端投影，Start 的按钮、点击入口和异步确认后复核使用同一规则。手动连接失败只显示一次错误框并回到节点选择，不再把同一错误长期重复在远端卡片副标题。已连接节点的下拉菜单复用 iOS 的“当前节点设置”入口，直接打开 AppKit 管理窗口并定位该节点。
- Mac Home 在 Photos 授权语义变化时会清空旧选择并重建本地月份索引，包括“完整访问”与“受限访问”之间的切换。本地索引加载期间到达的远端快照只保留最新一份，待本地结果稳定后再合并；连续 PHChange 和被取消的旧回调不能覆盖新状态。`SelectionControllerTests` 的 33 项测试、`RemoteIndexEngineTests` 的 8 项测试及 Mac Debug 构建通过。
- Mac 媒体浏览器只从同一次原子 AppSession 快照取得初始 profile、凭据与 generation，不再在会话已断开时回退到连接控制器残留的旧 profile；外接文件夹的无密码会话规范化为空凭据。当前展示的远端投影继续绑定到对应 generation；切换或断开目的地时立即移除旧远端关联，并在新投影完成前隐藏备份、恢复、远端删除、远端分享和远端元数据动作。备份、恢复与删除在确认框和 execution lease 两侧都校验动作开始时捕获的 profile/generation；浏览器备份在确认期间发生连接变化或缺少所需凭据时，会在创建执行租约与日志前拒绝。恢复还会重新检查 Photos 读写授权。无签名 Mac Debug 构建通过，编译清单、资源和二进制仍不包含 App Intents。
- iOS 与 Mac 媒体浏览器的单项/批量动作矩阵共用 `MediaLibraryActionPolicy`：仅本地项目可备份、仅远端项目可恢复，混选不提供补集式备份/恢复；本地视图删除时不触碰远端副本。Mac 保留 AppKit 的窗口、选择栏与确认菜单实现，不共享 UI。
- Mac 媒体浏览器的批量删除在确认前冻结 profile、generation 与凭据，并在进入执行租约、提交 PhotoKit 删除和逐项远端删除前持续验证同一节点；确认期间完成连接切换会直接中止，不会用新节点解释旧列表里的 fingerprint。远端 Live Photo、视频和图片预览也会在选中项目时冻结读取上下文，并在每段下载前后验证展示投影与连接会话；切换节点后不会用新凭据读取旧列表项。10 项初始会话、节点切换与读取上下文策略测试及完整 220 项 Mac 宿主测试通过。
- Mac 媒体浏览器在 Photos 授权从完整、受限或不可读状态之间变化时，会先撤销旧的本地投影和选择，再按新授权异步重建；远端独有项目可继续保留。正在提交的 PhotoKit 操作不被强行打断，失效处理延迟到动作安全结束。1 项授权转换矩阵测试及完整 220 项 Mac 宿主测试通过。
- 无目的地 Home 已再次以实际 AppKit 窗口检查：Remote 区域没有空白占位格或多余说明，只显示两个新增动作；非外接存储菜单由完整的共享存储类型顺序生成，包含 OneDrive。
- `WatermelonMacTests` 已作为 Swift 6 macOS 宿主测试 target 接入共享 `WatermelonMac` scheme。220 项测试覆盖 profile mutation、规范化 v2 凭据身份、凭据缺失时的编辑恢复、连接编辑字段策略、外接文件夹 profile/bookmark/客户端工厂到实际文件 I/O 的一次性目录贯通回归、外接文件夹经 `MacRemoteConnectionController` 完成远端索引加载、会话激活与断开清理的真实链路、预置 Lite 仓库经真实索引、批量校验计划和执行 controller 完成直接下载、Stop 取消与释放后重新执行的链路、真实完整校验、验证时间写回与干净仓库残留扫描、手动执行 controller 的上传与下载暂停/继续/停止、暂停后会话失效、同步月份上传提交后仅恢复下载、日志收尾与 execution lease 生命周期、StoreKit 商品、权益、购买取消、失败、恢复与事务更新、OneDrive Mac redirect、URL scheme 与 MSAL 初始化、WebDAV 测试连接基础路径、连接凭据提示的过期尝试与成功后持久化门控、SFTP 编辑期间的实时主机密钥刷新、已连接节点的会话刷新、Home 远端快照所有权、浏览器初始会话原子捕获、备份执行前的 profile/generation/凭据门控、远端预览读取上下文、浏览器照片授权转换、远端缩略图按需加载、维护会话归属、排队取消策略、设置实时可用状态与清理终态、仓库维护窗口的目标/generation/凭据会话归属、列表式精简引导页、Help 与 Tools 菜单、媒体动作、恢复计划、执行失败处置、月份执行状态与进度投影、执行设置、hash-index 预检查、下载前本地指纹 freshness、执行前不完整项目扫描签名、残留清理的 manifest 月份枚举与读取故障分类、恢复量旁路估算的取消与过期结果隔离、退出与窗口关闭、执行期间防止系统空闲睡眠及终态释放、结果页期间后续任务的退出门禁、媒体浏览器与重复照片删除写入边界的取消门禁、媒体浏览器取消后对已提交删除的状态协调、旧版迁移取消/失败后的部分进度与远端重载门控、迟到停止确认的终态保护与安全停止幂等性、执行取消归属、连接进度、SFTP profile 接纳、凭据输入、Home 授权/远端空状态、缺失侧零计数、执行日志选择、删除门禁、实时状态、速度与剩余时间、独立 demo 状态、照片/视频计数呈现和评分资格；测试宿主、App 编译清单、链接库与资源均不包含 App Intents。
- Mac 手动执行在启动时冻结节点并发覆盖或共享全局默认值及 iCloud 模式，Pause / Resume 沿用同一快照，iCloud-only 预检仍可在快照之上强制单 worker。iOS 与 Mac 的预检查结果合并和单 worker 判断使用同一纯策略；Mac 预检查写入任何有效指纹后都会安排 Home 重载，即使任务随后因其他不完整资产失败，也不会在用户关闭结果页后保留旧匹配状态。
- Mac 在每月下载及下载大小预估前都会用当前 PhotoKit 资产重新筛选 hash 记录；已经删除的本地资产和编辑后过期的指纹不再把远端项目误判为本机已有，也不会造成恢复漏项或进度总量偏小。
- Mac 在 SFTP 首次信任或主机密钥变更确认后会立即接纳数据库返回的最新 profile；即使随后远端索引读取失败，错误状态和下一次重试也使用新指纹，不会把旧指纹恢复成“已连接”。路径、凭据引用或认证方式在连接期间发生变化仍会安全取消。
- Mac 的 SFTP 编辑 sheet 不再提前从打开窗口时的 profile 快照决定最终指纹；保存时会在 profile mutation lease 内重新读取数据库中的当前 profile，再结合本次匹配端点的连接测试结果计算。连接流程刚接受的新主机密钥不会被仍打开的旧编辑窗口覆盖，配置已删除时也不会回退到旧快照继续保存。3 项数据库回归测试通过。
- Mac 缺少已保存凭据时使用原生密码 sheet；用户取消会静默回到未连接状态，不再追加一个“凭据不可用”错误框。非空输入只参与当前 profile/epoch 的连接尝试，远端索引与数据库中的当前凭据引用均验证成功后才写入登录钥匙串；错误密码、主机密钥确认取消、远端读取失败、提示期间取消或切换目的地均不会留下 Keychain 副作用，空提交按无效配置处理。
- Mac 连接失败弹窗与 iOS 一样把“编辑”作为默认动作，把“确定”作为取消动作；关闭 sheet 后直接打开并定位失败节点。无持久 ID 的临时配置只显示“确定”，不会打开错误的节点。
- Mac Debug/Release 均已配置 App Sandbox、Hardened Runtime、Photos、出站网络、用户选择读写、安全书签和 MSAL macOS Keychain group；工程未链接 `AppIntents.framework`。归档签名后的 entitlement、OneDrive 与外接卷仍需在发布证书和真实环境下验证。
- iOS 与 Mac 现共用月份分组时区观察器；“跟随系统”会在系统时区变化后重建本地月份索引。Mac 在执行或索引加载期间收到的多次刷新会合并，并在阻塞结束后补做一次，不再直接丢弃或并发重载。`HomeMonthGroupingTimeZoneChangeObserverTests` 的 2 项测试及 Mac Debug 构建通过。
- iOS 与 Mac 的下载校验失败策略已收敛到共享规则：只有仍保留可信快照的暂时性错误可以继续，确认缺失、损坏或仓库所有权错误都会按月失败关闭。Mac 批量校验计划建立失败时退回逐月校验，但计划内的致命月份错误不会重复执行；跳过不完整远端项目会显示为部分完成。`LiteLeaseFailFastTests` 的 28 项测试及无签名 Mac Debug 构建通过。
- Mac 主菜单已补齐标准 AppKit Edit responder chain，资料编辑、密码输入和可选文本均可使用撤销、重做、剪切、粘贴、删除和全选键位；四个新增动作复用 `common.*` 共享文案并覆盖 14 种语言，不增加 Mac 专属 Key。Tools 菜单在 execution 期间禁用新建本地索引、重复照片和仓库维护窗口，但已打开窗口仍可置前，节点管理与日志保持可查看。已经打开的本地索引、重复照片与目的地管理会随全局执行状态实时禁用修改动作，OneDrive、SMB、WebDAV、S3 与 SFTP 编辑 sheet 也会同步更新。已经完成或失败、仅等待用户关闭的结果页继续保留，但不占用 execution lease 或防睡眠活动，也不会单独触发“仍在运行”确认、在退出时被改写为取消，或被运行期间已打开但迟到返回的停止确认覆盖；结果页期间若又启动了索引、维护或迁移等新任务，退出仍会识别并安全停止该任务。真正运行或暂停的任务始终要求安全停止。安全停止一旦进入 `.stopping` 就只等待已有清理，不会因再次 Quit 重复收尾；Quit 确认期间任务自然结束时会继续退出。退出提示统一为适用于备份、索引、维护和迁移的“任务”。目的地拖动排序与其它节点修改一样取得 profile mutation lease。
- 缩略图清理的存活集只纳入 manifest 中仍有可解析照片或视频媒体的 asset；config-only、audio-only 与 phantom 记录不再误保护无效 sidecar。直接缩略图扫描和残留文件扫描共用同一规则。
- Mac Home 在不完整项目预扫描前与确认框结束后，都会按最新 Local/Remote 选择重新验证原确认范围；失去对应侧或改变意图的月份会被移除，新出现的选择不会偷偷加入，并从稳定后的本地索引重新冻结资产 ID。扫描期间如果下载或同步月份及其本地资产集合发生变化，会按新计划重新统计，避免以过期计数静默跳过新出现的不完整项目；仅备份月份变化不触发重扫。`SelectionControllerTests` 的 34 项测试、3 项 Mac 扫描签名测试及无签名 Mac Debug 构建通过。
- Mac 手动执行会在上传事件流完全排空后再计算后续下载、进入暂停/停止/完成状态或关闭日志，避免立即 Resume 丢失刚提交月份的完成事件，也避免尾部上传日志被关闭后的 writer 丢弃。上传尝试即使以 Pause、Stop 或错误结束，也会在排空后把已回滚至实际提交边界的共享远端缓存投影回 Home，不必等到重连才看到已完成月份。
- 恢复量估算作为可取消的旁路任务运行，不再阻塞预检或上传；Pause、Stop 与 Resume 会使旧估算失效，只有最新计划的结果能更新 ETA。
- Mac 的 SMB、WebDAV、S3、SFTP 与 OneDrive 新建、编辑入口统一使用长度前缀身份字段生成 v2 凭据引用；主机、端口和路径先经过各协议的 canonical connection 规范化，OneDrive 空名称也改用已有本地化默认值。固定契约与等价输入测试共 2 项通过。
- 五种远端连接编辑页统一隐藏由管理页独立维护的名称字段，并同步缩短 sheet；新建页仍保留名称输入。模式策略测试共 2 项通过。
- 已连接节点仅修改名称、并发、缩略图或其它非连接元数据时，Mac 原地刷新 AppSession，不再递增 generation、清空浏览器远端缓存或使正在进行的浏览器动作失效；密码、连接参数、凭据引用或 writer identity 变化仍会重新激活会话。4 项策略与状态测试通过。
- 任一节点正在连接时，Mac 目的地管理窗口统一进入只读状态，新增及所有节点的编辑、重命名、删除和设置不再显示为可提交；已经打开的 OneDrive sheet 会随连接生命周期实时禁用登录与保存，与全局 profile mutation lease 保持一致。
- 目的地缩略图补齐与清除也把连接过程纳入可用性门禁；确认前捕获节点、凭据与 AppSession generation，确认后再次验证远端身份和凭据。连接在临界点切换时不会让旧节点维护以新会话继续启动。5 项可用性和会话上下文测试及完整 220 项 Mac 宿主测试通过。
- 已打开的设置窗口会随执行生命周期和 Home 本地索引加载状态实时更新月份分组时区控件；不可修改时直接显示禁用，不再等选择后才提示失败。默认并发与 iCloud 原片访问仍保持可改，并由手动任务启动快照冻结。1 项 AppKit 控件状态测试及完整 220 项 Mac 宿主测试通过。
- Mac Home 的手动备份执行态优先于 app-wide execution 提示，生命周期通知不再把下载、上传、暂停或结果状态覆盖回 Start；执行行只增加一个无文字日志图标，直接打开并实时刷新当前日志。
- 同步月份进入内联恢复时，上传 manifest 已经持久提交；Mac 现在会在这一边界单独记录上传完成。随后若恢复被暂停或遇到可恢复错误，Resume 会把该月份拆为仅下载，不再重复上传；如果 Pause 与最后一项提交同时发生且已经没有剩余工作，则直接进入完成态，不再产生可 Resume 的空计划。反向单侧完成、双侧完成和最终提交竞态由 6 项纯策略测试覆盖，另有 1 项 controller 集成回归确认内联下载暂停后上传协调器不会再次执行。该协调策略保持在 Mac 执行层，iOS 继续使用自己的 `HomeExecutionSession`，没有为此扩展 `Shared`。
- Mac 执行错误处置已收敛到独立纯策略：显式 Pause 优先保留可恢复状态，Stop 始终取消；无命令的可恢复网络故障进入暂停，致命故障进入失败；若所有月份已越过提交边界则直接完成。8 项状态矩阵测试覆盖任务取消、传输层取消、可恢复故障、致命故障与提交完成组合，控制器不再以分散条件分支重复判断。
- Mac 月份状态直接消费控制器使用的上传事件序列：普通项目失败在月份提交后显示部分失败；manifest 未提交时，纯备份月与 iOS 一样显示部分失败，需要内联下载的互补月则整月失败；为 Pause 回滚而发出的零失败事件仍保持可恢复。终态使用与 iOS 一致的中性底色，运行和部分失败继续保留 Material 月份色；2 项事件序列测试覆盖提交失败与 Pause 回滚。
- iOS 的 `HomeProgressCalculator` 已按原实现移动到 `BackupCore/Home`，两端以同一规则计算备份、下载与互补月份的已有匹配比例。Mac 额外跟踪单次上传的唯一资产和当前下载资源位置，方向徽标以一位小数显示单月进度，只重载发生变化的表格行。6 项 Mac 进度测试和原有 12 项 iOS Home execution 测试通过。
- Mac 首页顶部、年份和月份统一用照片与视频 SF Symbols 分别显示数量，直接消费共享索引中的 `photoCount` / `videoCount`，不再重复显示“项目数”说明文字，也没有新增 Mac 专属本地化 Key。
- 仓库准备阶段用独立的 `writeBoundaryReached` 事件标记“后续可能已有持久写入”，不再用 `started(0)` 冒充上传开始；若准备阶段失败时还没有可安全投影的完整快照，Home 会等 execution lease 释放后重载当前目的地，重载失败则恢复原连接与原快照而不会卡在扫描 overlay。
- 纯下载与同步在 `verifyMonth` 成功、按月失败、Pause 或 Stop 后也会把共享远端缓存投影回 Home；确认缺失或损坏而被移除的月份、reconcile 后的 manifest 不再停留在旧 Remote 显示中。缓存验证与 Home 远端投影相关测试共 14 项及无签名 Mac Debug 构建通过。
- Mac 与 iOS 的恢复阶段继续共用逐项导入、即时 hash-index 写回和暂停续跑逻辑；完整性 hash 不符、旧 manifest 大小不符和非法远端路径的错误不再硬编码英文，三个必要的共享 Key 已覆盖 14 种语言且占位符一致，没有增加 Mac 专属 Key。Restore 完整性、临时文件清理与 Live Photo 导入计划共 26 项测试及无签名 Mac Debug 构建通过。
- iOS 与 Mac 的媒体类型、文件/图像信息分组和布尔值共用同一组本地化 Key，原有三个 `mac.browser.*` Key 已删除；旧版导入写入门禁的用户错误也不再硬编码英文。字符串目录编译、15 项元数据与写入门禁测试及无签名 Mac Debug 构建通过；Mac 编译源列表、链接库、资源和最终二进制均不包含 App Intents。
- Mac 的 Pause / Resume 保留必要的原生文本按钮 Key，但 14 语种均按“暂停 / 继续执行”动作语境校对；不再把 Resume 翻译成简历、履历或 CV。
- Mac 首页接入编辑后 fingerprint 失效与自动重算后，`WatermelonMac` Debug 构建通过；`WorkerTests` 的 10 项测试通过，共享 freshness 测试覆盖较旧、同时间、较新、无修改时间和无索引记录。真实 Photos 编辑事件仍需发布前手工回归。
- App 菜单的 Settings 与 Tools 的目的地管理均已实际重复触发验证，WindowServer 中各自始终只有一个窗口。
- 运行中的系统退出已用 `--demo-executing --demo-safe-quit` 实际验证：App 进入延迟终止，完成安全 Stop、日志收尾和 execution lease 释放后自动退出，不需要第二次 Quit。
- 独立长任务窗口的关闭路径已统一接入 `NSWindowDelegate`；本地索引、媒体浏览、目的地缩略图维护、仓库维护和旧版迁移在停止确认框显示期间若任务自然结束，会立即关闭而不留下过期的“完成后关闭”状态。重复照片的 PhotoKit 提交阶段不可可靠撤回，仍只允许等待完成。3 项统一关闭策略测试及完整 220 项 Mac 宿主测试通过。
- 旧版迁移的成功、失败与取消现在都保留真实部分进度，按顺序写完并关闭执行日志后才释放 execution lease；已导入或已提交月份会在租约释放后强制重载当前连接的 Remote，未发生写入的取消不制造额外刷新。3 项终态策略测试及完整 220 项 Mac 宿主测试通过。
- 仓库残留清理在部分删除后失败时会丢弃旧扫描结果，避免继续操作已经过期的条目；远端缩略图清理以明确终态区分取消与失败，连接阶段取消不再显示失败提示。
- 仓库完整校验在成功、取消或失败后都会把已重载的远端快照应用回 Home；维护窗口冻结打开时的远端目标、AppSession generation 与凭据，空闲时遇到断开、切换、同 ID 目的地修改或凭据重连会自动关闭；运行中的维护不会被强行打断，但结束后会关闭过期窗口。残留删除的确认框返回后也必须再次通过同一会话校验，不能用新凭据执行旧列表。7 项节点会话策略测试及完整 220 项 Mac 宿主测试通过。
- 仓库维护窗口在任务运行中关闭时提供停止或取消；选择停止后会等待任务退栈并释放执行租约再自动关闭，停止残留删除时不会为即将关闭的窗口重新扫描。3 项关闭终态策略测试及完整 220 项 Mac 宿主测试通过。
- 媒体浏览器的远端缩略图请求在进入共享连接池前受两并发门控；cell 离屏或窗口关闭会从等待队列移除请求，不再让快速滚动产生的过期任务随后占用网络连接。3 项并发、排队取消与关闭策略测试及完整 220 项 Mac 宿主测试通过。
- 后续 DEBUG 窗口复查确认 App 内部已创建可见窗口；但宿主当时处于 `loginwindow` 锁屏会话，WindowServer 不提供应用窗口画面，因此该轮不计为新的截图验证。
- Home、设置、目的地、媒体浏览、索引、重复照片与维护窗口均保留 DEBUG demo 入口用于持续视觉检查。
- `AppRuntimeFlagsTests`、`WriteLockServiceTests`、`LegacyV1WriteGateTests`、`RepoFormatRouterTests` 与 `RepoLeaseReconnectTests` 已在 iOS Simulator 通过；完整测试集仍需在发布前重跑。
- 外接目录身份、重复检测、换目录失效和 profile mutation lease 的针对性测试共 18 项通过；同一共享保存器已分别通过 Mac Debug 与 iOS Simulator Debug 构建。
- 无签名 Mac Debug 构建通过；本机缺少 `com.zizicici.watermelon-mac` provisioning profile，签名构建在签名阶段停止，未擅自请求 Xcode 联网创建团队配置。
- Mac WebDAV 的“测试连接”现在与 iOS 一样列举备份基础路径；endpoint 已包含的挂载路径不会再被重复拼接。独立路径策略回归及完整 220 项 Mac 宿主测试通过。
