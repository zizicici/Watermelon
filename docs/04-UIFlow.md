# UI 流程与状态（当前主链路）

## 1. 启动与首页

App 启动后直接进入 `HomeViewController`。

### 顶部区域

左右两栏 header：

1. 左侧：`本地相册`
   - 全选 / 取消全选 toggle
   - 照片 / 视频 / 体积汇总
2. 右侧：`远端存储`
   - 当前连接名称
   - profile 下拉菜单
   - 全选 / 取消全选 toggle
   - 照片 / 视频 / 体积汇总

右侧连接菜单内容：

1. 已保存 profile 列表
2. 当前连接项打勾
3. `未连接` 断开项

### 右侧 overlay

远端未就绪时，右半栏会被 overlay 覆盖：

1. `connecting`：spinner + `连接中...`
2. `disconnected`：`未连接远端存储` + `选择存储` 按钮
3. `connected`：overlay 隐藏

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

### 可交互条件

只有在下面两个条件都满足时，月份选择才允许：

1. 已连接远端存储
2. 当前不在执行态

### 选择行为

1. 支持单月选择
2. 支持年级 toggle
3. 支持顶部左右全选 toggle
4. 连接状态变化时，已选月份会被清空

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

正式上传前，首页执行链路会先冻结一次本次任务的设置：

1. `上传并发`
2. `允许访问 iCloud 原件`

然后执行前置检查：

1. 如果本次包含上传且启用了 `允许访问 iCloud 原件`，会先对 `upload + sync` 月份做 availability probe
2. 若 probe 发现仅存于 iCloud 的本地上传资源，本次 upload 自动改为 `1` 个 worker
3. 随后对本次涉及的所有本地 asset 做离线 hash 预检查，默认 2 个 worker
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

## 7. Cell 执行态样式

当前 `MonthPlan.Phase` 与视觉大致对应：

1. `pending`
   - 正常样式
2. `uploading / downloading`
   - 正常底色 + spinner
3. `uploadPaused / downloadPaused`
   - 正常底色 + 暂停标记
4. `uploadDone`
   - 仍按运行中样式显示，等待 sync 下载完成
5. `completed`
   - 灰底 + 绿色勾
6. `partiallyFailed`
   - 运行态底色 + warning 指示
7. `failed`
   - 失败样式

## 8. 进度规则

### 上传

箭头百分比取：

1. `BackupSessionController` 回传的 session 进度
2. reconcile `matchedCount` 计算出的基线进度

二者中的较大值，保证百分比单调前进。

### 下载 / 同步下载

1. 直接依赖 reconcile `matchedCount`
2. 每个 item 下载成功后立即刷新本地索引，因此进度按 item 推进

## 9. 暂停 / 恢复 / 停止

### 暂停

1. 上传阶段：请求 backup pause
2. 下载阶段：取消下载 task，并把月份状态切为 paused

### 恢复

1. 已完成月份不会重跑
2. sync 月份若已上传但未下载完，会从下载态继续

### 停止

1. 弹确认框
2. 停止后退出执行态
3. 用户需要重新选择月份再执行

## 10. More 页面

入口：

1. 首页右下角 FAB

当前自定义项：

1. `远端存储` → `管理存储`
2. `本地数据` → `本地 Hash 索引`
3. `备份` → `上传并发`
4. `备份` → `允许访问 iCloud 原件`
5. `通用` → 系统语言入口

## 11. 本地 Hash 索引管理页

`LocalHashIndexManagerViewController` 当前支持：

1. `创建` / `更新` 两种模式
2. 是否移除本地已不存在条目
3. 进度条与滚动日志
4. 顶部按钮：开始 / 暂停 / 停止 / 重置
5. 索引统计与覆盖率查看
