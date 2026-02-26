# UI 流程与状态

## 1. 登录前流程

## `ServerSelectionViewController`

1. 两个 section：`局域网发现`、`已保存`。
2. 支持 Bonjour 自动发现 SMB（`_smb._tcp`）。
3. 进入页面后可自动登录最近/当前活跃服务器。
4. 右上角 `+` 进入添加流程。

## 添加 SMB 三步

1. `AddSMBServerLoginViewController`
2. 输入 Host/Port/Username/Password/Domain，登录后拉 Share 列表。
3. 页面是 `UIScrollView`，键盘弹起会调整 inset，可点击空白收键盘。
4. `SMBSharePathPickerViewController`
5. 选 Share，再浏览目录路径。
6. 更换 Share 会重置路径到 `/` 并重新加载目录。
7. `AddSMBServerViewController`
8. 确认信息，保存 Profile + Keychain 密码，然后回根页面并触发登录。

## 2. 登录后主页面（Album）

`AlbumViewController` 是唯一主业务页。

### 顶部

1. `UISegmentedControl`：`本地 - x项` / `远端 - x项`。
2. 左上角：Settings。
3. 右上角：
4. 本地模式：Filter 图标（筛选/排序/显示样式 menu）。
5. 远端模式：`刷新` + `导回`。

### 内容区

1. `UICollectionView` 按“年月 section”展示。
2. cell 样式：默认 4 列正方形；可切换原始比例网格。
3. 角标：
4. 左上：`LIVE` / `VIDEO` / `PHOTO`
5. 右上：`未备份`（仅本地）
6. 左下：日期（MM-dd）
7. 远端支持多选（用于导回）。

### 底部

1. 单个蓝色浮动按钮（动态文案和颜色）。
2. 点按后 present `BackupStatusViewController`。
3. collectionView 底部 inset 会为该按钮留出空间。

## 3. 备份状态页（BackupStatusViewController）

1. 导航栏右侧 3 个 symbol 按钮：
2. 开始（play）
3. 暂停（pause）
4. 停止（stop）
5. 顶部两行 segment：`全部/成功/失败/跳过/日志` + 数量副标题。
6. 列表展示处理结果（含缩略图、文件名、状态、原因）。
7. 日志页显示追加日志。

数据源来自 `BackupSessionController` 快照：

1. `state/statusText/succeeded/failed/skipped/total`
2. `processedItems`
3. `failedItems`
4. `logs`

## 4. Settings 页面

`SettingsViewController` 当前功能：

1. 显示当前服务器、权限状态、索引统计。
2. 切换 SMB 服务器（回登录页）。
3. 请求照片权限。
4. 手动重同步远端索引（调用 `backupExecutor.reloadRemoteIndex`）。
5. 清空本地 `content_hash_index`。

## 5. Live Photo 展示策略

1. 备份层按资源拆分（photo + pairedVideo）。
2. Album 远端展示层按规则聚合成一个 Live 项（同月、同时间戳、同名 stem）。
3. 导回时按组写回（有 photo+pairedVideo 时会作为同组资源导入）。
