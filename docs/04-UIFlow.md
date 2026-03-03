# UI 流程与状态（当前主链路）

## 1. 启动与 Home

App 启动后直接进入 `HomeViewController`。

### 顶部导航（右上角连接按钮）

1. 状态文本：`加载中……` / 当前连接标识 / `单机模式`
2. 当前连接菜单 section：
3. `单机模式`
4. 已保存连接（带类型图标：SMB=`network`、WebDAV=`globe`、外接存储=`externaldrive`）
5. 添加存储菜单 section：
6. `SMB 局域网发现`
7. `添加 SMB 存储`
8. `添加 WebDAV 存储`
9. `添加外接存储目录`
10. 菜单末项：`更多`

### 底部工具栏

1. 左：`筛选`
2. 右：`备份`（运行中显示 `备份中`）

### 内容区

1. `UICollectionView` 按年月 section 展示本地/远端匹配条目
2. Home 在备份运行中会节流刷新远端 section
3. 运行结束后会触发一次全量刷新

## 2. 筛选菜单

`筛选` 菜单包含：

1. 来源：全部 / 仅本地 / 仅远端 / 远端+本地
2. 排序：正序 / 倒序
3. 显示：正方形网格 / 原始比例网格
4. 已连接远端时提供“重建远端索引”（备份运行中禁用）

## 3. 备份页（`BackupViewController`）

### 顶部与控制

1. 通过 Home 工具栏“备份”以 sheet 打开
2. 导航栏按钮：开始、暂停、停止
3. 顶部“备份范围”卡片：显示全选/部分/未选、数量与估算容量，并可“调整”
4. 状态卡片显示当前上传项与总体进度
5. 多 worker 时显示 `W1/W2/...` 分段切换查看不同 worker 的实时上传状态

### 列表与日志

1. 过滤：全部 / 成功 / 失败 / 跳过 / 日志
2. 列表项显示缩略图、名称、状态、原因、资源摘要
3. 日志视图增量追加（避免整段重渲染）

### 范围调整交互

1. 任务未运行时：可直接进入范围选择器并修改
2. 任务运行中：弹窗支持“仅查看当前范围”或“停止并调整”

## 4. 范围选择器（`BackupRangeSelectorViewController`）

1. 按月份分组展示，默认折叠
2. Header 支持该月全选/取消与展开/收起
3. 月内网格复用 `AlbumGridCell`，可点选资产
4. 导航栏底部 toolbar 提供“全选/全不选”
5. 统计信息优先使用本地 hash 索引 size；缺失时显示待统计
6. 运行中只读打开，不允许修改

## 5. More 页面（`MoreViewController`）

入口：Home 右上角连接菜单末项 `更多`。

分组：

1. `远端存储`：`管理存储` -> `ManageStorageProfilesViewController`
2. `本地数据`：`本地 Hash 索引` -> `LocalHashIndexManagerViewController`
3. `备份`：`上传并发`（automatic/1/2/3/4）
4. 其他通用设置与关于信息

## 6. 存储添加与管理流程

### 添加 SMB

1. `AddSMBServerLoginViewController`
2. `SMBSharePathPickerViewController`
3. `AddSMBServerViewController`

### 添加 WebDAV

1. `AddWebDAVStorageViewController`
2. 保存到 `server_profiles`（`storageType=webdav` + `connectionParams`），密码写 Keychain

### 添加外接存储

1. `AddExternalStorageViewController`
2. 目录授权后保存 security-scoped bookmark 到 `connectionParams`

### 管理存储

1. 位于 More 页“远端存储”分组
2. `ManageStorageProfilesViewController` 支持删除、排序、按类型编辑连接参数
