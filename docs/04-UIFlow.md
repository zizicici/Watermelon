# UI 流程与状态（当前主链路）

## 1. 启动与主页面

App 启动后直接进入 `HomeViewController`。

### 顶部导航

1. 右上角连接按钮显示当前状态：`加载中……` / `当前连接标识` / `单机模式`。
2. `SMB` 连接时标识是 `username@share/basePath`。
3. `WebDAV` 连接时标识是 `username@profileName`。
4. `外接存储` 连接时标识是 profile 名称（用户输入的名称）。
5. 连接按钮菜单包含三段：
6. 当前模式切换（单机模式/已保存连接）。已保存连接有类型图标：SMB=`network`，WebDAV=`globe`，外接存储=`externaldrive`；单机模式无图标。
7. 添加存储子菜单：`SMB 局域网发现`、`添加 SMB 存储`、`添加 WebDAV 存储`、`添加外接存储目录`。
8. 管理入口：`管理存储`（无已保存连接时禁用；单机模式不在管理页）。

### 底部工具栏

1. 左侧：`筛选` 菜单。
2. 右侧：`备份`（备份运行时显示 `备份中`）。

### 内容区

1. `UICollectionView` 按年月 section 展示合并后的条目。
2. 条目来源：`localOnly` / `remoteOnly` / `both`。
3. 远端项严格按 manifest 的 `assets + asset_resources + resources` 关系组装。

## 2. Home 的数据刷新行为

1. 首次进入与会话变化后会 `reloadAllData`。
2. 连接任意存储类型成功后都会先 `backupExecutor.reloadRemoteIndex` 再刷新 UI。
3. 备份运行中会节流刷新远端 section（不是每次 progress 都全量重建）。
4. 备份从 running 变为非 running 时，会自动重载一次远端索引。

## 3. 筛选菜单

`筛选` 菜单包含：

1. 来源筛选：全部 / 仅本地 / 仅远端 / 远端+本地。
2. 排序：正序 / 倒序。
3. 显示选项：正方形网格 / 原始比例网格。
4. 已连接远端时额外显示：重建远端索引（备份运行中禁用）。

## 4. 备份状态页（`BackupStatusViewController`）

1. 通过 Home 工具栏“备份”按钮以 sheet 打开。
2. 顶部导航按钮：开始（play）、暂停（pause）、停止（stop）。
3. 过滤项：全部 / 成功 / 失败 / 跳过 / 日志。
4. 列表项显示：缩略图、Asset 显示名、状态、资源摘要与原因。
5. 日志视图增量追加，不重复整段刷新。

## 5. 失败重试

1. `BackupSessionController` 聚合失败 Asset 列表。
2. 支持重试全部失败项或单项重试。
3. retry 是按 Asset ID 集合执行。

## 6. 存储添加与管理流程（由 Home 触发）

### 添加 SMB

1. `AddSMBServerLoginViewController`
2. `SMBSharePathPickerViewController`
3. `AddSMBServerViewController`

保存成功后回到 Home，并尝试连接该 profile。

### 添加外接存储

1. `AddExternalStorageViewController`
2. 选择目录并保存 bookmark 到 `connectionParams`

保存成功后回到 Home，并尝试连接该 profile。

### 添加 WebDAV

1. `AddWebDAVStorageViewController`
2. 输入名称、Endpoint URL、备份根路径、用户名、密码
3. 保存后写入 `server_profiles`（`storageType=webdav` + `connectionParams.endpointURLString`）并写 Keychain

保存成功后回到 Home，并尝试连接该 profile。

### 管理存储

1. `ManageStorageProfilesViewController`
2. 支持删除、排序（编辑模式拖拽）、编辑连接参数。
3. 点击某个连接项会直接进入“按类型的连接参数编辑页”（不再先弹“编辑名称/编辑连接参数”二选一；WebDAV 也同样直接进入参数页）。
4. 名称修改包含在参数编辑页内。

## 7. 当前未接入主入口的页面

1. `ServerSelectionViewController`
2. `SettingsViewController`

这两个页面目前不在 App 启动路径中。
