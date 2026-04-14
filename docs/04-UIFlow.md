# UI 流程与状态（当前主链路）

## 1. 启动与 Home

App 启动后直接进入 `HomeViewController`。

### 顶部区域

左右两栏 header，各带全选 toggle：
1. 左侧：`本地相册`（全选/取消）
2. 右侧：`远端存储`（下拉菜单切换连接 + 全选/取消）

远端菜单内容：
1. 已保存连接列表（当前连接标 ✓）
2. `未连接` 断开选项

### 内容区

1. `UICollectionViewCompositionalLayout` 按年-月 section 展示
2. 每月一行：左 cell（本地）+ 右 cell（远端），中间箭头 badge
3. 箭头方向由选择组合决定：只选本地→(→)、只选远端→(←)、两侧→(↔)
4. 箭头旁显示进度百分比（基于 reconciliation `matchedCount`）
5. Section header 按年分组，显示年份、照片/视频计数、大小、年级全选 toggle
6. Cell 季节配色（1-3月绿、4-6月蓝、7-9月琥珀、10-12月红）

### 底部面板（SelectionActionPanel）

选中月份后弹出，显示三个分类按钮：
1. 备份(→) 计数 — 长按弹出月份详情菜单
2. 下载(←) 计数
3. 同步(↔) 计数
4. 执行按钮

执行模式下切换为：
1. 分类进度（pending → running x/y → completed ✓）
2. 暂停/恢复按钮 + 停止按钮
3. 完成后显示”完成”按钮退出

### 执行模式 Cell 状态

1. **待处理**：正常颜色 + 选中勾
2. **运行中**：正常颜色 + activity indicator
3. **已完成**：灰色背景 + 绿色勾

## 2. 执行流程

### 上传阶段

1. 收集所有上传+同步月份的本地 asset IDs
2. 创建 `BackupScopeSelection`，通过 `BackupSessionController.startBackup()` 执行
3. `handleBackupSnapshot` 跟踪 startedMonths/flushedMonths/processedCountByMonth
4. 进度更新：非终态 → `refreshRemoteDataInPlace + syncRemoteDataIfNeeded + reconfigureVisibleCells/Arrows`
5. 终态 `.completed` → 转入下载阶段（如有）或显示完成

### 下载阶段

1. 逐月执行 `ensureHashIndexAndDownload`
2. 先跑 scoped backup 填充 hash index → 刷新 local index（safety net）
3. `processDownloadMonth` 下载 remoteOnly items
4. 每个 item 完成后：`writeHashIndexForItem` + `refreshLocalIndex`（增量持久化）
5. 进度基于 reconciliation `matchedCount`（每 item 更新，支持断点续传）

### 停止/暂停

1. 上传阶段：`backupSessionController.stopBackup()`（cooperative cancellation）
2. 下载阶段：`downloadTask.cancel()` + `backupSessionController.stopBackup()` + `RestoreService` 循环内 `Task.checkCancellation`

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
