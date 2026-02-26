# PhotoBackup / Watermelon 接手总览

本仓库的 App 名称和模块名目前是 `Watermelon`，仓库名是 `PhotoBackup`。  
当前主线已经切到“登录 SMB 后进入单一 Album 页 + 备份状态页”的结构。

## 1. 先读这几处

1. `Watermelon/App/AppCoordinator.swift`
2. `Watermelon/UI/Album/AlbumViewController.swift`
3. `Watermelon/UI/Backup/BackupSessionController.swift`
4. `Watermelon/Services/Backup/BackupExecutor.swift`
5. `Watermelon/Services/Backup/MonthManifestStore.swift`
6. `Watermelon/Services/Backup/RemoteLibraryScanner.swift`

构建入口请优先使用 `Watermelon.xcodeproj`（仓库里还存在一个空壳 `PhotoBackup.xcodeproj` workspace）。

## 2. 当前产品流（真实代码）

1. 启动进入 `ServerSelectionViewController`。
2. 自动尝试用已保存服务器+Keychain 密码登录（可关闭自动登录）。
3. 登录成功后进入 `AlbumViewController`（无 TabBar）。
4. Album 左上角 `Settings`，右上角根据模式显示：
5. 本地模式：Filter 菜单。
6. 远端模式：`刷新` + `导回`。
7. 底部浮动按钮打开 `BackupStatusViewController`，在该页执行开始/暂停/停止。

## 3. 备份核心机制（当前实现）

1. 远端目录：`/{YYYY}/{MM}/`。
2. 每个月目录有 `.watermelon_manifest.sqlite`。
3. 去重主依据：`contentHash`（SHA-256 32-byte BLOB）。
4. 本地持久化只保留 `content_hash_index` 表（asset+resource -> hash）。
5. 备份顺序：`PHAsset` 按 `creationDate ASC`，逐月处理；切月时 flush manifest。
6. 同名冲突：小文件下载远端同名比 hash，大文件比 size，不同则 `_n` 重命名。

## 4. 代码分层

1. App 入口/组装：`Watermelon/App/*`
2. Data（GRDB/Keychain）：`Watermelon/Data/*`
3. Domain 类型：`Watermelon/Domain/*`
4. Services（Backup/SMB/Photo/Restore）：`Watermelon/Services/*`
5. UI：`Watermelon/UI/*`

## 5. 接手注意点

1. `Records.swift` 里有不少旧结构体（`BackupResourceRecord` 等），但当前迁移只创建了三张表，详见 `03-DataModel.md`。
2. `BackupPlanner`、`ManifestSyncService`、`UI/Browser/*`、`BackupViewController` 多为旧链路遗留，主流程不再使用。
3. 远端缩略图已切 Kingfisher `ImageDataProvider` + `RemoteThumbnailService`，但仍是“先下载远端文件到临时目录再下采样”。
