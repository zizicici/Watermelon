# 数据模型（本地 SQLite + 远端月 manifest + 内存快照）

## 1. 本地数据库（`DatabaseManager`）

当前本地数据库由 GRDB 管理，迁移：`v1_initial`。

### `server_profiles`

```sql
CREATE TABLE server_profiles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  storageType TEXT NOT NULL DEFAULT 'smb',
  connectionParams BLOB,
  sortOrder INTEGER NOT NULL DEFAULT 0,
  host TEXT NOT NULL,
  port INTEGER NOT NULL,
  shareName TEXT NOT NULL,
  basePath TEXT NOT NULL,
  username TEXT NOT NULL,
  domain TEXT,
  credentialRef TEXT NOT NULL,
  backgroundBackupEnabled INTEGER NOT NULL DEFAULT 1,
  createdAt DATETIME NOT NULL,
  updatedAt DATETIME NOT NULL
);

CREATE UNIQUE INDEX idx_server_profiles_unique_smb
ON server_profiles(host, port, shareName, basePath, username, IFNULL(domain, ''))
WHERE storageType = 'smb';
```

说明：

1. `storageType` 当前取值：`smb` / `webdav` / `externalVolume`
2. SMB 唯一性由 host/port/shareName/basePath/username/domain 决定
3. WebDAV 和外接存储的类型特定参数放在 `connectionParams`

### `sync_state`

```sql
CREATE TABLE sync_state (
  stateKey TEXT PRIMARY KEY NOT NULL,
  stateValue TEXT NOT NULL,
  updatedAt DATETIME NOT NULL
);
```

当前主要用途：

1. 记录 `active_server_profile_id`

### `local_assets`

```sql
CREATE TABLE local_assets (
  assetLocalIdentifier TEXT NOT NULL,
  assetFingerprint BLOB,
  resourceCount INTEGER NOT NULL DEFAULT 0,
  totalFileSizeBytes INTEGER NOT NULL DEFAULT 0,
  modificationDateNs INTEGER,
  updatedAt DATETIME NOT NULL,
  PRIMARY KEY(assetLocalIdentifier)
);

CREATE INDEX idx_local_assets_has_fingerprint
ON local_assets(assetLocalIdentifier)
WHERE assetFingerprint IS NOT NULL;
```

说明：

1. `assetFingerprint` 可为 `NULL`，表示该资产尚未完成资源级 hash 建索引（比如仅存于 iCloud 的资产）
2. `modificationDateNs` 用来做体积缓存失效判断，上传 / 体积扫描路径都会复用

### `local_asset_resources`

```sql
CREATE TABLE local_asset_resources (
  assetLocalIdentifier TEXT NOT NULL,
  role INTEGER NOT NULL,
  slot INTEGER NOT NULL,
  contentHash BLOB NOT NULL,
  fileSize INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(assetLocalIdentifier, role, slot)
);

CREATE INDEX idx_local_asset_resources_hash
ON local_asset_resources(contentHash);
```

## 2. `connectionParams` 的真实内容

当前代码中的两种 typed payload：

### WebDAV

```swift
struct WebDAVConnectionParams: Codable {
    let endpointURLString: String
}
```

### 外接存储

```swift
struct ExternalVolumeConnectionParams: Codable {
    let rootBookmarkData: Data
    let displayPath: String
}
```

说明：

1. SMB 主要信息直接落在 `host / port / shareName / basePath / username / domain`
2. WebDAV 仍复用通用字段，但 endpoint 通过 `connectionParams` 保存
3. 外接存储依赖 security-scoped bookmark，不需要密码

## 3. 本地 hash 索引语义

### `local_assets`

1. `assetFingerprint`
   - 由 `(role, slot, contentHash)` 组合计算
   - 是首页 reconcile 和下载去重的关键标识
2. `resourceCount`
   - 该 asset 下纳入备份的资源数
3. `totalFileSizeBytes`
   - 该 asset 所有选中资源的总大小
   - 用于月份体积估算、首页文件大小统计

### `local_asset_resources`

1. 主键是 `(assetLocalIdentifier, role, slot)`
2. `contentHash` 是资源级 SHA-256
3. `fileSize` 是资源级文件大小缓存

## 4. 远端月 manifest（`MonthManifestStore`）

路径：

1. `/{YYYY}/{MM}/.watermelon_manifest.sqlite`

当前迁移：`month_manifest_v1_initial`

### `resources`

```sql
CREATE TABLE resources (
  fileName TEXT PRIMARY KEY NOT NULL,
  contentHash BLOB NOT NULL,
  fileSize INTEGER NOT NULL,
  resourceType INTEGER NOT NULL,
  creationDateNs INTEGER,
  backedUpAtNs INTEGER NOT NULL
);

CREATE UNIQUE INDEX idx_resources_contentHash
ON resources(contentHash);
```

### `assets`

```sql
CREATE TABLE assets (
  assetFingerprint BLOB PRIMARY KEY NOT NULL,
  creationDateNs INTEGER,
  backedUpAtNs INTEGER NOT NULL,
  resourceCount INTEGER NOT NULL,
  totalFileSizeBytes INTEGER NOT NULL
);
```

### `asset_resources`

```sql
CREATE TABLE asset_resources (
  assetFingerprint BLOB NOT NULL,
  resourceHash BLOB NOT NULL,
  role INTEGER NOT NULL,
  slot INTEGER NOT NULL,
  PRIMARY KEY(assetFingerprint, role, slot)
);

CREATE INDEX idx_asset_resources_asset
ON asset_resources(assetFingerprint);

CREATE INDEX idx_asset_resources_hash
ON asset_resources(resourceHash);
```

## 5. 远端 manifest 字段语义

### `resources`

1. 一行对应远端月目录中的一个真实文件
2. `resourceType` 保存 `PHAssetResourceType.rawValue`
3. `creationDateNs` 尽量保留资源创建时间
4. `backedUpAtNs` 是备份写入时间

### `assets`

1. 一行对应一个逻辑资产
2. 通过 `assetFingerprint` 去重
3. `resourceCount` 和 `totalFileSizeBytes` 用于统计、首页汇总和校验

### `asset_resources`

1. 连接逻辑资产与资源 hash
2. 保留 `role / slot`，方便重建资源实例与媒体类型判定

## 6. `assetFingerprint` 计算规则

当前规则：

1. 对每个资源生成 token：`role|slot|hashHex`
2. token 排序
3. 用 `\n` 连接
4. 对最终字符串做 SHA-256

## 7. 内存态远端快照

Home 当前不直接反复扫远端文件系统，而是消费 `RemoteLibrarySnapshotCache` 暴露的状态。

### 全量快照

```swift
struct RemoteLibrarySnapshot {
    let resources: [RemoteManifestResource]
    let assets: [RemoteManifestAsset]
    let assetResourceLinks: [RemoteAssetResourceLink]
}
```

### 增量状态

```swift
struct RemoteLibrarySnapshotState {
    let revision: UInt64
    let isFullSnapshot: Bool
    let monthDeltas: [RemoteLibraryMonthDelta]
}
```

说明：

1. `revision` 用来让 Home 只消费“上次之后的变化”
2. `monthDeltas` 是月级增量，不是整库重建
3. 这部分是内存状态，不写回 SQLite

## 8. Keychain 与会话密码

1. 密码不写入 SQLite
2. 通过 `KeychainService` 保存
3. keychain service：`com.zizicici.watermelon.credentials`
4. `AppSession` 里保留当前连接的会话密码
5. SMB / WebDAV 需要密码；外接存储不需要
