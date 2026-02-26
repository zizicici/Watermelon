# 数据结构（本地 GRDB + 远端月级 manifest）

## 1. 本地 SQLite（`DatabaseManager` 当前迁移）

迁移名：`v3_dev_reset_schema`  
开发期策略：直接重建（会 drop 一批旧表）。

实际创建表只有 3 张：

### `server_profiles`

```sql
CREATE TABLE server_profiles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  host TEXT NOT NULL,
  port INTEGER NOT NULL,
  shareName TEXT NOT NULL,
  basePath TEXT NOT NULL,
  username TEXT NOT NULL,
  domain TEXT,
  credentialRef TEXT NOT NULL,
  createdAt DATETIME NOT NULL,
  updatedAt DATETIME NOT NULL,
  UNIQUE(host, shareName, basePath, username)
);
```

### `sync_state`

```sql
CREATE TABLE sync_state (
  stateKey TEXT PRIMARY KEY NOT NULL,
  stateValue TEXT NOT NULL,
  updatedAt DATETIME NOT NULL
);
```

用途示例：`active_server_profile_id`。

### `content_hash_index`

```sql
CREATE TABLE content_hash_index (
  assetLocalIdentifier TEXT NOT NULL,
  resourceLocalIdentifier TEXT NOT NULL,
  contentHash BLOB NOT NULL,
  PRIMARY KEY(assetLocalIdentifier, resourceLocalIdentifier)
);

CREATE INDEX idx_content_hash_index_hash
ON content_hash_index(contentHash);
```

用途：本地资源 hash 索引（快速镜像匹配/已备份判断）。

## 2. 远端月级 manifest（`MonthManifestStore`）

路径：`/{YYYY}/{MM}/.watermelon_manifest.sqlite`

```sql
CREATE TABLE manifest_items (
  fileName TEXT PRIMARY KEY,
  contentHash BLOB NOT NULL,      -- 32-byte SHA-256
  fileSize INTEGER NOT NULL,
  resourceType INTEGER NOT NULL,
  creationDateNs INTEGER,
  backedUpAtNs INTEGER NOT NULL
);

CREATE UNIQUE INDEX idx_manifest_items_contentHash
ON manifest_items(contentHash);
```

## 3. 字段语义

### manifest_items

1. `fileName`：月目录内最终文件名（可能是重命名后的 `_n`）。
2. `contentHash`：资源字节 hash（SHA-256 32-byte Data）。
3. `fileSize`：上传资源大小。
4. `resourceType`：`PHAssetResourceType` 映射的整数。
5. `creationDateNs`：原资源创建时间（ns since epoch，可空）。
6. `backedUpAtNs`：写入 manifest 的时间（ns since epoch）。

### content_hash_index

1. `assetLocalIdentifier`：本地 `PHAsset.localIdentifier`。
2. `resourceLocalIdentifier`：当前策略是 `assetID::resourceIndex::resourceTypeRawValue`。
3. `contentHash`：该资源导出字节 hash。

## 4. Keychain

密码不入库，保存在 Keychain：

1. service: `com.zizicici.watermelon.credentials`
2. account: `credentialRef`（host/share/user 组合）

## 5. 遗留结构说明

`Records.swift` 中仍定义了 `BackupAssetRecord` / `BackupResourceRecord` / `BackupJobRecord` 等类型，  
但当前迁移不会创建这些表。新开发请以 `DatabaseManager.migrator` 的真实 schema 为准。
