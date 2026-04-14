# 数据模型（本地 GRDB + 远端月级 SQLite）

## 1. 本地数据库（`DatabaseManager`）

当前迁移策略：

1. `v7_dev_schema_reset`
2. 开发期直接重建表结构（drop 后按最新 schema 创建）

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
  createdAt DATETIME NOT NULL,
  updatedAt DATETIME NOT NULL
);

CREATE UNIQUE INDEX idx_server_profiles_unique_smb
ON server_profiles(host, port, shareName, basePath, username, IFNULL(domain, ''))
WHERE storageType = 'smb';
```

### `sync_state`

```sql
CREATE TABLE sync_state (
  stateKey TEXT PRIMARY KEY NOT NULL,
  stateValue TEXT NOT NULL,
  updatedAt DATETIME NOT NULL
);
```

### `local_assets`

```sql
CREATE TABLE local_assets (
  assetLocalIdentifier TEXT NOT NULL,
  assetFingerprint BLOB NOT NULL,
  resourceCount INTEGER NOT NULL,
  totalFileSizeBytes INTEGER NOT NULL DEFAULT 0,
  updatedAt DATETIME NOT NULL,
  PRIMARY KEY(assetLocalIdentifier)
);

CREATE INDEX idx_local_assets_fingerprint
ON local_assets(assetFingerprint);
```

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

## 2. 远端月 manifest（`MonthManifestStore`）

路径：`/{YYYY}/{MM}/.watermelon_manifest.sqlite`

当前迁移策略：

1. `month_manifest_v3_dev_schema_reset`
2. 迁移中 drop 旧表后按 baseline 重新建表

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

## 3. 字段语义

### 本地侧

1. `local_assets.assetFingerprint`：由 Asset 下 `(role, slot, contentHash)` 组合计算。
2. `local_assets.totalFileSizeBytes`：该 asset 资源总大小（用于范围估算与统计）。
3. `local_asset_resources.fileSize`：资源级大小缓存。
4. `server_profiles.storageType`：`smb` / `webdav` / `externalVolume`。
5. `server_profiles.connectionParams`：类型特定参数（WebDAV endpoint、外接存储 bookmark 等）。

### 远端侧

1. `resources`：月目录实际文件对象。
2. `assets`：逻辑资产对象（按 fingerprint 唯一）。
3. `asset_resources`：资产到资源的关联，保留 role/slot。
4. `assets.totalFileSizeBytes`：该逻辑资产的总大小。

## 4. assetFingerprint 计算规则

1. 对每个资源构造 token：`role|slot|hashHex`
2. token 排序后用换行连接
3. 对结果做 SHA-256，得到 32-byte `assetFingerprint`

## 5. Keychain

1. 密码不写入 SQLite，仅存 Keychain
2. keychain service：`com.zizicici.watermelon.credentials`
3. keychain account：`credentialRef`
4. SMB/WebDAV 用密码；外接存储依赖 bookmark（`connectionParams`）
