# 数据结构（本地 GRDB + 远端月级 manifest）

## 1. 本地 SQLite（`DatabaseManager`）

迁移名：`v3_dev_reset_schema`、`v4_server_profiles_storage_type`、`v5_server_profiles_sort_order`、`v6_server_profiles_partial_unique_smb`。

开发期策略：重建 schema（会 drop 已知旧表）。

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
ON server_profiles(host, shareName, basePath, username)
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

用途示例：`active_server_profile_id`。

### `local_assets`

```sql
CREATE TABLE local_assets (
  assetLocalIdentifier TEXT NOT NULL,
  assetFingerprint BLOB NOT NULL,
  resourceCount INTEGER NOT NULL,
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
  PRIMARY KEY(assetLocalIdentifier, role, slot)
);

CREATE INDEX idx_local_asset_resources_hash
ON local_asset_resources(contentHash);
```

## 2. 远端月级 manifest（`MonthManifestStore`）

路径：`/{YYYY}/{MM}/.watermelon_manifest.sqlite`

迁移名：`month_manifest_v2_reset_schema` + `month_manifest_v2_schema_baseline`（幂等基线迁移，非破坏式）。

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
  resourceCount INTEGER NOT NULL
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

### local side

1. `local_assets.assetFingerprint`：由 Asset 下资源的 `(role, slot, contentHash)` 组合计算得到。
2. `local_asset_resources`：记录单个 Asset 在本地对应资源位点（role/slot）的 hash。
3. `server_profiles.storageType`：连接类型（`smb` / `webdav` / `externalVolume`）。
4. `server_profiles.connectionParams`：类型特定参数（如 WebDAV 的 `endpointURLString`、外接存储的 bookmark + displayPath）。
5. `server_profiles.sortOrder`：连接列表排序值，管理页拖拽后更新。

### remote manifest side

1. `resources`：月目录里实际可访问文件对象。
2. `assets`：逻辑 Asset 对象（按 fingerprint 唯一）。
3. `asset_resources`：Asset 与 Resource 的关系表，同时保留 role/slot 顺序语义。

## 4. assetFingerprint 规则

当前实现：

1. 对 Asset 内每个资源构造 token：`role|slot|hashHex`。
2. token 排序后以 `\n` 连接。
3. 对拼接文本做 SHA-256，得到 `assetFingerprint`（32-byte Data）。

## 5. Keychain

密码不入库，保存在 Keychain：

1. service：`com.zizicici.watermelon.credentials`
2. account：`credentialRef`
3. SMB / WebDAV 连接使用该密码；外接存储不依赖密码（bookmark 在 `connectionParams`）。
