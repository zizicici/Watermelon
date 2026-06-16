# 数据模型（本地 SQLite + 远端月 manifest + 内存快照）

## 1. 本地数据库（`DatabaseManager`）

`DatabaseManager` 实现位于 `Shared/Data/Database/DatabaseManager.swift`，由 GRDB 管理。当前注册的迁移按顺序：

1. `v1_initial`
2. `v2_ms_timestamps`
3. `v3_writer_id`（给 `server_profiles` 加 `writerID` 列）

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
  updatedAt DATETIME NOT NULL,
  writerID TEXT
);

CREATE UNIQUE INDEX idx_server_profiles_unique_smb
ON server_profiles(host, port, shareName, basePath, username, IFNULL(domain, ''))
WHERE storageType = 'smb';
```

说明：

1. `storageType` 当前取值：`smb` / `webdav` / `s3` / `sftp` / `externalVolume`
2. SMB 唯一性由 host/port/shareName/basePath/username/domain 决定
3. WebDAV / S3 / SFTP / 外接存储的类型特定参数放在 `connectionParams`，结构化字段（host / port / shareName / basePath / username）尽量复用通用列
4. SFTP 唯一性由调用方通过 `(host, port, basePath, username)` 在保存时校验（`AddSFTPStorageViewController.findExistingProfile`）；DB 层没有像 SMB 那样的部分唯一索引
5. `writerID` 由 `v3_writer_id` 迁移加入，是机器侧持久身份（小写 UUID，懒生成）；Repo V2 写锁用它标识本写入方，内存值永不覆盖 DB 实际值

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
-- v1_initial 建表（modificationDate 列名最初是 modificationDateNs）
CREATE TABLE local_assets (
  assetLocalIdentifier TEXT NOT NULL,
  assetFingerprint BLOB,
  resourceCount INTEGER NOT NULL DEFAULT 0,
  totalFileSizeBytes INTEGER NOT NULL DEFAULT 0,
  modificationDateNs INTEGER,
  updatedAt DATETIME NOT NULL,
  PRIMARY KEY(assetLocalIdentifier)
);

-- v2_ms_timestamps 重命名并把已有值除以 1_000_000，对齐为毫秒
ALTER TABLE local_assets RENAME COLUMN modificationDateNs TO modificationDateMs;
UPDATE local_assets
SET modificationDateMs = modificationDateMs / 1000000
WHERE modificationDateMs IS NOT NULL;

CREATE INDEX idx_local_assets_has_fingerprint
ON local_assets(assetLocalIdentifier)
WHERE assetFingerprint IS NOT NULL;
```

说明：

1. `assetFingerprint` 可为 `NULL`，表示该资产尚未完成资源级 hash 建索引（比如仅存于 iCloud 的资产）
2. `modificationDateMs` 用来做体积缓存失效判断，上传 / 体积扫描路径都会复用（Int64 毫秒；窗口 ±2.9 亿年，足够覆盖任何 `PHAsset` 时间戳）
3. 从老版本升上来的设备会在第一次启动 `v2_ms_timestamps` 自动完成迁移；不需要重新建索引

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

类型定义位于 `Shared/Domain/StorageProfile.swift`：

### WebDAV

```swift
struct WebDAVConnectionParams: Codable {
    let scheme: String
}
```

说明：

1. 当前 WebDAV 的完整 endpoint 是从结构化字段拼出来的（`scheme + host + port + shareName 作 path`）
2. 早期版本曾把整个 `endpointURLString` 落到 `connectionParams` 里；解码器会兜底——遇到老格式时从 `endpointURLString` 反推出 scheme
3. 仅 `scheme` 会被写回，老字段在升级后随首次保存自动消失

### S3

```swift
struct S3ConnectionParams: Codable {
    let scheme: String     // "https" / "http"
    let region: String     // 用户填的或者从 host 推出来的，如 "us-east-1"
    let usePathStyle: Bool // path-style vs virtual-host-style
}
```

说明：

1. `host` 是 S3 endpoint host（如 `s3.amazonaws.com` / `play.min.io` / 自部署 `minio.lan`）
2. `port` 0 / 80 / 443 都视为默认端口，不会出现在 display URL 里
3. `shareName` 复用为 bucket 名
4. `basePath` 为 bucket 内的 key 前缀（默认 `/`）
5. `username` 是 access key ID；secret access key 落 Keychain
6. `usePathStyle = true` 时使用 `scheme://host[:port]/bucket/...`，否则 `scheme://bucket.host[:port]/...`

### SFTP

```swift
struct SFTPConnectionParams: Codable {
    enum AuthMethod: String, Codable { case password, privateKey }
    let authMethod: AuthMethod
    let hostKeyFingerprintSHA256: String
}
```

说明：

1. `host / port / basePath / username` 走结构化列
2. `authMethod` 仅指示 UI 用哪个分支编辑；真正的凭证 (`SFTPCredentialBlob`，下同) 落 Keychain，不落 DB
3. `hostKeyFingerprintSHA256` 形如 `SHA256:<base64-no-padding>`，由两阶段 TOFU 在保存时钉住；运行期连接以 `.pin` 模式拒绝任何不一致的服务端
4. `port == 0` 解释为 22

#### `SFTPCredentialBlob`（保存到 Keychain，不进 DB）

```swift
enum SFTPCredentialBlob: Codable {
    case password(String)
    case privateKey(pem: String, passphrase: String?)
}
```

1. `encodedJSONString()` / `decode(from:)` 是 JSON round-trip；写入 Keychain 用 `account = "sftp|host:port|username|basePath"`
2. 私钥仅支持 OpenSSH ed25519 / RSA；其它类型在握手时抛 `SFTPUnsupportedKeyTypeError`
3. `passphrase` 为空字符串等价于 `nil`

### 外接存储

```swift
struct ExternalVolumeConnectionParams: Codable {
    let rootBookmarkData: Data
    let displayPath: String
}
```

说明：

1. SMB 主要信息直接落在 `host / port / shareName / basePath / username / domain`
2. WebDAV / S3 用结构化字段 + `connectionParams` 里的 scheme（以及 S3 的 region / usePathStyle）一起拼 URL
3. SFTP 的结构化字段为 `host / port / basePath / username`；`shareName` 不使用，`domain` 为 `nil`
4. 外接存储依赖 security-scoped bookmark，不需要密码

## 3. 本地 hash 索引语义

记录类型在 `Shared/Data/Database/Records.swift`。

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

实现位于 `Shared/Services/Backup/`：核心入口在 `MonthManifestStore.swift`，初始化 / seed 在 `+Loading.swift`，schema 与迁移在 `+Schema.swift`。

路径（两种布局，schema 相同，仅 manifest 文件位置不同，见 `ManifestLayout`）：

1. `.v1`（当前生产）：`/{YYYY}/{MM}/.watermelon_manifest.sqlite`
2. `.lite`（Repo V2）：`/.watermelon/months/{YYYY-MM}.sqlite`

当前迁移：`month_manifest_v1_initial`

### `resources`

```sql
CREATE TABLE resources (
  fileName TEXT PRIMARY KEY NOT NULL,
  contentHash BLOB NOT NULL,
  fileSize INTEGER NOT NULL,
  resourceType INTEGER NOT NULL,
  creationDateMs INTEGER,
  backedUpAtMs INTEGER NOT NULL
);

CREATE UNIQUE INDEX idx_resources_contentHash
ON resources(contentHash);
```

### `assets`

```sql
CREATE TABLE assets (
  assetFingerprint BLOB PRIMARY KEY NOT NULL,
  creationDateMs INTEGER,
  backedUpAtMs INTEGER NOT NULL,
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
3. `creationDateMs` 尽量保留资源创建时间（Int64 毫秒）
4. `backedUpAtMs` 是备份写入时间（Int64 毫秒）

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

类型都定义在 `Shared/Domain/RemoteLibraryDomain.swift`。Home 当前不直接反复扫远端文件系统，而是消费 `RemoteLibrarySnapshotCache`（`Shared/Services/Backup/`）暴露的状态。

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

struct RemoteLibraryMonthDelta {
    let month: LibraryMonthKey
    let resources: [RemoteManifestResource]
    let assets: [RemoteManifestAsset]
    let assetResourceLinks: [RemoteAssetResourceLink]
}
```

### 同步进度

```swift
struct RemoteSyncProgress {
    enum Kind { case scanningRemoteIndex, remoteIndex, repoUpgrade }
    let current: Int
    let total: Int
    let kind: Kind   // 默认 .remoteIndex
}
```

`kind` 区分进度语义：扫描远端索引 / 远端索引同步 / 仓库升级。

附属 digest（用于轻量摘要日志、不构造 per-asset 数组）：

```swift
struct RemoteIndexSyncDigest: Sendable {
    let resourceCount: Int
    let assetCount: Int
    let linkCount: Int
    var totalEntryCount: Int { resourceCount + assetCount + linkCount }
}
```

说明：

1. `revision` 用来让 Home 只消费“上次之后的变化”
2. `monthDeltas` 是月级增量，不是整库重建
3. `RemoteIndexSyncDigest` 是远端同步的廉价摘要，避免每次都把 per-asset 数组拷一份给只想看总数的调用方
4. 这部分是内存状态，不写回 SQLite
5. `RemoteMonthManifestDigest`（month / manifestSize / manifestModifiedAtMs）是月级 manifest 文件的轻量指纹，用于判断远端 manifest 是否变化
6. `RemoteAssetResourceInstance`（role / slot / resourceHash / fileName / fileSize / remoteRelativePath / creationDateMs）是重建单个远端资源实例的视图，`resourceType` 由 role 推回 `PHAssetResourceType`

## 8. Keychain 与会话密码

1. 密码不写入 SQLite
2. 通过 `KeychainService`（`Shared/Data/Security/KeychainService.swift`）保存
3. keychain service：`com.zizicici.watermelon.credentials`
4. `AppSession`（`Shared/Domain/AppSession.swift`）里保留当前连接的会话密码
5. SMB / WebDAV / S3（secret access key）需要密码；外接存储不需要
6. SFTP 在 Keychain 里存的是 `SFTPCredentialBlob` 序列化后的 JSON（password 模式存明文密码、privateKey 模式存 PEM 与可选 passphrase 的明文），`AppSession` 里同样保留这个 JSON 串作为"密码"传给 `StorageClientFactory`。`StorageProfile.supportsPasswordPrompt = false`：destination menu 在缺凭证时不会弹通用密码框，只能进编辑页重填

## 9. Repo V2（Lite）锁与仓库数据结构

### 写锁文件（`Shared/Services/Repo/RepoLockFile.swift`）

路径 `.watermelon/locks/<writerID>.lock`，body 为 JSON：

```swift
struct LockFileBody {
    let writerID: String
    let sessionToken: String
    let lockToken: String
    let generation: Int
    let writtenAt: Date?
}
```

1. `LockFileCodec.decode` 对空 / 无法解码内容返回 nil
2. `RemoteLockReader.Snapshot` = { rawData, body, modificationDate }
3. 新鲜度优先取后端 mtime，缺失时回退 body `writtenAt`；两者皆缺视为无效锁

### 自有锁冲突（`WriteLockService.OwnLockBlock`）

```swift
struct OwnLockBlock {
    enum Reason { case stillFresh, missingTimeEvidence, changedDuringConfirmation, ownershipUnverified }
    let reason: Reason
    let retryAfter: Date?
}
```

由 `Acquisition.blockedByOwnLock` / `.skippedByOwnLock` 与 `LiteRepoError.ownLockConflict(OwnLockBlock?)` 携带。

### V1→Lite 迁移进度

```swift
struct V1ToLiteMigrationProgress { let current: Int; let total: Int }
```
