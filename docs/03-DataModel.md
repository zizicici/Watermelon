# 数据模型（本地 SQLite + 远端 V1/V2 元数据 + 内存快照）

## 1. 本地数据库（`DatabaseManager`）

`DatabaseManager` 实现位于 `Shared/Data/Database/DatabaseManager.swift`，由 GRDB 管理。当前注册的迁移按顺序：

1. `v1_initial`
2. `v2_ms_timestamps`
3. `v3_repo_local_state`
4. `v4_duplicate_candidate_index`

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

-- v3_repo_local_state
ALTER TABLE server_profiles ADD COLUMN writerID TEXT;
```

说明：

1. `storageType` 当前取值：`smb` / `webdav` / `s3` / `sftp` / `externalVolume`
2. SMB 唯一性由 host/port/shareName/basePath/username/domain 决定
3. WebDAV / S3 / SFTP / 外接存储的类型特定参数放在 `connectionParams`，结构化字段（host / port / shareName / basePath / username）尽量复用通用列
4. SFTP 唯一性由调用方通过 `(host, port, basePath, username)` 在保存时校验（`AddSFTPStorageViewController.findExistingProfile`）；DB 层没有像 SMB 那样的部分唯一索引
5. `writerID` 由 V2 repo 写入路径 lazy ensure；同一个 profile 跨 run 复用，作为 commit / snapshot / liveness / 迁移 marker 的 writer identity

### `repo_state`

`v3_repo_local_state` 新增。它把 profile 与远端 repo identity 绑定，并持久化本 writer 的 Lamport clock / commit seq 高水位。

```sql
CREATE TABLE repo_state (
  profileID INTEGER NOT NULL,
  repoID TEXT NOT NULL,
  writerID TEXT NOT NULL,
  lastClock INTEGER NOT NULL DEFAULT 0,
  lastSeq INTEGER NOT NULL DEFAULT 0,
  migrationCompleted INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(profileID, repoID)
);
```

说明：

1. `repoID` 来自 identity claim election / `repo-identity.json` finalization；`.watermelon/repo.json` 是 read-cache，`BackupV2RuntimeBuilder` 会拒绝本地记录与远端 observed repoID 不一致的情况
2. `lastClock` / `lastSeq` 在 cold start 时会先与远端 materialize 的 observed clock / own-writer max seq 对齐，再继续分配
3. `migrationCompleted` 用于 V1→V2 迁移重入；删除 profile 时会同时删除对应 `repo_state`

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
2. 按 profile 记录远端校验时间：`remote_verified_at_<profileID>`

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

-- v3_repo_local_state
ALTER TABLE local_assets ADD COLUMN selectionVersion INTEGER NOT NULL DEFAULT 0;
ALTER TABLE local_assets ADD COLUMN resourceSignature BLOB;

-- v4_duplicate_candidate_index
CREATE INDEX idx_local_assets_fingerprint_candidates
ON local_assets(assetFingerprint, assetLocalIdentifier)
WHERE assetFingerprint IS NOT NULL AND resourceSignature IS NOT NULL;
```

说明：

1. `assetFingerprint` 可为 `NULL`，表示该资产尚未完成资源级 hash 建索引（比如仅存于 iCloud 的资产）
2. `modificationDateMs` 用来做体积缓存失效判断，上传 / 体积扫描路径都会复用（Int64 毫秒；窗口 ±2.9 亿年，足够覆盖任何 `PHAsset` 时间戳）
3. `selectionVersion` / `resourceSignature` 由 `BackupAssetResourcePlanner` 写入，用来判断既有 hash 是否仍匹配当前资源选择规则；默认 `0` 会强制下次索引构建重新校验
4. 从老版本升上来的设备会在第一次启动迁移时自动补齐 schema；不需要人工处理

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
2. `port = 0` 视为未指定；display URL 只隐藏当前 scheme 的默认端口（`https:443` / `http:80`），非当前 scheme 默认端口会显示
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

## 4. 远端元数据格式

当前写入路径已 cutover 到 V2。V1 per-month sqlite manifest 仍用于：

1. 识别 / 迁移旧仓库
2. `RemoteIndexSyncService` 兼容读取尚未迁移的 V1 仓库
3. `MonthManifestStore` 的 V1 verify / 迁移辅助路径

V2 仓库的 canonical metadata 位于 `.watermelon/`，由 `RepoBootstrap`、`CommitLogWriter`、`SnapshotWriter`、`RepoMaterializer` 与 `V2MonthSession` 维护；主流程不再把 `.watermelon_manifest.sqlite` 作为 V2 月份状态源。

## 5. V1 远端月 manifest（`MonthManifestStore`）

实现位于 `Shared/Services/Backup/`：核心入口在 `MonthManifestStore.swift`，初始化 / seed 在 `+Loading.swift`，schema 与迁移在 `+Schema.swift`。

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

## 6. V1 manifest 字段语义

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

## 7. V2 repo 元数据

V2 详细设计和残留项见 `docs/06-RepoV2.md`。数据模型层面只需要记住：

1. `.watermelon/version.json` 声明远端格式版本和最低客户端版本
2. `.watermelon/identity/*.json` / `.watermelon/repo-identity.json` 决定 canonical `repoID`；`.watermelon/repo.json` 是 read-cache
3. `.watermelon/commits/{YYYY-MM}--{writerID}--{seq16}.jsonl` 记录 asset/resource upsert 与 tombstone op
4. `.watermelon/snapshots/{YYYY-MM}--{lamport16}--{writerID}--{runIDPrefix}.jsonl` 保存月级 materialized snapshot 和 covered commit ranges
5. `.watermelon/liveness/{writerID}.json` 用于 active writer 判断与 orphan metadata cleanup gate
6. `.watermelon/retention/{YYYY-MM}--{lamport16}--{writerID}--{runIDPrefix}.json` 保存 checkpoint barrier / delete prefix / liveness gate，用于保守删除已被 checkpoint 覆盖的 commit 前缀
7. `.watermelon/migrations/*.json` 记录 V1→V2 迁移阶段；迁移完成后仅可能留下 cleanup residue，`RemoteFormatInspection.v2WithPendingMigrationCleanup` 会驱动前台清理

`V2MonthSession` 是 V2 worker 的月份状态容器：启动时按 repoID 过滤 materialize 单月，再叠加真实月份目录 listing；row-writing asset 返回前先写 per-asset commit，flush 时主要写 snapshot，并通过 `BackupMonthFlushDelta` 告诉 `RemoteIndexSyncService` 哪些 asset / tombstone commit 已 durable。干净 flush 后会尝试 checkpoint / retention barrier / commit 前缀删除维护。

### Retention manifest wire schema

文件名由 `RetentionManifestStore.filename(for:)` 生成：

`{YYYY-MM}--{barrierLamport16}--{createdByWriterID}--{runIDPrefix6}.json`

约束：

1. `barrierLamport16` 是 16 位小写 hex，且 `0 < value < LamportClock.maxAdoptableValue`
2. `createdByWriterID` 是完整小写 UUID，不是 `writerIDShort`
3. `runIDPrefix6` 是去掉连字符后的 run UUID 前 6 位小写 hex
4. 文件名里的 month / lamport / writer / run prefix 必须和 JSON body 对应字段一致；不一致会被 `RetentionManifestRemoteStore` 归为 invalid

JSON body 由 `RetentionManifestStore.encode` 以 sorted keys 写出，当前版本只接受 `version == 1`。必填字段：

| JSON key | Swift 字段 | 形状 / 约束 |
|---|---|---|
| `version` | `version` | 整数，当前为 `1` |
| `repo_id` | `repoID` | UUID；编码为小写 |
| `month` | `month` | `YYYY-MM` |
| `created_by_writer_id` | `createdByWriterID` | 完整小写 UUID |
| `run_id` | `runID` | UUID；编码为小写 |
| `created_at_ms` | `createdAtMs` | 非负 Int64 毫秒时间戳 |
| `barrier_lamport` | `barrierLamport` | 16 位小写 hex 字符串，不是 JSON number |
| `checkpoint_snapshot` | `checkpointSnapshotName` | snapshot 文件名，必须和 `month / barrier_lamport / created_by_writer_id / run_id` prefix 匹配 |
| `checkpoint_sha256` | `checkpointSHA256Hex` | 64 位小写 hex |
| `covered_ranges` | `coveredRanges` | `[writerID: [[low, high], ...]]`；writerID 必须有效，每个 range 非空且 `low > 0 && high >= low` |
| `delete_prefix_by_writer` | `deletePrefixByWriter` | `[writerID: UInt64]`；writer 必须存在于 `covered_ranges`，prefix 必须大于 0 且不超过保守连续 covered prefix |
| `observed_seq_high_by_writer` | `observedSeqHighByWriter` | `[writerID: UInt64]`；只校验 writer key 形状 |
| `policy` | `policy` | 见下表 |
| `liveness_gate` | `livenessGate` | 见下表 |

`policy` 子对象：

| JSON key | Swift 字段 | 形状 / 约束 |
|---|---|---|
| `keep_uncovered_commits` | `keepUncoveredCommits` | Bool |
| `keep_corrupt_or_untrusted_commits` | `keepCorruptOrUntrustedCommits` | Bool |
| `keep_tombstones` | `keepTombstones` | Bool |
| `snapshot_keep_count` | `snapshotKeepCount` | 非负 Int |

`liveness_gate` 子对象：

| JSON key | Swift 字段 | 形状 / 约束 |
|---|---|---|
| `required_complete_view` | `requiredCompleteView` | Bool |
| `required_no_active_non_self_writers` | `requiredNoActiveNonSelfWriters` | Bool |
| `legacy_client_grace_ms` | `legacyClientGraceMs` | 非负 Int64 |

当前 `RepoRetentionBarrierService` 发布的 manifest 固定把 `policy.keep_*` 三个布尔值写为 `true`，`snapshot_keep_count` 来自 `RepoCompactionPolicy.snapshotFallbackKeepCount`，`liveness_gate.required_complete_view` 和 `required_no_active_non_self_writers` 写为 `true`，`legacy_client_grace_ms` 来自 `legacyClientGraceSeconds * 1000`。读取时未知字段会被忽略，但所有上表字段缺失或形状错误都会 fail-closed。

## 8. `assetFingerprint` 计算规则

当前规则：

1. 对每个资源生成 token：`role|slot|hashHex`
2. token 排序
3. 用 `\n` 连接
4. 对最终字符串做 SHA-256

## 9. 内存态远端快照

快照 / row 类型主要定义在 `Shared/Domain/RemoteLibraryDomain.swift`，presence overlay 定义在 `Shared/Services/Backup/RemotePresenceSnapshot.swift`。Home 当前不直接反复扫远端文件系统，而是消费 `RemoteLibrarySnapshotCache`（`Shared/Services/Backup/`）暴露的状态。

### 全量快照

```swift
struct RemoteLibrarySnapshot {
    let resources: [RemoteManifestResource]
    let assets: [RemoteManifestAsset]
    let assetResourceLinks: [RemoteAssetResourceLink]
    let presence: RemotePresenceSnapshot
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
    let presence: RemotePresenceSnapshot.Month
}
```

核心远端 row 里还有 V2 字段：

```swift
struct RemoteManifestAsset {
    let assetFingerprint: Data
    let stamp: OpStamp? // legacy / in-flight 时为 nil
}

struct RemoteManifestResource {
    let physicalRemotePath: String
    let contentHash: Data
    let crypto: ResourceCryptoMetadata?
}

struct RemoteAssetResourceInstance {
    let remoteRelativePath: String
    let alternateRemoteRelativePaths: [String]
}

struct RemotePresenceSnapshot {
    struct Month {
        let missingHashes: Set<Data>
        let isAuthoritative: Bool
    }
}
```

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
4. `presence` 来自 physical-presence overlay，同时表达每月 missing hashes 与 freshness / authority；Home 使用 `delta.presence.missingHashes`，cache health / month / count summaries 通过 `missingHashesByMonth` 兼容 adapter 扣减，restore 消费 Home 生成的可恢复 items 并在下载后校验字节
5. `alternateRemoteRelativePaths` 记录同 hash 的备用物理路径，`RestoreService` 下载 primary 失败时会 fallback
6. 这部分是内存状态，不写回 SQLite

## 10. Keychain 与会话密码

1. 密码不写入 SQLite
2. 通过 `KeychainService`（`Shared/Data/Security/KeychainService.swift`）保存
3. keychain service：`com.zizicici.watermelon.credentials`
4. `AppSession`（`Shared/Domain/AppSession.swift`）里保留当前连接的会话密码
5. SMB / WebDAV / S3（secret access key）需要密码；外接存储不需要
6. SFTP 在 Keychain 里存的是 `SFTPCredentialBlob` 序列化后的 JSON（password 模式存明文密码、privateKey 模式存 PEM 与可选 passphrase 的明文），`AppSession` 里同样保留这个 JSON 串作为"密码"传给 `StorageClientFactory`。`StorageProfile.supportsPasswordPrompt = false`：destination menu 在缺凭证时不会弹通用密码框，只能进编辑页重填
