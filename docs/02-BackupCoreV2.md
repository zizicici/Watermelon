# 备份核心 V2（当前实现）

## 1. 入口

`BackupSessionController.startBackup()/retryFailedItems()/resumeFromPause()` 最终调用：

`BackupEngineActor.startRun/resumeRun` -> `BackupCoordinator.runBackup(..., context: BackupRunContext)`

## 2. 主流程（按当前代码）

1. 校验/请求照片权限。
2. 通过 `StorageClientFactory` 建立远端连接（SMB / WebDAV / 外接存储），确保 `basePath` 存在。
3. `RemoteIndexSyncService` 基于 `RemoteManifestIndexScanner` 摘要扫描并增量刷新远端快照。
4. 按 `creationDate ASC` 遍历 `PHAsset`。
5. 每个 Asset 取 `PHAssetResource.assetResources(for:)`，经 `BackupAssetResourcePlanner` 生成有序资源列表（role + slot）。
6. 月份变化时先 flush 上个月 manifest，再 `loadOrCreate` 当前月份 manifest。
7. 处理完所有 Asset 后，flush 当前月份 manifest。

## 3. 单 Asset 处理（`AssetProcessor.process`）

1. 逐资源导出到临时文件并计算 SHA-256。
2. 每个资源 hash 写入本地 `local_asset_resources`（键：assetLocalIdentifier+role+slot）。
3. 用 `(role|slot|hashHex)` 集合计算 `assetFingerprint`。
4. 若当前月 manifest 已包含该 fingerprint：
5. 记 Asset `skipped(asset_exists)`。
6. 写本地 `local_assets`（assetFingerprint + resourceCount）。
7. 若该 fingerprint 不存在：逐资源执行上传/复用逻辑。
8. 只要有任意资源失败，整个 Asset 判定失败（不会写 `assets`/`asset_resources` 关系）。
9. 全部资源非失败时写入：
10. 月 manifest 的 `assets` + `asset_resources`。
11. 本地 `local_assets`。
12. 内存远端快照增量 upsert。

## 4. 单资源处理（`uploadResource` 路径）

1. 若当前月 manifest 已有相同 hash：`skipped(hash_exists)`。
2. 否则准备目标文件名（sanitize）。
3. 若同名冲突：
4. 小文件（< 5 MiB）下载远端同名后比 hash，相同则 `skipped(name_same_hash)`。
5. 大文件（>= 5 MiB）仅在该同名不在 manifest 时按 size 启发式比较，相同则 `skipped(name_same_size)`。
6. 仍冲突则用 `RemoteNameCollisionResolver` 生成 `_n` 文件名。
7. 上传最多 3 次重试（指数退避）。
8. 若遇 `STATUS_OBJECT_NAME_COLLISION`，即时换名重试。
9. 上传成功后 upsert `resources`，并写远端快照缓存。

## 5. 月 manifest flush 策略

1. `upsertResource/upsertAsset` 只改本地 sqlite，标记 `dirty = true`。
2. `flushToRemote()` 触发时机：
3. 切月时 flush 上个月。
4. runBackup 结束时 flush 当前月。
5. `loadOrCreate` 遇到远端该月 manifest 不存在时，会先上传一份初始 manifest。
6. `flushToRemote(ignoreCancellation:)` 在暂停/停止收尾阶段允许忽略取消，尽量保证落盘。

## 6. 远端扫描与内存快照

1. `RemoteManifestIndexScanner` 是只读扫描：只读取已有 manifest，不创建目录，不写远端。
2. `RemoteIndexSyncService` 通过 digest 计算 changed/removed month，再按月替换 `RemoteLibrarySnapshotCache`。
3. 备份过程中资源/资产写入后会增量 upsert 到缓存，Home 可实时读取。

## 7. 暂停/停止/恢复语义

1. `pause/stop` 命令统一进入 `BackupEngineActor`。
2. engine 同时触发：
3. run 级 `BackupCancellationController.cancel()`。
4. run task `Task.cancel()`。
5. `BackupCoordinator` / `AssetProcessor` 通过 `throwIfCancelled + Task.checkCancellation` 协作退出。
6. 取消不是强杀单次网络 I/O；当前步骤收尾后退出。
7. `runBackup` 返回 `paused` 标记，Session 按 intent 决定落地为 paused 或 stopped。
8. `resume` 的 pending 计算已下沉到 `BackupEngineActor.resumeRun`（`BackupSessionController` 不再自己维护恢复准备任务）。

## 8. Retry 模式

1. `retryFailedItems` 以 Asset ID 集合调用 `startRun(mode: .retry(assetIDs:))`。
2. `runBackup` 只处理被指定的 Asset（按创建时间排序后执行）。

## 9. 关键常量

1. 同名冲突“小文件”阈值：`5 * 1024 * 1024` bytes。
2. 上传重试次数：3。
3. hash 流式缓冲：64 KiB。
