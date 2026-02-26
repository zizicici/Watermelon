# 备份核心 V2（当前实现）

## 1. 入口

`BackupSessionController.startBackup()/retryFailedItems()` 最终调用：

`BackupExecutor.runBackup(profile:password:onlyResourceIdentifiers:onProgress:onLog:)`

## 2. 运行时流程（按实际代码）

1. 校验/请求照片权限。
2. 建立 SMB 连接，确保 `basePath` 目录存在。
3. 扫描远端库结构（`RemoteLibraryScanner.scanYearMonthTree`），更新内存快照。
4. `PHAsset.fetchAssets` 按创建时间升序，遍历每个 asset。
5. 每个 asset 拿 `PHAssetResource.assetResources(for:)`，逐资源处理。
6. 月份变化时：
7. flush 上个月 `MonthManifestStore` 到远端。
8. 加载/创建新月份 manifest（`MonthManifestStore.loadOrCreate`）。
9. 处理完成后，flush 当前月 manifest。
10. 末尾再做一次远端重扫，刷新内存快照。

## 3. 单资源处理规则（`processResource`）

1. 用 `PhotoLibraryService.exportResourceToTempFile` 导出原始字节。
2. 对导出文件做 SHA-256（`Data` 32 bytes）。
3. 若当前月 manifest 已包含此 hash：
4. 记 `skipped(hash_exists)`。
5. 本地 `content_hash_index` UPSERT。
6. 若 hash 不存在，先取目标文件名（sanitize 后原名）。
7. 若 manifest/目录里同名已存在：
8. 小于 5 MiB：下载远端同名文件比 hash；相同则 `skipped(name_same_hash)`。
9. 大于等于 5 MiB：比文件大小；相同则 `skipped(name_same_size)`。
10. 仍冲突：`RemoteNameCollisionResolver` 生成 `_n` 新文件名。
11. 执行上传（最多 3 次重试，指数退避）。
12. 上传成功后：
13. manifest `upsertItem`。
14. `markRemoteFile` 更新远端文件名集合。
15. 更新内存远端快照。
16. 本地 `content_hash_index` UPSERT。

## 4. 月 manifest 刷新策略（当前代码）

1. `MonthManifestStore.upsertItem` 只改本地 sqlite，并标记 `dirty = true`。
2. 只有在以下时机会 `flushToRemote()`：
3. 切月时（flush 上一个月）。
4. runBackup 结束时（flush 当前月）。
5. `loadOrCreate` 发现远端该月无 manifest 时，会先创建空 manifest 并上传一次。

说明：当前实现不是“每 N 个资源强制 flush”。

## 5. 暂停/停止语义

1. `BackupSessionController.pauseBackup/stopBackup` 本质是 `Task.cancel()`。
2. `BackupExecutor` 在多个点检查 `Task.isCancelled` / `Task.checkCancellation()`。
3. 取消是协作式：不会强杀正在进行的单个 SMB 传输。
4. 传输返回后会尽快退出主循环，并走尾部 flush。

## 6. 远端快照

`BackupExecutor` 内维护 `cachedRemoteSnapshot`（`NSLock` 保护）：

1. 登录或手动刷新时全量扫描更新。
2. 备份成功/跳过时局部 upsert 到快照（避免 UI 全量重扫）。
3. 备份结束后再全量重扫一次校准。

## 7. Retry 模式

`onlyResourceIdentifiers` 非空时只处理指定资源 ID。  
`BackupSessionController` 用失败列表的 `resourceLocalIdentifier` 发起 retry。

## 8. 关键常量

1. 同名冲突小文件阈值：`5 * 1024 * 1024` bytes。
2. 上传重试次数：3。
3. hash 流式计算缓冲：64 KiB。

## 9. 当前实现与目标设计差异（务必注意）

1. 远端存在性判断主要基于“当前月 manifest + 同名文件规则”，不是全库 hash 直接判重。
2. 暂停/停止并非“立即中断网络 IO”，而是“当前资源收尾后退出”。
3. flush 粒度是“切月/结束”，不是“每 10 张强制同步”。
