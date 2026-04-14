# 备份核心流程（当前实现）

## 1. 执行入口

调用链：

1. `BackupSessionController.startBackup()/resumeFromPause()`
2. `BackupCoordinator.runBackup(request:eventStream:)`

说明：

1. 每个 run 使用独立 `BackupEventStream`。
2. `BackupRunRequest` 包含 `profile/password/onlyAssetLocalIdentifiers/workerCountOverride`。

## 2. run 主流程

`BackupCoordinator.runBackup` 执行步骤：

1. 校验或申请相册权限。
2. 创建并连接远端 client，确保 `basePath` 目录存在。
3. `RemoteIndexSyncService.syncIndex` 同步远端快照，并生成月级 seed（用于 `MonthManifestStore.loadOrCreate(seed:)`）。
4. 准备待处理资产：
5. 全量：`fetchAssetsResult(ascendingByCreationDate: true)`
6. 范围/重试：按 ID 拉取并按创建时间排序
7. 加载本地 hash 缓存（全量或按指定资产）。
8. 将资产按月份分桶，按“预计字节数优先、数量次之、月份次之”排序。
9. 计算 worker 数（协议默认或用户覆盖）并创建 `StorageClientPool`。
10. worker 动态从 `MonthWorkQueue` 领取月份任务。
11. 每个月份处理完成后执行 manifest flush。
12. 汇总结果并发出 `finished` 事件。

## 3. 并发与调度

1. worker 数上限策略：
2. 协议默认：SMB/WebDAV = 2，外接存储 = 3
3. 用户可在设置中覆盖（1~4）
4. 最终会按月份数再裁剪

连接池策略：

1. SMB/WebDAV 连接池最多 2 条连接（防止会话过多）。
2. 外接存储连接池按 worker 数。
3. worker 释放 client 时支持不可复用连接替换。

## 4. 单 Asset 处理（`AssetProcessor.process`）

1. 先尝试本地缓存快速命中（`processWithLocalCache`）。
2. 命中且月 manifest 已有同 fingerprint 时可直接跳过。
3. 未命中则逐资源导出到临时文件并计算 SHA-256。
4. 计算 `assetFingerprint`（由 role/slot/hash 组合得到）。
5. 逐资源上传（或跳过）并收集 link。
6. 若任一资源失败，整个 asset 记失败，不写 `assets/asset_resources`。
7. 全部非失败时写：
8. 月 manifest 的 `assets/asset_resources`
9. 本地 `local_assets/local_asset_resources`
10. 远端内存快照缓存增量

## 5. 单资源处理（`uploadResource`）

1. 若 manifest 中已有同 hash，直接 `skipped(hash_exists)`。
2. 同名冲突处理：
3. 小文件（<5 MiB）且 size 未知或一致时，下载远端同名文件比 hash。
4. hash 相同则 `skipped(name_same_hash)`。
5. 否则使用 `AssetProcessor` 内联方法 `resolveNextAvailableName` 生成 `_n` 新文件名。
6. 上传重试最多 3 次（含碰名重试）。
7. 上传成功后调用 `setModificationDate`（按各 client 实现；WebDAV 会尝试 PROPPATCH）。
8. 写入 manifest `resources` 与远端快照缓存。

## 6. 月 manifest 与 flush 语义

1. `upsertResource/upsertAsset` 先写本地月 sqlite 并标记 `dirty`。
2. 月末或任务收尾调用 `flushToRemote` 同步远端 manifest 文件。
3. flush 失败抛出异常，默认终止 run。外接硬盘断开时跳过 flush。
4. 暂停/停止收尾时会对当前月使用 `ignoreCancellation`，尽量完成 flush。

## 7. 暂停 / 停止 / 恢复

1. `BackupSessionController` 统一处理控制命令与 intent。
2. 暂停/停止通过协作取消实现（`Task.cancel` + 检查点退出）。
3. 恢复 full run 时会重新扫描图库并减去已完成资产集合。
4. 恢复 scoped/retry run 时直接用剩余 ID 集合。
5. run 终态由 `BackupSessionController.finishRun/handleRunFailure` 映射到 UI 状态。

## 8. 关键常量

1. 小文件阈值：`5 * 1024 * 1024` bytes
2. 上传重试次数：`3`
3. hash 读取缓冲：`64 KiB`
4. 阶段耗时日志窗口：每 `200` 项汇总一次
