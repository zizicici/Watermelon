# 当前风险点 / 技术债（给后续 LLM）

## 1. 备份流程与性能

1. `BackupExecutor.runBackup` 仍是“全量遍历所有 asset/resource，再逐个判定是否跳过”。
2. 资源 hash 需要先导出本地临时文件，I/O 较重；库很大时总耗时明显。
3. 暂停/停止是协作式取消，不会立即打断正在进行的单文件上传。

## 2. manifest 刷新时机

1. 当前是“切月 + 结束”flush。
2. 没有“每 N 张强制 flush”机制。
3. 如果用户强制杀进程，当前月最近改动可能还停留在本地临时 manifest。

## 3. 远端预览成本

1. 远端缩略图依赖 `RemoteThumbnailService`：先下载原文件，再本地下采样。
2. 大图/慢网下会带来网络、CPU、临时文件开销。
3. 已做控制：actor 限流、Kingfisher 缓存、prefetch cancel。

## 4. 代码遗留与混淆点

1. `Records.swift` 里有旧表 record，和当前迁移不一致。
2. `UI/Browser/*`、`BackupViewController`、`BackupPlanner` 等旧链路文件仍在仓库，主流程不用。
3. `DependencyContainer` 还注入 `ManifestSyncService`，但 Backup 新链路基本不用它。
4. 仓库同时存在 `Watermelon.xcodeproj` 与空壳 `PhotoBackup.xcodeproj` workspace，实际可构建工程是前者。

## 5. 运行时数据一致性

1. Album“本地是否已备份”依赖：
2. 本地 `content_hash_index`
3. `backupExecutor.currentRemoteSnapshot().hashSet`
4. 这套判断在备份进行中通过内存快照增量更新，不是每次都重扫 SMB。

## 6. 测试现状

1. 当前仓库没有完整单测/集成测试目录。
2. 大部分验证依赖真机 + NAS 手工回归。

## 7. 建议后续改造优先级

1. 先清理未使用旧链路文件，减少误改风险。
2. 再拆分 `BackupExecutor`（扫描/调度/资源处理/flush 策略）。
3. 最后补最小关键测试：命名冲突、月序调度、pause/stop flush、hash 去重正确性。
