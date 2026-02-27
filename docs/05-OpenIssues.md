# 当前风险点 / 技术债

## 1. 备份性能与扫描成本

1. 每次 full backup 仍会遍历整库 `PHAsset` 并导出资源计算 hash，图库大时耗时明显。
2. pause 后恢复 full run 时，`BackupSessionController` 仍需再次遍历照片库计算 pending asset 集合。
3. 资源导出到临时文件再 hash 的 I/O 开销较高。

## 2. flush 粒度与中断风险

1. manifest flush 触发点是“切月 + 任务结束”。
2. 如果应用在当前月大量变更后被系统强杀，最近变更可能尚未写回远端 manifest。

## 3. 远端缩略图成本

1. `RemoteThumbnailService` 依然是“下载原文件 -> 本地下采样”。
2. 慢网或大文件下会带来网络与临时文件开销。

## 4. 代码结构仍可继续收口

1. `BackupExecutor` 已做拆分（planner/snapshot cache），但仍承担较多职责（调度 + 上传细节 +重试策略）。
2. `SettingsViewController`、`ServerSelectionViewController` 当前不在启动路由，属于未接线页面。
3. `UI/Browser` 下部分调试/浏览页面未纳入主流程，维护时要避免误判为在线功能。

## 5. 匹配策略准确性边界

1. Home 本地/远端匹配依赖本地索引与远端快照，索引缺失时会退化为较弱匹配。
2. 远端条目组装严格依赖 `asset_resources` 关系；若远端 manifest 异常缺链，会直接丢失对应 remote item 展示。

## 6. 测试现状

1. 仓库缺少成体系单测/集成测试。
2. 关键链路仍以真机 + NAS 手工回归为主。

## 7. 建议后续优先级

1. 给 `BackupExecutor` 增加最小可测单元（命名冲突、asset failure 语义、flush 时机）。
2. 明确未接线页面去留（接回主链路或删除）。
3. 若要提升恢复速度，可考虑持久化 pending 集而非每次暂停后重扫照片库。
