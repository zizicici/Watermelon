# 当前风险点 / 技术债（按当前实现）

## 1. 大图库扫描与恢复成本

1. full backup 仍需遍历完整 `PHAsset` 集合。
2. full run 暂停后恢复，需要再次扫描图库计算 pending 集。
3. 图库规模很大时，开始与恢复阶段仍有明显耗时。

## 2. 资源处理 I/O 成本

1. 资源通常先导出到临时文件再 hash/上传，磁盘 I/O 较重。
2. 多 worker 下临时文件峰值会增加（按 worker 并发叠加）。

## 3. 远端缩略图链路

1. `RemoteThumbnailService` 仍是“下载原文件 -> 本地下采样”。
2. 大视频或慢网场景会增加网络与临时文件开销。

## 4. 运行控制复杂度

1. `BackupSessionController + BackupRunCommandActor` 的状态组合较多（starting/resuming/pausing/stopping + run intent）。
2. 已较过去稳定，但后续重构仍需谨慎验证快速切换场景（开始/暂停/停止交替）。

## 5. flush 与强杀窗口

1. 当前 flush 触发主要在“每月处理完成”与“任务收尾”。
2. 若应用在当前月大量变更后被系统强杀，仍存在最后一批改动尚未 flush 的窗口。

## 6. 并发策略仍为静态默认 + 手动覆盖

1. 默认并发是按协议的固定值（SMB/WebDAV=2，本地=3）。
2. 目前没有带宽/延迟驱动的自适应并发调节。

## 7. 自动化测试覆盖不足

1. 项目仍缺少成体系单测/集成测试。
2. 关键链路主要依赖真机手工回归（SMB/WebDAV/外接存储）。

## 8. 建议优先级

1. 优先补 `BackupRunCommandActor` 状态切换与取消语义测试。
2. 评估持久化 pending 集，降低 full resume 重扫成本。
3. 评估按文件/月份动态并发策略，减少手动调参成本。
