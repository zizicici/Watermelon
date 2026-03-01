# Watermelon Photo Backup

仓库名是 `PhotoBackup`，实际 App/Target 名称是 `Watermelon`。

当前主链路：

1. 启动直接进入 `HomeViewController`。
2. 远端存储支持 `SMB`、`WebDAV` 与 `外接存储目录`（iOS security-scoped bookmark）。
3. 备份按 `Asset` 粒度执行，远端按月写入 `.watermelon_manifest.sqlite`。
4. 月 manifest 使用三表结构：`resources`、`assets`、`asset_resources`。
5. 本地索引使用两张表：`local_assets`、`local_asset_resources`。
6. 备份控制统一由 `BackupEngineActor` 处理，执行由 `BackupCoordinator` 负责。

建议先读 `docs/`：

- `docs/00-LLM-HANDOVER.md`：快速接手说明
- `docs/01-Architecture.md`：模块结构与依赖关系
- `docs/02-BackupCoreV2.md`：备份主流程与关键规则
- `docs/03-DataModel.md`：本地与远端 schema
- `docs/04-UIFlow.md`：当前 UI 交互流
- `docs/05-OpenIssues.md`：现有风险与后续建议
