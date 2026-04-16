# 当前风险点 / 技术债（按当前实现）

## 1. 自动化测试仍不足

1. 目前没有成体系单测 / 集成测试。
2. 连接切换、上传暂停恢复、sync 月份内联下载、外接存储拔出等链路仍主要依赖手工回归。

## 2. 本地索引预检查会阻塞部分下载 / 同步场景

1. 首页执行前会强制补本地 hash 索引。
2. 该预检查明确 `allowNetworkAccess = false`，不会为了算 hash 去下载 iCloud-only 原图。
3. 因此如果用户本机没有原始资源，下载 / 同步会直接被拦下。
4. 这是当前为避免重复图片而采取的保守策略，但会影响可用性。

## 3. full run 的恢复成本仍高

1. full backup 或其恢复流程，仍需要重新遍历图库并重新计算 pending 集。
2. 大图库下，开始执行和恢复执行都会有明显前置耗时。

## 4. manifest flush 仍存在强杀窗口

1. manifest 主要在“月份完成”与“任务收尾”时 flush。
2. 如果应用在 flush 前被系统强杀，最近一批增量仍可能没写回远端 manifest。
3. `MonthManifestStore.loadSeeded(...)` 已通过列出真实远端目录来规避重名碰撞，但不能消除未 flush 元数据丢失本身。

## 5. 首页状态机复杂度依然不低

当前首页的状态由多层协作完成：

1. `HomeScreenStore`
2. `HomeConnectionController`
3. `HomeExecutionCoordinator`
4. `HomeExecutionDataRefresher`
5. `HomeIncrementalDataManager`

这套分层已经比旧版清晰，但连接变化、执行变化、相册变更同时发生时，仍需谨慎处理：

1. refresh 合并
2. deferred photo changes 排空
3. 连接失败后的远端快照恢复

## 6. 大图库文件大小扫描仍有成本

1. 首页会异步扫描每个月本地资源总大小。
2. 当前已改为主 actor 上逐月 `Task.yield()` 的安全实现，但大图库初次进入仍可能较慢。
3. 在文件大小全部补齐前，部分汇总会暂时显示 `-`。

## 7. 并发策略仍是“固定默认 + 手动覆盖”

1. 默认并发：`SMB/WebDAV=2`、`externalVolume=3`
2. 用户可手动覆盖到 `1...4`
3. 目前没有根据带宽、远端 RTT、失败率动态调节 worker 数

## 8. 下载取消粒度仍是 item 级

1. `RestoreService.restoreItems(...)` 在 item 循环边界检查取消。
2. 一个 item 内部若包含多资源（如 Live Photo），中断时仍可能丢掉该 item 的部分临时进度。
3. 不过成功完成的 item 会立即写回 hash 索引，所以下次能跳过整 item。

## 9. 建议优先级

1. 优先补首页执行链路的自动化测试，特别是暂停 / 恢复 / stop / 连接丢失。
2. 评估为 full run 持久化 pending 集，减少恢复时重扫。
3. 评估“允许网络补本地索引”的可选模式，降低 iCloud-only 资源对下载 / 同步的阻塞。
4. 评估按失败率和吞吐量自适应调整 worker 数。
