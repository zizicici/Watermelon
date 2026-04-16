# 当前风险点 / 技术债（按当前实现）

## 1. 自动化测试仍不足

1. 目前没有成体系单测 / 集成测试。
2. 连接切换、上传暂停恢复、sync 月份内联下载、外接存储拔出等链路仍主要依赖手工回归。

## 2. iCloud-only 资源仍有重复 I/O 成本

1. 当前首页执行已支持 `允许访问 iCloud 原件`：会先做 availability probe，download / sync 还支持对 `unavailableAssetIDs` 做联网补索引。
2. 但在某些场景，同一条 iCloud-only 本地资源仍可能被重复读取：
   - probe 先做一次轻量本地可用性判断
   - 联网补索引时完整导出一次原件并计算 hash
   - 上传阶段若远端未命中，可能再次完整导出并上传
3. 对大视频、Live Photo 或蜂窝网络场景，这个额外成本会比较明显。
4. 后续可评估复用 preflight 产物，或把 probe 结果传给离线 preflight，减少重复尝试。

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
3. 评估复用 iCloud recovery / probe 结果，降低 iCloud-only 资源的重复 I/O 成本。
4. 评估按失败率和吞吐量自适应调整 worker 数。
