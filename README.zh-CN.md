<div align="center">
  <img src="https://i.v2ex.co/T03Pw3rXb.png" alt="西瓜备份 App 图标" width="120">
  <h1>西瓜备份</h1>
  <p><strong>备份 iPhone 照片到自己的存储</strong></p>
  <p>支持备份到外接存储、SMB、WebDAV、S3 兼容存储桶和 SFTP。<br>在 GitHub 开源。</p>
  <p>
    <a href="https://apps.apple.com/cn/app/id6762260596"><strong>免费下载</strong></a>
    ·
    <a href="https://watermelonbackup.com/?lang=zh-Hans">官方网站</a>
    ·
    <a href="README.md">English</a>
  </p>
  <a href="https://apps.apple.com/cn/app/id6762260596">
    <img src="https://i.v2ex.co/wwG672a0.png" alt="西瓜备份首页截图，显示节点和月份任务" width="320">
  </a>
</div>

## 简单易用

| 步骤 | 说明 |
| --- | --- |
| 选择节点 | 外接存储、SMB、WebDAV、S3 兼容存储桶或 SFTP。 |
| 勾选月份 | 备份、下载、互补按月份处理。 |
| 开始执行 | 同步远端索引后开始任务，需要时可暂停或继续。 |

## 备份、下载、互补

| 操作 | 含义 |
| --- | --- |
| 备份 | 把本机照片和视频原件写入当前节点。 |
| 下载 | 从节点取回已备份内容并导入「照片」。 |
| 互补 | 先备份、再下载：先把本机有但节点缺失的内容备份到节点，再把节点有但本机缺失的内容下载回「照片」。 |

## 代码开源，可审查

我们相信隐私和数据所有权同样重要。

- 源码：[github.com/zizicici/Watermelon](https://github.com/zizicici/Watermelon)
- 隐私政策：[watermelonbackup.com/privacy.html?lang=zh-Hans](https://watermelonbackup.com/privacy.html?lang=zh-Hans)
- 官方网站：[watermelonbackup.com/?lang=zh-Hans](https://watermelonbackup.com/?lang=zh-Hans)

## 价格

一次购买，非订阅。

| 项目 | 免费版 | Pro |
| --- | --- | --- |
| 价格 | - | ¥54，约 2 杯喜茶 |
| 前台备份 | 包含 | 包含 |
| 节点 | 1 | 无限制 |
| 后台自动备份 | 不包含 | 包含 |
| 画中画进度 | 不包含 | 包含 |
| 专注模式 | 不包含 | 包含 |

Pro 是一次性购买，购买后终生可用。更换设备后，可以通过「恢复已购」恢复权益。

## 常见问题

- 节点是你在 App 里添加的一个存储连接，支持外接存储、SMB、WebDAV、S3 兼容存储桶和 SFTP。
- 节点所需的凭据保存在 iOS Keychain 中，不会上传到西瓜备份的服务器。
- 可以备份和还原 Live 图。西瓜备份会把每张 Live 图分别保存为静态图和配套视频；再次导入时，会自动合并回 Live 图。
- 不会重新压缩照片或视频。西瓜备份会尽可能保留原始文件，拍摄时间和 EXIF 等嵌入元数据也会随原件保留。
- 如果开启了 iCloud 照片，需要在 App 设置中开启「允许访问 iCloud 原件」，西瓜备份才能按需获取 iCloud 原件。

## 下载

- App Store：[https://apps.apple.com/cn/app/id6762260596](https://apps.apple.com/cn/app/id6762260596)
- 官方网站：[https://watermelonbackup.com/?lang=zh-Hans](https://watermelonbackup.com/?lang=zh-Hans)
- App 名称：西瓜备份

<details>
<summary>开发者信息</summary>

## 项目状态

iOS App 是这个仓库的主要产品目标。

`WatermelonMac` 是单独的 macOS target，仅用于遗留数据迁移。它没有 App Store、TestFlight 或签名分发版本。请不要把它用于重要相册或生产环境存储。

## 从源码运行

App 确实是开源的，用户可以直接在 Xcode 上编译 App。

1. 用 Xcode 打开 `Watermelon.xcodeproj`。
2. 选择 `Watermelon` scheme 运行 iOS App。
3. 在模拟器或真机上启动。
4. 运行 `WatermelonTests` target 执行已有单元测试。

## 仓库结构

| 路径 | 用途 |
| --- | --- |
| `Watermelon/` | iOS App 源码：首页、引导、设置、备份编排、PhotoKit 集成 |
| `Shared/` | 共享存储客户端、数据库、Keychain、领域模型、manifest、repo 服务 |
| `WatermelonMac/` | macOS 遗留迁移 target，不负责 iOS 备份链路 |
| `WatermelonTests/` | XCTest，覆盖纯逻辑、S3 签名、SFTP 凭据、写锁、清理逻辑 |
| `docs/` | 架构、备份链路、数据模型、UI 流程、已知技术问题 |

## 技术文档

- `AGENTS.md`：面向 coding agent 的项目速览
- `docs/01-Architecture.md`：模块分层与依赖关系
- `docs/02-BackupCoreV2.md`：上传、同步、下载、预检查和重试细节
- `docs/03-DataModel.md`：SQLite schema 与快照模型
- `docs/04-UIFlow.md`：首页、连接、引导、更多页和执行状态
- `docs/05-OpenIssues.md`：当前风险点与技术债

</details>
