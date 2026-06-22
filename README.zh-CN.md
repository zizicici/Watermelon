<div align="center">
  <img src="https://i.v2ex.co/T03Pw3rX.png" alt="Watermelon Backup App 图标" width="118">
  <h1>Watermelon Backup</h1>
  <p><strong>把 iPhone 照片和视频备份到你真正掌控的存储里。</strong></p>
  <p>支持 NAS、自建服务器、S3 兼容对象存储、WebDAV、SFTP、SMB 和外接硬盘。</p>
  <p>
    <a href="https://apps.apple.com/app/id6762260596"><strong>前往 App Store 下载</strong></a>
    ·
    <a href="README.md">English</a>
  </p>
  <a href="https://apps.apple.com/app/id6762260596">
    <img src="https://i.v2ex.co/wwG672a0.png" alt="Watermelon Backup 中文界面截图" width="320">
  </a>
</div>

## 你的照片，你的存储

Watermelon Backup 帮你把 iPhone 相册留一份独立备份，而不是只放在别人的云里。连接你信任的 NAS、服务器、对象存储或外接硬盘，选择要保护的月份，照片和视频会直接从设备写入你的存储目的地。

[在 App Store 下载 Watermelon Backup](https://apps.apple.com/app/id6762260596)

## 它适合你，如果你想

- 按月份备份照片和视频，大相册也能看得清楚。
- 在上传、下载或同步前，清楚看到本地和远端分别缺什么。
- 把相册备份到自己的 NAS、服务器、对象存储桶、SFTP 目录、WebDAV 目录或外接硬盘。
- 长时间任务可以暂停和继续，不必每次从头开始。
- 需要时把远端备份恢复回系统相册。
- 在完整备份需要原件时，处理只保存在 iCloud 照片里的内容。
- 在首页查看备份进度，Pro 用户可以使用画中画进度。
- 为不同存储配置开启后台备份，后台备份为 Pro 功能。

## 支持的备份目的地

| 目的地 | 例子 |
| --- | --- |
| SMB / NAS | 群晖、威联通、TrueNAS、Windows 共享 |
| WebDAV | 自建 WebDAV、兼容 WebDAV 的文件服务 |
| S3 兼容对象存储 | S3 风格对象存储、私有 bucket |
| SFTP | Linux 服务器、VPS、基于 SSH 的归档目录 |
| 外接存储 | iOS 可访问的本地磁盘或外接存储 |

## 使用很简单

1. 从 App Store 安装 Watermelon Backup。
2. 授权访问系统相册。
3. 添加一个备份目的地。
4. 选择要保护的月份。
5. 开始上传、下载或同步。

Watermelon Backup 会维护本地索引和远端 manifest，尽量避免重复传输已经存在的内容。

## 隐私

Watermelon Backup 会直接从你的 iPhone 或 iPad 写入你配置的存储目的地。凭据通过系统 Keychain 保存；这个仓库不包含任何 Watermelon 托管云服务。

## 下载地址

- App Store：[https://apps.apple.com/app/id6762260596](https://apps.apple.com/app/id6762260596)
- App 名称：Watermelon Backup
- 分类：照片与视频

<details>
<summary>开发者信息</summary>

## 项目状态

iOS App 是这个仓库的主要产品目标。

`WatermelonMac` 是单独的 macOS target，仅用于遗留数据迁移。它没有 App Store、TestFlight 或签名分发版本。请不要把它用于重要相册或生产环境存储。

## 从源码运行

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
