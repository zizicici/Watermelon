# Dropbox 接入

## 1. 范围与安全边界

Dropbox 节点只访问 Dropbox App Folder。API 路径 `/` 对应用户 Dropbox 中应用专属目录，不允许浏览或写入其它位置。iOS 使用 OAuth 2 authorization code + PKCE，不在客户端保存 app secret；登录请求 `token_access_type=offline`，refresh token 保存到 Keychain，短期 access token 只在内存缓存。

Dropbox 是 Repo V2-only 后端。iOS 转换路由会把 V1 仓库判定为损坏，旧版 macOS 迁移工具不展示 Dropbox 节点。

Profile 的发布身份由 `appKey + accountID` 组成。Keychain 中的 `DropboxCredentialBlob.accountID` 必须匹配 profile；每次 refresh 得到的新 access token 还会通过 `users/get_current_account` 核验真实 `account_id`，防止错误账号向已有仓库写入。

## 2. Dropbox App Console 配置

1. 创建 Scoped access app，内容访问类型选择 App folder。
2. 在 Permissions 中启用 `account_info.read`、`files.metadata.read`、`files.content.read`、`files.content.write`。
3. 把 app key 写入 iOS target 的 `DROPBOX_APP_KEY` build setting；不要提交 app secret。
4. iOS 回调 scheme 由 `Info.plist` 生成：`db-$(DROPBOX_APP_KEY)`；OAuth redirect URI 为 `db-<APP_KEY>://2/token`。
5. App Console 的应用名称决定用户看到的 `Dropbox/Apps/<应用名>` 目录。当前 UI 文案按 `Dropbox/Apps/Watermelon` 展示，发布应用名应保持为 Watermelon。

官方依据：

- [OAuth 与 refresh token](https://developers.dropbox.com/oauth-guide)
- [App Folder 权限模型](https://developers.dropbox.com/dbx-file-access-guide)
- [iOS PKCE 与回调 scheme](https://dropbox.github.io/SwiftyDropbox/api-docs/10.0.0/)

## 3. 运行时组件

- `DropboxOAuthService`：启动系统网页登录、校验 state、交换 authorization code、读取当前账号。
- `DropboxTokenService`：用 refresh token 获取 access token，按账号缓存并合并并发 refresh；新 token 通过远端账号核验后才发布，token endpoint 与文件 API 共用节流窗口。
- `DropboxProfileSetupCoordinator`：登录后执行通用 write probe，只有探针成功才允许保存 profile；编辑已有节点时通过 `force_reauthentication=true` 明确切换账号。
- `DropboxClient`：实现 `RemoteStorageClientProtocol`，覆盖列表、元数据、上传、下载、目录、移动、复制和删除。
- `DropboxErrorClassifier`：解析 endpoint-specific tagged union，区分对象不存在、upload-session 失效、目标名字冲突与源端错误，并归一化认证、限流、空间不足和网络离线。
- `DropboxSharedState`：按 `appKey + accountID` 共享 `Retry-After` 节流窗口，防止并发 client 在 429 后继续请求。

## 4. 文件 API 语义

1. 目录列表必须消费 `list_folder/continue` 返回的 opaque cursor，直到 `has_more=false`。
2. 140 MiB 及以下文件走 direct upload（低于 Dropbox 的 150 MiB 单请求上限）；更大文件走 8 MiB 顺序 upload session，首块直接随 session start 发送。
3. `.replace` 使用 overwrite；`.createIfAbsent` 使用 `mode=add`、`autorename=false`、`strict_conflict=true`，满足 Repo V2 写锁的原子抢占要求。
4. 上传时把本地文件时间写入 `client_modified`；Dropbox 不支持事后修改该字段，因此 `shouldSetModificationDate=false`。
5. 所有 API 都在 App Folder 根内使用绝对路径；路径组件经过 Dropbox 专用规则清洗，并拒绝尾随空白。
6. 非成功响应保留 endpoint、`.tag` 链和 `X-Dropbox-Request-Id` 供日志诊断；Dropbox Business 的 `user_message` 优先展示给用户。
7. 429 严格遵守 `Retry-After`；endpoint-specific `too_many_write_operations` 也进入通用重试分类。
8. `DropboxClient` 的 move/copy 只执行原生 relocation，目标冲突交给上层 `RemoteMoveReplace` 的 ownership、备份和恢复状态机处理，避免客户端内部恢复步骤跨越 Repo V2 所有权边界。
9. 条件上传遇到网络结果未知后再收到目标冲突时，会用 Dropbox `content_hash` 核对远端内容；内容相同即确认原上传成功，避免改名重传形成孤儿副本。

## 5. 验证清单

自动测试覆盖 credential round-trip、canonical identity / factory、分页 cursor、条件创建参数、`client_modified`、Unicode header、`content_hash` 内容核对、对象与 session 的 not-found 区分、token 单航班刷新、远端账号错配、401 重试、token 与文件 API 的共享限流、目标冲突层级、原生 move 冲突、Business 用户提示、切换账号重认证、路径规则和写锁行为。发布前仍需使用真实 Dropbox 账号在真机验证：首次授权、切换账号、撤销授权后重登、小文件上传、大文件 session 上传、下载恢复、move/copy 中断恢复、停止后续跑、写锁竞争、限流提示、空间不足提示以及后台备份。
