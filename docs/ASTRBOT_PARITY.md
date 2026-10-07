# QQ 官机收发能力对照与验收边界

参考本地 AstrBot 的 qqofficial_platform_adapter.py、qqofficial_message_event.py 和 qqofficial_chunked_upload.py，并核对腾讯 Node SDK 的 MessageApi、协议类型与 Gateway intents。这里比较消息传输能力，不比较 AstrBot 的 AI、插件、管理面板、定时任务或消息理解。

本次参考时本地 AstrBot HEAD 为 `65cbe98f9a3551ff78e4f1f40b11ffa75cf625e4`，实际读取本地文件；未修改 AstrBot。

| 能力 | AstrBot 参考实现 | qqbot-gateway 0.2 |
| --- | --- | --- |
| 群/C2C 入站与被动回复 | 有 | 有，原始事件和上下文隔离 |
| 全量群消息 @ | 有 | 有，显式身份配置，不自动学习 |
| 子频道 @ / 频道私信 | 有 | 有，channel/dm 分别路由，不用群 v2 接口 |
| 群/C2C 主动发送 | 有，省略 msg_id | 有，独立 messages/send，与 wake/五分钟窗口无绑定 |
| 文本、图片、语音、视频、文件 | 有 | 群/C2C 有；频道图片 URL/multipart 有，其他内容原生字段按 QQ 接口支持范围处理 |
| 大文件分片 | 有 | 有，本地默认大小限制可配置 |
| Markdown/模板/键盘/ARK/引用 | 有 | 原生 QQ body 透传；交互接收 ACK 与事件读取已闭合 |
| C2C 流式 | legacy stream/state/index/id | legacy 与新 stream_messages 两种显式协议；本地状态持久化和请求幂等 |
| 主动唤醒/输入中提示 | 依实现与平台权限 | 显式 wakeup 模式和 typing 入口 |
| 频道私信创建/消息召回 | 可经 botpy API 调用 | 专用入口，结果查询 |
| 事件生命周期、审计、反应 | SDK 事件接收 | 原始 /events，客户端解释和动作；未把审计事件自动关联成送达状态 |
| 查询群/频道资料及其他管理 API | botpy API / 适配器信息方法 | 通过 api/request 显式白名单访问；未提供全部管理端点的单独快捷接口 |
| Markdown 拒绝后普通文本回退 | AstrBot 自动处理 | 客户端可在确认拒绝后明确选择格式；服务端不改写原生消息内容 |
| 被动失败后主动回退、发送自动重试 | AstrBot 有相应路径 | 不照搬；用户此前明确接受未知发送不自动重发。提供主动入口，转换由客户端明确决定 |
| 语音转码、媒体下载/理解 | AstrBot 可在上层做 | 留在客户端，服务端上传已编码内容，不读取客户端机器的本地路径 |

因此，本版补齐了上一版缺失的核心收发入口，不能宣称替代 AstrBot 所有上层功能或所有 botpy 管理 API。实际 QQ 成功率仍未验收，不能写成“已确认账号可主动推送”。

平台资料存在时间差：旧官方文档有主动推送停止公告，而新腾讯 SDK 与本地 AstrBot 仍有省略 msg_id 的发送实现。本项目保留发送路径并返回真实 QQ 错误，不硬编码某个历史月额度，也不承诺账号可绕过限制。

## 上线验收

1. 保持原配置默认 intents，先验收群 @、C2C 收发、普通被动回复兼容。
2. 对真实 group_openid/user_openid 分别请求独立主动文本、图片和视频/文件，记录平台返回码，确认没有入站 msg_id 时是否实际成功。重复相同 request_id 应只出现一次消息。
3. 获得频道/私信权限后打开对应 intents，分别验证 AT_MESSAGE_CREATE、DIRECT_MESSAGE_CREATE 的 target_id，以及图片 URL/base64 收发。
4. C2C 分别测试当前账号支持的流式协议，验证开始、连续更新、DONE、取消、错误、断线重启；按协议传全文或增量，观察实际客户端呈现。
5. 验证 keyboard 回调事件和及时 ACK、输入中提示额度、撤回及私信创建。
6. 故意丢失 HTTP 响应后查询 operation，不使用新 ID 自动重发；验证部分失败不会再发送已完成部分。
7. 未获对应 QQ 权限的能力应在客户端标为未验收/平台拒绝，不能仅凭 capabilities=true 展示成可用。

部署仍用 venv + systemd。升级前停服务并备份数据库；新表和字段自动增加，真实数据迁移不能由本地测试代替。

参考来源：

- [腾讯 Node SDK 使用指南](https://github.com/tencent-connect/qqbot-nodejs/blob/main/USAGE.md)
- [腾讯 Node SDK 消息接口](https://github.com/tencent-connect/qqbot-nodejs/blob/main/src/protocol/api/messages.ts)
- [腾讯协议类型](https://github.com/tencent-connect/qqbot-nodejs/blob/main/src/protocol/types.ts)
- [腾讯 Gateway intents](https://github.com/tencent-connect/qqbot-nodejs/blob/main/src/protocol/gateway/constants.ts)
- [官方发送消息文档](https://github.com/tencent-connect/bot-docs/blob/main/docs/develop/api-v2/server-inter/message/send-receive/send.md)
