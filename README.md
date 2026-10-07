# qqbot-gateway

QQ 官方机器人服务端：接收 Gateway 事件、保存原始数据、维护被动回复队列，向客户端提供带 Bearer 认证的 HTTP 接口。客户端决定回复策略、话术、人设和媒体内容。

这是实现了群聊、C2C 私聊、文字子频道和频道私信的独立服务，不依赖 AstrBot 或 `gateway_client.py`。0.2.0 补齐显式主动发送和 C2C 流式会话。当前自动化测试使用模拟 QQ 接口和临时 SQLite；真实机器人权限、媒体格式限制与沙箱域名尚需上线验证。

## 服务边界

服务端负责四件事：

1. Gateway WS 连接、心跳、会话恢复，以及 AppID/AppSecret 直接获取 token。
2. 保存收到的原始 dispatch 事件，包括富媒体字段和未知字段。
3. 将官方 @ 事件、显式配置识别的全量消息 @、C2C 和频道私信消息放入队列。绝不自动学习 mention ID。
4. 可配置的即时 ack 和 HTTP 收发接口，包括显式主动发送、交互确认和流式协议状态。

稳定目标是协议层尽量不动。QQ 官方 API 变更，或需要增加关键词等新的服务端触发类型时，才调整服务端。回复策略、话术、人设和新群身份映射由客户端或配置处理。新群还必须已加入机器人并具备相应平台权限；仅改配置无法获得权限。新媒体原始字段通过 `raw_event` 直接透传；已有 QQ 消息接口的新 body 字段通过 `qq_payload` 透传。需要不同端点、上传协议或新的状态机时，仍可能需要协议适配，不能保证任意未来功能无需改代码。

## 已实现的能力

| 能力 | 行为 |
| --- | --- |
| 群聊 / C2C / 频道 / 频道私信 | 保存消息、按四种 scope 隔离上下文、被动回复 |
| 主动发送 | `/messages/send`，无需 wake_id 或入站 msg_id；文本、媒体、原生 body、多条消息；持久幂等和结果查询 |
| 群聊/C2C 图片 / 视频 / 语音 / 文件 | URL 或标准 base64；上传得到 file_info，再发送消息 |
| 频道/频道私信图片 | URL image 或 base64 multipart file_image，使用频道 API，不套用 v2 files/msg_type |
| 大文件 | base64 解码后大于 5 MiB 使用分片上传；默认本地上限 16 MiB，QQ 自身限制仍适用 |
| Markdown / 键盘 / ARK 等 | 通过原生 `qq_payload` 提交；具体组合依赖 QQ 接口、账号权限和模板 |
| 多条回复 | 有序发送，事先检查剩余额度；记录每条结果，不自动补发失败或未知部分 |
| C2C 流式回复 | start/update/complete/cancel；新 stream_messages 全文替换与 AstrBot legacy stream 增量协议，显式选择，不在未知发送后切协议重发 |
| 按钮交互 | 原生 keyboard 发送、原始 INTERACTION_CREATE 透传、默认即时接收 ACK；客户端保留业务动作 |
| 输入中提示 / 召回 / 创建频道私信 | `/typing`、`/messages/recall`、`/dms/create`；QQ 权限和范围限制仍适用 |
| 其他事件 | `/events` 按游标读取完整 dispatch 帧，业务解释由客户端负责 |
| 其他 QQ HTTP API | `/api/request`，默认关闭，需配置精确 method/path 白名单 |

`/capabilities` 的 schema_version 为 2，旧 wakes/reply 接口继续兼容。这里的 implemented 表示网关代码实现能力，不表示账号已经获批。原生 body 保留未知字段，同时禁止客户端覆盖 msg_id、msg_seq、event_id、is_wakeup 和 stream 字段；主动、事件回复、召回通过显式入口管理这些关联字段。

主动推送可用性存在资料冲突：腾讯旧 [发送消息文档](https://github.com/tencent-connect/bot-docs/blob/main/docs/develop/api-v2/server-inter/message/send-receive/send.md) 写有停止主动推送的公告，而新 SDK 和本地 AstrBot 提供无 msg_id 的发送路径。服务端实现这条路径，原样返回 QQ 拒绝、额度和权限错误，不承诺绕过平台限制，也不把被动失败自动转成主动发送。能力对照与实测边界见 [docs/ASTRBOT_PARITY.md](docs/ASTRBOT_PARITY.md)。

实现参考腾讯官方 [Node SDK](https://github.com/tencent-connect/qqbot-nodejs)、[Agent SDK](https://github.com/tencent-connect/qqbot-agent-sdk)、[BotGo](https://github.com/tencent-connect/botgo) 和本地 AstrBot 的 QQ official 适配。参考实现及文档不代表真实账号已经获得对应能力。

## 配置

复制 `config.example.json` 为 `config.json`，填写 appid、appsecret 和随机 api_bearer（至少 32 个无空白 ASCII 字符），例如 `openssl rand -hex 32`。示例占位值会被启动校验拒绝。配置与数据库均不要提交 Git。

- `env`: 默认 formal，使用 `https://api.bot.qq.com`。sandbox 沿用历史域名，必须实际验证。
- `intents`: 默认 `33554432`（群聊/C2C）；频道 @ 为 `1 << 30`、频道私信为 `1 << 12`、按钮回调为 `1 << 26`。全部这些位与默认位合并是 `1174409216`，仅在账号权限允许时配置。全量群/频道消息需要另外的官方权限和 intent，不擅自替你打开。
- `bot_mention_ids`: `{"真实group_openid": ["该群事件里机器人的真实身份ID"]}`。各群分别配置，不能假设身份跨群相同。官方 GROUP_AT_MESSAGE_CREATE 和 AT_MESSAGE_CREATE 自身已经表明 @ 机器人，无需该映射；全量消息用映射匹配 content 的 @ 或 mentions。频道全量消息的映射键使用 channel_id，同样不自动学习。
- `ack_enabled`: 默认 true；`c2c_ack_enabled`: 默认 false。ack 文本由 `ack_text` 配置，ack 失败/超时不自动重发。
- `proactive_enabled`: 默认 true，允许认证客户端明确请求主动发送；不代表 QQ 账号权限。设为 false 可禁用主动与 wakeup 模式。
- `interaction_auto_ack`: 默认 true，收到按钮事件后即时回复 code=0 的接收 ACK，不执行客户端业务。若要客户端自行决定 ACK code，设为 false，且客户端必须采用能满足 QQ 约 5 秒时限的事件处理方式；一分钟轮询无法及时确认按钮。
- `stream_protocol`: 默认 stream_messages（新 SDK 的全文 replace）；可改 legacy（本地 AstrBot 的 stream 增量），或每次 start 显式指定。两种协议绝不自动试发回退。
- `reply_windows`: 群默认 300 秒且不允许超过 300；C2C 保守默认 300 秒，可配置到 3600，但必须先验证平台实际接受范围。
- `context_window`: 每个会话保留消息条数，默认 500；`context_limit`: 每个 wake 带出的上下文，默认 30，截止到唤醒消息，避免混入之后的消息。
- `wake_batch_size`: 默认 20；`retention_days`: 默认 7，清理终态任务和原始事件；原始事件另有 100000 条上限。
- `max_media_bytes`: base64 解码大小上限，默认 16 MiB；`max_request_bytes`: HTTP JSON body 上限，默认 24 MiB。多媒体总 base64 大小也必须适配 HTTP 上限。
- `max_http_workers`: 默认 8；`http_port`: 默认 8082，仅监听 `127.0.0.1`。
- `db_path`: 默认配置文件所在目录中的 `qqbot_events.db`。生产环境应使用持久磁盘，不要放网络文件系统。
- `api_routes`: 默认 `[]`。例如 `[{"method":"GET","pattern":"/users/@me"}]`，使用正则 fullmatch。不要开放通配写路由；此扩展入口不提供 wake 幂等保护。
- `log_level`: 默认 INFO；DEBUG 日志可辅助核对事件身份，原始事件通过认证后的 `/events` 读取。

服务端对同一数据库持有操作系统锁，避免启动两个 Gateway。SQLite 使用 WAL、事务内去重/领取/序号分配，每次操作独立连接。升级旧 fixed 数据库时自动添加字段；停服务并备份后再升级，旧历史消息没有原始富媒体，无法补回。

## HTTP 与回复语义

所有请求带 `Authorization: Bearer <api_bearer>`。接口契约见 [docs/CLIENT_PROTOCOL.md](docs/CLIENT_PROTOCOL.md)。

```text
GET  /capabilities
GET  /wakes
GET  /wakes/{wake_id}
GET  /events?after=0&limit=100
GET  /targets?after=0&limit=100
GET  /operations/{request_id}
GET  /streams/{stream_id}
POST /wakes/reply
POST /messages/send
POST /streams/start|update|complete|cancel
POST /interactions/ack
POST /typing
POST /dms/create
POST /messages/recall
POST /media/upload-target
POST /media/upload
POST /api/request
```

兼容原文本回复：`{"wake_id":1,"text":"hello"}`。

媒体回复：`{"wake_id":1,"text":"caption","media":[{"type":"image","url":"https://example.com/a.png"}]}`。media 可选，媒体可单独发送；支持 image/video/audio/voice/file，url 与 base64 二选一，不接受 data URI。服务端不下载 URL、不识别或转码媒体；由 QQ 获取 URL 内容。多媒体拆成多条 QQ 消息，文本只放在第一条，消耗对应条数的回复额度。

原生回复：`{"wake_id":1,"qq_payload":{"msg_type":2,"markdown":{"content":"hello"}}}`。原生字段不做内容理解，QQ 返回的拒绝会作为失败记录。

多条回复：`{"wake_id":1,"messages":[{"text":"first"},{"qq_payload":{"msg_type":2,"markdown":{"content":"second"}}}]}`。messages 与顶层 text/media/qq_payload 互斥；qq_payload 与同条 text/media 互斥。

群聊按每条唤醒消息最多 5 条被动回复、C2C 最多 4 条保守管理。ack 占一条额度，群默认最多剩 4 条，C2C 默认不 ack。额度在开始发送前整体检查；计划超额返回 pending 和 passive_reply_budget_exhausted，不领取、不发送，客户端可缩减计划后重提，实际 QQ 限制以平台为准。过期前预留 5 秒。同一 wake 同一归一化请求已成功时重放结果，不重新发送；请求内容改变、partial、failed、unknown 等终态均不会自动重发。租约失效的 claimed 转 unknown，防止进程崩溃后重复发送。

HTTP 返回 200 不等于发送成功，应读取 `ok/status`。等待超过 30 秒返回 202 processing，后台仍可能执行；通过 `/wakes/{id}` 查看结果。连接超时或丢失响应同样先查状态。unknown 表示可能已经送达；partial 表示部分完成，均需人工/客户端策略处理。`/media/upload`、`/api/request` 没有独立操作查询；超时返回 unknown 时，写操作不要盲目重试。

`reply.py` 是服务器本地 CLI，读取同一配置和数据库，不要求额外 gateway_client：

```bash
.venv/bin/python reply.py --wake-id 1 --text 'hello'
.venv/bin/python reply.py --wake-id 1 --text-file reply.txt --media '[{"type":"image","url":"https://example.com/a.png"}]'
.venv/bin/python reply.py --wake-id 1 --payload '{"msg_type":2,"markdown":{"content":"hello"}}'
```

复杂多条消息可使用 `--messages` JSON 参数。HTTP 和 CLI 在执行过程中均会续租；进程退出或租约失效时保守标记 unknown。

## Linux 部署：venv + systemd

先创建专用账户和目录，将仓库文件放入 `/opt/qqbot-gateway`，以该账户创建 venv、安装依赖并填写配置：

```bash
sudo useradd --system --home /opt/qqbot-gateway --shell /usr/sbin/nologin qqbot
sudo mkdir -p /opt/qqbot-gateway
sudo chown -R qqbot:qqbot /opt/qqbot-gateway
cd /opt/qqbot-gateway
sudo -u qqbot python3 -m venv .venv
sudo -u qqbot .venv/bin/pip install -r requirements.txt
sudo -u qqbot cp config.example.json config.json
sudo chmod 600 config.json
# Edit config.json, then test in the foreground as qqbot.
sudo -u qqbot .venv/bin/python qq_gateway_server.py --config /opt/qqbot-gateway/config.json
```

前台确认 READY 后停止，再安装服务：

```bash
sudo cp deploy/qqbot-gateway.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now qqbot-gateway
sudo systemctl status qqbot-gateway
sudo journalctl -u qqbot-gateway -f
```

有旧 gateway/token relay 占用同端口时先停止旧服务；确认客户端切换后再删除旧服务。使用 1Panel/Nginx 提供 HTTPS，反代到 `http://127.0.0.1:8082`，传递 Authorization、允许至少 24 MiB body、超时不少于 45 秒。1Panel 处于容器时须确认能访问宿主机 loopback；不要把无法访问的容器 localhost 当宿主机。8082 无需对公网开放。客户端轮询改为每 1 分钟，服务端不会替你修改客户端定时任务。

此次提供 Git 项目与 systemd 部署，不含 Docker；后续可以加镜像，但 SQLite 必须持久挂载，仍保持单实例。

## 验证

```bash
.venv/bin/python -m unittest discover -s tests -v
```

测试覆盖旧 schema 升级、并发领取、序号上限、超时不重发、WS 恢复、原始富媒体、C2C 隔离、URL/base64/分片上传、多条部分失败和原生字段。上线还需实际测：群 @ / 私聊收发、媒体 URL 可达与平台格式限制、账号模板/键盘权限、断线恢复与重启。测试通过不等于实际 QQ 接口验收。

0.2 另有主动发送并发幂等、未知结果恢复、四 scope 路由、频道图片 multipart、两种流式协议、交互 ACK、typing、撤回和私信创建测试。给客户端 agent 的升级说明见 [docs/CLIENT_HANDOFF.md](docs/CLIENT_HANDOFF.md)。旧配置仍可使用；新增键有默认值，无需覆盖原有 config.json，但频道/按钮事件需要按权限显式修改 intents。

向现有服务发测试消息后，记录事件类型、group_openid、author 的 member_openid/user_openid、content 内 @ 字符串、mentions 内机器人的 ID。不同群分别采集，不用提供 AppSecret/Bearer/token。判断机器人身份应与已知的测试 @ 对照，不能把所有被 @ 的人加入配置。
