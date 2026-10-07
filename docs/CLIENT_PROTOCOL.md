# 客户端接入约定（schema_version = 2，兼容旧 wakes 接口）

请把客户端改为访问 qqbot-gateway 的 HTTPS HTTP 接口，使用独立 Bearer；不要在客户端保存 QQ AppSecret 或取 QQ token。每 1 分钟轮询 `/wakes`，先检查 `/capabilities`，不要假设所有账号支持所有媒体或模板。

每个 wake 含 id、msg_id、scope（group/c2c/channel/dm）、target_id、content、timestamp、deadline、attachments、raw_event、context。group_openid 保留兼容，其他 scope 为空，不能再以它作为所有会话的标识；以 `(scope,target_id)` 隔离会话。channel 的 target_id 是子频道 channel_id；dm 的 target_id 是私信会话 guild_id，不能用用户 ID 或来源 guild_id 替代。相关原始 ID 在 raw_event.d 中。

`attachments` 是原始字段的对象包装，如 `{"attachments":[...],"msg_elements":[...],"embeds":[...]}`；保留字段原名和原值，没有字段时 `{}`。`raw_event` 是完整 dispatch 帧，未知未来字段从 raw_event.d 获取。旧历史行可能为 `{}`。服务端只透传，不做 OCR、语音识别、媒体下载或业务解释。

回复用 `POST /wakes/reply`：

```json
{"wake_id":1,"text":"caption","media":[{"type":"image","url":"https://example.com/image.png"}]}
```

media 类型 image/video/audio/voice/file；每项 url 或 base64 二选一，可加 file_name。base64 使用标准格式，无 data URI 前缀；默认单文件解码大小 16 MiB，整个请求最大 24 MiB，读取 capabilities 中的实际限制。每个媒体拆成一条消息，文字只作为第一条 caption。

模板/Markdown/键盘等用 `qq_payload` 原生 body，必须带 msg_type。禁止填写 msg_id/msg_seq/event_id/is_wakeup/stream/stream_messages。可用 `messages` 数组提交有序多条，各项是 text/media 或独占 qq_payload，顶层不能再带 text/media/qq_payload。群默认 ack 后剩余最多 4 条；C2C 默认最多 4 条。不要把客户端长文本自动拆成无限条。

同一 wake 只负责一次正式回复。序号与领取由服务端管理，客户端不构造序号。HTTP 200 需检查 ok/status；202 processing 或网络超时后查询 `GET /wakes/{id}`。done 表示确认成功；pending 可在剩余时间内提交；claimed 继续查询；unknown/partial/failed/expired 不自动重发。已有成功请求可用相同 body 重放查询结果，但改变 body 无法再发送。不要用 unknown 生成新的替代 wake。

查询结果 wake.parts 为每部分状态及 QQ 结果，wake.reply_result 为聚合结果。QQ 的消息返回不确定时必须保守处理；客户端收到返回 body 后不要把 HTTP 成功直接显示成“送达”。

可先调用 `/media/upload`：body `{"wake_id":1,"media":{"type":"image","url":"https://example.com/a.png"}}`，取得 upload.file_info，再通过原生 payload `{"msg_type":7,"media":{"file_info":"..."},"content":"caption"}` 回复。file_info 可能有期限和目标绑定，不能假设可跨群复用。上传不等于消息发送；该入口没有独立操作状态，超时应标记未知。

其他平台事件可轮询 `/events?after=<cursor>&limit=100`，持久化 next_cursor，按 cursor 去重。若旧游标落后 oldest_cursor，说明保留期内数据已有缺口，应记录缺口后向前继续；这不是永久事件归档。按钮接收 ACK 有专用入口与默认自动处理，见下文。其他管理 API 仍需要 `/api/request` 显式白名单，不应用它绕过回复保护。

保持业务逻辑在客户端：回复策略、人设、话术、多媒体理解、决定调用模板、处理部分失败。服务器不自动学习机器人 ID；新群发送已知 @ 测试后采集实际 group_openid 与机器人的 mention 身份，更新服务器配置并重启。客户端只提交 wake_id，不接受模型自行生成任意目标 ID。

验收时请分别测试群文本、私聊文本、图片 URL/base64、视频/语音/文件权限、原生模板、202 后查询、多条部分失败；记录实际 QQ 返回码，不暴露 token、AppSecret 或 Bearer。

## 0.2 新增：独立主动发送

```json
{"request_id":"唯一UUID","scope":"group","target_id":"真实group_openid","text":"主动提醒"}
```

提交到 `POST /messages/send`。这条路径无需 wake_id 或入站 msg_id，绝不会偷偷复用最后一条消息。scope 支持 group/c2c/channel/dm；格式复用 text/media/qq_payload/messages。媒体与普通被动发送同样处理。channel/dm 的媒体助手支持图片 URL 或 base64 multipart；原生 body 不要求 msg_type，服务端移除 v2 msg_type/msg_seq，调用频道接口。

`mode` 默认 proactive；`mode=wakeup` 仅支持 C2C，注入 is_wakeup=true；`mode=event` 必须带顶层 event_id，由 QQ 校验事件回复权限与时效。其他模式不能混入 event_id；本入口禁止 wake_id/msg_id/msg_seq 等关联字段。不要因为旧 wake 过期就自动生成主动发送。

所有新增写入口都要求 request_id：1..128 个 ASCII 字母、数字、下划线、点、冒号、短横线，推荐 UUID。每次新动作生成新 ID；重试同一动作必须保留原 ID 和完整 body。服务端持久记录请求内容 hash，同 ID 同 body 重放结果，同 ID 不同 body 返回 conflict。进程崩溃、网络超时、部分成功都不会自动续发或重发。

`GET /operations/{request_id}` 返回 `operation.status/ok/parts/api_result`。外层 ok 只表示查询到记录，发送结果看 operation 内部字段。HTTP 202 后轮询；running 等待，done 已确认，failed/partial/unknown 不自动换 ID 重发。尚未取得 token 时可能暂时没有 operation 记录，404 不代表上一个 HTTP 后台任务不会执行。幂等记录保留 retention_days，保留期后仍不要复用旧 ID。

QQ 主动消息权限、额度及是否获平台接受，以 api_result 为准。失败码原样保留，客户端应展示清楚，不得把网关实现能力当作账号已获许可。

`GET /targets?after=0&limit=100` 按页读取服务端已收到消息的会话，返回 scope/target_id、相关 guild/channel/user ID 和最近消息信息。它是目标目录，不是授权列表，也不是永久的 ID 发现流；需要更新时从 after=0 重新读快照，实时变化仍看 /events。主动发送可以明确指定目录外已知 ID，QQ 最终校验是否可达。

## C2C 流式生命周期

1. `POST /streams/start`：`{"request_id":"start-uuid","wake_id":1,"protocol":"stream_messages"}`。必须是五分钟内的 C2C wake。返回 stream_id、next_index=0、deadline；仅领取并预留一条回复额度，不立即发送。之后不能再通过普通 wakes/reply 使用同一 wake。
2. `POST /streams/update`：`{"request_id":"frame-uuid","stream_id":"...","index":0,"text":"当前完整内容"}`。读取返回 next_index，下一帧必须匹配它。各帧 request_id 不同；重试同一帧保持 ID/body 不变。服务端按会话串行占用状态，最少间隔 300ms，共用同一 msg_seq。
3. `POST /streams/complete`：`{"request_id":"end-uuid","stream_id":"...","index":1,"text":"完整最终内容"}`。发 DONE 并将 wake 标为 done；新协议可以省略 text 来复用最后成功的全文。
4. `POST /streams/cancel`：`{"request_id":"cancel-uuid","stream_id":"..."}`。停止后续帧，不发送 DONE、不撤回已经显示的内容；wake 保守终结为 unknown，stream 为 cancelled。

通过 `GET /streams/{stream_id}` 查询协议、state、next_index 和 QQ id；通过 `/operations/{request_id}` 查某一帧结果。start 的 operation done 仅代表成功创建本地会话，update 的 done 仅代表该帧确认成功，不能当作整个流完成。任何帧未知时停止继续提交并核对状态，不自动用普通回复或另一协议重发。

`protocol=stream_messages` 使用新 SDK 的 /stream_messages，text 必须是完整全文，input_mode=replace。`protocol=legacy` 对照本地 AstrBot /messages 中的 stream：update.text 是增量片段，可传 reset=true 重置；complete.text 默认为空结束片段。服务端补齐 Markdown 末尾换行。两种文本语义不同，不能混用；没有自动协议试发回退。默认协议由 stream_protocol 配置指定。

## 其他独立写入口

| 入口 | body（均需 request_id） | 说明 |
| --- | --- | --- |
| POST /media/upload-target | scope、target_id、media 对象 | 群/C2C 独立上传，无需 wake；返回 upload.file_info，不等于发送消息 |
| POST /interactions/ack | interaction_id、code=0..5、可选 data 对象 | PUT QQ 交互确认；默认已自动接收 ACK，避免再重复确认 |
| POST /typing | wake_id、seconds=1..60（默认30） | 仅 pending C2C；发送 input_notify，wake 保持 pending，但消耗一个 msg_seq/回复额度 |
| POST /dms/create | recipient_id、source_guild_id | 创建频道私信会话，返回 api_result.guild_id，用于 dm target_id |
| POST /messages/recall | scope、target_id、message_id | 请求 QQ 撤回指定消息；各 scope 权限/时限由 QQ 校验 |

interaction_auto_ack 默认 true，服务端立即确认收到按钮事件，原始事件仍进入 /events，实际业务由客户端处理。需要业务决定 ACK code 时关闭自动 ACK，并实现满足 QQ 约五秒时限的快速事件消费；每分钟轮询不能满足这个时限。其他管理能力仍走显式白名单 /api/request。

旧 /media/upload（wake 绑定）及 /api/request 没有持久 operation，保留兼容但不具备新增入口的幂等保障。
