# 客户端接入约定（schema_version = 1）

请把客户端改为访问 qqbot-gateway 的 HTTPS HTTP 接口，使用独立 Bearer；不要在客户端保存 QQ AppSecret 或取 QQ token。每 1 分钟轮询 `/wakes`，先检查 `/capabilities`，不要假设所有账号支持所有媒体或模板。

每个 wake 含 id、msg_id、scope（group/c2c）、target_id、content、timestamp、deadline、attachments、raw_event、context。group_openid 保留兼容，C2C 为空，不能再以它作为所有会话的标识；以 `(scope,target_id)` 隔离会话。

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

其他平台事件可轮询 `/events?after=<cursor>&limit=100`，持久化 next_cursor，按 cursor 去重。若旧游标落后 oldest_cursor，说明保留期内数据已有缺口，应记录缺口后向前继续；这不是永久事件归档。交互动作需要 `/api/request` 显式白名单，与用户确认用途后再启用；不应将此入口用于绕过 wake 的重复回复保护。

保持业务逻辑在客户端：回复策略、人设、话术、多媒体理解、决定调用模板、处理部分失败。服务器不自动学习机器人 ID；新群发送已知 @ 测试后采集实际 group_openid 与机器人的 mention 身份，更新服务器配置并重启。客户端只提交 wake_id，不接受模型自行生成任意目标 ID。

验收时请分别测试群文本、私聊文本、图片 URL/base64、视频/语音/文件权限、原生模板、202 后查询、多条部分失败；记录实际 QQ 返回码，不暴露 token、AppSecret 或 Bearer。
