# 转交客户端 Agent：升级到服务端 0.2 / schema_version 2

请读取 D:\Projects\Astrbot\qqbot-gateway 的 README.md、docs/CLIENT_PROTOCOL.md、docs/ASTRBOT_PARITY.md，并检查实际 HTTP 代码。本次服务端已补齐原先缺失的独立主动发送、频道/频道私信、C2C 流式生命周期、按钮 ACK、输入中提示、私信创建及撤回，不要沿用“服务端只能被动回复”的旧结论。

请先检查客户端现有实现，列出差异和最小修改方案，不直接修改。重点核对：

1. 原有 wakes/reply 兼容，新增 scope 为 group/c2c/channel/dm，会话按 scope+target_id 隔离；dm target_id 是私信会话 guild_id。
2. 主动消息用 POST /messages/send，显式 scope/target_id，无需 wake_id/msg_id。text/media/qq_payload/messages 格式复用，必须为每个新动作生成 request_id；同一动作重试保持相同 ID/body，GET /operations/{id} 查询结果。不能自动把超时或过期被动回复转成新主动发送。
3. C2C 流式使用 streams/start、update、complete、cancel。每帧都有独立 request_id，并携带服务端返回的 next_index；一个流共用 msg_seq。stream_messages 的 text 是全文替换，legacy 的 text 是增量，两者不能混用或自动试发切换。每帧 done 不代表整个流完成。
4. 原始媒体从 attachments 包装和完整 raw_event.d 读取；服务端不做语音转码/OCR/媒体理解，客户端提交 QQ 可接受的编码数据。频道图片使用专门路由与 multipart，不套用群 v2 files。
5. 服务端默认立即确认收到按钮事件（interaction_auto_ack=true），客户端仍消费原始事件执行业务。需要业务 ACK code 时必须关闭自动 ACK 并采用能满足约五秒时限的处理方式；每分钟轮询无法完成及时确认。
6. typing、media/upload-target、dms/create、messages/recall 也使用持久 request_id。旧 media/upload 和 api/request 没有这项幂等保护。
7. capabilities 表示代码实现能力，不表示真实 QQ 权限。主动推送、流式协议和模板都必须记录真实 QQ 验收结果；根据返回码呈现平台限制，不硬编码历史额度。
8. HTTP 200 看内部 ok/status；202、超时后查询。unknown/partial/failed 不自动换 ID 重发；保留已经成功部分。原始事件游标去重、配置显式 mention ID 和一分钟 wake 轮询仍保留。

输出：当前兼容性、严重问题、最小改造步骤、需要业务决定的选项及建议、真实 QQ 验收清单。不要读写真实密钥，也不要修改服务端。
