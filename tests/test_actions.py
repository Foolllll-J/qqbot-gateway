import asyncio
import concurrent.futures
from datetime import datetime, timezone, timedelta
import http.client
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import qq_actions as actions
import qq_gateway_server as server
import qq_runtime as runtime
import qq_media as media


class ActionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cfg = {
            "appid": "id",
            "appsecret": "secret",
            "env": "formal",
            "db_path": str(Path(self.tmp.name) / "events.db"),
            "context_window": 500,
            "retention_days": 7,
            "wake_batch_size": 20,
            "context_limit": 30,
            "ack_enabled": False,
            "interaction_auto_ack": False,
        }
        self.gw = server.GatewayClient(self.cfg)
        self.store = self.gw.store
        self.engine = self.gw.actions
        self.push = {
            "request_id": "push-1",
            "scope": "group",
            "target_id": "group",
            "text": "hello",
        }
        self.event = {
            "id": "inbound",
            "content": "hi",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "author": {"user_openid": "user"},
        }

    def tearDown(self):
        self.tmp.cleanup()

    def wake(self):
        self.store.save_event(self.event, True, "C2C_MESSAGE_CREATE")

    def start(self, protocol="stream_messages"):
        self.wake()
        out = self.engine.execute(
            "/streams/start",
            {"request_id": "start", "wake_id": 1, "protocol": protocol},
            "token",
        )
        self.assertTrue(out["ok"])
        return out["stream_id"]

    def test_proactive_without_msg_id_and_concurrent_idempotency(self):
        def send(*args, **kwargs):
            time.sleep(0.02)
            return {"_http_status": 200, "id": "out"}

        with patch.object(actions, "request_json", side_effect=send) as request:
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                list(
                    pool.map(
                        lambda _: self.engine.execute(
                            "/messages/send", self.push, "token"
                        ),
                        range(8),
                    )
                )
            replay = self.engine.execute("/messages/send", self.push, "token")
            conflict = self.engine.execute(
                "/messages/send", {**self.push, "text": "changed"}, "token"
            )
        self.assertEqual(request.call_count, 1)
        self.assertNotIn("msg_id", request.call_args.args[4])
        self.assertNotIn("event_id", request.call_args.args[4])
        self.assertTrue(replay["ok"])
        self.assertEqual(conflict["status"], "conflict")
        self.assertEqual(
            actions.Actions(self.store, self.cfg, "api").get("push-1")["status"], "done"
        )

    def test_push_media_partial_then_no_retry(self):
        body = {
            **self.push,
            "messages": [
                {"text": "first"},
                {"media": [{"type": "video", "url": "https://example.org/v"}]},
            ],
        }
        body.pop("text")
        with (
            patch.object(actions, "upload_media", return_value={"file_info": "f"}),
            patch.object(
                actions,
                "request_json",
                side_effect=[
                    {"_http_status": 200, "id": "one"},
                    {"_http_status": 400, "code": 22009},
                ],
            ) as request,
        ):
            result = self.engine.execute("/messages/send", body, "token")
            self.engine.execute("/messages/send", body, "token")
        self.assertEqual(result["status"], "partial")
        self.assertEqual(request.call_count, 2)
        self.assertEqual(request.call_args.args[4]["media"], {"file_info": "f"})
        self.assertEqual(request.call_args.args[4]["content"], " ")
        self.assertNotIn("msg_id", request.call_args.args[4])

    def test_timeout_and_crashed_operation_never_resends(self):
        with patch.object(
            actions, "request_json", side_effect=TimeoutError()
        ) as request:
            result = self.engine.execute("/messages/send", self.push, "token")
            self.engine.execute("/messages/send", self.push, "token")
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(request.call_count, 1)
        data = {**self.push, "request_id": "crashed"}
        self.engine._claim("/messages/send", data)
        with self.store.connection() as conn:
            conn.execute("UPDATE operations SET lease=0 WHERE request_id='crashed'")
            conn.commit()
        with patch.object(actions, "request_json") as request:
            self.assertEqual(
                self.engine.execute("/messages/send", data, "token")["status"],
                "unknown",
            )
            request.assert_not_called()

    def test_modes_and_explicit_disable(self):
        with patch.object(
            actions, "request_json", return_value={"_http_status": 200, "id": "sent"}
        ) as request:
            result = self.engine.execute(
                "/messages/send",
                {**self.push, "scope": "c2c", "mode": "wakeup"},
                "token",
            )
        self.assertTrue(result["ok"])
        self.assertTrue(request.call_args.args[4]["is_wakeup"])
        self.assertNotIn("msg_id", request.call_args.args[4])
        self.cfg["proactive_enabled"] = False
        with patch.object(actions, "request_json") as request:
            self.assertFalse(
                self.engine.execute(
                    "/messages/send", {**self.push, "request_id": "disabled"}, "token"
                )["ok"]
            )
            request.assert_not_called()
        with self.assertRaises(ValueError):
            self.engine.validate(
                "/messages/send",
                {**self.push, "qq_payload": {"msg_type": 0, "msg_id": "override"}},
            )
        with self.assertRaises(ValueError):
            self.engine.validate("/messages/send", {**self.push, "target_id": ".."})

    def test_channel_and_dm_receive_context_and_passive_reply(self):
        channel = {
            **self.event,
            "id": "channel-in",
            "channel_id": "channel",
            "guild_id": "guild",
            "author": {"id": "member"},
        }
        dm = {**channel, "id": "dm-in", "guild_id": "dm-session"}

        async def dispatch():
            await self.gw.handle_dispatch("AT_MESSAGE_CREATE", channel)
            await self.gw.handle_dispatch("DIRECT_MESSAGE_CREATE", dm)

        asyncio.run(dispatch())
        wakes = self.store.get_pending_wakes()
        self.assertEqual(
            [(w["scope"], w["target_id"]) for w in wakes],
            [("channel", "channel"), ("dm", "dm-session")],
        )
        self.assertEqual([len(w["context"]) for w in wakes], [1, 1])
        plan = media.normalize_reply(
            {"qq_payload": {"content": "reply", "embed": {"title": "x"}}},
            scope="channel",
        )
        with patch.object(
            runtime,
            "send_guild_message",
            return_value={"_http_status": 200, "id": "sent"},
        ) as send:
            self.assertTrue(
                runtime.reply_to_wake(self.store, "api", "token", 1, plan=plan)["ok"]
            )
        self.assertEqual(send.call_args.args[2:4], ("channel", "channel"))
        self.assertEqual(send.call_args.args[4]["msg_id"], "channel-in")
        self.assertEqual(len(self.store.get_targets()["targets"]), 2)

    def test_channel_image_url_and_multipart_base64(self):
        with patch.object(
            media, "request_json", return_value={"_http_status": 200, "id": "out"}
        ) as request:
            media.send_guild_message(
                "api",
                "token",
                "channel",
                "id",
                {"msg_type": 7, "msg_seq": 1, "content": "caption"},
                {"type": "image", "url": "https://example.org/i"},
            )
        self.assertEqual(request.call_args.args[3], "/channels/id/messages")
        self.assertEqual(
            request.call_args.args[4],
            {"content": "caption", "image": "https://example.org/i"},
        )
        response = Mock(status_code=200, headers={})
        response.json.return_value = {"id": "image"}
        with patch.object(media.requests, "post", return_value=response) as post:
            media.send_guild_message(
                "api",
                "token",
                "dm",
                "session",
                {"content": "caption"},
                {"type": "image", "base64": "YQ=="},
            )
        self.assertEqual(post.call_args.kwargs["files"]["file_image"][1], b"a")
        self.assertIn("/dms/session/messages", post.call_args.args[0])
        with self.assertRaises(ValueError):
            media.normalize_reply(
                {"media": [{"type": "video", "url": "https://example.org/v"}]},
                scope="channel",
            )

    def test_current_stream_full_replace_shared_seq_and_completion(self):
        sid = self.start()
        update = {"request_id": "frame0", "stream_id": sid, "index": 0, "text": "hello"}
        with patch.object(
            actions,
            "request_json",
            return_value={"_http_status": 200, "id": "qq-stream"},
        ) as request:
            self.assertTrue(
                self.engine.execute("/streams/update", update, "token")["ok"]
            )
            self.assertTrue(
                self.engine.execute("/streams/update", update, "token")["replayed"]
            )
            done = self.engine.execute(
                "/streams/complete",
                {
                    "request_id": "frame1",
                    "stream_id": sid,
                    "index": 1,
                    "text": "hello world",
                },
                "token",
            )
        self.assertTrue(done["ok"])
        self.assertEqual(request.call_count, 2)
        first, last = [c.args[4] for c in request.call_args_list]
        self.assertEqual(first["input_mode"], "replace")
        self.assertEqual(last["content_raw"], "hello world")
        self.assertEqual(last["input_state"], 10)
        self.assertEqual(last["stream_msg_id"], "qq-stream")
        self.assertEqual(first["msg_seq"], last["msg_seq"])
        self.assertEqual([first["index"], last["index"]], [0, 1])
        self.assertEqual(self.store.get_wake(1)["status"], "done")
        self.assertEqual(self.engine.get_stream(sid)["state"], "complete")

    def test_legacy_stream_delta_protocol(self):
        sid = self.start("legacy")
        with patch.object(
            actions,
            "request_json",
            return_value={"_http_status": 200, "id": "qq-stream"},
        ) as request:
            self.engine.execute(
                "/streams/update",
                {"request_id": "f0", "stream_id": sid, "index": 0, "text": "delta"},
                "token",
            )
            self.engine.execute(
                "/streams/complete",
                {"request_id": "f1", "stream_id": sid, "index": 1},
                "token",
            )
        first, last = [c.args[4] for c in request.call_args_list]
        self.assertNotIn("id", first["stream"])
        self.assertEqual(last["stream"]["id"], "qq-stream")
        self.assertEqual(last["stream"]["state"], 10)
        self.assertTrue(first["markdown"]["content"].endswith("\n"))
        self.assertTrue(request.call_args.args[3].endswith("/messages"))

    def test_stream_order_timeout_and_restart_no_duplicate(self):
        sid = self.start()
        with patch.object(actions, "request_json") as request:
            wrong = self.engine.execute(
                "/streams/update",
                {"request_id": "wrong", "stream_id": sid, "index": 1, "text": "oops"},
                "token",
            )
            self.assertEqual(wrong["reason"], "stream_index_mismatch")
            request.assert_not_called()
        with patch.object(
            actions, "request_json", side_effect=TimeoutError()
        ) as request:
            result = self.engine.execute(
                "/streams/update",
                {"request_id": "f0", "stream_id": sid, "index": 0, "text": "hello"},
                "token",
            )
            replay = actions.Actions(self.store, self.cfg, "api").execute(
                "/streams/update",
                {"request_id": "f0", "stream_id": sid, "index": 0, "text": "hello"},
                "token",
            )
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(replay["status"], "unknown")
        self.assertEqual(request.call_count, 1)
        self.assertEqual(self.engine.get_stream(sid)["state"], "unknown")
        self.assertEqual(self.store.get_wake(1)["status"], "unknown")

    def test_stream_cancel_and_window(self):
        sid = self.start()
        self.engine.execute(
            "/streams/cancel", {"request_id": "cancel", "stream_id": sid}, "token"
        )
        self.assertEqual(self.engine.get_stream(sid)["state"], "cancelled")
        self.assertEqual(self.store.get_wake(1)["status"], "unknown")
        self.event["id"] = "old"
        self.event["timestamp"] = (
            datetime.now(timezone.utc) - timedelta(minutes=6)
        ).isoformat()
        self.store.save_event(self.event, True, "C2C_MESSAGE_CREATE")
        result = self.engine.execute(
            "/streams/start", {"request_id": "old-start", "wake_id": 2}, "token"
        )
        self.assertEqual(result["reason"], "stream_window_expired")

    def test_typing_preserves_pending_and_consumes_sequence(self):
        self.wake()
        with patch.object(
            actions,
            "request_json",
            return_value={"_http_status": 200, "ext_info": {"ref_idx": "r"}},
        ) as request:
            self.assertTrue(
                self.engine.execute(
                    "/typing",
                    {"request_id": "typing", "wake_id": 1, "seconds": 30},
                    "token",
                )["ok"]
            )
        self.assertEqual(request.call_args.args[4]["msg_type"], 6)
        self.assertEqual(self.store.get_wake(1)["status"], "pending")
        self.assertEqual(self.store.reserve_ack("inbound"), 2)

    def test_interaction_auto_ack_dedup_and_raw_storage(self):
        self.cfg["interaction_auto_ack"] = True

        async def token():
            return "fake"

        self.gw.tokens.get_token = token
        frame = {
            "t": "INTERACTION_CREATE",
            "d": {"id": "button", "data": {"resolved": {"button_data": "opaque"}}},
        }

        async def run():
            await self.gw.handle_dispatch(frame["t"], frame["d"], frame)
            await asyncio.gather(*list(self.gw.tasks))
            await self.gw.handle_dispatch(frame["t"], frame["d"], frame)
            await asyncio.gather(*list(self.gw.tasks))

        with patch.object(
            actions, "request_json", return_value={"_http_status": 204}
        ) as request:
            asyncio.run(run())
        self.assertEqual(request.call_count, 1)
        self.assertEqual(
            request.call_args.args[2:5], ("PUT", "/interactions/button", {"code": 0})
        )
        self.assertEqual(self.store.get_events()["events"][0]["payload"], frame)

    def test_http_proactive_and_result_query(self):
        async def token():
            return "fake"

        self.gw.tokens.get_token = token
        loop = asyncio.new_event_loop()
        self.gw.loop = loop
        runner = threading.Thread(target=loop.run_forever)
        runner.start()
        server.WakeHandler.gateway = self.gw
        server.WakeHandler.api_bearer = b"a" * 64
        httpd = server.BoundedHTTPServer(("127.0.0.1", 0), server.WakeHandler, 4)
        thread = threading.Thread(target=httpd.serve_forever)
        thread.start()

        def request(method, path, data=None):
            client = http.client.HTTPConnection(
                "127.0.0.1", httpd.server_port, timeout=3
            )
            client.request(
                method,
                path,
                json.dumps(data) if data is not None else None,
                {"Authorization": "Bearer " + "a" * 64},
            )
            reply = client.getresponse()
            out = (reply.status, json.loads(reply.read()))
            client.close()
            return out

        try:
            with patch.object(
                actions,
                "request_json",
                return_value={"_http_status": 200, "id": "sent"},
            ) as send:
                self.assertTrue(request("POST", "/messages/send", self.push)[1]["ok"])
                self.assertTrue(
                    request("POST", "/messages/send", self.push)[1]["replayed"]
                )
                self.assertEqual(send.call_count, 1)
            self.assertEqual(
                request("GET", "/operations/push-1")[1]["operation"]["status"], "done"
            )
            self.assertEqual(request("GET", "/capabilities")[1]["schema_version"], 2)
            self.assertEqual(
                request("POST", "/messages/send", {**self.push, "request_id": False})[
                    0
                ],
                400,
            )
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join()
            loop.call_soon_threadsafe(loop.stop)
            runner.join()
            loop.close()

    def test_dm_creation_recall_and_direct_upload(self):
        with patch.object(
            actions,
            "request_json",
            return_value={"_http_status": 200, "guild_id": "session"},
        ) as request:
            out = self.engine.execute(
                "/dms/create",
                {
                    "request_id": "dm",
                    "recipient_id": "user",
                    "source_guild_id": "guild",
                },
                "token",
            )
        self.assertTrue(out["ok"])
        self.assertEqual(request.call_args.args[3], "/users/@me/dms")
        with patch.object(
            actions, "request_json", return_value={"_http_status": 204}
        ) as request:
            out = self.engine.execute(
                "/messages/recall",
                {
                    "request_id": "recall",
                    "scope": "channel",
                    "target_id": "channel",
                    "message_id": "msg",
                },
                "token",
            )
        self.assertTrue(out["ok"])
        self.assertEqual(
            request.call_args.args[2:4], ("DELETE", "/channels/channel/messages/msg")
        )
        with patch.object(actions, "upload_media", return_value={"file_info": "f"}):
            out = self.engine.execute(
                "/media/upload-target",
                {
                    "request_id": "upload",
                    "scope": "group",
                    "target_id": "group",
                    "media": {"type": "image", "url": "https://example.org/i"},
                },
                "token",
            )
        self.assertEqual(out["upload"]["file_info"], "f")

    def test_stream_crash_freezes_frame_and_wake(self):
        sid = self.start()
        data = {
            "request_id": "crash-frame",
            "stream_id": sid,
            "index": 0,
            "text": "maybe sent",
        }
        self.engine._claim("/streams/update", data)
        with self.store.connection() as conn:
            conn.execute(
                "UPDATE streams SET state='sending',active_request=? WHERE stream_id=?",
                (data["request_id"], sid),
            )
            conn.execute(
                "UPDATE operations SET lease=0 WHERE request_id=?",
                (data["request_id"],),
            )
            conn.commit()
        resumed = actions.Actions(self.store, self.cfg, "api")
        with patch.object(actions, "request_json") as request:
            self.assertEqual(
                resumed.execute("/streams/update", data, "token")["status"], "unknown"
            )
            request.assert_not_called()
        self.assertEqual(resumed.get_stream(sid)["state"], "unknown")
        self.assertEqual(self.store.get_wake(1)["status"], "unknown")

    def test_old_messages_populate_target_directory(self):
        self.wake()
        with self.store.connection() as conn:
            conn.execute("DELETE FROM targets")
            conn.commit()
        upgraded = runtime.EventStore(self.store.db_path)
        row = upgraded.get_targets()["targets"][0]
        self.assertEqual(
            (row["scope"], row["target_id"], row["last_message_id"]),
            ("c2c", "user", "inbound"),
        )

    def test_empty_204_qq_response_is_success_for_ack(self):
        response = Mock(status_code=204, content=b"", headers={})
        with patch.object(media.requests, "request", return_value=response):
            result = media.request_json(
                "api", "token", "PUT", "/interactions/id", {"code": 0}
            )
        self.assertTrue(actions.api_success(result))
        response.json.assert_not_called()


if __name__ == "__main__":
    unittest.main()
