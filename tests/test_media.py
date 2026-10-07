import asyncio
import base64
import concurrent.futures
import http.client
import io
import json
import threading
import pathlib
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import patch, Mock
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import qq_runtime as runtime
import qq_media as media
import qq_gateway_server as server
import reply


class MediaTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = runtime.EventStore(pathlib.Path(self.tmp.name) / "events.db")
        self.event = {
            "id": "rich1",
            "group_openid": "group1",
            "author": {"member_openid": "member1"},
            "content": "hello",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def tearDown(self):
        self.tmp.cleanup()

    def wake(self, event=None, kind="GROUP_MESSAGE_CREATE"):
        self.store.save_event(event or self.event, True, kind)

    def test_raw_attachments_and_unknown_fields_survive(self):
        d = {
            **self.event,
            "attachments": [{"new_type": {"a": [1, None]}}],
            "msg_elements": [{"type": 999, "data": {"b": True}}],
            "embeds": [{"x": "y"}],
            "future_media": {"opaque": "value"},
        }
        frame = {"op": 0, "s": 55, "t": "GROUP_MESSAGE_CREATE", "d": d, "id": "event1"}
        self.store.save_event(d, True, frame["t"], frame)
        row = self.store.get_pending_wakes()[0]
        self.assertEqual(
            row["attachments"],
            {k: d[k] for k in ("attachments", "msg_elements", "embeds")},
        )
        self.assertEqual(row["context"][0]["attachments"], row["attachments"])
        self.assertEqual(row["raw_event"], frame)
        self.assertEqual(self.store.get_events()["events"][0]["payload"], frame)
        self.store.save_event(d, True, frame["t"], frame)
        self.assertEqual(len(self.store.get_events()["events"]), 1)

    def test_url_media_upload_then_reply_and_replay(self):
        self.wake()
        plan = media.normalize_reply(
            {
                "text": "caption",
                "media": [{"type": "image", "url": "https://example.org/a.png"}],
            }
        )
        with (
            patch.object(
                runtime, "upload_media", return_value={"file_info": "opaque"}
            ) as upload,
            patch.object(
                runtime,
                "request_json",
                return_value={"_http_status": 200, "id": "sent"},
            ) as send,
        ):
            result = runtime.reply_to_wake(self.store, "api", "token", 1, plan=plan)
            replay = runtime.reply_to_wake(self.store, "api", "token", 1, plan=plan)
        self.assertTrue(result["ok"])
        self.assertTrue(replay["replayed"])
        self.assertEqual(upload.call_count, 1)
        self.assertEqual(send.call_count, 1)
        payload = send.call_args.args[4]
        self.assertEqual(
            payload,
            {
                "msg_type": 7,
                "content": "caption",
                "media": {"file_info": "opaque"},
                "msg_id": "rich1",
                "msg_seq": 1,
            },
        )

    def test_base64_simple_upload_uses_file_data(self):
        source = {"type": "video", "base64": base64.b64encode(b"video").decode()}
        with patch.object(
            media, "request_json", return_value={"_http_status": 200, "file_info": "f"}
        ) as request:
            media.upload_media("api", "token", "group", "g", source)
        self.assertEqual(
            request.call_args.args[4],
            {"file_type": 2, "srv_send_msg": False, "file_data": source["base64"]},
        )

    def test_chunked_upload_no_credentials_and_complete_coverage(self):
        content = b"x" * (5 * 1024 * 1024 + 1)
        source = {
            "type": "file",
            "base64": base64.b64encode(content).decode(),
            "file_name": "test.bin",
        }
        responses = [
            {
                "_http_status": 200,
                "data": {
                    "upload_id": "up",
                    "block_size": 4 * 1024 * 1024,
                    "parts": [
                        {"index": 0, "presigned_url": "https://storage.example/0"},
                        {"index": 1, "presigned_url": "https://storage.example/1"},
                    ],
                },
            },
            {"_http_status": 200},
            {"_http_status": 200},
            {"_http_status": 200, "file_info": "merged"},
        ]
        with (
            patch.object(media, "request_json", side_effect=responses) as request,
            patch.object(
                media.requests, "put", return_value=Mock(status_code=200)
            ) as put,
        ):
            result = media.upload_media("api", "token", "c2c", "u", source)
        self.assertEqual(result["file_info"], "merged")
        self.assertEqual(
            sum(len(call.kwargs["data"]) for call in put.call_args_list), len(content)
        )
        self.assertTrue(
            all("headers" not in call.kwargs for call in put.call_args_list)
        )
        self.assertEqual(request.call_args.args[4]["upload_id"], "up")
        self.assertFalse(request.call_args.args[4]["srv_send_msg"])

    def test_native_payload_retains_unknown_fields(self):
        self.wake()
        payload = {
            "msg_type": 2,
            "markdown": {"content": "# Hi"},
            "keyboard": {"id": "key"},
            "future": {"a": 1},
        }
        plan = media.normalize_reply({"qq_payload": payload})
        with patch.object(
            runtime, "request_json", return_value={"_http_status": 200, "id": "sent"}
        ) as request:
            self.assertTrue(
                runtime.reply_to_wake(self.store, "api", "token", 1, plan=plan)["ok"]
            )
        self.assertEqual(request.call_args.args[4]["future"], payload["future"])
        for key in media.PROTECTED_FIELDS:
            with self.assertRaises(ValueError):
                media.normalize_reply({"qq_payload": {**payload, key: "bad"}})

    def test_c2c_context_isolation_and_reply_budget(self):
        d = {k: v for k, v in self.event.items() if k != "group_openid"}
        d["author"] = {"user_openid": "user1"}
        self.wake(d, "C2C_MESSAGE_CREATE")
        self.store.save_event({**self.event, "id": "other"}, False)
        row = self.store.get_pending_wakes()[0]
        self.assertEqual((row["scope"], row["target_id"]), ("c2c", "user1"))
        self.assertEqual(len(row["context"]), 1)
        plan = media.normalize_reply({"text": "private"})
        with patch.object(
            runtime, "request_json", return_value={"_http_status": 200, "id": "sent"}
        ) as request:
            self.assertTrue(
                runtime.reply_to_wake(self.store, "api", "token", 1, plan=plan)["ok"]
            )
        self.assertEqual(request.call_args.args[3], "/v2/users/user1/messages")

    def test_multi_part_failure_does_not_retry_successful_parts(self):
        self.wake()
        plan = media.normalize_reply(
            {"messages": [{"text": "first"}, {"text": "second"}]}
        )
        with patch.object(
            runtime,
            "send_group_message",
            side_effect=[
                {"_http_status": 200, "id": "first"},
                {"_http_status": 400, "code": 123},
            ],
        ) as send:
            result = runtime.reply_to_wake(self.store, "api", "token", 1, plan=plan)
            runtime.reply_to_wake(self.store, "api", "token", 1, plan=plan)
        self.assertEqual(result["status"], "partial")
        self.assertEqual(send.call_count, 2)
        self.assertEqual(
            [p["status"] for p in self.store.get_wake(1)["parts"]], ["done", "failed"]
        )

    def test_media_validation_and_atomic_budget_rejection(self):
        for source in (
            {"type": "image", "base64": "@@"},
            {"type": "image", "url": "file:///a"},
            {"type": "image", "url": "https://a", "base64": "YQ=="},
        ):
            with self.assertRaises(ValueError):
                media.normalize_reply({"media": [source]})
        self.wake()
        self.store.reserve_ack(self.event["id"])
        plan = media.normalize_reply({"messages": [{"text": str(i)} for i in range(5)]})
        result = runtime.reply_to_wake(self.store, "api", "token", 1, plan=plan)
        self.assertFalse(result["ok"])
        self.assertEqual(self.store.get_wake(1)["status"], "pending")
        self.assertEqual(self.store.get_parts(1), [])

    def test_gateway_session_and_other_events_stored(self):
        cfg = {
            "appid": "id",
            "appsecret": "secret",
            "env": "formal",
            "db_path": self.store.db_path,
            "context_window": 500,
            "retention_days": 7,
        }
        gw = server.GatewayClient(cfg)

        async def run():
            await gw.handle_dispatch(
                "READY", {"session_id": "session", "user": {"id": "bot"}}
            )
            await gw.handle_dispatch(
                "INTERACTION_CREATE", {"id": "button", "new": {"opaque": 1}}
            )

        asyncio.run(run())
        self.assertEqual(
            [r["payload"]["t"] for r in self.store.get_events()["events"]],
            ["READY", "INTERACTION_CREATE"],
        )
        self.store.checkpoint("session", 42)
        self.assertEqual(server.GatewayClient(cfg).seq, 42)

    def test_http_media_events_capabilities_and_disabled_extension(self):
        cfg = {
            "appid": "id",
            "appsecret": "secret",
            "env": "formal",
            "db_path": self.store.db_path,
            "context_window": 500,
            "retention_days": 7,
            "wake_batch_size": 20,
            "context_limit": 30,
        }
        gw = server.GatewayClient(cfg)
        self.wake()

        async def token():
            return "fake"

        gw.tokens.get_token = token
        loop = asyncio.new_event_loop()
        gw.loop = loop
        runner = threading.Thread(target=loop.run_forever)
        runner.start()
        server.WakeHandler.gateway = gw
        server.WakeHandler.api_bearer = b"a" * 64
        httpd = server.BoundedHTTPServer(("127.0.0.1", 0), server.WakeHandler, 2)
        thread = threading.Thread(target=httpd.serve_forever)
        thread.start()

        def request(method, path, body=None):
            connection = http.client.HTTPConnection(
                "127.0.0.1", httpd.server_port, timeout=3
            )
            connection.request(
                method,
                path,
                json.dumps(body) if body is not None else None,
                {"Authorization": "Bearer " + "a" * 64},
            )
            response = connection.getresponse()
            result = (response.status, json.loads(response.read()))
            connection.close()
            return result

        try:
            self.assertEqual(
                request("GET", "/capabilities")[1]["scopes"], ["group", "c2c"]
            )
            self.assertEqual(
                request("GET", "/events")[1]["events"][0]["payload"]["d"]["id"], "rich1"
            )
            self.assertEqual(
                request(
                    "POST",
                    "/api/request",
                    {"method": "POST", "path": "/v2/groups/g/messages", "body": {}},
                )[0],
                403,
            )
            self.assertEqual(
                request(
                    "POST",
                    "/wakes/reply",
                    {"wake_id": 1, "media": [{"type": "image", "base64": "bad"}]},
                )[0],
                400,
            )
            with (
                patch.object(runtime, "upload_media", return_value={"file_info": "f"}),
                patch.object(
                    runtime,
                    "request_json",
                    return_value={"_http_status": 200, "id": "sent"},
                ),
            ):
                result = request(
                    "POST",
                    "/wakes/reply",
                    {
                        "wake_id": 1,
                        "media": [{"type": "image", "url": "https://example.org/a"}],
                    },
                )
                self.assertTrue(result[1]["ok"])
            self.assertEqual(
                request("GET", "/wakes/1")[1]["wake"]["parts"][0]["status"], "done"
            )
            job = concurrent.futures.Future()
            gw.config["max_http_workers"] = 1
            gw.http_jobs.add(job)
            self.assertEqual(
                request(
                    "POST",
                    "/media/upload",
                    {
                        "wake_id": 1,
                        "media": {"type": "image", "url": "https://example.org/a"},
                    },
                )[0],
                503,
            )
            gw.http_jobs.discard(job)
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join()
            loop.call_soon_threadsafe(loop.stop)
            runner.join()
            loop.close()

    def test_cli_media_only_uses_shared_state(self):
        self.wake()
        cfg = {
            "appid": "id",
            "appsecret": "secret",
            "env": "formal",
            "db_path": self.store.db_path,
            "context_window": 500,
            "retention_days": 7,
            "reply_windows": {"group": 300, "c2c": 300},
            "max_media_bytes": 16 * 1024 * 1024,
        }

        async def token():
            return "fake"

        manager = Mock()
        manager.get_token = token
        with (
            patch.object(
                sys,
                "argv",
                [
                    "reply.py",
                    "--wake-id",
                    "1",
                    "--media",
                    '[{"type":"image","url":"https://example.org/a"}]',
                ],
            ),
            patch.object(reply, "load_config", return_value=cfg),
            patch.object(reply, "TokenManager", return_value=manager),
            patch.object(runtime, "upload_media", return_value={"file_info": "f"}),
            patch.object(
                runtime,
                "request_json",
                return_value={"_http_status": 200, "id": "sent"},
            ),
            patch.object(sys, "stdout", new_callable=io.StringIO) as output,
        ):
            with self.assertRaises(SystemExit) as exited:
                reply.main()
            self.assertEqual(exited.exception.code, 0)
            self.assertTrue(json.loads(output.getvalue())["ok"])
        self.assertEqual(self.store.get_wake(1)["status"], "done")

    def test_upload_auth_failure_invalidates_token_without_message_send(self):
        self.wake()
        cfg = {
            "appid": "id",
            "appsecret": "secret",
            "env": "formal",
            "db_path": self.store.db_path,
            "context_window": 500,
            "retention_days": 7,
        }
        gw = server.GatewayClient(cfg)

        async def token():
            return "stale"

        gw.tokens.get_token = token
        plan = media.normalize_reply(
            {"media": [{"type": "image", "url": "https://example.org/a"}]}
        )
        error = media.UploadError("unauthorized", {"_http_status": 401, "code": 1})
        with (
            patch.object(runtime, "upload_media", side_effect=error),
            patch.object(gw.tokens, "invalidate") as invalidate,
            patch.object(runtime, "request_json") as send,
        ):
            result = asyncio.run(gw.reply_to_wake_text(1, plan=plan))
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["parts"][0]["api_result"]["_http_status"], 401)
        invalidate.assert_called_once()
        send.assert_not_called()

    def test_single_server_os_lock(self):
        with runtime.exclusive_instance(self.store.db_path):
            with self.assertRaises(RuntimeError):
                with runtime.exclusive_instance(self.store.db_path):
                    pass
        with runtime.exclusive_instance(self.store.db_path):
            pass


if __name__ == "__main__":
    unittest.main()
