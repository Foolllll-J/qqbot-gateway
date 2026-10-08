import asyncio
import concurrent.futures
from datetime import datetime, timezone, timedelta
import http.client
import json
import pathlib
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import qq_runtime as runtime
import qq_gateway_server as server


class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(pathlib.Path(self.tmp.name) / "events.db")
        self.store = runtime.EventStore(self.db)
        self.d = {
            "id": "m1",
            "group_openid": "g",
            "content": "hello",
            "author": {"member_openid": "u"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.cfg = {
            "appid": "id",
            "appsecret": "secret",
            "env": "formal",
            "db_path": self.db,
            "context_window": 500,
            "retention_days": 7,
            "context_limit": 30,
            "wake_batch_size": 20,
        }

    def tearDown(self):
        self.tmp.cleanup()

    def test_duplicate_and_atomic_event(self):
        self.assertEqual(self.store.save_event(self.d, True), (True, True))
        self.assertEqual(self.store.save_event(self.d, True), (False, False))
        self.assertEqual(len(self.store.get_pending_wakes()), 1)

    def test_upgrade_event_and_context_anchor(self):
        self.store.save_event(self.d, False)
        self.assertEqual(self.store.save_event(self.d, True), (False, True))
        later = {**self.d, "id": "m2", "content": "later"}
        self.store.save_event(later, False)
        ctx = self.store.get_pending_wakes()[0]["context"]
        self.assertEqual([r["content"] for r in ctx], ["hello"])

    def test_concurrent_replies_one_send_and_replay(self):
        self.store.save_event(self.d, True)
        calls = []

        def send(*args):
            calls.append(args)
            time.sleep(0.03)
            return {"_http_status": 200, "id": "sent1"}

        with patch.object(runtime, "send_group_message", send):
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                list(
                    pool.map(
                        lambda _: runtime.reply_to_wake(
                            self.store, "api", "token", 1, "text"
                        ),
                        range(8),
                    )
                )
            replay = runtime.reply_to_wake(self.store, "api", "token", 1, "text")
            changed = runtime.reply_to_wake(self.store, "api", "token", 1, "changed")
        self.assertEqual(len(calls), 1)
        self.assertTrue(replay["ok"])
        self.assertTrue(replay["replayed"])
        self.assertFalse(changed["ok"])
        self.assertEqual(self.store.get_wake(1)["status"], "done")

    def test_sequences_atomic_and_capped(self):
        self.store.save_event(self.d, True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            results = list(pool.map(lambda _: self.store.reserve_ack("m1"), range(5)))
        self.assertEqual(sorted(results), [1, 2, 3, 4, 5])
        with self.assertRaises(ValueError):
            self.store.reserve_ack("m1")

    def test_timeout_unknown_no_retry(self):
        self.store.save_event(self.d, True)
        with patch.object(
            runtime, "send_group_message", side_effect=TimeoutError()
        ) as send:
            out = runtime.reply_to_wake(self.store, "api", "token", 1, "text")
            runtime.reply_to_wake(self.store, "api", "token", 1, "text")
        self.assertEqual(send.call_count, 1)
        self.assertEqual(out["status"], "unknown")
        self.assertEqual(self.store.get_pending_wakes(), [])

    def test_expired_and_bad_timestamp_no_send(self):
        for timestamp in (
            (datetime.now(timezone.utc) - timedelta(minutes=6)).isoformat(),
            "bad",
        ):
            self.store.save_event(
                {**self.d, "id": timestamp, "timestamp": timestamp}, True
            )
        self.assertEqual(self.store.get_pending_wakes(), [])
        with patch.object(runtime, "send_group_message") as send:
            self.assertEqual(
                runtime.reply_to_wake(self.store, "api", "token", 1, "text")["status"],
                "expired",
            )
            send.assert_not_called()

    def test_claim_lease_and_legacy_schema(self):
        legacy = str(pathlib.Path(self.tmp.name) / "legacy.db")
        with sqlite3.connect(legacy) as conn:
            conn.execute(
                "CREATE TABLE pending_wakes(id INTEGER PRIMARY KEY,msg_id TEXT UNIQUE,group_openid TEXT,member_openid TEXT,author_name TEXT,content TEXT,timestamp TEXT,status TEXT,created_at TEXT)"
            )
            conn.execute(
                "INSERT INTO pending_wakes VALUES(1,'old','g',NULL,NULL,'hi',?,'claimed',datetime('now'))",
                (self.d["timestamp"],),
            )
        conn.close()
        old = runtime.EventStore(legacy)
        self.assertEqual(old.get_wake(1)["status"], "unknown")
        self.store.save_event(self.d, True)
        self.store.claim_reply(1, "text")
        with self.store.connection() as conn:
            conn.execute("UPDATE pending_wakes SET claimed_until=0")
            conn.commit()
        self.assertEqual(self.store.get_wake(1)["status"], "unknown")

    def test_group_scoped_mentions_no_learning(self):
        cfg = {**self.cfg, "bot_mention_ids": {"g": ["BOT"]}}
        gw = server.GatewayClient(cfg)
        self.assertTrue(
            gw.is_at("GROUP_AT_MESSAGE_CREATE", {**self.d, "content": "<@OTHER>"})
        )
        self.assertFalse(
            gw.is_at("GROUP_MESSAGE_CREATE", {**self.d, "content": "<@OTHER>"})
        )
        self.assertTrue(
            gw.is_at(
                "GROUP_MESSAGE_CREATE",
                {**self.d, "content": "<@BOT>"},
            )
        )
        self.assertFalse(
            gw.is_at(
                "GROUP_MESSAGE_CREATE",
                {**self.d, "group_openid": "g2", "content": "<@BOT>"},
            )
        )

    def test_official_group_mentions_override_config(self):
        cfg = {**self.cfg, "bot_mention_ids": {"g": ["OTHER"]}}
        gw = server.GatewayClient(cfg)
        cases = [
            ([{"id": "OTHER", "is_you": False, "bot": False}], False),
            ([{"id": "BOT", "is_you": True}], True),
            ([{"is_you": False}, {"is_you": True}], True),
            ([], False),
            (None, False),
            ([{"id": "OTHER"}], False),
            ([{"is_you": "true"}], False),
            ([{"is_you": 1}], False),
            ([None, "invalid", {"is_you": False}], False),
            ({"is_you": True}, False),
        ]
        for mentions, expected in cases:
            with self.subTest(mentions=mentions):
                self.assertEqual(
                    gw.is_at(
                        "GROUP_MESSAGE_CREATE",
                        {**self.d, "content": "<@OTHER>", "mentions": mentions},
                    ),
                    expected,
                )
        self.assertEqual(cfg["bot_mention_ids"], {"g": ["OTHER"]})

    def test_dedicated_at_and_channel_mentions_unchanged(self):
        gw = server.GatewayClient({**self.cfg, "bot_mention_ids": {"ch": ["BOT"]}})
        for event in ("GROUP_AT_MESSAGE_CREATE", "AT_MESSAGE_CREATE"):
            self.assertTrue(gw.is_at(event, {**self.d, "mentions": []}))
        self.assertTrue(
            gw.is_at(
                "MESSAGE_CREATE", {"channel_id": "ch", "mentions": [{"id": "BOT"}]}
            )
        )

    def test_dispatch_only_self_mention_creates_wake_and_can_reply(self):
        cfg = {**self.cfg, "bot_mention_ids": {"g": ["OTHER"]}, "ack_enabled": False}
        gw = server.GatewayClient(cfg)
        mentions = [{"id": "BOT", "is_you": True, "extra": {"future": [1, 2]}}]
        other = {
            **self.d,
            "content": "<@OTHER>",
            "mentions": [{"id": "OTHER", "is_you": False}],
        }
        own = {**self.d, "id": "self", "content": "<@BOT>", "mentions": mentions}
        for data in (other, own):
            asyncio.run(
                gw.handle_dispatch(
                    "GROUP_MESSAGE_CREATE",
                    data,
                    {"t": "GROUP_MESSAGE_CREATE", "d": data},
                )
            )
        wakes = gw.store.get_pending_wakes()
        self.assertEqual(len(wakes), 1)
        wake = wakes[0]
        self.assertEqual(wake["msg_id"], "self")
        self.assertEqual(wake["mentions"], mentions)
        self.assertEqual(wake["raw_event"]["d"]["mentions"], mentions)
        detail = gw.store.get_wake(wake["id"])
        self.assertEqual(detail["mentions"], mentions)
        self.assertEqual(detail["created_at"], wake["created_at"])
        expected = datetime.fromisoformat(own["timestamp"]).timestamp() + 300
        self.assertEqual(wake["deadline"], expected)
        with patch.object(
            runtime,
            "send_group_message",
            return_value={"_http_status": 200, "id": "sent"},
        ) as send:
            result = runtime.reply_to_wake(
                gw.store, "api", "token", wake["id"], "reply"
            )
        self.assertTrue(result["ok"])
        send.assert_called_once()

    def test_missing_mentions_fallback_and_c2c_unchanged(self):
        cfg = {**self.cfg, "bot_mention_ids": {"g": ["BOT"]}, "ack_enabled": False}
        gw = server.GatewayClient(cfg)
        data = {**self.d, "content": "<@BOT>"}
        asyncio.run(gw.handle_dispatch("GROUP_MESSAGE_CREATE", data))
        private = {
            "id": "private",
            "author": {"user_openid": "u"},
            "content": "hello",
            "timestamp": self.d["timestamp"],
            "mentions": [],
        }
        asyncio.run(gw.handle_dispatch("C2C_MESSAGE_CREATE", private))
        wakes = gw.store.get_pending_wakes()
        self.assertEqual([w["scope"] for w in wakes], ["group", "c2c"])
        self.assertIsNone(wakes[0]["mentions"])
        self.assertEqual(wakes[1]["mentions"], [])

    def test_token_failure_leaves_pending(self):
        gw = server.GatewayClient(self.cfg)
        gw.store.save_event(self.d, True)

        async def fail():
            raise RuntimeError("no token")

        gw.tokens.get_token = fail
        with self.assertRaises(RuntimeError):
            asyncio.run(gw.reply_to_wake_text(1, "text"))
        self.assertEqual(gw.store.get_wake(1)["status"], "pending")

    def test_http_input_and_status(self):
        gw = server.GatewayClient(self.cfg)
        mentions = [{"id": "BOT", "is_you": True, "extra": {"future": [1, 2]}}]
        gw.store.save_event({**self.d, "mentions": mentions}, True)
        server.WakeHandler.gateway = gw
        server.WakeHandler.api_bearer = b"a" * 64
        httpd = server.ThreadingHTTPServer(("127.0.0.1", 0), server.WakeHandler)
        thread = threading.Thread(target=httpd.serve_forever)
        thread.start()
        import http.client as client_module

        def request(method, path, body=None, auth=True):
            conn = client_module.HTTPConnection(
                "127.0.0.1", httpd.server_port, timeout=3
            )
            conn.request(
                method,
                path,
                body,
                {"Authorization": "Bearer " + ("a" * 64 if auth else "bad")},
            )
            response = conn.getresponse()
            result = (response.status, json.loads(response.read()))
            conn.close()
            return result

        try:
            self.assertEqual(request("GET", "/wakes", auth=False)[0], 401)
            status, body = request("GET", "/wakes?unused=1")
            self.assertEqual(status, 200)
            self.assertEqual(body["wakes"][0]["mentions"], mentions)
            detail = request("GET", "/wakes/1")[1]["wake"]
            self.assertEqual(detail["status"], "pending")
            self.assertEqual(detail["mentions"], mentions)
            for body in (
                "[]",
                '{"wake_id":1,"text":42}',
                '{"wake_id":"oops","text":"hi"}',
                '{"wake_id":true,"text":"hi"}',
            ):
                self.assertEqual(request("POST", "/wakes/reply", body)[0], 400)
            self.assertEqual(
                request("POST", "/wakes/reply", '{"wake_id":1,"text":"hi"}')[0], 503
            )
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join()

    def test_http_send_replay_in_running_loop(self):
        gw = server.GatewayClient(self.cfg)
        gw.store.save_event(self.d, True)
        loop = asyncio.new_event_loop()
        gw.loop = loop
        loop_thread = threading.Thread(target=loop.run_forever)
        loop_thread.start()

        async def token():
            return "fake"

        gw.tokens.get_token = token
        server.WakeHandler.gateway = gw
        server.WakeHandler.api_bearer = b"a" * 64
        httpd = server.ThreadingHTTPServer(("127.0.0.1", 0), server.WakeHandler)
        thread = threading.Thread(target=httpd.serve_forever)
        thread.start()

        def post():
            conn = http.client.HTTPConnection("127.0.0.1", httpd.server_port, timeout=3)
            conn.request(
                "POST",
                "/wakes/reply",
                json.dumps({"wake_id": 1, "text": "text"}),
                {"Authorization": "Bearer " + "a" * 64},
            )
            response = conn.getresponse()
            data = json.loads(response.read())
            conn.close()
            return response.status, data

        try:
            with patch.object(
                runtime,
                "send_group_message",
                return_value={"_http_status": 200, "id": "sent1"},
            ) as send:
                first = post()
                second = post()
            self.assertTrue(first[1]["ok"])
            self.assertTrue(second[1]["replayed"])
            self.assertEqual(send.call_count, 1)
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join()
            loop.call_soon_threadsafe(loop.stop)
            loop_thread.join()
            loop.close()

    def test_gateway_identify_resume_invalid_session_and_auth_failure(self):
        gw = server.GatewayClient(self.cfg)

        async def token():
            return "fake"

        gw.tokens.get_token = token

        class Response:
            status = 200

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def json(self, **kwargs):
                return {"url": "wss://fake.example/websocket"}

        class WS:
            closed = False
            close_code = 1000

            def __init__(self, packets):
                self.packets = packets
                self.sent = []

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                self.closed = True

            async def receive_json(self, **kwargs):
                return {"op": 10, "d": {"heartbeat_interval": 100000}}

            async def send_json(self, data):
                self.sent.append(data)

            def __aiter__(self):
                return self

            async def __anext__(self):
                if not self.packets:
                    raise StopAsyncIteration
                packet = self.packets.pop(0)
                return type(
                    "Message",
                    (),
                    {"type": server.aiohttp.WSMsgType.TEXT, "data": json.dumps(packet)},
                )()

        class Session:
            def __init__(self, ws):
                self.ws = ws

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            def get(self, *args, **kwargs):
                return Response()

            def ws_connect(self, *args, **kwargs):
                return self.ws

        async def run(ws):
            with patch.object(
                server.aiohttp, "ClientSession", return_value=Session(ws)
            ):
                await gw._run_once()

        first = WS(
            [
                {
                    "op": 0,
                    "s": 1,
                    "t": "READY",
                    "d": {"session_id": "sess", "user": {"id": "BOT"}},
                },
                {"op": 7},
            ]
        )
        asyncio.run(run(first))
        self.assertEqual(first.sent[0]["op"], 2)
        self.assertEqual(gw.session_id, "sess")
        self.assertEqual(gw.seq, 1)
        second = WS([{"op": 9, "d": False}])
        asyncio.run(run(second))
        self.assertEqual(second.sent[0]["op"], 6)
        self.assertEqual(second.sent[0]["d"]["seq"], 1)
        self.assertIsNone(gw.session_id)
        self.assertIsNone(gw.seq)
        gw.tokens._token = "cached"
        third = WS([])
        third.close_code = 4004
        asyncio.run(run(third))
        self.assertIsNone(gw.tokens._token)
        gw.session_id = "sess"
        gw.seq = 1
        failing = WS([{"op": 0, "s": 2, "t": "GROUP_MESSAGE_CREATE", "d": self.d}])
        with patch.object(
            gw.store, "save_event", side_effect=sqlite3.OperationalError("locked")
        ):
            with self.assertRaises(sqlite3.OperationalError):
                asyncio.run(run(failing))
        self.assertEqual(gw.seq, 1)

        class StalledWS(WS):
            def __init__(self):
                super().__init__([])
                self.closed_event = asyncio.Event()

            async def receive_json(self, **kwargs):
                return {"op": 10, "d": {"heartbeat_interval": 10}}

            async def __anext__(self):
                await self.closed_event.wait()
                raise StopAsyncIteration

            async def close(self):
                self.closed = True
                self.closed_event.set()

        with self.assertRaises(TimeoutError):
            asyncio.run(run(StalledWS()))

    def test_config_path_and_validation(self):
        cfg = {**self.cfg, "api_bearer": "a" * 64, "db_path": "relative.db"}
        p = pathlib.Path(self.tmp.name) / "config.json"
        p.write_text(json.dumps(cfg))
        self.assertEqual(
            runtime.load_config(p)["db_path"], str(p.parent / "relative.db")
        )
        cfg["api_bearer"] = "中文占位"
        p.write_text(json.dumps(cfg))
        with self.assertRaises(ValueError):
            runtime.load_config(p)

    def test_response_classification(self):
        self.assertEqual(
            runtime.send_status({"_http_status": 200, "code": 123}), "failed"
        )
        self.assertEqual(runtime.send_status({"_http_status": 200}), "unknown")
        self.assertEqual(runtime.send_status({"_http_status": 502}), "unknown")
        self.assertEqual(runtime.send_status({"_http_status": 200, "id": "ok"}), "done")


if __name__ == "__main__":
    unittest.main(verbosity=2)
