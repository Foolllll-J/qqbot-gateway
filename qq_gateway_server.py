#!/usr/bin/env python3
"""QQ Gateway on the server; VM polls authenticated wake endpoints."""

import argparse
import asyncio
import hmac
import hashlib
import json
import logging
import re
import threading
import signal
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, parse_qs

import aiohttp
from qq_media import normalize_reply, upload_media, request_json, UploadError
from qq_actions import Actions, PATHS as ACTION_PATHS, identifier
from qq_runtime import (
    exclusive_instance,
    API_BASE,
    EventStore,
    TokenManager,
    load_config,
    reply_to_wake,
    send_group_message,
    send_status,
)

log = logging.getLogger("qq-gateway")


class FatalGatewayError(RuntimeError):
    """A configuration/platform failure that must not loop forever."""


class GatewayClient:
    def __init__(self, config):
        self.config = config
        self.api_base = API_BASE[config["env"]]
        self.tokens = TokenManager(config["appid"], config["appsecret"])
        self.store = EventStore(
            config["db_path"],
            config["context_window"],
            config["retention_days"],
            config.get("reply_windows"),
        )
        self.loop = None
        self.session_id, self.seq = self.store.load_checkpoint()
        self.bot_id = None
        self.ready = False
        self.tasks = set()
        self.message_locks = {}
        self.http_jobs = set()
        self.http_jobs_lock = threading.Lock()
        self.actions = Actions(self.store, config, self.api_base)

    def is_at(self, event, d):
        if event in ("GROUP_AT_MESSAGE_CREATE", "AT_MESSAGE_CREATE"):
            return True
        if event == "GROUP_MESSAGE_CREATE" and "mentions" in d:
            mentions = d["mentions"]
            return isinstance(mentions, list) and any(
                isinstance(item, dict) and item.get("is_you") is True
                for item in mentions
            )
        known = set(
            self.config.get("bot_mention_ids", {}).get(
                d.get("group_openid") or d.get("channel_id"), []
            )
        )
        ids = set(re.findall(r"<@!?([A-Za-z0-9_-]+)>", d.get("content") or ""))
        for item in d.get("mentions") or []:
            if isinstance(item, dict):
                ids.update(str(item[k]) for k in ("id", "member_openid") if item.get(k))
        return bool(ids & known)

    def _spawn(self, coro):
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def handle_dispatch(self, event, d, frame=None):
        if event == "READY":
            self.session_id = d["session_id"]
            self.bot_id = str((d.get("user") or {}).get("id") or "")
            self.ready = True
            log.info("QQ Gateway READY bot_id=%s", self.bot_id)
        elif event == "RESUMED":
            self.ready = True
            log.info("QQ Gateway RESUMED")
        elif event in (
            "GROUP_AT_MESSAGE_CREATE",
            "GROUP_MESSAGE_CREATE",
            "C2C_MESSAGE_CREATE",
            "AT_MESSAGE_CREATE",
            "MESSAGE_CREATE",
            "DIRECT_MESSAGE_CREATE",
        ):
            mentions = [
                {k: m[k] for k in ("id", "member_openid") if m.get(k)}
                for m in (d.get("mentions") or [])
                if isinstance(m, dict)
            ]
            log.debug(
                "身份核对 event=%s group_openid=%s mentions=%s",
                event,
                d.get("group_openid"),
                json.dumps(mentions),
            )
            is_at = (
                event in ("C2C_MESSAGE_CREATE", "DIRECT_MESSAGE_CREATE")
                or self.is_at(event, d)
            ) and not (d.get("author") or {}).get("bot", False)
            new_message, new_wake = await asyncio.to_thread(
                self.store.save_event, d, is_at, event, frame
            )
            if new_message:
                log.info("群事件已保存 (at=%s)", is_at)
            if (
                new_wake
                and self.config.get("ack_enabled", True)
                and (
                    event != "C2C_MESSAGE_CREATE"
                    or self.config.get("c2c_ack_enabled", False)
                )
            ):
                self._spawn(self._send_instant_ack(d, event))
        else:
            await asyncio.to_thread(
                self.store.record_event, frame or {"t": event, "d": d}
            )
        if event in ("READY", "RESUMED"):
            await asyncio.to_thread(
                self.store.record_event, frame or {"t": event, "d": d}
            )
        if event == "INTERACTION_CREATE" and self.config.get(
            "interaction_auto_ack", True
        ):
            interaction_id = d.get("id") or (frame or {}).get("id")
            if interaction_id:
                self._spawn(self._auto_ack_interaction(interaction_id))
        if event in (
            "GROUP_ADD_ROBOT",
            "GROUP_DEL_ROBOT",
            "GROUP_MSG_RECEIVE",
            "GROUP_MSG_REJECT",
        ):
            log.info("QQ event %s", event)

    def _message_lock(self, msg_id):
        # Ref counts avoid retaining one lock per historical message.
        item = self.message_locks.setdefault(msg_id, [asyncio.Lock(), 0])
        item[1] += 1
        return item

    def _release_message_lock(self, msg_id, item):
        item[1] -= 1
        if item[1] == 0:
            self.message_locks.pop(msg_id, None)

    async def _send_instant_ack(self, d, event="GROUP_MESSAGE_CREATE"):
        msg_id = d["id"]
        item = self._message_lock(msg_id)
        try:
            async with item[0]:
                token = await self.tokens.get_token()
                seq = await asyncio.to_thread(self.store.reserve_ack, msg_id)
                if seq is None:
                    return
                scope = {
                    "C2C_MESSAGE_CREATE": "c2c",
                    "DIRECT_MESSAGE_CREATE": "dm",
                    "AT_MESSAGE_CREATE": "channel",
                    "MESSAGE_CREATE": "channel",
                }.get(event, "group")
                if scope == "group":
                    result = await asyncio.to_thread(
                        send_group_message,
                        self.api_base,
                        token,
                        d["group_openid"],
                        self.config.get(
                            "ack_text", "Received; your reply is being prepared."
                        ),
                        msg_id,
                        seq,
                    )
                elif scope == "c2c":
                    from qq_media import api_path

                    result = await asyncio.to_thread(
                        request_json,
                        self.api_base,
                        token,
                        "POST",
                        api_path("c2c", d["author"]["user_openid"]),
                        {
                            "msg_type": 0,
                            "content": self.config.get("ack_text", "Received."),
                            "msg_id": msg_id,
                            "msg_seq": seq,
                        },
                        20,
                    )
                else:
                    from qq_media import send_guild_message

                    result = await asyncio.to_thread(
                        send_guild_message,
                        self.api_base,
                        token,
                        scope,
                        d["guild_id"] if scope == "dm" else d["channel_id"],
                        {
                            "content": self.config.get("ack_text", "Received."),
                            "msg_id": msg_id,
                        },
                    )
                if result.get("_http_status") == 401:
                    self.tokens.invalidate()
                log.info("秒回 ack result=%s seq=%s", send_status(result), seq)
        except Exception as exc:
            log.warning("秒回 ack 失败或结果未知: %s", type(exc).__name__)
        finally:
            self._release_message_lock(msg_id, item)

    async def reply_to_wake_text(self, wake_id, text="", plan=None):
        wake = await asyncio.to_thread(self.store.get_wake, wake_id)
        if not wake:
            return {"ok": False, "status": "missing"}
        item = self._message_lock(wake["msg_id"])
        try:
            async with item[0]:
                # A token failure before claiming leaves the task eligible.
                token = await self.tokens.get_token()

                async def renew():
                    while True:
                        await asyncio.sleep(20)
                        await asyncio.to_thread(self.store.touch_claim, wake_id)

                lease = asyncio.create_task(renew())
                try:
                    result = await asyncio.to_thread(
                        reply_to_wake,
                        self.store,
                        self.api_base,
                        token,
                        wake_id,
                        text,
                        plan,
                        self.config.get("max_media_bytes", 16 * 1024 * 1024),
                    )
                finally:
                    lease.cancel()
                    await asyncio.gather(lease, return_exceptions=True)
                if result.get("api_result", {}).get("_http_status") == 401:
                    self.tokens.invalidate()
                return result
        finally:
            self._release_message_lock(wake["msg_id"], item)

    async def run_action(self, path, data):
        token = await self.tokens.get_token()

        async def renew():
            while True:
                await asyncio.sleep(20)
                await asyncio.to_thread(self.actions.renew, data["request_id"])

        lease = asyncio.create_task(renew())
        try:
            result = await asyncio.to_thread(self.actions.execute, path, data, token)
            if result.get("api_result", {}).get("_http_status") == 401:
                self.tokens.invalidate()
            return result
        finally:
            lease.cancel()
            await asyncio.gather(lease, return_exceptions=True)

    async def _auto_ack_interaction(self, interaction_id):
        try:
            result = await self.run_action(
                "/interactions/ack",
                {
                    "request_id": "auto-ack-"
                    + hashlib.sha256(interaction_id.encode()).hexdigest(),
                    "interaction_id": interaction_id,
                    "code": 0,
                },
            )
            log.info("Interaction receipt ACK status=%s", result["status"])
        except Exception:
            log.exception("Interaction receipt ACK failed")

    async def _run_once(self):
        token = await self.tokens.get_token()
        self.ready = False
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                self.api_base + "/gateway",
                headers={"Authorization": f"QQBot {token}"},
                timeout=aiohttp.ClientTimeout(total=20),
            ) as response:
                data = await response.json(content_type=None)
                if response.status == 401:
                    self.tokens.invalidate()
                if (
                    response.status != 200
                    or not isinstance(data, dict)
                    or data.get("code")
                    or not data.get("url")
                ):
                    raise RuntimeError("获取 Gateway 失败")
                gateway_url = data["url"]
                if urlsplit(gateway_url).scheme != "wss":
                    raise RuntimeError("Gateway 必须使用 wss")
            async with session.ws_connect(
                gateway_url, max_msg_size=10 * 1024 * 1024
            ) as ws:
                hello = await ws.receive_json(timeout=20)
                if hello.get("op") != 10:
                    raise RuntimeError("首帧不是 HELLO")
                interval = float(hello["d"]["heartbeat_interval"]) / 1000
                if interval <= 0:
                    raise RuntimeError("心跳周期无效")
                resume = bool(self.session_id)
                if resume:
                    await ws.send_json(
                        {
                            "op": 6,
                            "d": {
                                "token": f"QQBot {token}",
                                "session_id": self.session_id,
                                "seq": self.seq,
                            },
                        }
                    )
                else:
                    self.seq = None
                    await ws.send_json(
                        {
                            "op": 2,
                            "d": {
                                "token": f"QQBot {token}",
                                "intents": self.config.get("intents", 1 << 25),
                                "shard": [0, 1],
                            },
                        }
                    )
                login_started = time.monotonic()
                awaiting_ack = False
                heartbeat_error = None

                async def heartbeat():
                    nonlocal awaiting_ack, heartbeat_error
                    try:
                        while not ws.closed:
                            await asyncio.sleep(interval)
                            if awaiting_ack or (
                                not self.ready and time.monotonic() - login_started > 60
                            ):
                                raise TimeoutError("Gateway 心跳/登录 ACK 超时")
                            await ws.send_json({"op": 1, "d": self.seq})
                            awaiting_ack = True
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        heartbeat_error = exc
                        await ws.close()

                hb = asyncio.create_task(heartbeat())
                try:
                    async for message in ws:
                        if message.type == aiohttp.WSMsgType.TEXT:
                            pkt = json.loads(message.data)
                            op = pkt.get("op")
                            if op == 0:
                                await self.handle_dispatch(
                                    pkt.get("t"), pkt.get("d") or {}, pkt
                                )
                                self.seq = pkt.get("s", self.seq)
                                await asyncio.to_thread(
                                    self.store.checkpoint, self.session_id, self.seq
                                )
                            elif op == 11:
                                awaiting_ack = False
                            elif op == 1:
                                await ws.send_json({"op": 1, "d": self.seq})
                            elif op == 7:
                                break
                            elif op == 9:
                                # Only resume when QQ explicitly says the session is resumable.
                                if pkt.get("d") is not True:
                                    self.session_id = None
                                    self.seq = None
                                break
                        elif message.type == aiohttp.WSMsgType.ERROR:
                            raise ws.exception() or RuntimeError("WebSocket error")
                    if ws.close_code == 4004:
                        self.tokens.invalidate()
                    if ws.close_code in (4006, 4007, 9001, 9005):
                        self.session_id = None
                        self.seq = None
                    if ws.close_code == 4008:
                        await asyncio.sleep(60)
                    if ws.close_code in (
                        4001,
                        4002,
                        4010,
                        4011,
                        4012,
                        4013,
                        4014,
                        4914,
                        4915,
                    ):
                        raise FatalGatewayError(
                            f"Fatal QQ Gateway close code: {ws.close_code}"
                        )
                    await asyncio.to_thread(
                        self.store.checkpoint, self.session_id, self.seq
                    )
                    if heartbeat_error:
                        raise heartbeat_error
                finally:
                    self.ready = False
                    hb.cancel()
                    await asyncio.gather(hb, return_exceptions=True)

    async def _maintenance(self):
        while True:
            try:
                await asyncio.to_thread(self.store.cleanup)
                await asyncio.to_thread(self.actions.cleanup)
            except Exception:
                log.exception("Storage maintenance failed")
            await asyncio.sleep(60)

    async def run_forever(self):
        self.loop = asyncio.get_running_loop()
        maintenance = asyncio.create_task(self._maintenance())
        backoff = 1
        try:
            while True:
                started = time.monotonic()
                try:
                    await self._run_once()
                except (asyncio.CancelledError, FatalGatewayError):
                    raise
                except Exception as exc:
                    log.warning("Gateway 断线: %s", type(exc).__name__)
                self.ready = False
                if time.monotonic() - started > 60:
                    backoff = 1
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
        finally:
            maintenance.cancel()
            await asyncio.gather(maintenance, return_exceptions=True)
            # Give ongoing sends time to persist their outcome before shutdown.
            if self.tasks:
                await asyncio.gather(*list(self.tasks), return_exceptions=True)


class WakeHandler(BaseHTTPRequestHandler):
    gateway = None
    api_bearer = b""

    def setup(self):
        super().setup()
        self.connection.settimeout(10)

    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _authenticated(self):
        supplied = self.headers.get("Authorization", "").encode("utf-8")
        if not hmac.compare_digest(supplied, b"Bearer " + self.api_bearer):
            self._send(401, {"ok": False, "reason": "unauthorized"})
            return False
        return True

    def do_GET(self):
        if not self._authenticated():
            return
        path = urlsplit(self.path).path.rstrip("/")
        gw = self.gateway
        try:
            if path == "/wakes":
                wakes = gw.store.get_pending_wakes(
                    gw.config["wake_batch_size"], gw.config["context_limit"]
                )
                self._send(200, {"ok": True, "gateway_ready": gw.ready, "wakes": wakes})
            elif path == "/events":
                query = parse_qs(urlsplit(self.path).query)
                after = int(query.get("after", ["0"])[0])
                limit = int(query.get("limit", ["100"])[0])
                if after < 0 or not 1 <= limit <= 100:
                    raise ValueError("invalid event cursor")
                self._send(200, {"ok": True, **gw.store.get_events(after, limit)})
            elif path == "/targets":
                query = parse_qs(urlsplit(self.path).query)
                after = int(query.get("after", ["0"])[0])
                limit = int(query.get("limit", ["100"])[0])
                if after < 0 or not 1 <= limit <= 100:
                    raise ValueError("invalid target cursor")
                self._send(200, {"ok": True, **gw.store.get_targets(after, limit)})
            elif path.startswith("/operations/"):
                result = gw.actions.get(
                    identifier(path.rsplit("/", 1)[1], "request_id")
                )
                self._send(
                    200 if result else 404, {"ok": bool(result), "operation": result}
                )
            elif path.startswith("/streams/"):
                result = gw.actions.get_stream(
                    identifier(path.rsplit("/", 1)[1], "stream_id")
                )
                self._send(
                    200 if result else 404, {"ok": bool(result), "stream": result}
                )
            elif path == "/capabilities":
                self._send(
                    200,
                    {
                        "ok": True,
                        "schema_version": 2,
                        "scopes": ["group", "c2c", "channel", "dm"],
                        "implemented": [
                            "raw_events",
                            "native_payload",
                            "image",
                            "video",
                            "voice",
                            "file",
                            "chunked_upload",
                            "proactive_send",
                            "wakeup",
                            "event_reply",
                            "interaction_ack",
                            "typing",
                            "recall",
                            "dm_create",
                        ],
                        "qq_permissions_verified": False,
                        "streaming": True,
                        "stream_scopes": ["c2c"],
                        "stream_protocols": ["stream_messages", "legacy"],
                        "stream_protocol": gw.config.get(
                            "stream_protocol", "stream_messages"
                        ),
                        "channels": True,
                        "proactive": gw.config.get("proactive_enabled", True),
                        "interaction_auto_ack": gw.config.get(
                            "interaction_auto_ack", True
                        ),
                        "intents": gw.config.get("intents", 1 << 25),
                        "media_by_scope": {
                            "group": ["image", "video", "voice", "file"],
                            "c2c": ["image", "video", "voice", "file"],
                            "channel": ["image"],
                            "dm": ["image"],
                        },
                        "max_request_bytes": gw.config.get(
                            "max_request_bytes", 24 * 1024 * 1024
                        ),
                        "max_media_bytes": gw.config.get(
                            "max_media_bytes", 16 * 1024 * 1024
                        ),
                        "reply_windows": gw.store.reply_windows,
                        "gateway_ready": gw.ready,
                    },
                )
            elif re.fullmatch(r"/wakes/[1-9][0-9]*", path):
                wake = gw.store.get_wake(int(path.rsplit("/", 1)[1]))
                self._send(200 if wake else 404, {"ok": bool(wake), "wake": wake})
            else:
                self._send(404, {"ok": False, "reason": "not_found"})
        except ValueError:
            self._send(400, {"ok": False, "reason": "invalid_query"})
        except Exception:
            log.exception("Read failed")
            self._send(503, {"ok": False, "reason": "storage_unavailable"})

    def do_POST(self):
        if not self._authenticated():
            return
        path = urlsplit(self.path).path.rstrip("/")
        if (
            path not in ("/wakes/reply", "/media/upload", "/api/request")
            and path not in ACTION_PATHS
        ):
            self._send(404, {"ok": False, "reason": "not_found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if (
                not 0
                < length
                <= self.gateway.config.get("max_request_bytes", 24 * 1024 * 1024)
            ):
                raise ValueError()
            data = json.loads(self.rfile.read(length).decode("utf-8"))
            if not isinstance(data, dict):
                raise ValueError()
            if path in ACTION_PATHS:
                self.gateway.actions.validate(path, data)
                wake_id = data.get("wake_id")
            elif path == "/api/request":
                method, route = data.get("method"), data.get("path")
                if (
                    not isinstance(route, str)
                    or not route.startswith("/")
                    or any(x in route for x in ("..", "%", "?", "#", ":", "\\", "//"))
                ):
                    raise ValueError()
                allowed = any(
                    rule["method"] == method and re.fullmatch(rule["pattern"], route)
                    for rule in self.gateway.config.get("api_routes", [])
                )
                if not allowed:
                    self._send(403, {"ok": False, "reason": "route_not_enabled"})
                    return
                wake_id = None
            else:
                wake_id = data.get("wake_id")
                if type(wake_id) is not int or not 0 < wake_id < 2**63:
                    raise ValueError()
                if path == "/wakes/reply":
                    wake = self.gateway.store.get_wake(wake_id)
                    plan = normalize_reply(
                        data,
                        self.gateway.config.get("max_media_bytes", 16 * 1024 * 1024),
                        wake["scope"] if wake else "group",
                    )
                else:
                    from qq_media import validate_media

                    validate_media(
                        data.get("media"),
                        self.gateway.config.get("max_media_bytes", 16 * 1024 * 1024),
                    )
        except (ValueError, UnicodeError, TimeoutError):
            self._send(400, {"ok": False, "reason": "invalid_request"})
            return
        gw = self.gateway
        if not gw.loop or not gw.loop.is_running():
            self._send(503, {"ok": False, "reason": "gateway_loop_unavailable"})
            return

        async def execute():
            if path in ACTION_PATHS:
                return await gw.run_action(path, data)
            if path == "/wakes/reply":
                return await gw.reply_to_wake_text(wake_id, plan=plan)
            token = await gw.tokens.get_token()
            if path == "/media/upload":
                wake = await asyncio.to_thread(gw.store.get_wake, wake_id)
                if not wake:
                    return {"ok": False, "status": "missing"}
                try:
                    result = await asyncio.to_thread(
                        upload_media,
                        gw.api_base,
                        token,
                        wake["scope"],
                        wake["target_id"],
                        data["media"],
                        gw.config.get("max_media_bytes", 16 * 1024 * 1024),
                    )
                except UploadError as exc:
                    result = exc.result or {"error": type(exc).__name__}
                    if result.get("_http_status") == 401:
                        gw.tokens.invalidate()
                    return {"ok": False, "status": "failed", "api_result": result}
                return {"ok": True, "upload": result}
            result = await asyncio.to_thread(
                request_json, gw.api_base, token, method, route, data.get("body")
            )
            if result.get("_http_status") == 401:
                gw.tokens.invalidate()
            return {
                "ok": 200 <= result.get("_http_status", 0) < 300
                and not result.get("code")
                and not result.get("err_code")
                and not result.get("error"),
                "api_result": result,
            }

        # Keep timed-out background jobs within the same bounded capacity.
        with gw.http_jobs_lock:
            if len(gw.http_jobs) >= gw.config.get("max_http_workers", 8):
                self._send(503, {"ok": False, "reason": "operation_capacity_exhausted"})
                return
            future = asyncio.run_coroutine_threadsafe(execute(), gw.loop)
            gw.http_jobs.add(future)

        def completed(job):
            with gw.http_jobs_lock:
                gw.http_jobs.discard(job)

        future.add_done_callback(completed)
        try:
            result = future.result(timeout=30)
        except TimeoutError:
            if path in ACTION_PATHS:
                self._send(
                    202,
                    {
                        "ok": False,
                        "status": "processing",
                        "request_id": data["request_id"],
                    },
                )
                return
            # Upload/API operations have no wake claim; an unknown result must never trigger a blind retry.
            if path != "/wakes/reply":
                self._send(
                    202,
                    {
                        "ok": False,
                        "status": "unknown",
                        "reason": "operation_may_continue",
                    },
                )
                return
            # Work continues; caller must query status, not assume send failure.
            self._send(202, {"ok": False, "status": "processing", "wake_id": wake_id})
            return
        except Exception:
            log.exception("回复处理失败")
            self._send(
                503, {"ok": False, "reason": "reply_unavailable", "wake_id": wake_id}
            )
            return
        self._send(200, result)

    def log_message(self, *args):
        pass


class BoundedHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, handler, workers):
        self.slots = threading.BoundedSemaphore(workers)
        super().__init__(address, handler)

    def process_request(self, request, address):
        if not self.slots.acquire(blocking=False):
            try:
                request.sendall(
                    b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n"
                )
            finally:
                self.shutdown_request(request)
            return
        try:
            super().process_request(request, address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request, address)
        finally:
            self.slots.release()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config", default=str(Path(__file__).with_name("config.json"))
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    config = load_config(args.config)
    log.setLevel(config.get("log_level", "INFO"))
    with exclusive_instance(config["db_path"]):
        client = GatewayClient(config)
        WakeHandler.gateway = client
        WakeHandler.api_bearer = config["api_bearer"].encode("ascii")
        server = BoundedHTTPServer(
            ("127.0.0.1", config["http_port"]), WakeHandler, config["max_http_workers"]
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        async def run():
            task = asyncio.create_task(client.run_forever())
            previous = signal.getsignal(signal.SIGTERM)
            signal.signal(signal.SIGTERM, lambda *_: task.cancel())
            try:
                await task
            except asyncio.CancelledError:
                pass
            finally:
                signal.signal(signal.SIGTERM, previous)

        try:
            asyncio.run(run())
        except FatalGatewayError:
            log.exception("Gateway configuration requires administrator attention")
            raise SystemExit(78)
        except KeyboardInterrupt:
            log.info("Service stopped")
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    main()
