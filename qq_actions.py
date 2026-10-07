"""Durable explicit QQ actions: proactive sends, streams and interaction ACKs."""

import hashlib
import json
import re
import time
import uuid
from urllib.parse import quote

from qq_media import (
    api_path,
    normalize_reply,
    request_json,
    send_guild_message,
    upload_media,
    UploadError,
)
from qq_runtime import send_status

PATHS = {
    "/messages/send",
    "/streams/start",
    "/streams/update",
    "/streams/complete",
    "/streams/cancel",
    "/interactions/ack",
    "/typing",
    "/dms/create",
    "/messages/recall",
    "/media/upload-target",
}
SCOPES = ("group", "c2c", "channel", "dm")


def identifier(value, label="id"):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value):
        raise ValueError(f"Invalid {label}")
    if value in (".", ".."):
        raise ValueError(f"Invalid {label}")
    return value


def target(data):
    scope = data.get("scope")
    if scope not in SCOPES:
        raise ValueError("scope must be group/c2c/channel/dm")
    return scope, identifier(data.get("target_id"), "target_id")


def api_success(result):
    return 200 <= result.get("_http_status", 0) < 300 and not any(
        result.get(k) for k in ("code", "err_code", "error")
    )


class Actions:
    def __init__(self, store, config, base):
        self.store, self.config, self.base = store, config, base
        with store.connection() as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS operations(request_id TEXT PRIMARY KEY,hash TEXT NOT NULL,
                status TEXT NOT NULL,lease REAL,result TEXT,created_at TEXT DEFAULT (datetime('now')));
            CREATE TABLE IF NOT EXISTS streams(stream_id TEXT PRIMARY KEY,wake_id INTEGER UNIQUE,
                protocol TEXT NOT NULL,state TEXT NOT NULL,seq INTEGER NOT NULL,next_index INTEGER DEFAULT 0,
                qq_id TEXT,content TEXT DEFAULT '',last_sent REAL DEFAULT 0,active_request TEXT);
            CREATE INDEX IF NOT EXISTS idx_operations_lease ON operations(status,lease);
            CREATE INDEX IF NOT EXISTS idx_streams_active ON streams(state,active_request);
            """)

    def validate(self, path, data):
        identifier(data.get("request_id"), "request_id")
        if path == "/messages/send":
            if any(
                k in data
                for k in (
                    "wake_id",
                    "msg_id",
                    "msg_seq",
                    "is_wakeup",
                    "stream",
                    "stream_messages",
                )
            ):
                raise ValueError(
                    "Use wake replies or explicit send mode; association fields cannot be mixed"
                )
            scope, _ = target(data)
            mode = data.get("mode", "proactive")
            if mode not in ("proactive", "wakeup", "event"):
                raise ValueError("Invalid send mode")
            if mode == "wakeup" and scope != "c2c":
                raise ValueError("wakeup is C2C only")
            if mode == "event":
                identifier(data.get("event_id"), "event_id")
            elif "event_id" in data:
                raise ValueError("event_id requires event mode")
            normalize_reply(
                data, self.config.get("max_media_bytes", 16 * 1024 * 1024), scope
            )
        elif path == "/media/upload-target":
            scope, _ = target(data)
            if scope not in ("group", "c2c"):
                raise ValueError(
                    "Guild images are sent directly, without upload-target"
                )
            from qq_media import validate_media

            validate_media(
                data.get("media"), self.config.get("max_media_bytes", 16 * 1024 * 1024)
            )
        elif path in ("/streams/start", "/typing"):
            if type(data.get("wake_id")) is not int or not 0 < data["wake_id"] < 2**63:
                raise ValueError("wake_id must be positive")
            if path == "/streams/start" and data.get(
                "protocol", self.config.get("stream_protocol", "stream_messages")
            ) not in ("stream_messages", "legacy"):
                raise ValueError("Unsupported stream protocol")
            if path == "/typing" and (
                type(data.get("seconds", 30)) is not int
                or not 1 <= data.get("seconds", 30) <= 60
            ):
                raise ValueError("seconds must be 1..60")
        elif path.startswith("/streams/"):
            identifier(data.get("stream_id"), "stream_id")
            if path != "/streams/cancel":
                if type(data.get("index")) is not int or data["index"] < 0:
                    raise ValueError("Stream frame needs a nonnegative index")
                if not isinstance(data.get("text", ""), str) or (
                    path.endswith("update") and not data.get("text")
                ):
                    raise ValueError("Stream text must be nonempty for update")
                if "reset" in data and type(data["reset"]) is not bool:
                    raise ValueError("reset must be boolean")
        elif path == "/interactions/ack":
            identifier(data.get("interaction_id"), "interaction_id")
            if (
                type(data.get("code", 0)) is not int
                or not 0 <= data.get("code", 0) <= 5
            ):
                raise ValueError("ACK code must be 0..5")
            if "data" in data and not isinstance(data["data"], dict):
                raise ValueError("ACK data must be an object")
        elif path == "/dms/create":
            identifier(data.get("recipient_id"), "recipient_id")
            identifier(data.get("source_guild_id"), "source_guild_id")
        elif path == "/messages/recall":
            target(data)
            identifier(data.get("message_id"), "message_id")
        else:
            raise ValueError("Unsupported action")

    def _expire(self, conn):
        conn.execute(
            "UPDATE operations SET status='unknown' WHERE status='running' AND lease<?",
            (time.time(),),
        )
        conn.execute(
            "UPDATE streams SET state='unknown' WHERE state='sending' AND active_request IN (SELECT request_id FROM operations WHERE status='unknown')"
        )
        conn.execute(
            "UPDATE pending_wakes SET status='unknown',claimed_until=NULL WHERE status='claimed' AND id IN (SELECT wake_id FROM streams WHERE state='unknown')"
        )

    def get(self, request_id):
        with self.store.connection() as conn:
            self._expire(conn)
            row = conn.execute(
                "SELECT * FROM operations WHERE request_id=?", (request_id,)
            ).fetchone()
            conn.commit()
        if not row:
            return None
        result = json.loads(row["result"]) if row["result"] else {}
        return {
            **result,
            "request_id": request_id,
            "status": row["status"],
            "ok": row["status"] == "done",
        }

    def renew(self, request_id):
        with self.store.connection() as conn:
            conn.execute(
                "UPDATE operations SET lease=? WHERE request_id=? AND status='running'",
                (time.time() + 90, request_id),
            )
            conn.commit()

    def cleanup(self):
        with self.store.connection() as conn:
            self._expire(conn)
            conn.execute(
                "DELETE FROM operations WHERE status!='running' AND created_at<datetime('now',?)",
                (f"-{self.store.retention_days} days",),
            )
            conn.execute(
                "DELETE FROM streams WHERE wake_id NOT IN (SELECT id FROM pending_wakes)"
            )
            conn.commit()

    def _claim(self, path, data):
        digest = hashlib.sha256(
            json.dumps(
                [path, data], sort_keys=True, ensure_ascii=False, separators=(",", ":")
            ).encode()
        ).hexdigest()
        key = data["request_id"]
        with self.store.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._expire(conn)
            row = conn.execute(
                "SELECT hash FROM operations WHERE request_id=?", (key,)
            ).fetchone()
            if row:
                conn.commit()
                if row["hash"] != digest:
                    return {"ok": False, "status": "conflict", "request_id": key}
                return {**self.get(key), "replayed": True}
            conn.execute(
                "INSERT INTO operations(request_id,hash,status,lease) VALUES(?,?,'running',?)",
                (key, digest, time.time() + 90),
            )
            conn.commit()
        return None

    def _finish(self, key, result):
        with self.store.connection() as conn:
            conn.execute(
                "UPDATE operations SET status=?,result=?,lease=NULL WHERE request_id=?",
                (result["status"], json.dumps(result, ensure_ascii=False), key),
            )
            conn.commit()
        return {**result, "request_id": key}

    def execute(self, path, data, token):
        self.validate(path, data)
        cached = self._claim(path, data)
        if cached is not None:
            return cached
        try:
            if path == "/messages/send":
                result = self._send(data, token)
            elif path == "/media/upload-target":
                scope, tid = target(data)
                result = {
                    "ok": True,
                    "status": "done",
                    "upload": upload_media(
                        self.base,
                        token,
                        scope,
                        tid,
                        data["media"],
                        self.config.get("max_media_bytes", 16 * 1024 * 1024),
                    ),
                }
            elif path == "/streams/start":
                result = self._start(data)
            elif path.startswith("/streams/"):
                result = self._stream(path, data, token)
            elif path == "/typing":
                wake = self.store.get_wake(data["wake_id"])
                if not wake or wake["scope"] != "c2c" or wake["status"] != "pending":
                    result = {
                        "ok": False,
                        "status": "failed",
                        "reason": "pending_c2c_wake_required",
                    }
                else:
                    seq = self.store.reserve_ack(wake["msg_id"])
                    if seq is None:
                        result = {
                            "ok": False,
                            "status": "failed",
                            "reason": "wake_unavailable",
                        }
                    else:
                        reply = request_json(
                            self.base,
                            token,
                            "POST",
                            api_path("c2c", wake["target_id"]),
                            {
                                "msg_id": wake["msg_id"],
                                "msg_seq": seq,
                                "msg_type": 6,
                                "input_notify": {
                                    "input_type": 1,
                                    "input_second": data.get("seconds", 30),
                                },
                            },
                        )
                        result = self._api_result(reply)
            else:
                if path == "/interactions/ack":
                    route = "/interactions/" + quote(data["interaction_id"], safe="")
                    body = {"code": data.get("code", 0)}
                    if "data" in data:
                        body["data"] = data["data"]
                    reply = request_json(self.base, token, "PUT", route, body)
                elif path == "/dms/create":
                    reply = request_json(
                        self.base,
                        token,
                        "POST",
                        "/users/@me/dms",
                        {k: data[k] for k in ("recipient_id", "source_guild_id")},
                    )
                    if api_success(reply) and not reply.get("guild_id"):
                        reply = {**reply, "error": "missing_dm_guild_id"}
                else:
                    scope, tid = target(data)
                    reply = request_json(
                        self.base,
                        token,
                        "DELETE",
                        api_path(scope, tid) + "/" + quote(data["message_id"], safe=""),
                    )
                result = self._api_result(reply)
        except Exception as exc:
            result = {
                "ok": False,
                "status": "failed"
                if isinstance(exc, (UploadError, ValueError))
                else "unknown",
                "reason": type(exc).__name__,
                "api_result": getattr(exc, "result", None) or {},
            }
        return self._finish(data["request_id"], result)

    @staticmethod
    def _api_result(reply):
        ok = api_success(reply)
        status = (
            "done"
            if ok
            else (
                "failed"
                if reply.get("code")
                or reply.get("err_code")
                or 400 <= reply.get("_http_status", 0) < 500
                else "unknown"
            )
        )
        return {"ok": ok, "status": status, "api_result": reply}

    def _send(self, data, token):
        scope, tid = target(data)
        mode = data.get("mode", "proactive")
        if mode in ("proactive", "wakeup") and not self.config.get(
            "proactive_enabled", True
        ):
            return {"ok": False, "status": "failed", "reason": "proactive_disabled"}
        plan = normalize_reply(
            data, self.config.get("max_media_bytes", 16 * 1024 * 1024), scope
        )
        parts = []
        for index, item in enumerate(plan):
            try:
                body = dict(item["payload"])
                if mode == "event":
                    body["event_id"] = data["event_id"]
                if mode == "wakeup":
                    body["is_wakeup"] = True
                if scope in ("channel", "dm"):
                    reply = send_guild_message(
                        self.base,
                        token,
                        scope,
                        tid,
                        body,
                        item.get("source"),
                        self.config.get("max_media_bytes", 16 * 1024 * 1024),
                    )
                else:
                    if mode == "event":
                        body["msg_seq"] = index + 1
                    if "source" in item:
                        uploaded = upload_media(
                            self.base,
                            token,
                            scope,
                            tid,
                            item["source"],
                            self.config.get("max_media_bytes", 16 * 1024 * 1024),
                        )
                        body["media"] = {"file_info": uploaded["file_info"]}
                        body["content"] = body.get("content") or " "
                    reply = request_json(
                        self.base, token, "POST", api_path(scope, tid), body, timeout=20
                    )
                state = send_status(reply)
            except Exception as exc:
                reply = getattr(exc, "result", None) or {"error": type(exc).__name__}
                state = "failed" if isinstance(exc, UploadError) else "unknown"
            parts.append({"index": index, "status": state, "api_result": reply})
            with self.store.connection() as conn:
                conn.execute(
                    "UPDATE operations SET result=?,lease=? WHERE request_id=?",
                    (
                        json.dumps({"parts": parts}),
                        time.time() + 90,
                        data["request_id"],
                    ),
                )
                conn.commit()
            if state != "done":
                return {
                    "ok": False,
                    "status": "unknown"
                    if state == "unknown"
                    else "partial"
                    if index
                    else "failed",
                    "parts": parts,
                    "api_result": reply,
                }
        return {
            "ok": True,
            "status": "done",
            "parts": parts,
            "api_result": parts[-1]["api_result"],
        }

    def _start(self, data):
        wake = self.store.get_wake(data["wake_id"])
        if not wake or wake["scope"] != "c2c":
            return {"ok": False, "status": "failed", "reason": "c2c_wake_required"}
        # Streaming is constrained to five minutes even when ordinary C2C replies use a longer window.
        from qq_runtime import event_deadline

        deadline = event_deadline(wake["timestamp"], 300)
        if deadline is None or deadline <= time.time() + 5:
            return {"ok": False, "status": "failed", "reason": "stream_window_expired"}
        cached, claimed = self.store.claim_reply(
            wake["id"], json.dumps(["stream", data["request_id"]])
        )
        if cached is not None:
            return {
                "ok": False,
                "status": "failed",
                "reason": "wake_unavailable",
                "wake_status": cached["status"],
            }
        stream_id = uuid.uuid4().hex
        protocol = data.get(
            "protocol", self.config.get("stream_protocol", "stream_messages")
        )
        with self.store.connection() as conn:
            conn.execute(
                "INSERT INTO streams(stream_id,wake_id,protocol,state,seq) VALUES(?,?,?,'open',?)",
                (stream_id, wake["id"], protocol, claimed["msg_seq"]),
            )
            conn.execute(
                "UPDATE pending_wakes SET claimed_until=? WHERE id=?",
                (deadline, wake["id"]),
            )
            conn.commit()
        return {
            "ok": True,
            "status": "done",
            "stream_id": stream_id,
            "next_index": 0,
            "protocol": protocol,
            "deadline": deadline,
        }

    def get_stream(self, stream_id):
        with self.store.connection() as conn:
            self._expire(conn)
            row = conn.execute(
                "SELECT * FROM streams WHERE stream_id=?", (stream_id,)
            ).fetchone()
            conn.commit()
        if not row:
            return None
        out = dict(row)
        wake = self.store.get_wake(out["wake_id"])
        if out["state"] not in ("complete", "cancelled") and (
            not wake or wake["status"] not in ("claimed", "done")
        ):
            out["state"] = "unknown"
        return out

    def _stream(self, path, data, token):
        stream_id = data["stream_id"]
        with self.store.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM streams WHERE stream_id=?", (stream_id,)
            ).fetchone()
            if not row or row["state"] != "open":
                conn.commit()
                return {"ok": False, "status": "failed", "reason": "stream_not_open"}
            row = dict(row)
            if path != "/streams/cancel" and data["index"] != row["next_index"]:
                conn.commit()
                return {
                    "ok": False,
                    "status": "failed",
                    "reason": "stream_index_mismatch",
                    "next_index": row["next_index"],
                }
            conn.execute(
                "UPDATE streams SET state='sending',active_request=? WHERE stream_id=?",
                (data["request_id"], stream_id),
            )
            conn.commit()
        wake = self.store.get_wake(row["wake_id"])
        if path == "/streams/cancel":
            self._close_stream(stream_id, wake, "cancelled")
            return {
                "ok": True,
                "status": "done",
                "stream_id": stream_id,
                "state": "cancelled",
            }
        from qq_runtime import event_deadline

        deadline = event_deadline(wake["timestamp"], 300) if wake else None
        if (
            not wake
            or wake["status"] != "claimed"
            or not deadline
            or deadline <= time.time() + 5
        ):
            self._close_stream(stream_id, wake, "unknown")
            return {"ok": False, "status": "failed", "reason": "stream_window_expired"}
        remaining = 0.3 - (time.time() - row["last_sent"])
        if remaining > 0:
            time.sleep(remaining)
        complete = path == "/streams/complete"
        text = data.get(
            "text", row["content"] if row["protocol"] == "stream_messages" else ""
        )
        state = 10 if complete else 1
        if row["protocol"] == "stream_messages":
            body = {
                "input_mode": "replace",
                "input_state": state,
                "content_type": "markdown",
                "content_raw": text,
                "event_id": wake["msg_id"],
                "msg_id": wake["msg_id"],
                "msg_seq": row["seq"],
                "index": row["next_index"],
            }
            if row["qq_id"]:
                body["stream_msg_id"] = row["qq_id"]
            route = api_path("c2c", wake["target_id"], "stream_messages")
        else:
            chunk = text if text.endswith("\n") else text + "\n"
            stream = {
                "state": state,
                "index": row["next_index"],
                "reset": data.get("reset", False),
            }
            if row["qq_id"]:
                stream["id"] = row["qq_id"]
            body = {
                "msg_type": 2,
                "markdown": {"content": chunk},
                "msg_id": wake["msg_id"],
                "msg_seq": row["seq"],
                "stream": stream,
            }
            route = api_path("c2c", wake["target_id"])
        try:
            reply = request_json(self.base, token, "POST", route, body, timeout=20)
            status = send_status(reply)
        except Exception as exc:
            reply = {"error": type(exc).__name__}
            status = "unknown"
        if status != "done":
            self.store.record_part(
                wake["id"], row["next_index"], row["seq"], status, reply
            )
            self._close_stream(stream_id, wake, status, reply)
            return {
                "ok": False,
                "status": status,
                "stream_id": stream_id,
                "api_result": reply,
            }
        with self.store.connection() as conn:
            conn.execute(
                "UPDATE streams SET state=?,next_index=next_index+1,qq_id=?,content=?,last_sent=? WHERE stream_id=?",
                (
                    "complete" if complete else "open",
                    reply["id"],
                    text,
                    time.time(),
                    stream_id,
                ),
            )
            conn.execute(
                "UPDATE pending_wakes SET claimed_until=? WHERE id=?",
                (deadline, wake["id"]),
            )
            conn.commit()
        self.store.record_part(wake["id"], row["next_index"], row["seq"], "done", reply)
        # record_part renews the short send lease; restore the idle stream's bounded deadline.
        with self.store.connection() as conn:
            conn.execute(
                "UPDATE pending_wakes SET claimed_until=? WHERE id=? AND status='claimed'",
                (deadline, wake["id"]),
            )
            conn.commit()
        if complete:
            self.store.finish_reply(wake["id"], "done", reply)
        return {
            "ok": True,
            "status": "done",
            "stream_id": stream_id,
            "next_index": row["next_index"] + 1,
            "state": "complete" if complete else "open",
            "api_result": reply,
        }

    def _close_stream(self, stream_id, wake, status, result=None):
        with self.store.connection() as conn:
            conn.execute(
                "UPDATE streams SET state=? WHERE stream_id=?", (status, stream_id)
            )
            conn.commit()
        if wake:
            self.store.finish_reply(
                wake["id"], "unknown" if status == "cancelled" else status, result
            )
