"""Shared QQ API and SQLite state; no network or database work on import."""

import asyncio
import hashlib
import json
import sqlite3
import time
from qq_media import api_path, normalize_reply, request_json, upload_media, UploadError
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import aiohttp
import requests

API_BASE = {
    "formal": "https://api.bot.qq.com",
    "sandbox": "https://sandbox.api.sgroup.qq.com",
}
TOKEN_URL = "https://api.bot.qq.com/app/getAppAccessToken"


def load_config(path):
    path = Path(path).resolve()
    cfg = json.loads(path.read_text(encoding="utf-8"))
    for key in ("appid", "appsecret", "api_bearer"):
        value = cfg.get(key)
        if (
            not isinstance(value, str)
            or not value.strip()
            or value.startswith("填")
            or "HERE" in value
        ):
            raise ValueError(f"请填写配置项 {key}")
    bearer = cfg["api_bearer"]
    if not bearer.isascii() or len(bearer) < 32 or any(c.isspace() for c in bearer):
        raise ValueError("api_bearer 需为至少 32 字符的无空白 ASCII 密钥")
    if cfg.get("token_mode", "direct") != "direct":
        raise ValueError("本服务端版本仅支持 direct token 模式")
    if cfg.get("env", "formal") not in API_BASE:
        raise ValueError("env 必须为 formal 或 sandbox")
    for key, default, low, high in (
        ("http_port", 8082, 1, 65535),
        ("context_window", 500, 1, 10000),
        ("context_limit", 30, 1, 500),
        ("wake_batch_size", 20, 1, 100),
        ("retention_days", 7, 1, 365),
        ("max_request_bytes", 24 * 1024 * 1024, 1024, 160 * 1024 * 1024),
        ("max_media_bytes", 16 * 1024 * 1024, 1024, 100 * 1024 * 1024),
        ("max_http_workers", 8, 1, 32),
    ):
        value = cfg.get(key, default)
        if type(value) is not int or not low <= value <= high:
            raise ValueError(f"{key} 必须为 {low}..{high} 的整数")
        cfg[key] = value
    if type(cfg.get("intents", 1 << 25)) is not int or cfg.get("intents", 1 << 25) <= 0:
        raise ValueError("intents 必须为正整数")
    identities = cfg.get("bot_mention_ids", {})
    if not isinstance(identities, dict) or any(
        not isinstance(k, str)
        or not isinstance(v, list)
        or any(not isinstance(x, str) or not x for x in v)
        for k, v in identities.items()
    ):
        raise ValueError(
            "bot_mention_ids 必须为 group_openid 到机器人身份字符串列表的映射"
        )
    for key in ("ack_enabled", "c2c_ack_enabled"):
        if key in cfg and type(cfg[key]) is not bool:
            raise ValueError(f"{key} must be boolean")
    cfg.setdefault("ack_enabled", True)
    cfg.setdefault("c2c_ack_enabled", False)
    cfg.setdefault("ack_text", "Received; your reply is being prepared.")
    if not isinstance(cfg["ack_text"], str) or not cfg["ack_text"].strip():
        raise ValueError("ack_text must be nonempty")
    cfg.setdefault("reply_windows", {"group": 300, "c2c": 300})
    if (
        not isinstance(cfg["reply_windows"], dict)
        or set(cfg["reply_windows"]) != {"group", "c2c"}
        or any(
            type(v) is not int or not 30 <= v <= 3600
            for v in cfg["reply_windows"].values()
        )
    ):
        raise ValueError("reply_windows must configure group/c2c seconds, 30..3600")
    if cfg["reply_windows"]["group"] > 300:
        raise ValueError("Group reply window cannot exceed 300 seconds")
    cfg.setdefault("api_routes", [])
    import re

    if not isinstance(cfg["api_routes"], list):
        raise ValueError("api_routes must be a list")
    for route in cfg["api_routes"]:
        if (
            not isinstance(route, dict)
            or route.get("method") not in ("GET", "POST", "PUT", "PATCH", "DELETE")
            or not isinstance(route.get("pattern"), str)
        ):
            raise ValueError("Each api_route requires method and full-match path regex")
        re.compile(route["pattern"])
    db = Path(cfg.get("db_path", "qqbot_events.db"))
    cfg["db_path"] = str(db if db.is_absolute() else path.parent / db)
    cfg.setdefault("env", "formal")
    return cfg


class TokenManager:
    def __init__(self, appid, appsecret):
        self.appid, self.appsecret = appid, appsecret
        self._token, self._expires_at = None, 0
        self._lock = asyncio.Lock()

    def invalidate(self):
        self._token, self._expires_at = None, 0

    async def get_token(self):
        async with self._lock:
            if self._token and time.monotonic() < self._expires_at - 45:
                return self._token
            requested_at = time.monotonic()
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    TOKEN_URL,
                    json={"appId": self.appid, "clientSecret": self.appsecret},
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as response:
                    data = await response.json(content_type=None)
                    if (
                        response.status != 200
                        or not isinstance(data, dict)
                        or data.get("code")
                        or not data.get("access_token")
                    ):
                        code = (
                            data.get("code")
                            if isinstance(data, dict)
                            else "invalid response"
                        )
                        raise RuntimeError(
                            f"QQ token 获取失败: HTTP {response.status}, code={code}"
                        )
            lifetime = int(data["expires_in"])
            if lifetime <= 0:
                raise RuntimeError("QQ token 有效期无效")
            self._token, self._expires_at = (
                data["access_token"],
                requested_at + lifetime,
            )
            return self._token


def send_group_message(api_base, token, group_openid, content, msg_id, msg_seq):
    if not msg_id or not group_openid:
        raise ValueError("被动回复必须包含 group_openid 和 msg_id")
    response = requests.post(
        api_base + api_path("group", group_openid),
        headers={"Authorization": f"QQBot {token}"},
        json={"content": content, "msg_type": 0, "msg_id": msg_id, "msg_seq": msg_seq},
        timeout=20,
        allow_redirects=False,
    )
    try:
        data = response.json()
    except ValueError:
        data = {"error": "non_json_response"}
    if not isinstance(data, dict):
        data = {"error": "invalid_response_shape"}
    data["_http_status"] = response.status_code
    return data


def send_status(result):
    if (
        result.get("_http_status") == 200
        and not result.get("code")
        and not result.get("err_code")
        and result.get("id")
    ):
        return "done"
    if result.get("code") or result.get("err_code"):
        return "failed"
    # A gateway/proxy error may conceal a successful upstream send.
    return "unknown"


def event_deadline(value, seconds=300):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        return parsed.timestamp() + seconds
    except (AttributeError, TypeError, ValueError):
        return None


class EventStore:
    def __init__(
        self, db_path, context_window=500, retention_days=7, reply_windows=None
    ):
        self.db_path, self.context_window, self.retention_days = (
            str(db_path),
            context_window,
            retention_days,
        )
        self.reply_windows = reply_windows or {"group": 300, "c2c": 300}
        with self.connection() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY AUTOINCREMENT,
                msg_id TEXT UNIQUE,group_openid TEXT NOT NULL,member_openid TEXT,author_name TEXT,
                content TEXT,is_at INTEGER DEFAULT 0,timestamp TEXT,created_at TEXT DEFAULT (datetime('now')));
            CREATE INDEX IF NOT EXISTS idx_messages_group ON messages(group_openid,id);
            CREATE TABLE IF NOT EXISTS pending_wakes(id INTEGER PRIMARY KEY AUTOINCREMENT,
                msg_id TEXT UNIQUE,group_openid TEXT NOT NULL,member_openid TEXT,author_name TEXT,
                content TEXT,timestamp TEXT,status TEXT DEFAULT 'pending',created_at TEXT DEFAULT (datetime('now')));
            CREATE TABLE IF NOT EXISTS msg_seqs(msg_id TEXT PRIMARY KEY,next_seq INTEGER DEFAULT 1);
            """)
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS raw_events(id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_key TEXT UNIQUE,event_type TEXT NOT NULL,payload TEXT NOT NULL,created_at TEXT DEFAULT (datetime('now')));
            CREATE TABLE IF NOT EXISTS send_parts(wake_id INTEGER NOT NULL,part_index INTEGER NOT NULL,
                msg_seq INTEGER,status TEXT NOT NULL,result TEXT,PRIMARY KEY(wake_id,part_index));
            CREATE TABLE IF NOT EXISTS gateway_state(id INTEGER PRIMARY KEY CHECK(id=1),session_id TEXT,seq INTEGER);
            """)
            for table in ("messages", "pending_wakes"):
                fields = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
                for key, kind in {
                    "scope": "TEXT DEFAULT 'group'",
                    "target_id": "TEXT",
                    "attachments": "TEXT DEFAULT '{}'",
                    "raw_event": "TEXT DEFAULT '{}'",
                }.items():
                    if key not in fields:
                        conn.execute(f"ALTER TABLE {table} ADD COLUMN {key} {kind}")
                conn.execute(
                    f"UPDATE {table} SET target_id=group_openid WHERE target_id IS NULL"
                )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_target ON messages(scope,target_id,id)"
            )
            # Additive migration; old records without a deadline expire safely.
            columns = {r[1] for r in conn.execute("PRAGMA table_info(pending_wakes)")}
            additions = {
                "deadline": "REAL",
                "claimed_until": "REAL",
                "reply_hash": "TEXT",
                "reply_result": "TEXT",
                "error": "TEXT",
                "message_row_id": "INTEGER",
            }
            for key, kind in additions.items():
                if key not in columns:
                    conn.execute(f"ALTER TABLE pending_wakes ADD COLUMN {key} {kind}")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_wakes_status ON pending_wakes(status,id)"
            )
            for row in conn.execute(
                "SELECT id,timestamp FROM pending_wakes WHERE deadline IS NULL"
            ).fetchall():
                conn.execute(
                    "UPDATE pending_wakes SET deadline=? WHERE id=?",
                    (event_deadline(row["timestamp"]), row["id"]),
                )
            conn.commit()

    @contextmanager
    def connection(self):
        conn = sqlite3.connect(self.db_path, timeout=5)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def save_event(self, d, is_at, event_name="GROUP_MESSAGE_CREATE", frame=None):
        scope = "c2c" if event_name == "C2C_MESSAGE_CREATE" else "group"
        msg_id = d.get("id")
        group = d.get("group_openid", "")
        target = (d.get("author") or {}).get("user_openid") if scope == "c2c" else group
        if (
            not isinstance(msg_id, str)
            or not msg_id
            or not isinstance(target, str)
            or not target
        ):
            raise ValueError("群消息缺少 id/group_openid")
        author = d.get("author") or {}
        values = (
            msg_id,
            group,
            author.get("user_openid")
            if scope == "c2c"
            else author.get("member_openid"),
            author.get("username") or author.get("nick"),
            d.get("content") or "",
            int(is_at),
            d.get("timestamp"),
        )
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            raw = json.dumps(frame or {"t": event_name, "d": d}, ensure_ascii=False)
            attachments = json.dumps(
                {
                    key: d[key]
                    for key in (
                        "attachments",
                        "msg_elements",
                        "embeds",
                        "embed",
                        "ark_data",
                        "message_scene",
                        "message_reference",
                    )
                    if key in d
                },
                ensure_ascii=False,
            )
            self._record_event(conn, frame or {"t": event_name, "d": d})
            cur = conn.execute(
                "INSERT OR IGNORE INTO messages(msg_id,group_openid,member_openid,author_name,content,is_at,timestamp) VALUES(?,?,?,?,?,?,?)",
                values,
            )
            new_message = cur.rowcount == 1
            if is_at:
                conn.execute("UPDATE messages SET is_at=1 WHERE msg_id=?", (msg_id,))
            message = conn.execute(
                "SELECT id FROM messages WHERE msg_id=?", (msg_id,)
            ).fetchone()
            conn.execute(
                "UPDATE messages SET scope=?,target_id=?,attachments=?,raw_event=? WHERE msg_id=?",
                (scope, target, attachments, raw, msg_id),
            )
            new_wake = False
            if is_at:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO pending_wakes(msg_id,group_openid,member_openid,author_name,content,timestamp,deadline,message_row_id) VALUES(?,?,?,?,?,?,?,?)",
                    values[:5]
                    + (
                        values[6],
                        event_deadline(values[6], self.reply_windows[scope]),
                        message["id"],
                    ),
                )
                new_wake = cur.rowcount == 1
                if new_wake:
                    conn.execute(
                        "UPDATE pending_wakes SET scope=?,target_id=?,attachments=?,raw_event=? WHERE msg_id=?",
                        (scope, target, attachments, raw, msg_id),
                    )
            conn.execute(
                "DELETE FROM messages WHERE scope=? AND target_id=? AND id NOT IN (SELECT id FROM messages WHERE scope=? AND target_id=? ORDER BY id DESC LIMIT ?)",
                (scope, target, scope, target, self.context_window),
            )
            conn.commit()
            return new_message, new_wake

    def _expire(self, conn):
        now = time.time()
        conn.execute(
            "UPDATE pending_wakes SET status='unknown',error='claim_expired_result_unknown' WHERE status='claimed' AND (claimed_until IS NULL OR claimed_until<?)",
            (now,),
        )
        conn.execute(
            "UPDATE pending_wakes SET status='expired',error='passive_window_expired' WHERE status='pending' AND (deadline IS NULL OR deadline<=?)",
            (now + 5,),
        )

    def get_pending_wakes(self, limit=20, context_limit=30):
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._expire(conn)
            rows = conn.execute(
                "SELECT * FROM pending_wakes WHERE status='pending' ORDER BY id LIMIT ?",
                (limit,),
            ).fetchall()
            out = []
            for row in rows:
                w = dict(row)
                anchor = w["message_row_id"]
                if anchor is None:
                    found = conn.execute(
                        "SELECT id FROM messages WHERE msg_id=?", (w["msg_id"],)
                    ).fetchone()
                    anchor = found[0] if found else 0
                ctx = conn.execute(
                    "SELECT msg_id,member_openid,author_name,content,is_at,timestamp,attachments,raw_event FROM messages WHERE scope=? AND target_id=? AND id<=? ORDER BY id DESC LIMIT ?",
                    (w["scope"], w["target_id"], anchor, context_limit),
                ).fetchall()
                w["context"] = [self._decode(dict(r)) for r in reversed(ctx)]
                self._decode(w)
                out.append(w)
            conn.commit()
            return out

    def get_wake(self, wake_id):
        with self.connection() as conn:
            self._expire(conn)
            row = conn.execute(
                "SELECT * FROM pending_wakes WHERE id=?", (wake_id,)
            ).fetchone()
            conn.commit()
            if not row:
                return None
            out = self._decode(dict(row))
            if out["reply_result"]:
                out["reply_result"] = json.loads(out["reply_result"])
            out["parts"] = self.get_parts(wake_id)
            return out

    def _next_seq(self, conn, msg_id, limit=5):
        row = conn.execute(
            "SELECT next_seq FROM msg_seqs WHERE msg_id=?", (msg_id,)
        ).fetchone()
        seq = row[0] if row else 1
        if seq > limit:
            raise ValueError("passive_reply_budget_exhausted")
        conn.execute(
            "INSERT INTO msg_seqs(msg_id,next_seq) VALUES(?,?) ON CONFLICT(msg_id) DO UPDATE SET next_seq=excluded.next_seq",
            (msg_id, seq + 1),
        )
        return seq

    def reserve_ack(self, msg_id):
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._expire(conn)
            row = conn.execute(
                "SELECT * FROM pending_wakes WHERE msg_id=? AND status='pending'",
                (msg_id,),
            ).fetchone()
            if not row:
                conn.commit()
                return None
            seq = self._next_seq(conn, msg_id, 4 if row["scope"] == "c2c" else 5)
            conn.commit()
            return seq

    def claim_reply(self, wake_id, text, part_count=1):
        digest = hashlib.sha256(text.encode()).hexdigest()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._expire(conn)
            row = conn.execute(
                "SELECT * FROM pending_wakes WHERE id=?", (wake_id,)
            ).fetchone()
            if not row:
                conn.commit()
                return {"ok": False, "status": "missing"}, None
            wake = dict(row)
            if wake["status"] != "pending":
                same = wake["reply_hash"] == digest
                result = {
                    "ok": same and wake["status"] == "done",
                    "status": wake["status"],
                    "replayed": same,
                }
                if same and wake["reply_result"]:
                    result["api_result"] = json.loads(wake["reply_result"])
                conn.commit()
                return result, None
            try:
                limit = 4 if wake["scope"] == "c2c" else 5
                next_row = conn.execute(
                    "SELECT next_seq FROM msg_seqs WHERE msg_id=?", (wake["msg_id"],)
                ).fetchone()
                if (next_row[0] if next_row else 1) + part_count - 1 > limit:
                    raise ValueError("passive_reply_budget_exhausted")
                seqs = [
                    self._next_seq(conn, wake["msg_id"], limit)
                    for _ in range(part_count)
                ]
                seq = seqs[0]
            except ValueError as e:
                # Reject an oversized plan without consuming the wake or any sequence.
                conn.rollback()
                return {"ok": False, "status": "pending", "reason": str(e)}, None
            conn.execute(
                "UPDATE pending_wakes SET status='claimed',claimed_until=?,reply_hash=? WHERE id=? AND status='pending'",
                (time.time() + 90, digest, wake_id),
            )
            conn.commit()
            wake["msg_seq"] = seq
            wake["msg_seqs"] = seqs
            return None, wake

    def finish_reply(self, wake_id, status, result=None, error=None):
        with self.connection() as conn:
            conn.execute(
                "UPDATE pending_wakes SET status=?,reply_result=?,error=?,claimed_until=NULL WHERE id=? AND status IN ('claimed','unknown')",
                (
                    status,
                    json.dumps(result, ensure_ascii=False) if result else None,
                    error,
                    wake_id,
                ),
            )
            conn.commit()

    def cleanup(self):
        with self.connection() as conn:
            self._expire(conn)
            conn.execute(
                "DELETE FROM pending_wakes WHERE status NOT IN ('pending','claimed') AND created_at<datetime('now',?)",
                (f"-{self.retention_days} days",),
            )
            conn.execute(
                "DELETE FROM send_parts WHERE wake_id NOT IN (SELECT id FROM pending_wakes)"
            )
            conn.execute(
                "DELETE FROM raw_events WHERE created_at<datetime('now',?)",
                (f"-{self.retention_days} days",),
            )
            conn.execute(
                "DELETE FROM raw_events WHERE id NOT IN (SELECT id FROM raw_events ORDER BY id DESC LIMIT 100000)"
            )
            conn.execute(
                "DELETE FROM msg_seqs WHERE msg_id NOT IN (SELECT msg_id FROM pending_wakes UNION SELECT msg_id FROM messages)"
            )
            conn.commit()

    @staticmethod
    def _decode(row):
        for key in ("attachments", "raw_event"):
            if isinstance(row.get(key), str):
                row[key] = json.loads(row[key] or "{}")
        return row

    def _record_event(self, conn, frame):
        d = frame.get("d")
        identity = frame.get("id") or (d.get("id") if isinstance(d, dict) else None)
        key = f"{frame.get('t')}:{identity}" if identity else None
        conn.execute(
            "INSERT OR IGNORE INTO raw_events(event_key,event_type,payload) VALUES(?,?,?)",
            (key, frame.get("t") or "UNKNOWN", json.dumps(frame, ensure_ascii=False)),
        )

    def record_event(self, frame):
        with self.connection() as conn:
            self._record_event(conn, frame)
            conn.commit()

    def get_events(self, after=0, limit=100):
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT id,payload,created_at FROM raw_events WHERE id>? ORDER BY id LIMIT ?",
                (after, limit),
            ).fetchall()
            first = conn.execute("SELECT MIN(id) FROM raw_events").fetchone()[0]
            return {
                "events": [
                    {
                        "cursor": r["id"],
                        "received_at": r["created_at"],
                        "payload": json.loads(r["payload"]),
                    }
                    for r in rows
                ],
                "next_cursor": rows[-1]["id"] if rows else after,
                "oldest_cursor": first,
            }

    def checkpoint(self, session_id, seq):
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO gateway_state VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET session_id=excluded.session_id,seq=excluded.seq",
                (session_id, seq),
            )
            conn.commit()

    def load_checkpoint(self):
        with self.connection() as conn:
            row = conn.execute(
                "SELECT session_id,seq FROM gateway_state WHERE id=1"
            ).fetchone()
            return (row[0], row[1]) if row else (None, None)

    def record_part(self, wake_id, index, seq, status, result=None):
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO send_parts VALUES(?,?,?,?,?) ON CONFLICT(wake_id,part_index) DO UPDATE SET status=excluded.status,result=excluded.result",
                (wake_id, index, seq, status, json.dumps(result) if result else None),
            )
            conn.execute(
                "UPDATE pending_wakes SET claimed_until=? WHERE id=? AND status='claimed'",
                (time.time() + 90, wake_id),
            )
            conn.commit()

    def touch_claim(self, wake_id):
        with self.connection() as conn:
            conn.execute(
                "UPDATE pending_wakes SET claimed_until=? WHERE id=? AND status='claimed'",
                (time.time() + 90, wake_id),
            )
            conn.commit()

    def get_parts(self, wake_id):
        with self.connection() as conn:
            return [
                {**dict(r), "result": json.loads(r["result"]) if r["result"] else None}
                for r in conn.execute(
                    "SELECT * FROM send_parts WHERE wake_id=? ORDER BY part_index",
                    (wake_id,),
                )
            ]


def reply_to_wake(
    store, api_base, token, wake_id, text="", plan=None, max_bytes=16 * 1024 * 1024
):
    plan = plan if plan is not None else normalize_reply({"text": text}, max_bytes)
    canonical = json.dumps(
        plan, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    existing, wake = store.claim_reply(wake_id, canonical, len(plan))
    if existing is not None:
        return existing
    results = []
    for index, item in enumerate(plan):
        seq = wake["msg_seqs"][index]
        try:
            store.touch_claim(wake_id)
            payload = dict(item["payload"])
            if "source" in item:
                upload = upload_media(
                    api_base,
                    token,
                    wake["scope"],
                    wake["target_id"],
                    item["source"],
                    max_bytes,
                )
                payload["media"] = {"file_info": upload["file_info"]}
            if wake["deadline"] <= time.time() + 5:
                status = "partial" if results else "expired"
                store.finish_reply(wake_id, status, error="passive_window_expired")
                return {"ok": False, "status": status, "parts": results}
            payload.update(msg_id=wake["msg_id"], msg_seq=seq)
            store.record_part(wake_id, index, seq, "sending")
            if (
                wake["scope"] == "group"
                and set(payload) == {"msg_type", "content", "msg_id", "msg_seq"}
                and payload["msg_type"] == 0
            ):
                result = send_group_message(
                    api_base,
                    token,
                    wake["target_id"],
                    payload["content"],
                    wake["msg_id"],
                    seq,
                )
            else:
                result = request_json(
                    api_base,
                    token,
                    "POST",
                    api_path(wake["scope"], wake["target_id"]),
                    payload,
                    timeout=20,
                )
            status = send_status(result)
            store.record_part(wake_id, index, seq, status, result)
            results.append({"index": index, "status": status, "api_result": result})
            if status != "done":
                terminal = (
                    "unknown"
                    if status == "unknown"
                    else ("partial" if index else "failed")
                )
                aggregate = {
                    "parts": results,
                    "_http_status": result.get("_http_status"),
                }
                store.finish_reply(wake_id, terminal, aggregate)
                return {
                    "ok": False,
                    "status": terminal,
                    "parts": results,
                    "api_result": result,
                }
        except Exception as exc:
            # Upload-only errors cannot have sent this part; transport send failures may have.
            status = "failed" if isinstance(exc, UploadError) else "unknown"
            failure = (exc.result if isinstance(exc, UploadError) else None) or {
                "error": type(exc).__name__
            }
            store.record_part(wake_id, index, seq, status, failure)
            terminal = "partial" if status == "failed" and results else status
            results.append({"index": index, "status": status, "api_result": failure})
            store.finish_reply(
                wake_id,
                terminal,
                {"parts": results, "_http_status": failure.get("_http_status")},
                type(exc).__name__,
            )
            return {
                "ok": False,
                "status": terminal,
                "parts": results,
                "api_result": failure,
                "reason": type(exc).__name__,
            }
    aggregate = {"parts": results}
    if len(results) == 1:
        aggregate = results[0]["api_result"]
    store.finish_reply(wake_id, "done", aggregate)
    return {"ok": True, "status": "done", "parts": results, "api_result": aggregate}


@contextmanager
def exclusive_instance(db_path):
    """Hold an OS lock without deleting the shared lock file."""
    import os

    lock_path = Path(str(db_path) + ".lock")
    with lock_path.open("a+b") as handle:
        handle.seek(0, 2)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                raise RuntimeError(
                    "Another gateway instance already owns this database"
                ) from None
        else:
            import fcntl

            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                raise RuntimeError(
                    "Another gateway instance already owns this database"
                ) from None
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle, fcntl.LOCK_UN)
