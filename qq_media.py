"""QQ upload protocol. Media content is never interpreted or transcoded."""

import base64
import binascii
import hashlib
from urllib.parse import quote, urlsplit

import requests


MEDIA_TYPES = {"image": 1, "video": 2, "audio": 3, "voice": 3, "file": 4}
PROTECTED_FIELDS = {
    "msg_id",
    "msg_seq",
    "event_id",
    "is_wakeup",
    "stream",
    "stream_messages",
}


def api_path(scope, target, suffix="messages"):
    if scope not in ("group", "c2c") or not isinstance(target, str) or not target:
        raise ValueError("Only group and c2c targets are supported")
    return f"/v2/{'groups' if scope == 'group' else 'users'}/{quote(target, safe='')}/{suffix}"


def validate_media(item, max_bytes):
    if not isinstance(item, dict) or item.get("type") not in MEDIA_TYPES:
        raise ValueError("media.type must be image, video, audio, voice or file")
    if set(item) - {"type", "url", "base64", "file_name"}:
        raise ValueError("Unknown media source field")
    if ("url" in item) == ("base64" in item):
        raise ValueError("Exactly one media URL or base64 source is required")
    if "file_name" in item and (
        not isinstance(item["file_name"], str) or not item["file_name"]
    ):
        raise ValueError("file_name must be a nonempty string")
    if "url" in item:
        value = item["url"]
        if not isinstance(value, str):
            raise ValueError("Media URL must be a string")
        parsed = urlsplit(value)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or parsed.username
            or parsed.password
        ):
            raise ValueError("Media URL must be an HTTP(S) URL without credentials")
        return None
    value = item["base64"]
    if (
        not isinstance(value, str)
        or not value
        or len(value) > ((max_bytes + 2) // 3) * 4
    ):
        raise ValueError("Media base64 is empty or exceeds configured size")
    try:
        content = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        raise ValueError(
            "Media base64 must be standard base64 without a data URI"
        ) from None
    if not content or len(content) > max_bytes:
        raise ValueError("Decoded media exceeds configured size")
    return content


def normalize_reply(data, max_bytes=16 * 1024 * 1024):
    """Build an ordered send plan while preserving unknown QQ body fields."""
    if not isinstance(data, dict):
        raise ValueError("Reply body must be an object")
    if "messages" in data:
        if any(key in data for key in ("text", "media", "qq_payload")):
            raise ValueError("messages cannot be combined with text/media/qq_payload")
        items = data["messages"]
        if not isinstance(items, list) or not 1 <= len(items) <= 5:
            raise ValueError("messages must contain between one and five entries")
    else:
        items = [
            {key: data[key] for key in ("text", "media", "qq_payload") if key in data}
        ]
    plan = []
    for item in items:
        if not isinstance(item, dict) or set(item) - {"text", "media", "qq_payload"}:
            raise ValueError("Each message must contain text, media or qq_payload")
        native = item.get("qq_payload")
        if native is not None:
            if (
                not isinstance(native, dict)
                or not native
                or "text" in item
                or "media" in item
            ):
                raise ValueError("qq_payload must be a nonempty exclusive object")
            if PROTECTED_FIELDS & native.keys():
                raise ValueError(
                    "QQ association, proactive and stream fields are server-controlled"
                )
            if type(native.get("msg_type")) is not int or native["msg_type"] < 0:
                raise ValueError("Native payload requires an integer msg_type")
            plan.append({"payload": native})
            continue
        text = item.get("text", "")
        media = item.get("media", [])
        if not isinstance(text, str) or not isinstance(media, list):
            raise ValueError("text must be a string and media a list")
        if not text.strip() and not media:
            raise ValueError("Message cannot be empty")
        if media:
            for index, source in enumerate(media):
                validate_media(source, max_bytes)
                plan.append(
                    {
                        "payload": {
                            "msg_type": 7,
                            "content": text if index == 0 else "",
                        },
                        "source": source,
                    }
                )
        else:
            plan.append({"payload": {"msg_type": 0, "content": text}})
    if len(plan) > 5:
        raise ValueError("Send plan exceeds five messages")
    return plan


class UploadError(RuntimeError):
    """An upload-only failure; it never implies a message was delivered."""

    def __init__(self, message, result=None):
        super().__init__(message)
        self.result = result


def request_json(base, token, method, path, body=None, timeout=60):
    response = requests.request(
        method,
        base + path,
        headers={"Authorization": f"QQBot {token}"},
        json=body,
        timeout=timeout,
        allow_redirects=False,
    )
    try:
        data = response.json()
    except ValueError:
        data = {"error": "non_json_response"}
    if not isinstance(data, dict):
        data = {"error": "invalid_response_shape"}
    data["_http_status"] = response.status_code
    data["_trace_id"] = response.headers.get("x-tps-trace-id")
    return data


def upload_media(base, token, scope, target, item, max_bytes=16 * 1024 * 1024):
    content = validate_media(item, max_bytes)
    path = api_path(scope, target, "files")
    body = {"file_type": MEDIA_TYPES[item["type"]], "srv_send_msg": False}
    if item.get("file_name"):
        body["file_name"] = item["file_name"]

    def checked(route, payload):
        result = request_json(base, token, "POST", route, payload)
        if (
            result.get("_http_status") != 200
            or result.get("code")
            or result.get("err_code")
            or result.get("error")
        ):
            raise UploadError(
                f"Upload rejected: HTTP {result.get('_http_status')}, code={result.get('code', result.get('err_code'))}",
                result,
            )
        value = result.get("data", result)
        if not isinstance(value, dict):
            raise UploadError("Malformed upload response")
        return value

    if content is not None and len(content) > 5 * 1024 * 1024:
        name = item.get("file_name", "media.bin")
        prepare = checked(
            api_path(scope, target, "upload_prepare"),
            {
                "file_type": body["file_type"],
                "file_size": str(len(content)),
                "file_name": name,
                "md5": hashlib.md5(content).hexdigest(),
                "sha1": hashlib.sha1(content).hexdigest(),
                "md5_10m": hashlib.md5(content[:10002432]).hexdigest(),
            },
        )
        block = int(prepare.get("block_size", 0))
        parts = prepare.get("parts")
        upload_id = prepare.get("upload_id")
        if block <= 0 or not isinstance(parts, list) or not parts or not upload_id:
            raise UploadError("Incomplete upload_prepare response")
        indexes = [int(p.get("index", p.get("part_index", -1))) for p in parts]
        first = min(indexes)
        if first not in (0, 1) or sorted(indexes) != list(
            range(first, first + len(parts))
        ):
            raise UploadError("Invalid upload part indexes")
        coverage = 0
        for part, index in sorted(zip(parts, indexes), key=lambda pair: pair[1]):
            start = (index - first) * block
            size = min(int(part.get("block_size") or block), len(content) - start)
            url = part.get("presigned_url", "")
            parsed = urlsplit(url)
            if (
                parsed.scheme != "https"
                or not parsed.hostname
                or parsed.username
                or parsed.password
                or start != coverage
                or size <= 0
            ):
                raise UploadError("Invalid upload part metadata")
            # No QQ credentials are sent to the presigned object-storage URL.
            chunk = content[start : start + size]
            response = requests.put(url, data=chunk, timeout=60, allow_redirects=False)
            if not 200 <= response.status_code < 300:
                raise UploadError("Object-storage upload failed")
            checked(
                api_path(scope, target, "upload_part_finish"),
                {
                    "upload_id": upload_id,
                    "part_index": index,
                    "block_size": size,
                    "md5": hashlib.md5(chunk).hexdigest(),
                },
            )
            coverage += size
        if coverage != len(content):
            raise UploadError("Upload parts do not cover the file")
        body.update(upload_id=upload_id, file_name=name)
    elif "url" in item:
        body["url"] = item["url"]
    else:
        body["file_data"] = item["base64"]
    result = checked(path, body)
    if not isinstance(result.get("file_info"), str) or not result["file_info"]:
        raise UploadError("Upload response lacks file_info")
    return result
