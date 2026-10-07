#!/usr/bin/env python3
"""Server-local passive reply CLI using the same state machine as HTTP."""

import argparse
import asyncio
import json
from pathlib import Path
from qq_media import normalize_reply
from qq_runtime import API_BASE, EventStore, TokenManager, load_config, reply_to_wake


def main():
    parser = argparse.ArgumentParser(description="QQ server-local passive reply")
    parser.add_argument("--wake-id", type=int, required=True)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--text")
    group.add_argument("--text-file")
    parser.add_argument("--media", help="JSON array of media sources")
    parser.add_argument("--payload", help="Exclusive native QQ message JSON object")
    parser.add_argument("--messages", help="Exclusive ordered message JSON array")
    parser.add_argument(
        "--config", default=str(Path(__file__).with_name("config.json"))
    )
    args = parser.parse_args()
    try:
        if args.wake_id <= 0:
            raise ValueError("wake-id must be positive")
        data = {}
        if args.text is not None or args.text_file:
            data["text"] = (
                Path(args.text_file).read_text(encoding="utf-8")
                if args.text_file
                else args.text
            )
        for arg, key in (
            (args.media, "media"),
            (args.payload, "qq_payload"),
            (args.messages, "messages"),
        ):
            if arg is not None:
                data[key] = json.loads(arg)
        cfg = load_config(args.config)
        store = EventStore(
            cfg["db_path"],
            cfg["context_window"],
            cfg["retention_days"],
            cfg["reply_windows"],
        )
        wake = store.get_wake(args.wake_id)
        plan = normalize_reply(
            data, cfg["max_media_bytes"], wake["scope"] if wake else "group"
        )
    except (ValueError, OSError) as exc:
        parser.error(str(exc))

    async def execute():
        token = await TokenManager(cfg["appid"], cfg["appsecret"]).get_token()

        async def renew():
            while True:
                await asyncio.sleep(20)
                await asyncio.to_thread(store.touch_claim, args.wake_id)

        lease = asyncio.create_task(renew())
        try:
            return await asyncio.to_thread(
                reply_to_wake,
                store,
                API_BASE[cfg["env"]],
                token,
                args.wake_id,
                plan=plan,
                max_bytes=cfg["max_media_bytes"],
            )
        finally:
            lease.cancel()
            await asyncio.gather(lease, return_exceptions=True)

    result = asyncio.run(execute())
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result["ok"] else 1)


if __name__ == "__main__":
    main()
