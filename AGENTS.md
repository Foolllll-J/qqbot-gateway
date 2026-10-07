# Project conventions

- Keep this service a QQ protocol and durable queue layer. Personality, response policy and media understanding belong in clients.
- Preserve explicit per-group mention identity configuration. Never learn identities from arbitrary mentions.
- Maintain group and C2C isolation and conservative unknown-send handling; do not retry uncertain sends automatically.
- Include channel/dm scope in routing and context isolation. Never route guild media through group/C2C file endpoints.
- Explicit proactive actions need durable request IDs and result queries. Do not silently convert passive failures to proactive sends.
- Keep both stream protocols explicit: stream_messages replaces full text; legacy stream accepts deltas. Preserve sequence ownership and never switch protocols after an uncertain send.
- Preserve complete raw dispatch fields; document changes to HTTP contracts in docs/CLIENT_PROTOCOL.md.
- Use UTF-8 without BOM. Keep code comments and log additions in English.
- Run unittest discovery with disposable SQLite and mocked QQ requests. Never run migrations or tests against production configuration/data.
- Document unsupported and unverified QQ capabilities truthfully. No real QQ credentials in repository or logs.
