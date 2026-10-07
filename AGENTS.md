# Project conventions

- Keep this service a QQ protocol and durable queue layer. Personality, response policy and media understanding belong in clients.
- Preserve explicit per-group mention identity configuration. Never learn identities from arbitrary mentions.
- Maintain group and C2C isolation and conservative unknown-send handling; do not retry uncertain sends automatically.
- Preserve complete raw dispatch fields; document changes to HTTP contracts in docs/CLIENT_PROTOCOL.md.
- Use UTF-8 without BOM. Keep code comments and log additions in English.
- Run unittest discovery with disposable SQLite and mocked QQ requests. Never run migrations or tests against production configuration/data.
- Document unsupported and unverified QQ capabilities truthfully. No real QQ credentials in repository or logs.
