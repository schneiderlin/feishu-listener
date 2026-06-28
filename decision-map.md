# Decision Map

Canonical decisions:

- [Codex Agent External Session Semantics](../../docs/decisions/codex-agent-external-session-semantics.md)
  Affects: Feishu should remain an external-channel adapter that chooses Feishu session ids, calls Codex Agent, and replies through callbacks without creating My Agent threads.

- [Feishu OpenAPI Lite Semantics](../../docs/decisions/feishu-openapi-lite-semantics.md)
  Affects: Feishu listener delegates OpenAPI requests, p2 event normalization, and long-connection frames to the bb-compatible lite SDK instead of depending on the official Java SDK.

Relevant implementation plans:

- [Codex Agent App-Server Extraction](../../docs/impl_plan/codex-agent-appserver-extraction.md)
