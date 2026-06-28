# Decision Map

Canonical decisions:

- [Codex Agent External Session Semantics](../../docs/decisions/codex-agent-external-session-semantics.md)
  Affects: Feishu should remain an external-channel adapter that chooses Feishu session ids, calls Codex Agent, and replies through callbacks without creating My Agent threads.

Relevant implementation plans:

- [Codex Agent App-Server Extraction](../../docs/impl_plan/codex-agent-appserver-extraction.md)
