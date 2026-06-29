# feishu-listener

Feishu app bot integration for receiving messages over the repository's
Babashka-compatible `feishu-openapi-lite` WebSocket client and sending text
messages through the Feishu IM API.

## Quick App And Bot Setup

Use the Feishu Open Platform one-click launcher:

```text
https://open.feishu.cn/page/launcher?from=backend_oneclick
```

This launcher is the preferred setup path for development because it quickly
creates an app + bot template with the right bot capability, event subscription,
and permissions for message receiving.

Feishu also provides an AI agent app launcher flow that preconfigures a broader
set of bot, message, menu, document, and Slash Command permissions:

```text
https://open.feishu.cn/document/mcp_open_tools/integrating-agents-with-feishu/overview
```

After creating the app, copy its credentials into local environment variables:

```bash
export FEISHU_APP_ID="cli_xxx"
export FEISHU_APP_SECRET="xxx"
```

Make sure the generated app/bot is published and visible to the test account
before testing from the Feishu client.

## Temporary nREPL

From the repo root:

```bash
.agents/skills/clojure-temp-nrepl/scripts/start-clojure-nrepl.sh \
  --component feishu-listener \
  --require com.zihao.feishu-listener.interface
```

The selected port is written to:

```text
.runtime/feishu-listener/nrepl-port
```

## REPL Example

```clojure
(require '[com.zihao.feishu-listener.interface :as feishu])

(def listener
  (feishu/make-listener
    {:app-id (System/getenv "FEISHU_APP_ID")
     :app-secret (System/getenv "FEISHU_APP_SECRET")
     :persist-message! prn
     :on-message prn}))

(feishu/start! listener)
(feishu/await-ready! listener)

(feishu/send-text! listener {:chat-id "oc_xxx" :text "hello"})
(feishu/reply-text! listener {:message-id "om_xxx" :text "received"})
```

## Codex Agent Routing

Feishu can route received messages through Codex Agent without creating My Agent
threads:

```clojure
(require '[com.zihao.codex-agent.interface :as codex-agent])

(def agent
  (codex-agent/start!
    {:store-path "var/codex-agent/sessions.edn"
     :codex-app-server {:model "gpt-5.5"
                        :experimental-api? true}}))

(def listener
  (feishu/make-listener
    {:app-id (System/getenv "FEISHU_APP_ID")
     :app-secret (System/getenv "FEISHU_APP_SECRET")
     :codex-agent-service agent}))
```

The adapter uses Feishu `thread-id` when Feishu supplies one. When a top-level
message has no `thread-id`, it starts a new Codex thread with a bootstrap
session id derived from the message id, such as `bootstrap:om_xxx`. The bot
reply is sent with `reply-in-thread? true`; if Feishu returns a thread id for
that reply, Codex Agent promotes the bootstrap session to the durable Feishu
thread id. Later replies inside that Feishu thread reuse the same Codex thread.

`chat-id` is only used by Feishu's API as the surrounding conversation target.
It is not used as the Codex Agent session id.

File messages are downloaded from Feishu message resources and saved under
`~/.feishu-codex-bridge/attachments` by default, or under `:attachment-dir` when
configured. Codex receives a text input with the local absolute path instead of
the file bytes.

Outbound file replies use a Codex app-server dynamic tool named
`send_feishu_file`. The model passes a local path, and the listener validates the
path, uploads the file through Feishu OpenAPI, then replies in the current
Feishu thread with `msg_type=file`. The tool uses Feishu's 30 MB upload limit.

Sending `/stop` as a plain text Feishu message is treated as a bridge control
command. It is not forwarded to Codex as user input; instead, the listener asks
Codex app-server to interrupt the currently active turn for that Feishu session.

By default, Codex app-server uses the normal Codex home from the process
environment, usually `~/.codex`. This is the expected local development setup
because it reuses the existing Codex login/auth state. Only set
`:codex-home` when that alternate directory already has valid Codex auth;
otherwise Codex turns may fail with an OpenAI authentication error.
