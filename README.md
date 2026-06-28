# feishu-listener

Feishu app bot integration for receiving messages over the official Java SDK
WebSocket client and sending text messages through the Feishu IM API.

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
     :codex-app-server {:codex-home "var/codex-agent/codex-home"}}))

(def listener
  (feishu/make-listener
    {:app-id (System/getenv "FEISHU_APP_ID")
     :app-secret (System/getenv "FEISHU_APP_SECRET")
     :codex-agent-service agent}))
```

The adapter uses `thread-id` when Feishu supplies one, otherwise `chat-id`, as
Codex Agent's external session id. Final replies are sent with `reply-text!`.
