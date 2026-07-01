(ns com.zihao.feishu-listener.runner
  "Babashka-friendly Feishu to Codex listener entrypoint."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [com.zihao.codex-agent.interface :as codex-agent]
   [com.zihao.feishu-listener.interface :as feishu])
  (:import [java.time Instant]))

(def ^:private diagnostic-prefix "feishu-codex diagnostic")

(defn- blank->nil
  [value]
  (when-not (str/blank? (str (or value "")))
    (str value)))

(defn- env
  [k]
  (blank->nil (System/getenv k)))

(defn- require-env!
  [k]
  (or (env k)
      (throw (ex-info "Missing required environment variable"
                      {:env k}))))

(defn- env-long
  [k default]
  (if-let [value (env k)]
    (Long/parseLong value)
    default))

(defn- env-bool
  [k default]
  (if-let [value (some-> (env k) str/lower-case)]
    (contains? #{"1" "true" "yes" "y" "on"} value)
    default))

(defn- diagnostics-enabled?
  []
  (env-bool "FEISHU_CODEX_DIAGNOSTICS" true))

(defn- default-store-path
  []
  (str (io/file (System/getProperty "user.home")
                ".feishu-codex-bridge"
                "sessions.edn")))

(defn- default-attachment-dir
  []
  (str (io/file (System/getProperty "user.home")
                ".feishu-codex-bridge"
                "attachments")))

(defn- codex-app-server-config
  []
  (cond-> {}
    (env "CODEX_MODEL") (assoc :model (env "CODEX_MODEL"))
    (env "CODEX_HOME") (assoc :codex-home (env "CODEX_HOME"))))

(defn- diagnostic-now
  []
  (str (Instant/now)))

(defn- diagnostic!
  [event]
  (binding [*out* *err*]
    (println diagnostic-prefix
             (pr-str (assoc event :at (diagnostic-now))))))

(defn- safe-diagnostic!
  [enabled? event]
  (when enabled?
    (try
      (diagnostic! event)
      (catch Throwable _))))

(defn- message-summary
  [message]
  (let [content (:content message)
        content-raw (:content-raw message)]
    (cond-> (select-keys message
                         [:message-id
                          :chat-id
                          :chat-type
                          :thread-id
                          :root-id
                          :parent-id
                          :message-type
                          :create-time
                          :update-time])
      (some? content) (assoc :content-present? true)
      (some? content-raw) (assoc :content-raw-present? true))))

(defn- safe-ex-data
  [throwable]
  (when-let [data (ex-data throwable)]
    (not-empty
     (select-keys data
                  [:type
                   :code
                   :status
                   :callback-key
                   :required-key
                   :required-any-of
                   :option
                   :timeout-ms]))))

(defn- throwable-summary
  [throwable]
  (when throwable
    (cond-> {:class (some-> throwable class .getName)}
      (blank->nil (.getMessage throwable))
      (assoc :message (.getMessage throwable))
      (safe-ex-data throwable)
      (assoc :data (safe-ex-data throwable)))))

(defn- listener-error-summary
  [{:keys [type throwable message callback-key]}]
  (cond-> {:phase :listener-error
           :type (or type :unknown)}
    callback-key (assoc :callback-key callback-key)
    message (assoc :message (message-summary message))
    throwable (assoc :error (throwable-summary throwable))))

(defn- codex-event-summary
  [{:keys [type throwable] :as event}]
  (when-not (contains? #{:codex-app-server/wire-message
                         :codex/command-delta}
                       type)
    (cond-> {:phase :codex-event
             :type (or type :unknown)}
      (:status event) (assoc :status (:status event))
      (:item-type event) (assoc :item-type (:item-type event))
      (:item-id event) (assoc :item-id (:item-id event))
      (:codex-thread-id event) (assoc :codex-thread-id (:codex-thread-id event))
      (:codex-turn-id event) (assoc :codex-turn-id (:codex-turn-id event))
      throwable (assoc :error (throwable-summary throwable)))))

(defn- feishu-reply-summary
  [event]
  (select-keys event
               [:type
                :message-id
                :reply-in-thread?
                :reply-thread-id
                :response]))

(defn- diagnostic-callbacks
  [diagnostics?]
  {:on-event!
   (fn [event]
     (when-let [summary (codex-event-summary event)]
       (safe-diagnostic! diagnostics? summary)))
   :on-feishu-reply!
   (fn [event]
     (safe-diagnostic! diagnostics?
                       (assoc (feishu-reply-summary event)
                              :phase :feishu-reply)))})

(defn- listener-config
  [agent-service diagnostics?]
  (cond-> {:app-id (require-env! "FEISHU_APP_ID")
           :app-secret (require-env! "FEISHU_APP_SECRET")
           :codex-agent-service agent-service
           :attachment-dir (or (env "FEISHU_CODEX_ATTACHMENT_DIR")
                               (default-attachment-dir))
           :reply-in-thread? (env-bool "FEISHU_REPLY_IN_THREAD" true)
           :persist-message! (fn [message]
                               (safe-diagnostic!
                                diagnostics?
                                {:phase :message-received
                                 :message (message-summary message)}))
           :codex-agent-callbacks (diagnostic-callbacks diagnostics?)
           :on-error (fn [{:keys [type throwable] :as event}]
                       (binding [*out* *err*]
                         (println "feishu-codex listener error"
                                  (or type :unknown)
                                  (some-> throwable .getMessage)))
                       (safe-diagnostic! diagnostics?
                                         (listener-error-summary event)))}
    (env "FEISHU_OPEN_BASE_URL") (assoc :open-base-url (env "FEISHU_OPEN_BASE_URL"))
    (env "FEISHU_DOMAIN") (assoc :domain (env "FEISHU_DOMAIN"))))

(defn- print-help
  []
  (println "Run the Feishu long-connection listener and route messages to Codex.")
  (println)
  (println "Required environment:")
  (println "  FEISHU_APP_ID")
  (println "  FEISHU_APP_SECRET")
  (println)
  (println "Optional environment:")
  (println "  FEISHU_CODEX_STORE_PATH     default: ~/.feishu-codex-bridge/sessions.edn")
  (println "  FEISHU_CODEX_ATTACHMENT_DIR default: ~/.feishu-codex-bridge/attachments")
  (println "  FEISHU_CONNECT_TIMEOUT_MS   default: 10000")
  (println "  FEISHU_REPLY_IN_THREAD      default: true")
  (println "  FEISHU_CODEX_DIAGNOSTICS    default: true")
  (println "  FEISHU_OPEN_BASE_URL")
  (println "  FEISHU_DOMAIN")
  (println "  CODEX_HOME")
  (println "  CODEX_MODEL"))

(defn- shutdown-promise
  []
  (let [p (promise)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. #(deliver p :shutdown)
                               "feishu-codex-bridge-shutdown"))
    p))

(defn -main
  [& args]
  (if (some #{"--help" "-h"} args)
    (print-help)
    (let [diagnostics? (diagnostics-enabled?)
          store-path (or (env "FEISHU_CODEX_STORE_PATH")
                         (default-store-path))
          attachment-dir (or (env "FEISHU_CODEX_ATTACHMENT_DIR")
                             (default-attachment-dir))
          agent-service (codex-agent/start!
                         {:store-path store-path
                          :codex-app-server (codex-app-server-config)})
          listener (feishu/make-listener (listener-config agent-service diagnostics?))
          shutdown (shutdown-promise)]
      (try
        (safe-diagnostic! diagnostics?
                          {:phase :startup
                           :store-path store-path
                           :attachment-dir attachment-dir
                           :codex-home-set? (boolean (env "CODEX_HOME"))
                           :model (or (env "CODEX_MODEL") :default)
                           :diagnostics? diagnostics?})
        (feishu/start! listener)
        (feishu/await-ready! listener (env-long "FEISHU_CONNECT_TIMEOUT_MS" 10000))
        (binding [*out* *err*]
          (println "feishu-codex listener connected")
          (println "session store:" store-path))
        (safe-diagnostic! diagnostics?
                          {:phase :listener-connected
                           :connection (:connection (feishu/state listener))
                           :store-path store-path})
        @shutdown
        (finally
          (safe-diagnostic! diagnostics?
                            {:phase :shutdown
                             :connection (:connection (feishu/state listener))})
          (try
            (feishu/stop! listener)
            (catch Throwable _))
          (try
            (codex-agent/close! agent-service)
            (catch Throwable _)))))))
