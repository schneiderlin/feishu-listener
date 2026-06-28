(ns com.zihao.feishu-listener.runner
  "Babashka-friendly Feishu to Codex listener entrypoint."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [com.zihao.codex-agent.interface :as codex-agent]
   [com.zihao.feishu-listener.interface :as feishu]))

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

(defn- default-store-path
  []
  (str (io/file (System/getProperty "user.home")
                ".feishu-codex-bridge"
                "sessions.edn")))

(defn- codex-app-server-config
  []
  (cond-> {}
    (env "CODEX_MODEL") (assoc :model (env "CODEX_MODEL"))
    (env "CODEX_HOME") (assoc :codex-home (env "CODEX_HOME"))))

(defn- listener-config
  [agent-service]
  (cond-> {:app-id (require-env! "FEISHU_APP_ID")
           :app-secret (require-env! "FEISHU_APP_SECRET")
           :codex-agent-service agent-service
           :reply-in-thread? (env-bool "FEISHU_REPLY_IN_THREAD" true)
           :on-error (fn [{:keys [type throwable]}]
                       (binding [*out* *err*]
                         (println "feishu-codex listener error"
                                  (or type :unknown)
                                  (some-> throwable .getMessage))))}
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
  (println "  FEISHU_CONNECT_TIMEOUT_MS   default: 10000")
  (println "  FEISHU_REPLY_IN_THREAD      default: true")
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
    (let [store-path (or (env "FEISHU_CODEX_STORE_PATH")
                         (default-store-path))
          agent-service (codex-agent/start!
                         {:store-path store-path
                          :codex-app-server (codex-app-server-config)})
          listener (feishu/make-listener (listener-config agent-service))
          shutdown (shutdown-promise)]
      (try
        (feishu/start! listener)
        (feishu/await-ready! listener (env-long "FEISHU_CONNECT_TIMEOUT_MS" 10000))
        (binding [*out* *err*]
          (println "feishu-codex listener connected")
          (println "session store:" store-path))
        @shutdown
        (finally
          (try
            (feishu/stop! listener)
            (catch Throwable _))
          (try
            (codex-agent/close! agent-service)
            (catch Throwable _)))))))
