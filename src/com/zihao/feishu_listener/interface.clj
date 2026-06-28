(ns com.zihao.feishu-listener.interface
  (:require
   [com.zihao.feishu-listener.core :as core]))

(defn make-listener
  [config]
  (core/make-listener config))

(defn start!
  [listener]
  (core/start! listener))

(defn await-ready!
  ([listener]
   (core/await-ready! listener))
  ([listener timeout-ms]
   (core/await-ready! listener timeout-ms)))

(defn stop!
  [listener]
  (core/stop! listener))

(defn state
  [listener]
  (core/state listener))

(defn raw-client
  [listener]
  (core/raw-client listener))

(defn raw-api-client
  [listener]
  (core/raw-api-client listener))

(defn event->message
  [event]
  (core/event->message event))

(defn send-text!
  [target opts]
  (core/send-text! target opts))

(defn reply-text!
  ([target message text]
   (core/reply-text! target message text))
  ([target opts]
   (core/reply-text! target opts)))
