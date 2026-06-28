(ns com.zihao.feishu-listener.core
  (:require
   [clojure.string :as str]
   [com.zihao.codex-agent.interface :as codex-agent]
   [com.zihao.feishu-openapi-lite.interface :as feishu-lite])
  (:import [java.util Base64]))

(def default-verification-token "")
(def default-encrypt-key "")
(def ^:private codex-progress-output-max-chars 3000)

(defrecord Listener [client api-client dispatcher config !state])

(declare reply-text!)
(declare state)
(declare target-api-client)

(defn- blank->nil
  [value]
  (when-not (str/blank? (str (or value "")))
    value))

(defn- require-nonblank!
  [config k]
  (or (blank->nil (get config k))
      (throw (ex-info "Missing required Feishu listener config"
                      {:type :feishu-listener/missing-config
                       :required-key k
                       :provided-keys (sort (keys config))}))))

(defn- callback-or-nil!
  [config k]
  (let [callback (get config k)]
    (when (and (some? callback)
               (not (fn? callback)))
      (throw (ex-info "Feishu listener callback must be a function"
                      {:type :feishu-listener/invalid-callback
                       :callback-key k})))
    callback))

(defn- message-callbacks!
  [config]
  (let [persist-message! (callback-or-nil! config :persist-message!)
        on-message (callback-or-nil! config :on-message)]
    (when-not (or persist-message! on-message)
      (throw (ex-info "Feishu listener requires a message callback"
                      {:type :feishu-listener/missing-message-callback
                       :required-any-of [:persist-message! :on-message]
                       :provided-keys (sort (keys config))})))
    {:persist-message! persist-message!
     :on-message on-message}))

(defn- safe-callback!
  [callback event]
  (when callback
    (try
      (callback event)
      (catch Throwable _))))

(defn- raw-payload-text
  [event]
  (cond
    (string? event) event
    (bytes? event) (String. ^bytes event "UTF-8")
    :else nil))

(defn event->message
  ([event]
   (event->message event (raw-payload-text event)))
  ([event raw-payload]
   (let [raw-event (feishu-lite/parse-event-payload event)
         normalized (feishu-lite/normalize-p2-event raw-event)
         message (:message normalized)]
     (merge
      {:type :feishu/message-received
       :schema (:schema normalized)
       :tenant-key (get-in normalized [:header :tenant-key])
       :header (:header normalized)
       :sender (:sender normalized)
       :message message
       :raw-payload raw-payload
       :raw-event raw-event}
      (select-keys message [:message-id
                            :chat-id
                            :chat-type
                            :thread-id
                            :message-type
                            :content
                            :content-raw
                            :create-time
                            :update-time])))))

(defn- call-message-callback!
  [callback-key callback message on-error]
  (when callback
    (try
      (callback message)
      (catch Throwable t
        (safe-callback! on-error
                        {:type :feishu-listener/message-callback-failed
                         :callback-key callback-key
                         :message message
                         :throwable t})
        (throw t)))))

(defn make-event-dispatcher
  [{:keys [on-error] :as config}]
  (let [{:keys [persist-message! on-message]} (message-callbacks! config)]
    (fn [event]
      (let [raw-payload (raw-payload-text event)
            raw-event (feishu-lite/parse-event-payload event)]
        (when (feishu-lite/message-received-event? raw-event)
          (let [message (event->message raw-event raw-payload)]
            (call-message-callback! :persist-message! persist-message! message on-error)
            (call-message-callback! :on-message on-message message on-error)))))))

(defn- api-domain
  [config]
  (or (blank->nil (:open-base-url config))
      (blank->nil (:domain config))))

(defn- lite-client
  [config domain]
  (cond-> {:app-id (require-nonblank! config :app-id)
           :app-secret (require-nonblank! config :app-secret)
           :token* (or (:token* config) (atom nil))}
    (blank->nil domain) (assoc :domain domain)
    (:tenant-access-token config) (assoc :tenant-access-token (:tenant-access-token config))
    (:request! config) (assoc :request! (:request! config))
    (:user-agent config) (assoc :user-agent (:user-agent config))
    (:timeout-ms config) (assoc :timeout-ms (:timeout-ms config))))

(defn make-api-client
  [config]
  (lite-client config (api-domain config)))

(defn- websocket-error-callback
  [!state on-error]
  (fn [error]
    (swap! !state assoc
           :connection :error
           :error error)
    (safe-callback! on-error
                    {:type :feishu-listener/websocket-error
                     :throwable error})))

(defn- make-websocket-client-config
  [{:keys [domain on-error] :as config} dispatcher !state]
  (assoc (lite-client config domain)
         :on-event dispatcher
         :on-error (websocket-error-callback !state on-error)))

(defn- feishu-thread-id
  [message]
  (or (blank->nil (:thread-id message))
      (blank->nil (get-in message [:message :thread-id]))))

(defn- feishu-message-id
  [message]
  (or (blank->nil (:message-id message))
      (blank->nil (get-in message [:message :message-id]))))

(defn- bootstrap-session-id
  [message]
  (when-let [message-id (feishu-message-id message)]
    (str "bootstrap:" message-id)))

(defn feishu-external-session-id
  [message]
  (or (feishu-thread-id message)
      (bootstrap-session-id message)
      (throw (ex-info "Feishu message has no reusable session id"
                      {:type :feishu-listener/missing-session-id
                       :message-id (:message-id message)}))))

(defn- message-text
  [message]
  (or (blank->nil (get-in message [:content :text]))
      (blank->nil (get-in message [:message :content :text]))
      (when-not (= "image" (:message-type message))
        (or (blank->nil (:content-raw message))
            (blank->nil (get-in message [:message :content-raw]))))))

(defn- image-key
  [message]
  (or (blank->nil (get-in message [:content :image_key]))
      (blank->nil (get-in message [:content :image-key]))
      (blank->nil (get-in message [:message :content :image_key]))
      (blank->nil (get-in message [:message :content :image-key]))))

(defn- image-message?
  [message]
  (= "image" (:message-type message)))

(defn- response-content-type->mime
  [content-type]
  (when-let [value (blank->nil content-type)]
    (some-> value
            (str/split #";")
            first
            str/trim)))

(defn- bytes-mime
  [bytes content-type]
  (let [n (alength ^bytes bytes)]
    (cond
      (and (>= n 4)
           (= (aget ^bytes bytes 0) (byte -119))
           (= (aget ^bytes bytes 1) (byte 80))
           (= (aget ^bytes bytes 2) (byte 78))
           (= (aget ^bytes bytes 3) (byte 71)))
      "image/png"

      (and (>= n 2)
           (= (aget ^bytes bytes 0) (byte -1))
           (= (aget ^bytes bytes 1) (byte -40)))
      "image/jpeg"

      (and (>= n 4)
           (= "GIF8" (String. ^bytes bytes 0 4 "US-ASCII")))
      "image/gif"

      :else
      (or (response-content-type->mime content-type)
          "application/octet-stream"))))

(defn- data-url
  [{:keys [bytes content-type]}]
  (str "data:"
       (bytes-mime bytes content-type)
       ";base64,"
       (.encodeToString (Base64/getEncoder) ^bytes bytes)))

(defn- image-download-failure-reason
  [throwable]
  (let [data (ex-data throwable)]
    (or (when-let [status (:status data)]
          (str "HTTP " status))
        (some-> (:type data) name)
        (blank->nil (.getMessage throwable))
        (some-> throwable class .getSimpleName))))

(defn- image-download-fallback-text
  [message throwable]
  (str "用户发送了一张图片，但系统下载图片内容失败了。"
       (when-let [message-id (feishu-message-id message)]
         (str "\nFeishu message id: " message-id))
       (when-let [key (image-key message)]
         (str "\nFeishu image key: " key))
       (when-let [reason (image-download-failure-reason throwable)]
         (str "\n下载失败原因: " reason))
       "\n请根据这个情况决定如何回应。"))

(defn- missing-image-key-fallback-text
  [message]
  (str "用户发送了一张图片，但消息内容里没有可下载的 image_key。"
       (when-let [message-id (feishu-message-id message)]
         (str "\nFeishu message id: " message-id))
       "\n请根据这个情况决定如何回应。"))

(defn- text-codex-content
  [message]
  [{:type :text
    :text (or (message-text message)
              (when (image-message? message)
                "用户发送了一张图片。")
              "")}])

(defn- image-codex-content!
  [reply-target message]
  (if-let [key (image-key message)]
    (try
      (let [api-client (target-api-client reply-target)
            resource (feishu-lite/download-message-resource!
                      api-client
                      {:message-id (feishu-message-id message)
                       :file-key key
                       :type "image"})]
        [{:type :text
          :text "用户发送了一张图片。"}
         {:type :image
          :url (data-url resource)}])
      (catch Throwable t
        [{:type :text
          :text (image-download-fallback-text message t)}]))
    [{:type :text
      :text (missing-image-key-fallback-text message)}]))

(defn message->codex-agent-message
  [message]
  {:channel :feishu
   :external-session-id (feishu-external-session-id message)
   :external-message-id (:message-id message)
   :content (text-codex-content message)})

(defn- message->codex-agent-message!
  [reply-target message]
  (cond-> (message->codex-agent-message message)
    (image-message? message)
    (assoc :content (image-codex-content! reply-target message))))

(defn- value-text
  [value]
  (cond
    (nil? value) nil
    (string? value) value
    :else (pr-str value)))

(defn- truncated-text
  [value]
  (when-let [text (blank->nil (value-text value))]
    (let [limit codex-progress-output-max-chars]
      (if (<= (count text) limit)
        text
        (str (subs text 0 limit) "\n... output truncated ...")))))

(defn- command-label
  [event]
  (when-let [command (blank->nil (value-text (:command event)))]
    (str ": " command)))

(defn- exit-zero?
  [exit-code]
  (cond
    (number? exit-code) (zero? (long exit-code))
    (string? exit-code) (= "0" (str/trim exit-code))
    :else false))

(defn- command-failed?
  [event]
  (or (= "failed" (:status event))
      (and (some? (:exit-code event))
           (not (exit-zero? (:exit-code event))))))

(defn- command-completed-text
  [event]
  (let [exit-code (:exit-code event)
        base (str "Command "
                  (if (command-failed? event) "failed" "completed")
                  (when (some? exit-code)
                    (str " (exit " exit-code ")"))
                  (command-label event))]
    (if-let [output (truncated-text (:output event))]
      (str base "\n\nOutput:\n" output)
      base)))

(defn- input-text-content
  [item]
  (when (and (= "inputText" (:type item))
             (string? (:text item)))
    (:text item)))

(defn- dynamic-tool-result-text
  [event]
  (->> (or (:content-items event) [])
       (keep input-text-content)
       (str/join "\n")
       truncated-text))

(defn- tool-label
  [event]
  (when-let [tool (blank->nil (value-text (:tool event)))]
    (str ": " tool)))

(defn- dynamic-tool-completed-text
  [event]
  (let [base (str "Tool "
                  (if (false? (:success event)) "failed" "completed")
                  (tool-label event))]
    (if-let [result (dynamic-tool-result-text event)]
      (str base "\n\nResult:\n" result)
      base)))

(defn codex-progress-reply-text
  [event]
  (case (:type event)
    :codex/item-started
    (case (:item-type event)
      "commandExecution" (str "Command started" (command-label event))
      "dynamicToolCall" (str "Tool started" (tool-label event))
      nil)

    :codex/item-completed
    (case (:item-type event)
      "commandExecution" (command-completed-text event)
      "dynamicToolCall" (dynamic-tool-completed-text event)
      nil)

    nil))

(defn- codex-progress-reply-uuid
  [agent-message event]
  (str "codex-agent-progress-"
       (Integer/toUnsignedString
        (int (hash [(:external-message-id agent-message)
                    (:type event)
                    (:item-id event)
                    (:status event)]))
        36)))

(defn- reply-codex-progress!
  [reply-target message agent-message reply-in-thread? event]
  (when-let [text (codex-progress-reply-text event)]
    (reply-text! reply-target
                 {:message-id (feishu-message-id message)
                  :text text
                  :reply-in-thread? reply-in-thread?
                  :uuid (codex-progress-reply-uuid agent-message event)})))

(defn- codex-agent-event-callback
  [base-on-event! reply-target message agent-message reply-in-thread?]
  (fn [event]
    (safe-callback! base-on-event! event)
    (reply-codex-progress! reply-target
                           message
                           agent-message
                           reply-in-thread?
                           event)))

(defn handle-codex-agent-message!
  ([codex-agent-service reply-target message]
   (handle-codex-agent-message! codex-agent-service reply-target message {}))
  ([codex-agent-service reply-target message opts]
   (let [agent-message (message->codex-agent-message! reply-target message)
         bootstrap-session? (nil? (feishu-thread-id message))
         reply-in-thread? (get opts :reply-in-thread? true)
         base-callbacks (or (:codex-agent-callbacks opts) {})
         callbacks (assoc base-callbacks
                          :on-event!
                          (codex-agent-event-callback (:on-event! base-callbacks)
                                                      reply-target
                                                      message
                                                      agent-message
                                                      reply-in-thread?)
                          :on-reply!
                          (fn [{:keys [text]}]
                            (let [reply-result
                                  (reply-text! reply-target
                                               (cond-> {:message-id (:message-id message)
                                                        :text text
                                                        :reply-in-thread? reply-in-thread?}
                                                 (:external-message-id agent-message)
                                                 (assoc :uuid (str "codex-agent-"
                                                                   (:external-message-id agent-message)))))
                                  reply-thread-id (get-in reply-result [:data :thread-id])]
                              (cond-> {:feishu-response reply-result}
                                (and bootstrap-session?
                                     (blank->nil reply-thread-id))
                                (assoc :promote-external-session-id reply-thread-id)))))]
     (codex-agent/handle-message! codex-agent-service agent-message callbacks))))

(defn codex-agent-message-handler
  ([codex-agent-service reply-target]
   (codex-agent-message-handler codex-agent-service reply-target {}))
  ([codex-agent-service reply-target opts]
   (fn [message]
     (handle-codex-agent-message! codex-agent-service reply-target message opts))))

(defn- config-with-codex-agent-handler
  [config reply-target]
  (if-let [codex-agent-service (:codex-agent-service config)]
    (let [existing-on-message (:on-message config)
          handler (codex-agent-message-handler codex-agent-service
                                               reply-target
                                               (select-keys config
                                                            [:reply-in-thread?
                                                             :codex-agent-callbacks]))]
      (assoc config
             :on-message (fn [message]
                           (when existing-on-message
                             (existing-on-message message))
                           (handler message))))
    config))

(defn make-listener
  "Build a Feishu WebSocket listener without opening the connection.

  Required config:
  - :app-id
  - :app-secret
  - at least one of:
    - :persist-message!, called with the normalized message map before :on-message
    - :on-message, called with the normalized message map before ACK success

  Optional config:
  - :domain, :open-base-url, :request!, :tenant-access-token, :token*
  - :on-error
  - :codex-agent-service to route messages through Codex Agent"
  [config]
  (let [!state (atom {:started? false
                      :connection :new})
        api-client (make-api-client config)
        callback-config (config-with-codex-agent-handler config api-client)
        dispatcher (make-event-dispatcher callback-config)
        client* (atom nil)]
    (->Listener client* api-client dispatcher callback-config !state)))

(defn start!
  [^Listener listener]
  (swap! (:!state listener) assoc
         :started? true
         :connection :connecting)
  (try
    (let [client (feishu-lite/start-event-ws!
                  (make-websocket-client-config (:config listener)
                                                (:dispatcher listener)
                                                (:!state listener)))]
      (reset! (:client listener) client)
      (swap! (:!state listener) merge
             {:started? true}
             (select-keys (feishu-lite/event-ws-state client)
                          [:connection :url :client-config]))
      listener)
    (catch Throwable t
      (swap! (:!state listener) assoc
             :started? false
             :connection :error
             :error t)
      (throw t))))

(defn await-ready!
  ([listener]
   (await-ready! listener 10000))
  ([^Listener listener timeout-ms]
   (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
     (loop []
       (let [connection (:connection (state listener))]
         (cond
           (= :connected connection)
           listener

           (< (System/currentTimeMillis) deadline)
           (do
             (Thread/sleep 50)
             (recur))

           :else
           (throw (ex-info "Timed out waiting for Feishu listener connection"
                           {:type :feishu-listener/await-ready-timeout
                            :timeout-ms timeout-ms
                            :state (state listener)}))))))))

(defn stop!
  [^Listener listener]
  (when-let [client @(:client listener)]
    (feishu-lite/close-event-ws! client))
  (swap! (:!state listener) assoc
         :started? false
         :connection :closed)
  listener)

(defn state
  [^Listener listener]
  (merge @(:!state listener)
         (when-let [client @(:client listener)]
           (select-keys (feishu-lite/event-ws-state client)
                        [:connection :url :client-config :error :close-status :close-reason]))))

(defn raw-client
  [^Listener listener]
  @(:client listener))

(defn raw-api-client
  [^Listener listener]
  (:api-client listener))

(defn text-content
  [text]
  (feishu-lite/text-content text))

(defn- target-api-client
  [target]
  (cond
    (instance? Listener target)
    (:api-client target)

    (and (map? target)
         (or (:app-id target)
             (:app-secret target)))
    (make-api-client target)

    (map? target)
    target

    :else
    (throw (ex-info "Unsupported Feishu API target"
                    {:type :feishu-listener/unsupported-api-target
                     :target-class (some-> target class str)}))))

(defn response->map
  [response]
  (if (:ok? response)
    response
    (feishu-lite/normalize-message-response response)))

(defn send-text!
  [target opts]
  (feishu-lite/send-text! (target-api-client target) opts))

(defn reply-text!
  ([target message text]
   (reply-text! target (assoc (select-keys message [:message-id]) :text text)))
  ([target opts]
   (feishu-lite/reply-text! (target-api-client target) opts)))

(defn dispatch-raw-event!
  "Run a raw Feishu p2 event JSON payload through the listener dispatcher.

  This mirrors the payload path used by the lite WebSocket client after it
  receives a data frame. Production WebSocket listening should use start!."
  [^Listener listener raw-json]
  ((:dispatcher listener) raw-json))
