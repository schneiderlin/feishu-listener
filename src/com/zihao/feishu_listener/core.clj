(ns com.zihao.feishu-listener.core
  (:require
   [clojure.data.json :as json]
   [clojure.string :as str]
   [com.zihao.codex-agent.interface :as codex-agent])
  (:import
   [com.lark.oapi Client]
   [com.lark.oapi.event EventDispatcher]
   [com.lark.oapi.service.im ImService$P2MessageReceiveV1Handler]
   [com.lark.oapi.service.im.v1.model CreateMessageReq CreateMessageReqBody EventMessage EventSender MentionEvent ReplyMessageReq ReplyMessageReqBody UserId]
   [com.lark.oapi.ws Client$Builder]
   [java.nio.charset StandardCharsets]))

(def default-verification-token "")
(def default-encrypt-key "")

(defrecord Listener [client api-client dispatcher config !state])

(declare reply-text!)

(defn- blank->nil
  [value]
  (when-not (str/blank? value)
    value))

(defn- require-nonblank!
  [config k]
  (or (blank->nil (get config k))
      (throw (ex-info "Missing required Feishu listener config"
                      {:type :feishu-listener/missing-config
                       :required-key k
                       :provided-keys (sort (keys config))}))))

(defn- require-nonblank-option!
  [opts k]
  (or (blank->nil (get opts k))
      (throw (ex-info "Missing required Feishu option"
                      {:type :feishu-listener/missing-option
                       :required-key k
                       :provided-keys (sort (keys opts))}))))

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

(defn- callback-runnable
  [!state status callback on-error]
  (reify Runnable
    (run [_]
      (let [event {:type :feishu-listener/connection-status
                   :status status}]
        (swap! !state assoc :connection status)
        (try
          (when callback
            (callback event))
          (catch Throwable t
            (safe-callback! on-error
                            {:type :feishu-listener/lifecycle-callback-failed
                             :status status
                             :throwable t})))))))

(defn- json-content
  [content]
  (when-not (str/blank? content)
    (try
      (json/read-str content :key-fn keyword)
      (catch Exception _
        nil))))

(defn user-id->map
  [^UserId user-id]
  (when user-id
    (cond-> {}
      (blank->nil (.getUserId user-id)) (assoc :user-id (.getUserId user-id))
      (blank->nil (.getOpenId user-id)) (assoc :open-id (.getOpenId user-id))
      (blank->nil (.getUnionId user-id)) (assoc :union-id (.getUnionId user-id)))))

(defn mention->map
  [^MentionEvent mention]
  (when mention
    (cond-> {:id (user-id->map (.getId mention))}
      (blank->nil (.getKey mention)) (assoc :key (.getKey mention))
      (blank->nil (.getMentionedType mention)) (assoc :mentioned-type (.getMentionedType mention))
      (blank->nil (.getName mention)) (assoc :name (.getName mention))
      (blank->nil (.getTenantKey mention)) (assoc :tenant-key (.getTenantKey mention)))))

(defn sender->map
  [^EventSender sender]
  (when sender
    (cond-> {:id (user-id->map (.getSenderId sender))}
      (blank->nil (.getSenderType sender)) (assoc :type (.getSenderType sender))
      (blank->nil (.getTenantKey sender)) (assoc :tenant-key (.getTenantKey sender)))))

(defn message->map
  [^EventMessage message]
  (when message
    (let [content-raw (.getContent message)
          content (json-content content-raw)]
      (cond-> {:message-id (.getMessageId message)
               :root-id (.getRootId message)
               :parent-id (.getParentId message)
               :create-time (.getCreateTime message)
               :update-time (.getUpdateTime message)
               :chat-id (.getChatId message)
               :thread-id (.getThreadId message)
               :chat-type (.getChatType message)
               :message-type (.getMessageType message)
               :content content
               :content-raw content-raw
               :mentions (mapv mention->map (or (some-> message .getMentions seq) []))
               :user-agent (.getUserAgent message)}
        (nil? content) (assoc :content-parse-failed? (some? (blank->nil content-raw)))))))

(defn- header->map
  [header]
  (when header
    (cond-> {}
      (blank->nil (.getEventId header)) (assoc :event-id (.getEventId header))
      (blank->nil (.getEventType header)) (assoc :event-type (.getEventType header))
      (blank->nil (.getAppId header)) (assoc :app-id (.getAppId header))
      (blank->nil (.getTenantKey header)) (assoc :tenant-key (.getTenantKey header))
      (blank->nil (.getCreateTime header)) (assoc :create-time (.getCreateTime header)))))

(defn- event-raw-payload
  [event]
  (when-let [event-req (some-> event .getEventReq)]
    (or (blank->nil (.getPlain event-req))
        (when-let [body (.getBody event-req)]
          (String. ^bytes body StandardCharsets/UTF_8)))))

(defn event->message
  [event]
  (let [event-data (.getEvent event)
        sender (some-> event-data .getSender sender->map)
        message (some-> event-data .getMessage message->map)
        header (header->map (.getHeader event))]
    (merge
     {:type :feishu/message-received
      :schema (.getSchema event)
      :request-id (.getRequestId event)
      :tenant-key (.getTenantKey event)
      :header header
      :sender sender
      :message message
      :raw-payload (event-raw-payload event)
      :raw-event event}
     (select-keys message [:message-id
                           :chat-id
                           :chat-type
                           :thread-id
                           :message-type
                           :content
                           :content-raw
                           :create-time
                           :update-time]))))

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

(defn- p2-message-handler
  [{:keys [persist-message! on-message]} on-error]
  (proxy [ImService$P2MessageReceiveV1Handler] []
    (handle [event]
      (let [message (event->message event)]
        (call-message-callback! :persist-message! persist-message! message on-error)
        (call-message-callback! :on-message on-message message on-error)))))

(defn make-event-dispatcher
  [{:keys [verification-token encrypt-key on-error]
    :or {verification-token default-verification-token
         encrypt-key default-encrypt-key}
    :as config}]
  (let [verification-token (or verification-token default-verification-token)
        encrypt-key (or encrypt-key default-encrypt-key)
        callbacks (message-callbacks! config)]
    (-> (EventDispatcher/newBuilder verification-token encrypt-key)
        (.onP2MessageReceiveV1 (p2-message-handler callbacks on-error))
        (.build))))

(defn- make-client
  [{:keys [auto-reconnect? domain headers source
           on-reconnecting on-reconnected on-error]
    :or {auto-reconnect? true}
    :as config}
   dispatcher
   !state]
  (let [app-id (require-nonblank! config :app-id)
        app-secret (require-nonblank! config :app-secret)]
    (cond-> (doto (Client$Builder. app-id app-secret)
              (.eventHandler dispatcher)
              (.autoReconnect (boolean auto-reconnect?))
              (.onReconnecting (callback-runnable !state :reconnecting on-reconnecting on-error))
              (.onReconnected (callback-runnable !state :connected on-reconnected on-error)))
      (blank->nil domain) (.domain domain)
      (seq headers) (.headers headers)
      (blank->nil source) (.source source)
      true (.build))))

(defn make-api-client
  [{:keys [open-base-url source]
    :as config}]
  (let [app-id (require-nonblank! config :app-id)
        app-secret (require-nonblank! config :app-secret)]
    (cond-> (Client/newBuilder app-id app-secret)
      (blank->nil open-base-url) (.openBaseUrl open-base-url)
      (blank->nil source) (.source source)
      true (.build))))

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
      (blank->nil (:content-raw message))
      (blank->nil (get-in message [:message :content-raw]))))

(defn message->codex-agent-message
  [message]
  (let [text (or (message-text message) "")]
    {:channel :feishu
     :external-session-id (feishu-external-session-id message)
     :external-message-id (:message-id message)
     :content [{:type :text
                :text text}]}))

(defn handle-codex-agent-message!
  ([codex-agent-service reply-target message]
   (handle-codex-agent-message! codex-agent-service reply-target message {}))
  ([codex-agent-service reply-target message opts]
   (let [agent-message (message->codex-agent-message message)
         bootstrap-session? (nil? (feishu-thread-id message))
         reply-in-thread? (get opts :reply-in-thread? true)
         callbacks (merge (:codex-agent-callbacks opts)
                          {:on-reply!
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
                                 (assoc :promote-external-session-id reply-thread-id))))})]
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
    - :on-message, called with the normalized message map before SDK ACK success

  Optional config:
  - :verification-token, :encrypt-key
  - :auto-reconnect?, :domain, :headers, :source
  - :on-reconnecting, :on-reconnected, :on-error
  - :codex-agent-service to route messages through Codex Agent"
  [config]
  (let [!state (atom {:started? false
                      :connection :new})
        api-client (make-api-client config)
        callback-config (config-with-codex-agent-handler config api-client)
        dispatcher (make-event-dispatcher callback-config)
        client (make-client config dispatcher !state)]
    (->Listener client api-client dispatcher callback-config !state)))

(defn start!
  [^Listener listener]
  (.start (:client listener))
  (swap! (:!state listener) assoc
         :started? true
         :connection :connecting)
  listener)

(defn await-ready!
  ([listener]
   (await-ready! listener 10000))
  ([^Listener listener timeout-ms]
   (.awaitReady (:client listener) (long timeout-ms))
   (swap! (:!state listener) assoc
          :started? true
          :connection :connected)
   listener))

(defn stop!
  [^Listener listener]
  (.close (:client listener))
  (swap! (:!state listener) assoc
         :started? false
         :connection :closed)
  listener)

(defn state
  [^Listener listener]
  @(:!state listener))

(defn raw-client
  [^Listener listener]
  (:client listener))

(defn raw-api-client
  [^Listener listener]
  (:api-client listener))

(defn text-content
  [text]
  (json/write-str {:text text}))

(defn create-text-message-req
  [{:keys [chat-id receive-id receive-id-type uuid]
    :or {receive-id-type "chat_id"}
    :as opts}]
  (let [receive-id (or (blank->nil receive-id) (blank->nil chat-id))
        _ (or receive-id
              (throw (ex-info "Missing Feishu receive id"
                              {:type :feishu-listener/missing-option
                               :required-any-of [:receive-id :chat-id]
                               :provided-keys (sort (keys opts))})))
        text (require-nonblank-option! opts :text)]
    (-> (CreateMessageReq/newBuilder)
        (.receiveIdType receive-id-type)
        (.createMessageReqBody
         (-> (cond-> (CreateMessageReqBody/newBuilder)
               true (.receiveId receive-id)
               true (.msgType "text")
               true (.content (text-content text))
               (blank->nil uuid) (.uuid uuid))
             (.build)))
        (.build))))

(defn reply-text-message-req
  [{:keys [reply-in-thread? uuid]
    :as opts}]
  (let [message-id (require-nonblank-option! opts :message-id)
        text (require-nonblank-option! opts :text)]
    (-> (ReplyMessageReq/newBuilder)
        (.messageId message-id)
        (.replyMessageReqBody
         (-> (cond-> (ReplyMessageReqBody/newBuilder)
               true (.msgType "text")
               true (.content (text-content text))
               (some? reply-in-thread?) (.replyInThread (boolean reply-in-thread?))
               (blank->nil uuid) (.uuid uuid))
             (.build)))
        (.build))))

(defn- target-api-client
  [target]
  (cond
    (instance? Listener target)
    (:api-client target)

    (instance? Client target)
    target

    (map? target)
    (make-api-client target)

    :else
    (throw (ex-info "Unsupported Feishu API target"
                    {:type :feishu-listener/unsupported-api-target
                     :target-class (some-> target class str)}))))

(defn- sent-message->map
  [message]
  (when message
    (cond-> {}
      (blank->nil (.getMessageId message)) (assoc :message-id (.getMessageId message))
      (blank->nil (.getRootId message)) (assoc :root-id (.getRootId message))
      (blank->nil (.getParentId message)) (assoc :parent-id (.getParentId message))
      (blank->nil (.getThreadId message)) (assoc :thread-id (.getThreadId message))
      (blank->nil (.getMsgType message)) (assoc :message-type (.getMsgType message))
      (blank->nil (.getCreateTime message)) (assoc :create-time (.getCreateTime message))
      (blank->nil (.getUpdateTime message)) (assoc :update-time (.getUpdateTime message))
      (some? (.getDeleted message)) (assoc :deleted? (.getDeleted message))
      (some? (.getUpdated message)) (assoc :updated? (.getUpdated message))
      (blank->nil (.getChatId message)) (assoc :chat-id (.getChatId message))
      (blank->nil (.getUpperMessageId message)) (assoc :upper-message-id (.getUpperMessageId message))
      (blank->nil (.getMessageAppLink message)) (assoc :message-app-link (.getMessageAppLink message)))))

(defn response->map
  [response]
  (if (.success response)
    (cond-> {:ok? true
             :code (.getCode response)
             :message (.getMsg response)
             :request-id (.getRequestId response)}
      (.getData response) (assoc :data (sent-message->map (.getData response))))
    (throw (ex-info "Feishu API request failed"
                    {:type :feishu-listener/api-error
                     :code (.getCode response)
                     :message (.getMsg response)
                     :request-id (.getRequestId response)
                     :raw-response response}))))

(defn send-text!
  [target opts]
  (let [api-client (target-api-client target)
        req (create-text-message-req opts)
        response (-> api-client .im .message (.create req))]
    (response->map response)))

(defn reply-text!
  ([target message text]
   (reply-text! target (assoc (select-keys message [:message-id]) :text text)))
  ([target opts]
   (let [api-client (target-api-client target)
         req (reply-text-message-req opts)
         response (-> api-client .im .message (.reply req))]
     (response->map response))))

(defn dispatch-raw-event!
  "Run a raw Feishu event JSON payload through the SDK dispatcher.

  This mirrors the SDK path used by the WebSocket client after it receives a
  data frame. It is useful for focused tests and local replay; production
  WebSocket listening should use start!."
  [^Listener listener raw-json]
  (.doWithoutValidation (:dispatcher listener)
                        (.getBytes (str raw-json) StandardCharsets/UTF_8)))
