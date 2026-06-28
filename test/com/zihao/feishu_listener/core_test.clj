(ns com.zihao.feishu-listener.core-test
  (:require
   [clojure.data.json :as json]
   [clojure.test :refer [deftest is testing]]
   [com.zihao.codex-agent.interface :as codex-agent]
   [com.zihao.feishu-listener.core :as sut])
  (:import
   [com.lark.oapi.core.request EventReq]
   [com.lark.oapi.event.model Header]
   [com.lark.oapi.service.im.v1.model CreateMessageResp CreateMessageRespBody EventMessage EventSender MentionEvent P2MessageReceiveV1 P2MessageReceiveV1Data ReplyMessageResp ReplyMessageRespBody UserId]
   [java.nio.charset StandardCharsets]))

(defn- user-id
  [{:keys [user-id open-id union-id]}]
  (doto (UserId.)
    (.setUserId user-id)
    (.setOpenId open-id)
    (.setUnionId union-id)))

(defn- mention
  []
  (doto (MentionEvent.)
    (.setKey "@_user_1")
    (.setId (user-id {:user-id "u_mention"
                      :open-id "ou_mention"
                      :union-id "on_mention"}))
    (.setMentionedType "user")
    (.setName "Mentioned User")
    (.setTenantKey "tenant_1")))

(defn- sample-java-event
  []
  (let [header (doto (Header.)
                 (.setEventId "ev_1")
                 (.setEventType "im.message.receive_v1")
                 (.setAppId "cli_1")
                 (.setTenantKey "tenant_1")
                 (.setCreateTime "1780000000000"))
        sender (doto (EventSender.)
                 (.setSenderId (user-id {:user-id "u_sender"
                                          :open-id "ou_sender"
                                          :union-id "on_sender"}))
                 (.setSenderType "user")
                 (.setTenantKey "tenant_1"))
        message (doto (EventMessage.)
                  (.setMessageId "om_1")
                  (.setRootId "om_root")
                  (.setParentId "om_parent")
                  (.setCreateTime "1780000000001")
                  (.setUpdateTime "1780000000002")
                  (.setChatId "oc_1")
                  (.setThreadId "omt_1")
                  (.setChatType "group")
                  (.setMessageType "text")
                  (.setContent "{\"text\":\"hello\"}")
                  (.setMentions (into-array MentionEvent [(mention)]))
                  (.setUserAgent "test-agent"))
        data (doto (P2MessageReceiveV1Data.)
               (.setSender sender)
               (.setMessage message))
        raw "{\"schema\":\"2.0\"}"
        event-req (doto (EventReq.)
                    (.setPlain raw)
                    (.setBody (.getBytes raw StandardCharsets/UTF_8)))]
    (doto (P2MessageReceiveV1.)
      (.setSchema "2.0")
      (.setHeader header)
      (.setEvent data)
      (.setEventReq event-req))))

(defn- sample-raw-event-json
  [chat-type text]
  (json/write-str
   {:schema "2.0"
    :header {:event_id (str "ev_" chat-type)
             :event_type "im.message.receive_v1"
             :app_id "cli_1"
             :tenant_key "tenant_1"
             :create_time "1780000000000"}
    :event {:sender {:sender_id {:user_id "u_sender"
                                  :open_id "ou_sender"
                                  :union_id "on_sender"}
                     :sender_type "user"
                     :tenant_key "tenant_1"}
            :message {:message_id (str "om_" chat-type)
                      :root_id ""
                      :parent_id ""
                      :create_time "1780000000001"
                      :update_time "1780000000002"
                      :chat_id (if (= "group" chat-type) "oc_group" "oc_p2p")
                      :thread_id ""
                      :chat_type chat-type
                      :message_type "text"
                      :content (json/write-str {:text text})
                      :mentions []}}}))

(deftest event->message-normalizes-sdk-message-event
  (testing "SDK event objects become Clojure maps with parsed content and raw payload"
    (let [message (sut/event->message (sample-java-event))]
      (is (= {:event-id "ev_1"
              :event-type "im.message.receive_v1"
              :app-id "cli_1"
              :tenant-key "tenant_1"
              :create-time "1780000000000"}
             (:header message)))
      (is (= {:id {:user-id "u_sender"
                   :open-id "ou_sender"
                   :union-id "on_sender"}
              :type "user"
              :tenant-key "tenant_1"}
             (:sender message)))
      (is (= "om_1" (:message-id message)))
      (is (= "oc_1" (:chat-id message)))
      (is (= "group" (:chat-type message)))
      (is (= {:text "hello"} (:content message)))
      (is (= [{:id {:user-id "u_mention"
                    :open-id "ou_mention"
                    :union-id "on_mention"}
               :key "@_user_1"
               :mentioned-type "user"
               :name "Mentioned User"
               :tenant-key "tenant_1"}]
             (get-in message [:message :mentions])))
      (is (= "{\"schema\":\"2.0\"}" (:raw-payload message))))))

(deftest dispatcher-delivers-group-and-direct-messages-without-code-filtering
  (testing "the component subscribes to receive-message events and does not filter chat_type"
    (let [persisted (atom [])
          delivered (atom [])
          listener (sut/make-listener {:app-id "cli_1"
                                       :app-secret "secret"
                                       :persist-message! #(swap! persisted conj [:persist (:chat-type %)])
                                       :on-message #(swap! delivered conj %)})]
      (sut/dispatch-raw-event! listener (sample-raw-event-json "group" "group hello"))
      (sut/dispatch-raw-event! listener (sample-raw-event-json "p2p" "dm hello"))
      (is (= [[:persist "group"] [:persist "p2p"]]
             @persisted))
      (is (= ["group" "p2p"]
             (mapv :chat-type @delivered)))
      (is (= [{:text "group hello"} {:text "dm hello"}]
             (mapv :content @delivered))))))

(deftest message-callback-errors-propagate-to-sdk-dispatcher
  (testing "persistence callback failure is not swallowed before ACK success"
    (let [errors (atom [])
          delivered (atom [])
          listener (sut/make-listener {:app-id "cli_1"
                                       :app-secret "secret"
                                       :persist-message! (fn [_message]
                                                           (throw (ex-info "persist failed" {})))
                                       :on-message #(swap! delivered conj %)
                                       :on-error #(swap! errors conj %)})]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"persist failed"
           (sut/dispatch-raw-event! listener (sample-raw-event-json "group" "will retry"))))
      (is (= [] @delivered))
      (is (= :feishu-listener/message-callback-failed
             (:type (first @errors))))
      (is (= :persist-message!
             (:callback-key (first @errors))))
      (is (= "om_group"
             (get-in (first @errors) [:message :message-id]))))))

(deftest create-text-message-request-builds-feishu-text-payload
  (testing "chat-id text sends use Feishu's message.create request shape"
    (let [req (sut/create-text-message-req {:chat-id "oc_1"
                                            :text "hello back"
                                            :uuid "uuid-1"})
          body (.getCreateMessageReqBody req)]
      (is (= "chat_id" (.getReceiveIdType req)))
      (is (= "oc_1" (.getReceiveId body)))
      (is (= "text" (.getMsgType body)))
      (is (= "{\"text\":\"hello back\"}" (.getContent body)))
      (is (= "uuid-1" (.getUuid body))))))

(deftest reply-text-message-request-builds-feishu-reply-payload
  (testing "message-id text replies use Feishu's message.reply request shape"
    (let [req (sut/reply-text-message-req {:message-id "om_1"
                                           :text "reply"
                                           :reply-in-thread? true
                                           :uuid "uuid-2"})
          body (.getReplyMessageReqBody req)]
      (is (= "om_1" (.getMessageId req)))
      (is (= "text" (.getMsgType body)))
      (is (= "{\"text\":\"reply\"}" (.getContent body)))
      (is (= true (.getReplyInThread body)))
      (is (= "uuid-2" (.getUuid body))))))

(deftest response->map-normalizes-success-and-throws-on-api-error
  (testing "successful Feishu API responses return a Clojure message summary"
    (let [data (doto (CreateMessageRespBody.)
                 (.setMessageId "om_sent")
                 (.setChatId "oc_1")
                 (.setMsgType "text")
                 (.setCreateTime "1780000000100")
                 (.setMessageAppLink "https://example.invalid/message"))
          resp (doto (CreateMessageResp.)
                 (.setCode 0)
                 (.setMsg "success")
                 (.setData data))
          result (sut/response->map resp)]
      (is (true? (:ok? result)))
      (is (= {:message-id "om_sent"
              :chat-id "oc_1"
              :message-type "text"
              :create-time "1780000000100"
              :message-app-link "https://example.invalid/message"}
             (:data result)))))
  (testing "Feishu API failures throw with the API error data"
    (let [resp (doto (ReplyMessageResp.)
                 (.setCode 999)
                 (.setMsg "failed")
                 (.setData (ReplyMessageRespBody.)))]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Feishu API request failed"
           (sut/response->map resp))))))

(deftest message->codex-agent-message-chooses-thread-or-chat-session
  (testing "threaded Feishu messages use thread-id as the external session"
    (is (= {:channel :feishu
            :external-session-id "omt_1"
            :external-message-id "om_1"
            :content [{:type :text :text "hello"}]}
           (sut/message->codex-agent-message
            {:message-id "om_1"
             :chat-id "oc_1"
             :thread-id "omt_1"
             :content {:text "hello"}}))))
  (testing "non-thread messages fall back to chat-id"
    (is (= "oc_1"
           (:external-session-id
            (sut/message->codex-agent-message
             {:message-id "om_1"
              :chat-id "oc_1"
              :content {:text "hello"}}))))))

(deftest handle-codex-agent-message-replies-through-feishu-callback
  (testing "Feishu adapter calls Codex Agent and replies to the original message"
    (let [agent-messages (atom [])
          replies (atom [])]
      (with-redefs [codex-agent/handle-message!
                    (fn [_service message callbacks]
                      (swap! agent-messages conj message)
                      ((:on-reply! callbacks) {:text "answer"})
                      {:status :completed
                       :reply-text "answer"})
                    sut/reply-text!
                    (fn [_target opts]
                      (swap! replies conj opts)
                      {:ok? true})]
        (is (= {:status :completed
                :reply-text "answer"}
               (sut/handle-codex-agent-message!
                ::codex-agent
                ::reply-target
                {:message-id "om_1"
                 :chat-id "oc_1"
                 :content {:text "hello"}}
                {:reply-in-thread? true})))
        (is (= [{:channel :feishu
                 :external-session-id "oc_1"
                 :external-message-id "om_1"
                 :content [{:type :text :text "hello"}]}]
               @agent-messages))
        (is (= [{:message-id "om_1"
                 :text "answer"
                 :reply-in-thread? true
                 :uuid "codex-agent-om_1"}]
               @replies))))))
