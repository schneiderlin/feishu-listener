(ns com.zihao.feishu-listener.core-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [com.zihao.codex-agent.interface :as codex-agent]
   [com.zihao.feishu-listener.core :as sut]
   [com.zihao.feishu-openapi-lite.json :as json]))

(defn- sample-raw-event
  [chat-type text]
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
                     :mentions [{:key "@_user_1"
                                  :id {:user_id "u_mention"
                                       :open_id "ou_mention"
                                       :union_id "on_mention"}
                                  :mentioned_type "user"
                                  :name "Mentioned User"
                                  :tenant_key "tenant_1"}]}}})

(defn- sample-raw-event-json
  [chat-type text]
  (json/write-str (sample-raw-event chat-type text)))

(deftest event->message-normalizes-lite-message-event
  (testing "raw p2 event maps become listener message maps with parsed content"
    (let [message (sut/event->message (sample-raw-event "group" "hello"))]
      (is (= {:event-id "ev_group"
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
      (is (= "om_group" (:message-id message)))
      (is (= "oc_group" (:chat-id message)))
      (is (= "group" (:chat-type message)))
      (is (= {:text "hello"} (:content message)))
      (is (= [{:id {:user-id "u_mention"
                    :open-id "ou_mention"
                    :union-id "on_mention"}
               :key "@_user_1"
               :mentioned-type "user"
               :name "Mentioned User"
               :tenant-key "tenant_1"}]
             (get-in message [:message :mentions]))))))

(deftest dispatcher-delivers-group-and-direct-messages-without-code-filtering
  (testing "the listener accepts receive-message events and does not filter chat_type"
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

(deftest message-callback-errors-propagate-to-websocket-ack
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

(deftest send-and-reply-text-use-lite-openapi-requests
  (testing "chat-id text sends use Feishu's message.create HTTP shape"
    (let [requests (atom [])
          result (sut/send-text! {:tenant-access-token "tenant-token"
                                  :request! (fn [request]
                                              (swap! requests conj request)
                                              {:status 200
                                               :body (json/write-str
                                                      {:code 0
                                                       :msg "success"
                                                       :data {:message_id "om_sent"
                                                              :chat_id "oc_1"
                                                              :msg_type "text"}})})}
                                 {:chat-id "oc_1"
                                  :text "hello back"
                                  :uuid "uuid-1"})
          request (first @requests)
          body (json/read-str (:body request))]
      (is (= "https://open.feishu.cn/open-apis/im/v1/messages"
             (:uri request)))
      (is (= {"receive_id_type" "chat_id"}
             (:query-params request)))
      (is (= {:receive_id "oc_1"
              :msg_type "text"
              :content "{\"text\":\"hello back\"}"
              :uuid "uuid-1"}
             body))
      (is (= {:message-id "om_sent"
              :chat-id "oc_1"
              :message-type "text"}
             (:data result)))))
  (testing "message-id text replies use Feishu's message.reply HTTP shape"
    (let [requests (atom [])
          result (sut/reply-text! {:tenant-access-token "tenant-token"
                                   :request! (fn [request]
                                               (swap! requests conj request)
                                               {:status 200
                                                :body (json/write-str
                                                       {:code 0
                                                        :msg "success"
                                                        :data {:message_id "om_reply"
                                                               :thread_id "omt_1"
                                                               :msg_type "text"}})})}
                                  {:message-id "om_1"
                                   :text "reply"
                                   :reply-in-thread? true
                                   :uuid "uuid-2"})
          request (first @requests)
          body (json/read-str (:body request))]
      (is (= "https://open.feishu.cn/open-apis/im/v1/messages/om_1/reply"
             (:uri request)))
      (is (= {:msg_type "text"
              :content "{\"text\":\"reply\"}"
              :reply_in_thread true
              :uuid "uuid-2"}
             body))
      (is (= {:message-id "om_reply"
              :thread-id "omt_1"
              :message-type "text"}
             (:data result))))))

(deftest response->map-normalizes-success-and-throws-on-api-error
  (testing "successful Feishu API maps return a Clojure message summary"
    (is (= {:ok? true
            :code 0
            :message "success"
            :data {:message-id "om_sent"
                   :chat-id "oc_1"
                   :message-type "text"
                   :create-time "1780000000100"
                   :message-app-link "https://example.invalid/message"}}
           (sut/response->map
            {:code 0
             :msg "success"
             :data {:message_id "om_sent"
                    :chat_id "oc_1"
                    :msg_type "text"
                    :create_time "1780000000100"
                    :message_app_link "https://example.invalid/message"}}))))
  (testing "Feishu API failures throw with the API error data"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Feishu message API request failed"
         (sut/response->map {:code 999
                             :msg "failed"
                             :data {}})))))

(deftest message->codex-agent-message-chooses-thread-or-bootstrap-session
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
  (testing "non-thread messages use a bootstrap session from message-id"
    (is (= "bootstrap:om_1"
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
                      {:ok? true
                       :data {:thread-id "omt_1"}})]
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
                 :external-session-id "bootstrap:om_1"
                 :external-message-id "om_1"
                 :content [{:type :text :text "hello"}]}]
               @agent-messages))
        (is (= [{:message-id "om_1"
                 :text "answer"
                 :reply-in-thread? true
                 :uuid "codex-agent-om_1"}]
               @replies))))))

(deftest handle-codex-agent-message-sends-downloaded-image-to-codex
  (testing "Feishu image messages are downloaded and forwarded as Codex image input"
    (let [agent-messages (atom [])
          resource-requests (atom [])
          reply-target {:tenant-access-token "tenant-token"
                        :request! (fn [request]
                                    (swap! resource-requests conj request)
                                    {:status 200
                                     :headers {"content-type" "image/jpeg"}
                                     :body (byte-array [(byte -1) (byte -40) (byte 1)])})}]
      (with-redefs [codex-agent/handle-message!
                    (fn [_service message _callbacks]
                      (swap! agent-messages conj message)
                      {:status :completed
                       :reply-text "saw image"})]
        (is (= {:status :completed
                :reply-text "saw image"}
               (sut/handle-codex-agent-message!
                ::codex-agent
                reply-target
                {:message-id "om_img"
                 :chat-id "oc_1"
                 :message-type "image"
                 :content {:image_key "img_v3_1"}})))
        (is (= "https://open.feishu.cn/open-apis/im/v1/messages/om_img/resources/img_v3_1"
               (:uri (first @resource-requests))))
        (is (= {"type" "image"}
               (:query-params (first @resource-requests))))
        (let [content (:content (first @agent-messages))]
          (is (= {:type :text
                  :text "用户发送了一张图片。"}
                 (first content)))
          (is (= :image (:type (second content))))
          (is (str/starts-with? (:url (second content))
                                "data:image/jpeg;base64,")))))))

(deftest handle-codex-agent-message-falls-back-to-text-when-image-download-fails
  (testing "download failures are represented to Codex as a normal text message"
    (let [agent-messages (atom [])
          reply-target {:tenant-access-token "tenant-token"
                        :request! (fn [_request]
                                    {:status 403
                                     :headers {"content-type" "application/json"}
                                     :body (byte-array [])})}]
      (with-redefs [codex-agent/handle-message!
                    (fn [_service message _callbacks]
                      (swap! agent-messages conj message)
                      {:status :completed
                       :reply-text "handled failure"})]
        (is (= {:status :completed
                :reply-text "handled failure"}
               (sut/handle-codex-agent-message!
                ::codex-agent
                reply-target
                {:message-id "om_img"
                 :chat-id "oc_1"
                 :message-type "image"
                 :content {:image_key "img_v3_1"}})))
        (let [content (:content (first @agent-messages))
              text (:text (first content))]
          (is (= 1 (count content)))
          (is (= :text (:type (first content))))
          (is (str/includes? text "用户发送了一张图片"))
          (is (str/includes? text "下载图片内容失败"))
          (is (str/includes? text "HTTP 403"))
          (is (str/includes? text "om_img"))
          (is (str/includes? text "img_v3_1"))
          (is (not (str/includes? text "{\"image_key\""))))))))

(deftest handle-codex-agent-message-replies-to-command-progress-events
  (testing "Feishu adapter turns coarse Codex command events into thread replies"
    (let [forwarded-events (atom [])
          replies (atom [])
          started-event {:type :codex/item-started
                         :item-type "commandExecution"
                         :item-id "cmd-1"
                         :command "ls"}
          delta-event {:type :codex/command-delta
                       :delta "apps\n"}
          completed-event {:type :codex/item-completed
                           :item-type "commandExecution"
                           :item-id "cmd-1"
                           :status "completed"
                           :command "ls"
                           :exit-code 0
                           :output "apps\nbases\n"}]
      (with-redefs [codex-agent/handle-message!
                    (fn [_service _message callbacks]
                      ((:on-event! callbacks) started-event)
                      ((:on-event! callbacks) delta-event)
                      ((:on-event! callbacks) completed-event)
                      ((:on-reply! callbacks) {:text "answer"})
                      {:status :completed
                       :reply-text "answer"})
                    sut/reply-text!
                    (fn [_target opts]
                      (swap! replies conj opts)
                      {:ok? true
                       :data {:thread-id "omt_1"}})]
        (is (= {:status :completed
                :reply-text "answer"}
               (sut/handle-codex-agent-message!
                ::codex-agent
                ::reply-target
                {:message-id "om_1"
                 :chat-id "oc_1"
                 :content {:text "try use ls"}}
                {:reply-in-thread? true
                 :codex-agent-callbacks {:on-event! #(swap! forwarded-events conj %)}})))
        (is (= [started-event delta-event completed-event]
               @forwarded-events))
        (is (= ["Command started: ls"
                "Command completed (exit 0): ls\n\nOutput:\napps\nbases\n"
                "answer"]
               (mapv :text @replies)))
        (is (every? #(= "om_1" %) (map :message-id @replies)))
        (is (every? true? (map :reply-in-thread? @replies)))
        (is (every? #(str/starts-with? % "codex-agent-progress-")
                    (map :uuid (take 2 @replies))))
        (is (= "codex-agent-om_1" (:uuid (last @replies))))))))

(deftest handle-codex-agent-message-promotes-bootstrap-after-threaded-reply
  (testing "top-level Feishu messages ask Codex Agent to promote to the created Feishu thread"
    (let [callback-result (atom nil)]
      (with-redefs [codex-agent/handle-message!
                    (fn [_service _message callbacks]
                      (reset! callback-result
                              ((:on-reply! callbacks) {:text "answer"}))
                      {:status :completed
                       :reply-text "answer"})
                    sut/reply-text!
                    (fn [_target _opts]
                      {:ok? true
                       :data {:thread-id "omt_created"}})]
        (sut/handle-codex-agent-message!
         ::codex-agent
         ::reply-target
         {:message-id "om_1"
          :chat-id "oc_1"
          :content {:text "hello"}}
         {:reply-in-thread? true})
        (is (= "omt_created"
               (:promote-external-session-id @callback-result)))))))
