(ns com.zihao.feishu-listener.core-test
  (:require
   [clojure.java.io :as io]
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
        (let [reply (first @replies)]
          (is (= {:message-id "om_1"
                  :text "answer"
                  :reply-in-thread? true}
                 (select-keys reply [:message-id :text :reply-in-thread?])))
          (is (str/starts-with? (:uuid reply) "codex-msg-"))
          (is (<= (count (:uuid reply)) 50)))))))

(deftest listener-stop-command-interrupts-codex-session
  (testing "/stop interrupts through Codex Agent instead of forwarding as user input"
    (let [stop-messages (atom [])
          replies (atom [])
          raw-event (-> (sample-raw-event "p2p" "/stop")
                        (assoc-in [:event :message :message_id] "om_stop")
                        (assoc-in [:event :message :thread_id] "omt_1")
                        (assoc-in [:event :message :root_id] "om_original"))]
      (with-redefs [codex-agent/handle-message!
                    (fn [& _]
                      (throw (ex-info "stop must not become a Codex message" {})))
                    codex-agent/stop-session!
                    (fn [_service message]
                      (swap! stop-messages conj message)
                      (if (= "bootstrap:om_original" (:external-session-id message))
                        {:interrupted? true
                         :codex-thread-id "thread-1"
                         :codex-turn-id "turn-1"}
                        {:interrupted? false
                         :reason :no-client}))
                    sut/reply-text!
                    (fn [_target opts]
                      (swap! replies conj opts)
                      {:ok? true
                       :data {:thread-id "omt_1"}})]
        (let [listener (sut/make-listener {:app-id "cli_1"
                                           :app-secret "secret"
                                           :codex-agent-service ::codex-agent
                                           :reply-in-thread? true})]
          (sut/dispatch-raw-event! listener (json/write-str raw-event))
          (is (= ["omt_1" "bootstrap:om_original"]
                 (mapv :external-session-id @stop-messages)))
          (let [reply (first @replies)]
            (is (= {:message-id "om_stop"
                    :text "已请求停止当前 Codex 后端任务。"
                    :reply-in-thread? true}
                   (select-keys reply [:message-id :text :reply-in-thread?])))
            (is (str/starts-with? (:uuid reply) "codex-stop-"))
            (is (<= (count (:uuid reply)) 50))))))))

(deftest listener-runs-codex-handler-without-blocking-stop-command
  (testing "a long Codex turn does not block the Feishu dispatcher from receiving /stop"
    (let [entered (promise)
          release (promise)
          stop-message (atom nil)
          raw-message (-> (sample-raw-event "p2p" "start long task")
                          (assoc-in [:event :message :message_id] "om_start")
                          (assoc-in [:event :message :thread_id] "omt_1"))
          raw-stop (-> (sample-raw-event "p2p" "/stop")
                       (assoc-in [:event :message :message_id] "om_stop")
                       (assoc-in [:event :message :thread_id] "omt_1"))]
      (with-redefs [codex-agent/handle-message!
                    (fn [_service message _callbacks]
                      (deliver entered message)
                      @release
                      {:status :completed
                       :reply-text "done"})
                    codex-agent/stop-session!
                    (fn [_service message]
                      (reset! stop-message message)
                      {:interrupted? true})
                    sut/reply-text!
                    (fn [_target _opts]
                      {:ok? true
                       :data {:thread-id "omt_1"}})]
        (let [listener (sut/make-listener {:app-id "cli_1"
                                           :app-secret "secret"
                                           :codex-agent-service ::codex-agent
                                           :reply-in-thread? true})]
          (try
            (let [dispatch-result (future
                                    (sut/dispatch-raw-event! listener
                                                             (json/write-str raw-message)))]
              (is (some? (deref entered 1000 nil)))
              (is (not= ::blocked (deref dispatch-result 100 ::blocked)))
              (sut/dispatch-raw-event! listener (json/write-str raw-stop))
              (is (= "omt_1" (:external-session-id @stop-message))))
            (finally
              (deliver release true))))))))

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

(deftest handle-codex-agent-message-sends-post-text-and-image-to-codex
  (testing "Feishu rich post messages can include text and image in one message"
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
                       :reply-text "saw post image"})]
        (is (= {:status :completed
                :reply-text "saw post image"}
               (sut/handle-codex-agent-message!
                ::codex-agent
                reply-target
                {:message-id "om_post"
                 :chat-id "oc_1"
                 :message-type "post"
                 :content {:title ""
                           :content [[{:tag "img"
                                       :image_key "img_v3_post"
                                       :width 654
                                       :height 384}]
                                     [{:tag "text"
                                       :text "这个图里面有几个对话"
                                       :style []}]]}})))
        (is (= "https://open.feishu.cn/open-apis/im/v1/messages/om_post/resources/img_v3_post"
               (:uri (first @resource-requests))))
        (is (= {"type" "image"}
               (:query-params (first @resource-requests))))
        (let [content (:content (first @agent-messages))]
          (is (= {:type :text
                  :text "这个图里面有几个对话"}
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

(deftest handle-codex-agent-message-downloads-file-to-local-attachment-path
  (testing "Feishu file messages are downloaded and forwarded to Codex as a local path"
    (let [agent-messages (atom [])
          resource-requests (atom [])
          attachment-dir (io/file "tmp"
                                  "feishu-listener-file-tests"
                                  (str (java.util.UUID/randomUUID)))
          expected-file (io/file attachment-dir "om_file" "report.txt")
          reply-target {:tenant-access-token "tenant-token"
                        :request! (fn [request]
                                    (swap! resource-requests conj request)
                                    {:status 200
                                     :headers {"content-type" "text/plain"}
                                     :body (.getBytes "hello file" "UTF-8")})}]
      (with-redefs [codex-agent/handle-message!
                    (fn [_service message _callbacks]
                      (swap! agent-messages conj message)
                      {:status :completed
                       :reply-text "saw file"})]
        (is (= {:status :completed
                :reply-text "saw file"}
               (sut/handle-codex-agent-message!
                ::codex-agent
                reply-target
                {:message-id "om_file"
                 :chat-id "oc_1"
                 :message-type "file"
                 :content {:file_key "file_v2_1"
                           :file_name "report.txt"}}
                {:attachment-dir (.getPath attachment-dir)})))
        (is (= "https://open.feishu.cn/open-apis/im/v1/messages/om_file/resources/file_v2_1"
               (:uri (first @resource-requests))))
        (is (= {"type" "file"}
               (:query-params (first @resource-requests))))
        (is (= "hello file" (slurp expected-file)))
        (let [content (:content (first @agent-messages))
              text (:text (first content))]
          (is (= 1 (count content)))
          (is (= :text (:type (first content))))
          (is (str/includes? text (.getAbsolutePath expected-file)))
          (is (str/includes? text "report.txt"))
          (is (str/includes? text "send_feishu_file")))))))

(deftest handle-codex-agent-message-exposes-file-upload-dynamic-tool
  (testing "Codex can call a Feishu upload tool with a local path"
    (let [upload-file (io/file "tmp"
                               "feishu-listener-upload-tests"
                               (str (java.util.UUID/randomUUID))
                               "out.pdf")
          _ (.mkdirs (.getParentFile upload-file))
          _ (spit upload-file "pdf bytes")
          requests (atom [])
          callbacks-seen (atom nil)
          tool-result (atom nil)
          reply-target {:tenant-access-token "tenant-token"
                        :request! (fn [request]
                                    (swap! requests conj request)
                                    (cond
                                      (:multipart request)
                                      {:status 200
                                       :body (json/write-str
                                              {:code 0
                                               :msg "success"
                                               :data {:file_key "file_uploaded"}})}

                                      (str/ends-with? (:uri request) "/reply")
                                      {:status 200
                                       :body (json/write-str
                                              {:code 0
                                               :msg "success"
                                               :data {:message_id "om_reply"
                                                      :thread_id "omt_1"
                                                      :msg_type "file"}})}))}]
      (with-redefs [codex-agent/handle-message!
                    (fn [_service _message callbacks]
                      (reset! callbacks-seen callbacks)
                      (reset! tool-result
                              ((:on-dynamic-tool-call! callbacks)
                               {:tool "send_feishu_file"
                                :arguments {:path (.getPath upload-file)}}))
                      {:status :completed
                       :reply-text "done"})]
        (sut/handle-codex-agent-message!
         ::codex-agent
         reply-target
         {:message-id "om_1"
          :chat-id "oc_1"
          :content {:text "send the file"}}
         {:reply-in-thread? true})
        (is (= true (:experimental-api? @callbacks-seen)))
        (is (= ["send_feishu_file"]
               (mapv :name (:dynamic-tools @callbacks-seen))))
        (is (= true (:success @tool-result)))
        (is (str/includes? (get-in @tool-result [:content-items 0 :text])
                           "file_uploaded"))
        (let [upload-request (first @requests)
              reply-request (second @requests)
              reply-body (json/read-str (:body reply-request))]
          (is (= "https://open.feishu.cn/open-apis/im/v1/files"
                 (:uri upload-request)))
          (is (= ["file_type" "file_name" "file"]
                 (mapv :name (:multipart upload-request))))
          (is (= "pdf" (get-in upload-request [:multipart 0 :content])))
          (is (= "https://open.feishu.cn/open-apis/im/v1/messages/om_1/reply"
                 (:uri reply-request)))
          (is (= {:msg_type "file"
                  :content "{\"file_key\":\"file_uploaded\"}"
                  :reply_in_thread true}
                 (select-keys reply-body [:msg_type :content :reply_in_thread])))
          (is (str/starts-with? (:uuid reply-body) "codex-file-"))
          (is (<= (count (:uuid reply-body)) 50)))))))

(deftest handle-codex-agent-message-reports-file-upload-http-failure
  (testing "failed file uploads report the Feishu HTTP phase and body to Codex"
    (let [upload-file (io/file "tmp"
                               "feishu-listener-upload-tests"
                               (str (java.util.UUID/randomUUID))
                               "out.md")
          _ (.mkdirs (.getParentFile upload-file))
          _ (spit upload-file "markdown bytes")
          requests (atom [])
          tool-result (atom nil)
          reply-target {:tenant-access-token "tenant-token"
                        :request! (fn [request]
                                    (swap! requests conj request)
                                    {:status 400
                                     :body (json/write-str
                                            {:code 234001
                                             :msg "Invalid request param."})})}]
      (with-redefs [codex-agent/handle-message!
                    (fn [_service _message callbacks]
                      (reset! tool-result
                              ((:on-dynamic-tool-call! callbacks)
                               {:tool "send_feishu_file"
                                :arguments {:path (.getPath upload-file)
                                            :file_name "category-theory-index.md"
                                            :file_type "stream"}}))
                      {:status :completed
                       :reply-text "done"})]
        (sut/handle-codex-agent-message!
         ::codex-agent
         reply-target
         {:message-id "om_1"
          :chat-id "oc_1"
          :content {:text "send the file"}}
         {:reply-in-thread? true})
        (is (= false (:success @tool-result)))
        (is (= 1 (count @requests)))
        (let [text (get-in @tool-result [:content-items 0 :text])]
          (is (str/includes? text "send_feishu_file failed during upload"))
          (is (str/includes? text "HTTP status: 400"))
          (is (str/includes? text "Invalid request param."))
          (is (str/includes? text "file name: category-theory-index.md"))
          (is (str/includes? text "file type: stream"))
          (is (str/includes? text "Feishu message id: om_1"))
          (is (not (str/includes? text "tenant-token"))))))))

(deftest handle-codex-agent-message-reports-file-reply-http-failure
  (testing "failed file replies report the reply phase and uploaded file key"
    (let [upload-file (io/file "tmp"
                               "feishu-listener-upload-tests"
                               (str (java.util.UUID/randomUUID))
                               "out.pdf")
          _ (.mkdirs (.getParentFile upload-file))
          _ (spit upload-file "pdf bytes")
          requests (atom [])
          tool-result (atom nil)
          reply-target {:tenant-access-token "tenant-token"
                        :request! (fn [request]
                                    (swap! requests conj request)
                                    (cond
                                      (:multipart request)
                                      {:status 200
                                       :body (json/write-str
                                              {:code 0
                                               :msg "success"
                                               :data {:file_key "file_uploaded"}})}

                                      (str/ends-with? (:uri request) "/reply")
                                      {:status 403
                                       :body (json/write-str
                                              {:code 99991663
                                               :msg "permission denied"})}))}]
      (with-redefs [codex-agent/handle-message!
                    (fn [_service _message callbacks]
                      (reset! tool-result
                              ((:on-dynamic-tool-call! callbacks)
                               {:tool "send_feishu_file"
                                :arguments {:path (.getPath upload-file)}}))
                      {:status :completed
                       :reply-text "done"})]
        (sut/handle-codex-agent-message!
         ::codex-agent
         reply-target
         {:message-id "om_1"
          :chat-id "oc_1"
          :content {:text "send the file"}}
         {:reply-in-thread? true})
        (is (= false (:success @tool-result)))
        (is (= 2 (count @requests)))
        (let [text (get-in @tool-result [:content-items 0 :text])]
          (is (str/includes? text "send_feishu_file failed during reply"))
          (is (str/includes? text "HTTP status: 403"))
          (is (str/includes? text "permission denied"))
          (is (str/includes? text "file type: pdf"))
          (is (str/includes? text "Feishu uploaded file key: file_uploaded"))
          (is (not (str/includes? text "tenant-token"))))))))

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
        (is (str/starts-with? (:uuid (last @replies)) "codex-msg-"))
        (is (<= (count (:uuid (last @replies))) 50))))))

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
