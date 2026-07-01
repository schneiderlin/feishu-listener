(ns com.zihao.feishu-listener.runner-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [com.zihao.feishu-listener.runner :as sut]))

(deftest message-summary-redacts-content
  (testing "diagnostic message summaries keep routing metadata without message body"
    (let [summary (#'sut/message-summary
                   {:message-id "om_1"
                    :chat-id "oc_1"
                    :chat-type "p2p"
                    :thread-id "omt_1"
                    :message-type "text"
                    :create-time "1780000000000"
                    :content {:text "secret text"}
                    :content-raw "{\"text\":\"secret text\"}"})]
      (is (= {:message-id "om_1"
              :chat-id "oc_1"
              :chat-type "p2p"
              :thread-id "omt_1"
              :message-type "text"
              :create-time "1780000000000"
              :content-present? true
              :content-raw-present? true}
             summary))
      (is (not (contains? summary :content)))
      (is (not (contains? summary :content-raw)))
      (is (not (str/includes? (pr-str summary) "secret text"))))))

(deftest listener-error-summary-redacts-ex-data
  (testing "diagnostic errors keep useful callback data without secrets or payload bodies"
    (let [summary (#'sut/listener-error-summary
                   {:type :feishu-listener/message-callback-failed
                    :callback-key :persist-message!
                    :message {:message-id "om_1"
                              :content {:text "secret text"}}
                    :throwable (ex-info "failed"
                                        {:type :example/error
                                         :code 400
                                         :callback-key :persist-message!
                                         :tenant-access-token "tenant-token"
                                         :app-secret "app-secret"
                                         :body "secret body"})})
          rendered (pr-str summary)]
      (is (= :listener-error (:phase summary)))
      (is (= :feishu-listener/message-callback-failed (:type summary)))
      (is (= :persist-message! (:callback-key summary)))
      (is (= "om_1" (get-in summary [:message :message-id])))
      (is (= "clojure.lang.ExceptionInfo" (get-in summary [:error :class])))
      (is (= {:type :example/error
              :code 400
              :callback-key :persist-message!}
             (get-in summary [:error :data])))
      (is (not (str/includes? rendered "tenant-token")))
      (is (not (str/includes? rendered "app-secret")))
      (is (not (str/includes? rendered "secret body")))
      (is (not (str/includes? rendered "secret text"))))))

(deftest codex-event-summary-filters-noisy-events
  (testing "wire and token-level events stay out of default diagnostics"
    (is (nil? (#'sut/codex-event-summary
               {:type :codex-app-server/wire-message
                :method "codex/event"})))
    (is (nil? (#'sut/codex-event-summary
               {:type :codex/command-delta
                :delta "stdout"})))))

(deftest codex-event-summary-keeps-coarse-events
  (testing "default diagnostics retain coarse turn and item lifecycle events"
    (is (= {:phase :codex-event
            :type :codex/item-completed
            :status "completed"
            :item-type "commandExecution"
            :item-id "cmd-1"}
           (#'sut/codex-event-summary
            {:type :codex/item-completed
             :status "completed"
             :item-type "commandExecution"
             :item-id "cmd-1"
             :output "large output omitted"})))))
