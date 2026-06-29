(ns com.zihao.feishu-listener.core
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [com.zihao.codex-agent.interface :as codex-agent]
   [com.zihao.feishu-openapi-lite.interface :as feishu-lite])
  (:import [java.nio.charset StandardCharsets]
           [java.util Base64 UUID]))

(def default-verification-token "")
(def default-encrypt-key "")
(def ^:private codex-progress-output-max-chars 3000)
(def ^:private max-feishu-upload-file-bytes (* 30 1024 1024))
(def ^:private feishu-tool-diagnostic-body-max-chars 1200)
(def ^:private send-feishu-file-tool-name "send_feishu_file")
(def ^:private stop-command "/stop")

(defrecord Listener [client api-client dispatcher config !state])

(declare reply-text!)
(declare state)
(declare target-api-client)

(defn- blank->nil
  [value]
  (when-not (str/blank? (str (or value "")))
    value))

(defn- short-reply-uuid
  [prefix & seed-parts]
  (let [seed (str/join "\n" (map #(str (or % "")) seed-parts))]
    (str prefix
         (UUID/nameUUIDFromBytes
          (.getBytes seed StandardCharsets/UTF_8)))))

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
                            :root-id
                            :parent-id
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

(defn- feishu-root-id
  [message]
  (or (blank->nil (:root-id message))
      (blank->nil (get-in message [:message :root-id]))))

(defn- feishu-parent-id
  [message]
  (or (blank->nil (:parent-id message))
      (blank->nil (get-in message [:message :parent-id]))))

(defn- bootstrap-session-id-for-message-id
  [message-id]
  (when-let [message-id (blank->nil message-id)]
    (str "bootstrap:" message-id)))

(defn- bootstrap-session-id
  [message]
  (bootstrap-session-id-for-message-id (feishu-message-id message)))

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

(defn- content-map
  [message]
  (or (:content message)
      (get-in message [:message :content])))

(defn- file-key
  [message]
  (or (blank->nil (get-in message [:content :file_key]))
      (blank->nil (get-in message [:content :file-key]))
      (blank->nil (get-in message [:message :content :file_key]))
      (blank->nil (get-in message [:message :content :file-key]))))

(defn- file-name
  [message]
  (or (blank->nil (get-in message [:content :file_name]))
      (blank->nil (get-in message [:content :file-name]))
      (blank->nil (get-in message [:message :content :file_name]))
      (blank->nil (get-in message [:message :content :file-name]))))

(defn- image-message?
  [message]
  (= "image" (:message-type message)))

(defn- file-message?
  [message]
  (= "file" (:message-type message)))

(defn- post-message?
  [message]
  (= "post" (:message-type message)))

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

(defn- image-key-download-fallback-text
  [message key throwable]
  (str "用户发送了一张图片，但系统下载图片内容失败了。"
       (when-let [message-id (feishu-message-id message)]
         (str "\nFeishu message id: " message-id))
       (when-let [key (blank->nil key)]
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

(defn- default-attachment-dir
  []
  (io/file (System/getProperty "user.home")
           ".feishu-codex-bridge"
           "attachments"))

(defn- safe-file-name
  [value fallback]
  (let [candidate (-> (str (or value fallback))
                      (str/replace #"[\\/:\r\n]" "_")
                      str/trim)]
    (if (str/blank? candidate)
      fallback
      candidate)))

(defn- message-attachment-dir
  [opts message]
  (io/file (or (:attachment-dir opts) (default-attachment-dir))
           (safe-file-name (feishu-message-id message) "unknown-message")))

(defn- save-resource-file!
  [dir name resource]
  (let [target (io/file dir (safe-file-name name "attachment.bin"))]
    (.mkdirs (.getParentFile target))
    (with-open [out (io/output-stream target)]
      (.write out ^bytes (:bytes resource)))
    (.getAbsolutePath target)))

(defn- downloaded-file-text
  [message file-path resource]
  (str "用户发送了一个文件，系统已经下载到本地。"
       (when-let [name (file-name message)]
         (str "\n文件名: " name))
       "\n本地路径: " file-path
       (str "\n大小: " (alength ^bytes (:bytes resource)) " bytes")
       (when-let [content-type (:content-type resource)]
         (str "\nContent-Type: " content-type))
       (when-let [message-id (feishu-message-id message)]
         (str "\nFeishu message id: " message-id))
       (when-let [key (file-key message)]
         (str "\nFeishu file key: " key))
       "\n如果需要分析这个文件，请使用本地路径读取。"
       "\n如果需要把文件发回飞书，请使用 "
       send-feishu-file-tool-name
       " 工具并传入本地文件路径。"))

(defn- file-download-fallback-text
  [message throwable]
  (str "用户发送了一个文件，但系统下载文件内容失败了。"
       (when-let [message-id (feishu-message-id message)]
         (str "\nFeishu message id: " message-id))
       (when-let [name (file-name message)]
         (str "\n文件名: " name))
       (when-let [key (file-key message)]
         (str "\nFeishu file key: " key))
       (when-let [reason (image-download-failure-reason throwable)]
         (str "\n下载失败原因: " reason))
       "\n请根据这个情况决定如何回应。"))

(defn- missing-file-key-fallback-text
  [message]
  (str "用户发送了一个文件，但消息内容里没有可下载的 file_key。"
       (when-let [message-id (feishu-message-id message)]
         (str "\nFeishu message id: " message-id))
       (when-let [name (file-name message)]
         (str "\n文件名: " name))
       "\n请根据这个情况决定如何回应。"))

(defn- text-codex-content
  [message]
  [{:type :text
    :text (or (message-text message)
              (when (image-message? message)
                "用户发送了一张图片。")
              "")}])

(defn- post-content-rows
  [message]
  (let [rows (:content (content-map message))]
    (if (sequential? rows)
      rows
      [])))

(defn- post-content-items
  [message]
  (->> (post-content-rows message)
       (mapcat (fn [row]
                 (if (sequential? row)
                   row
                   [row])))
       (filter map?)))

(defn- post-text
  [message]
  (let [lines (->> (post-content-rows message)
                   (keep (fn [row]
                           (let [text (->> (if (sequential? row) row [row])
                                           (keep (fn [item]
                                                   (when (and (map? item)
                                                              (= "text" (:tag item)))
                                                     (:text item))))
                                           (str/join ""))]
                             (blank->nil text)))))]
    (blank->nil (str/join "\n" lines))))

(defn- post-image-keys
  [message]
  (->> (post-content-items message)
       (keep (fn [item]
               (when (= "img" (:tag item))
                 (or (blank->nil (:image_key item))
                     (blank->nil (:image-key item))))))
       distinct
       vec))

(defn- image-content-item!
  [reply-target message key]
  (try
    (let [api-client (target-api-client reply-target)
          resource (feishu-lite/download-message-resource!
                    api-client
                    {:message-id (feishu-message-id message)
                     :file-key key
                     :type "image"})]
      [{:type :image
        :url (data-url resource)}])
    (catch Throwable t
      [{:type :text
        :text (image-key-download-fallback-text message key t)}])))

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

(defn- post-codex-content!
  [reply-target message]
  (let [text (post-text message)
        image-keys (post-image-keys message)
        text-items (cond
                     (blank->nil text)
                     [{:type :text :text text}]

                     (seq image-keys)
                     [{:type :text :text "用户发送了一条包含图片的富文本消息。"}]

                     :else
                     (text-codex-content message))
        image-items (mapcat #(image-content-item! reply-target message %) image-keys)]
    (vec (concat text-items image-items))))

(defn- file-codex-content!
  [reply-target message opts]
  (if-let [key (file-key message)]
    (try
      (let [api-client (target-api-client reply-target)
            resource (feishu-lite/download-message-resource!
                      api-client
                      {:message-id (feishu-message-id message)
                       :file-key key
                       :type "file"})
            path (save-resource-file! (message-attachment-dir opts message)
                                      (or (file-name message) key)
                                      resource)]
        [{:type :text
          :text (downloaded-file-text message path resource)}])
      (catch Throwable t
        [{:type :text
          :text (file-download-fallback-text message t)}]))
    [{:type :text
      :text (missing-file-key-fallback-text message)}]))

(defn message->codex-agent-message
  [message]
  {:channel :feishu
   :external-session-id (feishu-external-session-id message)
   :external-message-id (:message-id message)
   :content (text-codex-content message)})

(defn- message->codex-agent-message!
  ([reply-target message]
   (message->codex-agent-message! reply-target message {}))
  ([reply-target message opts]
   (cond
     (image-message? message)
     (assoc (message->codex-agent-message message)
            :content (image-codex-content! reply-target message))

     (post-message? message)
     (assoc (message->codex-agent-message message)
            :content (post-codex-content! reply-target message))

     (file-message? message)
     (assoc (message->codex-agent-message message)
            :content (file-codex-content! reply-target message opts))

     :else
     (message->codex-agent-message message))))

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

(defn- tool-result
  [success? text]
  {:success (boolean success?)
   :content-items [{:type "inputText"
                    :text (str text)}]})

(defn- argument-value
  [arguments k]
  (or (get arguments k)
      (get arguments (keyword (str/replace (name k) "-" "_")))
      (get arguments (name k))
      (get arguments (str/replace (name k) "-" "_"))))

(defn- expand-home-path
  [path]
  (let [path (str path)]
    (if (str/starts-with? path "~/")
      (str (System/getProperty "user.home") (subs path 1))
      path)))

(defn- canonical-file
  [path]
  (.getCanonicalFile (io/file (expand-home-path path))))

(defn- file-extension
  [file-name]
  (let [name (str/lower-case (str file-name))
        idx (.lastIndexOf name ".")]
    (when (and (<= 0 idx)
               (< idx (dec (count name))))
      (subs name (inc idx)))))

(defn- infer-feishu-file-type
  [file-name]
  (case (file-extension file-name)
    "mp4" "mp4"
    "opus" "opus"
    "pdf" "pdf"
    "doc" "doc"
    "docx" "doc"
    "xls" "xls"
    "xlsx" "xls"
    "ppt" "ppt"
    "pptx" "ppt"
    "stream"))

(defn- upload-reply-uuid
  [message file]
  (short-reply-uuid "codex-file-"
                    (or (feishu-message-id message) "unknown")
                    (.getAbsolutePath ^java.io.File file)))

(defn- bounded-text
  [text max-chars]
  (let [text (str text)]
    (if (<= (count text) max-chars)
      text
      (str (subs text 0 max-chars)
           "\n... truncated "
           (- (count text) max-chars)
           " chars"))))

(defn- diagnostic-body-text
  [body]
  (cond
    (nil? body)
    nil

    (string? body)
    body

    (bytes? body)
    (String. ^bytes body "UTF-8")

    :else
    (pr-str body)))

(defn- throwable-summary
  [throwable]
  (or (blank->nil (.getMessage ^Throwable throwable))
      (some-> throwable class .getName)))

(defn- append-line
  [lines label value]
  (if-let [value (blank->nil value)]
    (conj lines (str label value))
    lines))

(defn- send-file-failure-text
  [phase context throwable]
  (let [data (ex-data throwable)
        body (some-> (:body data)
                     diagnostic-body-text
                     (bounded-text feishu-tool-diagnostic-body-max-chars))]
    (str/join
     "\n"
     (cond-> [(str "send_feishu_file failed during " (name phase) ".")
              (str "error: " (throwable-summary throwable))
              (str "exception: " (some-> throwable class .getName))]
       (:path context)
       (append-line "local path: " (:path context))

       (:file-name context)
       (append-line "file name: " (:file-name context))

       (:file-type context)
       (append-line "file type: " (:file-type context))

       (:message-id context)
       (append-line "Feishu message id: " (:message-id context))

       (:uploaded-key context)
       (append-line "Feishu uploaded file key: " (:uploaded-key context))

       (:type data)
       (append-line "error type: " (:type data))

       (:status data)
       (append-line "HTTP status: " (:status data))

       (:code data)
       (append-line "Feishu API code: " (:code data))

       (:message data)
       (append-line "Feishu API message: " (:message data))

       body
       (append-line "Feishu response body: " body)))))

(defn- run-send-file-phase
  [phase context f]
  (try
    {:ok? true
     :value (f)}
    (catch Throwable t
      {:ok? false
       :phase phase
       :context context
       :throwable t})))

(defn- send-file-phase-failure-result
  [{:keys [phase context throwable]}]
  (tool-result false (send-file-failure-text phase context throwable)))

(defn- send-file-missing-key-result
  [context]
  (tool-result
   false
   (str/join
    "\n"
    (cond-> ["Feishu upload succeeded without returning file_key"
             "phase: upload"]
      (:path context)
      (append-line "local path: " (:path context))

      (:file-name context)
      (append-line "file name: " (:file-name context))

      (:file-type context)
      (append-line "file type: " (:file-type context))

      (:message-id context)
      (append-line "Feishu message id: " (:message-id context))))))

(defn- send-file-success-result
  [file upload-name uploaded-key reply-result]
  (tool-result true
               (str "文件已上传并回复到飞书。"
                    "\n本地路径: " (.getAbsolutePath ^java.io.File file)
                    "\n文件名: " upload-name
                    "\nFeishu file key: " uploaded-key
                    (when-let [reply-id (get-in reply-result [:data :message-id])]
                      (str "\nFeishu reply message id: " reply-id)))))

(defn- upload-and-reply-feishu-file!
  [reply-target message reply-in-thread? file arguments]
  (let [api-client (target-api-client reply-target)
        upload-name (or (blank->nil (argument-value arguments :file_name))
                        (.getName ^java.io.File file))
        file-type (or (blank->nil (argument-value arguments :file_type))
                      (infer-feishu-file-type upload-name))
        upload-context {:path (.getAbsolutePath ^java.io.File file)
                        :file-name upload-name
                        :file-type file-type
                        :message-id (feishu-message-id message)}
        upload-attempt (run-send-file-phase
                        :upload
                        upload-context
                        #(feishu-lite/upload-file!
                          api-client
                          {:file file
                           :file-name upload-name
                           :file-type file-type}))]
    (if-not (:ok? upload-attempt)
      (send-file-phase-failure-result upload-attempt)
      (let [uploaded-key (blank->nil (get-in upload-attempt [:value :data :file-key]))]
        (if-not uploaded-key
          (send-file-missing-key-result upload-context)
          (let [reply-context (assoc upload-context :uploaded-key uploaded-key)
                reply-attempt (run-send-file-phase
                               :reply
                               reply-context
                               #(feishu-lite/reply-file!
                                 api-client
                                 {:message-id (feishu-message-id message)
                                  :file-key uploaded-key
                                  :reply-in-thread? reply-in-thread?
                                  :uuid (upload-reply-uuid message file)}))]
            (if-not (:ok? reply-attempt)
              (send-file-phase-failure-result reply-attempt)
              (send-file-success-result file
                                        upload-name
                                        uploaded-key
                                        (:value reply-attempt)))))))))

(defn- send-feishu-file-tool-spec
  []
  {:name send-feishu-file-tool-name
   :description "Upload a local file path and reply with it in the current Feishu conversation. Use this when the user asks you to send, export, or return a file."
   :input-schema {:type "object"
                  :properties {:path {:type "string"
                                      :description "Absolute local file path to upload."}
                               :file_name {:type "string"
                                           :description "Optional Feishu display file name."}
                               :file_type {:type "string"
                                           :description "Optional Feishu file_type: pdf, doc, xls, ppt, mp4, opus, or stream."}}
                  :required ["path"]
                  :additionalProperties false}})

(defn- add-feishu-file-tool
  [tools]
  (let [tools (vec (or tools []))
        tool-name? #(= send-feishu-file-tool-name (:name %))]
    (if (some tool-name? tools)
      tools
      (conj tools (send-feishu-file-tool-spec)))))

(defn- send-feishu-file-tool!
  [reply-target message reply-in-thread? arguments]
  (try
    (let [path (blank->nil (argument-value arguments :path))]
      (if-not path
        (tool-result false "path is required")
        (let [file (canonical-file path)
              size (.length file)]
          (cond
            (not (.exists file))
            (tool-result false (str "file does not exist: " (.getAbsolutePath file)))

            (not (.isFile file))
            (tool-result false (str "path is not a regular file: " (.getAbsolutePath file)))

            (> size max-feishu-upload-file-bytes)
            (tool-result false
                         (str "file is too large for Feishu upload: "
                              size
                              " bytes; limit is "
                              max-feishu-upload-file-bytes
                              " bytes"))

            :else
            (upload-and-reply-feishu-file!
             reply-target
             message
             reply-in-thread?
             file
             arguments)))))
    (catch Throwable t
      (tool-result false
                   (send-file-failure-text :prepare {} t)))))

(defn- feishu-file-tool-callbacks
  [base-callbacks reply-target message reply-in-thread?]
  (let [base-handler (:on-dynamic-tool-call! base-callbacks)]
    (assoc base-callbacks
           :dynamic-tools (add-feishu-file-tool (:dynamic-tools base-callbacks))
           :experimental-api? true
           :on-dynamic-tool-call!
           (fn [{:keys [tool] :as call}]
             (if (= send-feishu-file-tool-name tool)
               (send-feishu-file-tool! reply-target
                                       message
                                       reply-in-thread?
                                       (:arguments call))
               (if base-handler
                 (base-handler call)
                 (tool-result false (str "Unsupported dynamic tool: " tool))))))))

(defn- stop-command-message?
  [message]
  (and (= "text" (:message-type message))
       (= stop-command (str/trim (str (or (message-text message) ""))))))

(defn- stop-session-messages
  [message]
  (let [base-message (message->codex-agent-message message)
        external-session-ids (->> [(feishu-external-session-id message)
                                   (bootstrap-session-id-for-message-id
                                    (feishu-root-id message))
                                   (bootstrap-session-id-for-message-id
                                    (feishu-parent-id message))]
                                  (keep blank->nil)
                                  distinct)]
    (mapv #(assoc base-message
                  :external-session-id %
                  :content [{:type :text
                             :text stop-command}])
          external-session-ids)))

(defn- stop-reply-text
  [result]
  (if (:interrupted? result)
    "已请求停止当前 Codex 后端任务。"
    "当前会话没有正在运行的 Codex 后端任务。"))

(defn- stop-codex-agent-session!
  [codex-agent-service reply-target message reply-in-thread?]
  (let [results (mapv #(codex-agent/stop-session! codex-agent-service %)
                      (stop-session-messages message))
        result (or (some #(when (:interrupted? %) %) results)
                   (last results)
                   {:interrupted? false
                    :reason :no-session-candidate})]
    (reply-text! reply-target
                 {:message-id (feishu-message-id message)
                  :text (stop-reply-text result)
                  :reply-in-thread? reply-in-thread?
                  :uuid (short-reply-uuid "codex-stop-"
                                          (or (feishu-message-id message)
                                              "unknown"))})
    result))

(defn- async-handle-codex-agent-message!
  [handler on-error message]
  (future
    (try
      (handler message)
      (catch Throwable t
        (safe-callback! on-error
                        {:type :feishu-listener/codex-agent-handler-failed
                         :message message
                         :throwable t})))))

(defn handle-codex-agent-message!
  ([codex-agent-service reply-target message]
   (handle-codex-agent-message! codex-agent-service reply-target message {}))
  ([codex-agent-service reply-target message opts]
   (let [reply-in-thread? (get opts :reply-in-thread? true)
         agent-message (message->codex-agent-message! reply-target message opts)
         bootstrap-session? (nil? (feishu-thread-id message))
         base-callbacks (feishu-file-tool-callbacks
                         (or (:codex-agent-callbacks opts) {})
                         reply-target
                         message
                         reply-in-thread?)
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
                                                 (assoc :uuid (short-reply-uuid
                                                               "codex-msg-"
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
          reply-in-thread? (get config :reply-in-thread? true)
          on-error (:on-error config)
          handler (codex-agent-message-handler codex-agent-service
                                               reply-target
                                               (select-keys config
                                                            [:reply-in-thread?
                                                             :attachment-dir
                                                             :codex-agent-callbacks]))]
      (assoc config
             :on-message (fn [message]
                           (when existing-on-message
                             (existing-on-message message))
                           (if (stop-command-message? message)
                             (do
                               (stop-codex-agent-session! codex-agent-service
                                                          reply-target
                                                          message
                                                          reply-in-thread?)
                               nil)
                             (do
                               (async-handle-codex-agent-message! handler
                                                                  on-error
                                                                  message)
                               nil)))))
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
  - :attachment-dir for downloaded Feishu files
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

(defn reply-file!
  [target opts]
  (feishu-lite/reply-file! (target-api-client target) opts))

(defn upload-file!
  [target opts]
  (feishu-lite/upload-file! (target-api-client target) opts))

(defn dispatch-raw-event!
  "Run a raw Feishu p2 event JSON payload through the listener dispatcher.

  This mirrors the payload path used by the lite WebSocket client after it
  receives a data frame. Production WebSocket listening should use start!."
  [^Listener listener raw-json]
  ((:dispatcher listener) raw-json))
