(ns slacker.zk
  (:import [org.apache.curator.retry RetryNTimes]
           [org.apache.curator.framework
            CuratorFramework
            CuratorFrameworkFactory]
           [org.apache.curator.framework.api
            CuratorWatcher
            CuratorListener
            CuratorEvent
            CuratorEventType
            UnhandledErrorListener]
           [org.apache.curator.framework.recipes.nodes
            PersistentEphemeralNode PersistentEphemeralNode$Mode]
           [org.apache.curator.framework.recipes.leader
            LeaderSelector LeaderSelectorListenerAdapter]
           [org.apache.zookeeper CreateMode WatchedEvent]
           [org.apache.zookeeper.data Stat]))

(defn connect [connect-string options]
  (doto (CuratorFrameworkFactory/newClient
         connect-string
         (or (:zk-session-timeout options) 5000) ;; session timeout
         (or (:zk-connect-timeout options) 5000) ;; connect timeout
         (RetryNTimes. Integer/MAX_VALUE 5000))
    (.start)))

(defn wrap-watcher [watcher-fn]
  (reify CuratorWatcher
    (process [this event]
      (watcher-fn {:event-type (keyword (str (.getType event)))
                   :path (.getPath event)
                   :keeper-state (keyword (str (.getState event)))}))))

(defn wrap-stat [^Stat s]
  (when s
    {:version (.getVersion s)
     :cversion (.getCversion s)
     :aversion (.getAversion s)
     :ctime (.getCtime s)
     :mtime (.getMtime s)
     :czxid (.getCzxid s)
     :mzxid (.getMzxid s)
     :ephemeralOwner (.getEphemeralOwner s)
     :dataLength (.getDataLength s)
     :numChildren (.getNumChildren s)
     :pzxid (.getPzxid s)}))

(defn create [^CuratorFramework conn
              ^String path
              & {:keys [persistent? data sequential?]
                 :or {persistent? true
                      sequential? false}}]
  (.. conn
      (create)
      (withMode ^CreateMode
       (cond
        (and persistent? sequential?) CreateMode/PERSISTENT_SEQUENTIAL
        persistent? CreateMode/PERSISTENT
        sequential? CreateMode/EPHEMERAL_SEQUENTIAL
        :else CreateMode/EPHEMERAL))
      (forPath path ^bytes data)))

(defn create-all [^CuratorFramework conn
                  ^String path
                  & {:keys [persistent? data sequential?]
                     :or {persistent? true
                          sequential? false}}]
  (let [mode (cond
              (and persistent? sequential?) CreateMode/PERSISTENT_SEQUENTIAL
              persistent? CreateMode/PERSISTENT
              sequential? CreateMode/EPHEMERAL_SEQUENTIAL
              :else CreateMode/EPHEMERAL)]
    (.. conn
        (create)
        (withMode ^CreateMode mode)
        (creatingParentsIfNeeded)
        (forPath path ^bytes data))))

(defn create-persistent-ephemeral-node [^CuratorFramework conn
                                        ^String path]
  (doto (PersistentEphemeralNode. conn
                                  PersistentEphemeralNode$Mode/EPHEMERAL
                                  path
                                  (byte-array 0))
    (.start)))

(defn uncreate-persistent-ephemeral-node [^PersistentEphemeralNode node]
  (.stop node))

(defn start-leader-election [^CuratorFramework conn
                             ^String mutex-path
                             listener-fn]
  (doto (LeaderSelector. conn mutex-path (proxy [LeaderSelectorListenerAdapter] []
                                           (takeLeadership [c]
                                             (listener-fn c))))
    (.start)))

(defn stop-leader-election [^LeaderSelector s]
  (.stop s))

(defn set-data [^CuratorFramework conn
                ^String path
                ^bytes data
                & {:keys [version]}]
  (wrap-stat
   (let [sdb (.setData conn)
         sdb (if (not (nil? version)) (.withVersion sdb version) sdb)]
     (.forPath sdb  path data))))

(defn children [^CuratorFramework conn
                ^String path
                & {:keys [watch? watcher]}]
  (let [gcb (.getChildren conn)
        gcb (if watch? (.watched gcb) gcb)
        gcb (if watcher (.usingWatcher gcb (wrap-watcher watcher)) gcb)]
    (.forPath gcb path)))

(defn data [^CuratorFramework conn
            ^String path
            & {:keys [watch? watcher]}]
  (let [gcb (.getData conn)
        gcb (if watch? (.watched gcb) gcb)
        gcb (if watcher (.usingWatcher gcb (wrap-watcher watcher)) gcb)]
    (.forPath gcb path)))

(defn delete [^CuratorFramework conn
              ^String path]
  (.. conn
      (delete)
      (forPath path)))

(defn delete-all [^CuratorFramework conn
                  ^String path]
  (.. conn
      (delete)
      (deletingChildrenIfNeeded)
      (forPath path)))

(defn exists [^CuratorFramework conn
              ^String path
              & {:keys [watch? watcher]}]
  (wrap-stat
   (let [gcb (.checkExists conn)
         gcb (if watch? (.watched gcb) gcb)
         gcb (if watcher (.usingWatcher gcb (wrap-watcher watcher)) gcb)]
     (.forPath gcb path))))

(defn close [^CuratorFramework conn]
  (.close conn))

(defn register-watcher [^CuratorFramework conn
                        watcher-fn]
  (.. conn
      (getCuratorListenable)
      (addListener (reify CuratorListener
                     (eventReceived [this conn* event]
                       (when (= (.getType ^CuratorEvent event)
                                CuratorEventType/WATCHED)
                         (.process (wrap-watcher watcher-fn)
                                   (.getWatchedEvent ^CuratorEvent event))))))))

(defn register-error-handler [^CuratorFramework conn
                              error-fn]
  (.. conn
      (getUnhandledErrorListenable)
      (addListener (reify UnhandledErrorListener
                     (unhandledError [this message e]
                       (error-fn message e))))))
