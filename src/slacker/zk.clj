(ns slacker.zk
  (:import [org.apache.curator.retry RetryNTimes]
           [org.apache.curator.framework
            CuratorFramework
            CuratorFrameworkFactory]
           [org.apache.zookeeper CreateMode]))

(defn connect [connect-string]
  (doto (CuratorFrameworkFactory/newClient
         connect-string
         (RetryNTimes. Integer/MAX_VALUE 5000))
    (.start)))

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
  (.. conn
      (create)
      (withMode ^CreateMode
       (cond
        (and persistent? sequential?) CreateMode/PERSISTENT_SEQUENTIAL
        persistent? CreateMode/PERSISTENT
        sequential? CreateMode/EPHEMERAL_SEQUENTIAL
        :else CreateMode/EPHEMERAL))
      (creatingParentsIfNeeded)
      (forPath path ^bytes data)))

(defn set-data [^CuratorFramework conn
                ^String path
                ^bytes data]
  (.. conn
      (setData)
      (forPath path data)))

(defn get-children [^CuratorFramework conn
                    ^String path
                    & {:keys [watched watcher]}]
  (let [gcb (.getChildren conn)
        gcb (if watched (.watched gcb) gcb)
        gcb (if watcher (.usingWatcher gcb watcher) gcb)]
    (.forPath gcb path)))

(defn data [^CuratorFramework conn
            ^String path
            & {:keys [watched watcher]}]
  (let [gcb (.getData conn)
        gcb (if watched (.watched gcb) gcb)
        gcb (if watcher (.usingWatcher gcb watcher) gcb)]
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

(defn close [^CuratorFramework conn]
  (.close conn))
