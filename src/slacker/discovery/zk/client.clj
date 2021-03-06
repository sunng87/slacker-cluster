(ns slacker.discovery.zk.client
  (:require [slacker.discovery.protocol :as dp]
            [slacker.zk :as zk]
            [slacker.serialization :as s]
            [slacker.utils :as utils]
            [clojure.tools.logging :as logging]))

(defn- ns-callback [e discover nsname]
  (case (:event-type e)
    :NodeDeleted (swap! (.-cached-ns-mapping discover) dissoc nsname)
    :NodeChildrenChanged (dp/fetch-ns-servers! discover nsname)
    :NodeDataChanged (dp/fetch-ns-servers! discover nsname)
    nil))

(defn- clients-callback [e discover]
  (case (:event-type e)
    :NodeChildrenChanged (dp/fetch-all-servers! discover)
    nil))

(defn- try-update-server-data! [e discover server-data-change-handler]
  (when-let [server-addr (second (re-matches #"/.+/servers/(.+?)" (:path e)))]
    (logging/debugf "received notification for server data change on %s" server-addr)
    (let [data (dp/fetch-server-data discover server-addr)]
      (when server-data-change-handler
        (server-data-change-handler server-addr data)))))

(defn- on-zk-events [e discover server-data-change-handler]
  (logging/info "getting zookeeper event" e)
  (cond
    ;; zookeeper path change, refresh servers
    (not= (:event-type e) :None)
    (cond
      ;; event on `servers` node
      (.endsWith ^String (:path e) "servers") (clients-callback e discover)

      ;; event on `servers/addr`, server data update
      (and
       (= :NodeDataChanged (:event-type e))
       (> (.indexOf ^String (:path e) "servers") 0))
      (try-update-server-data! e discover server-data-change-handler)

      ;; event on `namespaces` nodes
      :else
      (when-let [matcher (re-matches #"/.+/namespaces/?(.*?)(/.*)?" (:path e))]
        (ns-callback e discover (second matcher))))

    ;; zookeeper watcher lost, reattach watchers
    (and (= (:event-type e) :None)
         (= (:keeper-state e) :SyncConnected))
    (do
      (doseq [n (keys (dp/ns-server-mappings discover))]
        (dp/fetch-ns-servers! discover n))
      (when (not-empty (dp/ns-server-mappings discover))
        (dp/fetch-all-servers! discover)))))

(defrecord ZookeeperDiscover [zk-conn cluster-name
                              cached-ns-mapping
                              cached-server-data
                              ns-server-update-callback
                              servers-update-callback
                              options]
  dp/SlackerRegistryClient

  (fetch-ns-servers! [this the-ns-name]
    (logging/infof "starting to refresh servers of %s" the-ns-name)
    (let [node-path (utils/zk-path (:zk-root (.-options this))
                                   (.-cluster-name this) "namespaces" the-ns-name)
          servers (doall (remove utils/meta-path?
                                 (zk/children (.-zk-conn this) node-path :watch? true)))
          servers (or servers [])
          leader-node (when (not-empty servers)
                        (String. ^bytes (zk/data zk-conn (utils/zk-path node-path "_leader")
                                                 :watch? true)
                                 "UTF-8"))

          servers (if (and leader-node
                           (not-empty (filter #(= leader-node %) servers)))
                    ;; put leader node on the first
                    (apply vector leader-node
                           (remove #(= leader-node %) servers))
                    servers)]
      (logging/infof "Setting leader node %s" leader-node)
      ;; update servers for this namespace
      (logging/infof "Setting servers for %s: %s" the-ns-name servers)
      (swap! cached-ns-mapping assoc the-ns-name servers)
      (ns-server-update-callback the-ns-name servers)))

  (fetch-all-servers! [this]
    (logging/infof "starting to refresh online servers list")
    (let [node-path (utils/zk-path (:zk-root (.-options this))
                                   cluster-name "servers")
          servers (into #{} (zk/children (.-zk-conn this) node-path
                                         :watch? true))]
      (servers-update-callback (or servers #{}))))

  (fetch-server-data [this server]
    (let [zk-server-path (utils/zk-path (:zk-root options) (.-cluster-name this)
                                        "servers" server)]
      ;; added watcher for server data changes
      (try (when-let [raw-node (zk/data zk-conn zk-server-path :watch? true)]
             (let [result (utils/deserialize-clj-from-bytes raw-node)]
               (swap! (.-cached-server-data this) assoc server result)
               result))
           (catch Exception e
             (logging/warn e "Error getting server data from zookeeper.")
             nil))))

  (get-server-data-cache [this]
    @(.-cached-server-data this))

  (ns-server-mappings [this]
    @(.-cached-ns-mapping this))

  (fetch-fn-metadata [this fname]
    (let [zk-root (:zk-root (.-option this))
          fnode (utils/zk-path  cluster-name "functions" fname)]
      (when-let [node-data (zk/data (.-zk-conn this) fnode)]
        (utils/deserialize-clj-from-bytes node-data))))

  (fetch-ns-functions [this the-ns-name]
    (let [ns-root (utils/zk-path (:zk-root options) cluster-name
                                 "functions" the-ns-name)
          fnames (or (zk/children (.-zk-conn this) ns-root) [])]
      (map #(str the-ns-name "/" %) fnames)))

  (destroy! [this]
    (zk/close zk-conn)))

(defn zookeeper-discover [zk-addr cluster-name
                          ns-server-update-callback
                          servers-update-callback
                          server-data-handler options]
  (let [zk-conn (zk/connect zk-addr options)
        zk-root (:zk-root options)
        discover (ZookeeperDiscover. zk-conn cluster-name
                                     (atom {}) (atom {})
                                     ns-server-update-callback
                                     servers-update-callback
                                     options)]
    ;; watch 'servers' node
    (zk/register-watcher zk-conn (fn [e]
                                   (on-zk-events e discover server-data-handler)))
    (zk/register-error-handler zk-conn
                               (fn [msg e]
                                 (logging/warn e "Unhandled ZooKeeper Error" msg)))
    (zk/children zk-conn
                 (utils/zk-path zk-root cluster-name "servers")
                 :watch? true)
    discover))
