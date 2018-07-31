(ns slacker.discovery.zk.client
  (:require [slacker.discovery.protocol :as dp]
            [slacker.zk :as zk]
            [slacker.serialization :as s]
            [slacker.utils :as utils]
            [clojure.tools.logging :as logging]))

(defn- ns-callback [e discover nsname]
  (case (:event-type e)
    :NodeDeleted ((.-ns-server-delete-callback discover) nsname)
    :NodeChildrenChanged (fetch-ns-servers! discover nsname)
    :NodeDataChanged (fetch-ns-servers! sc nsname)
    nil))

(defn- clients-callback [e discover]
  (case (:event-type e)
    :NodeChildrenChanged (fetch-all-servers! discover)
    nil))

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
      (try-update-server-data! e sc server-data-change-handler)

      ;; event on `namespaces` nodes
      :else
      (when-let [matcher (re-matches #"/.+/namespaces/?(.*?)(/.*)?" (:path e))]
        (ns-callback e discover (second matcher))))

    ;; zookeeper watcher lost, reattach watchers
    (and (= (:event-type e) :None)
         (= (:keeper-state e) :SyncConnected))
    (do
      (doseq [n (keys (get-ns-mappings sc))]
        (fetch-ns-servers! discover n))
      (when (not-empty (get-connected-servers sc))
        (fetch-all-servers discover)))))

(defrecord ZookeeperDiscover [zk-conn cluster-name
                              ns-server-update-callback
                              ns-server-delete-callback
                              servers-update-callback
                              options]
  dp/SlackerRegistryClient

  (fetch-ns-servers! [this the-ns-name]
    (logging/infof "starting to refresh servers of %s" nsname)
    (let [node-path (utils/zk-path (:zk-root (.-options this))
                                   (.-cluster-name this) "namespaces" nsname)
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
      (logging/infof "Setting servers for %s: %s" nsname servers)
      (ns-server-update-callback servers)))

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
             (s/deserialize :clj (utils/buf-from-bytes raw-node)))
           (catch Exception e
             (logging/warn e "Error getting server data from zookeeper.")
             nil))))

  (destroy! [this]
    (zk/close zk-conn)))

(defn zookeeper-discover [zk-addr cluster-name server-data-handler options]
  (let [zk-conn (zk/connect zk-addr options)
        zk-root (:zk-root options)
        discover (ZookeeperDiscover. zk-conn cluster-name (atom nil) options)]
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
