(ns slacker.discovery.zk.server
  (:require [slacker.zk :as zk]
            [slacker.serialization :refer :all]
            [slacker.utils :as utils]
            [slacker.discovery.protocol :as dp]
            [clojure.string :as string :refer [split]]
            [clojure.tools.logging :as logging])
  (:import [java.net Socket InetSocketAddress]))

(defn- create-node
  "get zk connector & node  :persistent?
   check whether exist already
   if not ,create & set node data with func metadata
   "
  [zk-conn node-name
   & {:keys [data persistent?]
      :or {persistent? false}}]
  ;; delete ephemeral node before create it
  (when-not persistent?
    (try
      (zk/delete zk-conn node-name)
      (catch Exception _)))
  (when-not (zk/exists zk-conn node-name)
    (zk/create-all zk-conn node-name
                   :persistent? persistent?
                   :data data)))

(defn- auto-detect-ip
  "detect IP by connecting to zookeeper"
  [zk-addrs]
  (or (some (fn [zk-addr]
              (try
                (let [zk-address (split zk-addr #":")
                      zk-ip (first zk-address)
                      zk-port (Integer/parseInt (second zk-address))]
                  (with-open [socket (Socket.)]
                    (.connect ^Socket socket (InetSocketAddress. ^String zk-ip (int zk-port)) 5000)
                    (.getHostAddress (.getLocalAddress socket))))
                (catch Exception ex
                  (logging/warnf ex "Auto detect ip with zookeeper address:\"%s\" failed, try next one"
                                 zk-addr))))
            zk-addrs)
      (throw (RuntimeException. "Auto detect ip failed"))))



(defn- select-leader [zk-conn zk-root cluster-name ns-name server-node]
  (let [blocker (atom (promise))
        leader-path (utils/zk-path zk-root cluster-name "namespaces"
                                   ns-name "_leader")
        leader-mutex-path (utils/zk-path leader-path "mutex")]
    (create-node zk-conn leader-mutex-path
                 :persistent? true)
    (let [selector (zk/start-leader-election zk-conn
                                             leader-mutex-path
                                             (fn [conn]
                                               (logging/infof "%s is becoming the leader of %s" server-node ns-name)
                                               (zk/set-data conn
                                                            leader-path
                                                            (.getBytes ^String server-node "UTF-8"))
                                               ;; block forever
                                               @@blocker))]
      {:selector selector
       :blocker blocker})))

(defn- select-leaders [zk-conn zk-root cluster-name nss server-node]
  (doall (map #(select-leader zk-conn zk-root cluster-name % server-node) nss)))

(defn- release-leader [{blocker :blocker}]
  (deliver @blocker nil))

(defn- try-acquire-leader [{selector :selector blocker :blocker}]
  (reset! blocker (promise))
  (zk/requeue-leader-election selector))

(defrecord ZookeeperData [server-ephemeral-node ns-ephemeral-nodes leader-selectors])

(defn publish-cluster
  "publish server information to zookeeper as cluster for client"
  [zk-conn cluster port ns-names funcs-map server-data]
  (let [cluster-name (cluster :name)
        zk-root (cluster :zk-root "/slacker/cluster/")
        server-node (str (or (cluster :node)
                             (auto-detect-ip (split (:zk cluster) #",")))
                         ":" port)
        server-path (utils/zk-path zk-root cluster-name "servers" server-node)
        server-path-data (utils/serialize-clj-to-bytes server-data)
        funcs (keys funcs-map)

        ns-path-fn (fn [p] (utils/zk-path zk-root cluster-name "namespaces" p server-node))
        ephemeral-ns-node-paths (map ns-path-fn ns-names)]

    ;; persistent nodes
    (create-node zk-conn (utils/zk-path zk-root cluster-name "servers")
                 :persistent? true)

    (doseq [nn ns-names]
      (create-node zk-conn (utils/zk-path zk-root cluster-name "namespaces" nn)
                   :persistent? true))

    (doseq [fname funcs]
      (let [node-data (utils/serialize-clj-to-bytes (select-keys
                                                     (meta (funcs-map fname))
                                                     [:name :doc :arglists]))]
        (create-node zk-conn
                     (utils/zk-path zk-root cluster-name "functions" fname)
                     :persistent? true
                     :data (utils/bytes-from-buf node-data))))

    (let [server-ephemeral-node (do
                                  (try (zk/delete zk-conn server-path) (catch Exception _))
                                  (zk/create-persistent-ephemeral-node
                                   zk-conn server-path server-path-data))
          ns-ephemeral-nodes (doall (map #(do
                                            (try (zk/delete zk-conn %) (catch Exception _))
                                            {:path %
                                             :node (atom (zk/create-persistent-ephemeral-node zk-conn %))})
                                         ephemeral-ns-node-paths))
          leader-selectors (select-leaders zk-conn zk-root cluster-name ns-names server-node)]
      (ZookeeperData. server-ephemeral-node ns-ephemeral-nodes leader-selectors))))

(defrecord ZookeeperInfo [zk-conn zk-data]
  dp/SlackerServiceRegistry
  (init! [this cluster-info port ns-names funcs-map server-data]
    (let [zk-data (publish-cluster (.-zk-conn this) cluster-info port
                                   ns-names funcs-map server-data)]
      (reset! (.-zk-data this) zk-data)))

  (destroy! [this]
    (let [{server-ephemeral-node :server-ephemeral-node
           ns-ephemeral-nodes :ns-ephemeral-nodes
           leader-selectors :leader-selectors} (.-zk-data this)]
      ;; delete server nodes
      (zk/uncreate-persistent-ephemeral-node server-ephemeral-node)
      ;; delete ns nodes
      (doseq [{node :node} ns-ephemeral-nodes]
        (zk/uncreate-persistent-ephemeral-node @node))
      ;; stop leader selector
      (doseq [{selector :selector blocker :blocker :as ls} leader-selectors]
        (release-leader ls)
        (zk/stop-leader-election selector))
      ;; close connection
      (logging/info "closing zk connection")
      (zk/close zk-conn)))

  (publish-ns! [this the-ns-name]
    (let [{zk-ephemeral-nodes :ns-ephemeral-nodes
           leader-selectors :leader-selectors} @(.-zk-data this)]
      (doseq [[{path :path node :node} lead-sel]
              (partition 2 (interleave zk-ephemeral-nodes leader-selectors))]
        (when (string/index-of path (str "/namespaces/" the-ns-name "/"))
          (let [new-node (zk/create-persistent-ephemeral-node (.-zk-conn this) path)]
            (reset! node new-node))
          (try-acquire-leader lead-sel)))))

  (unpublish-ns! [this the-ns-name]
    (let [{zk-ephemeral-nodes :ns-ephemeral-nodes
           leader-selectors :leader-selectors} @(.-zk-data this)]
      (doseq [[{path :path node :node} lead-sel]
              (partition 2 (interleave zk-ephemeral-nodes leader-selectors))]
        (when (string/index-of path (str "/namespaces/" the-ns-name "/"))
          (zk/uncreate-persistent-ephemeral-node @node)
          (release-leader lead-sel)))))

  (unpublish-all! [this]
    (let [{zk-ephemeral-nodes :ns-ephemeral-nodes
           leader-selectors :leader-selectors} @(.-zk-data this)]
      (doseq [{n :node} zk-ephemeral-nodes]
        (zk/uncreate-persistent-ephemeral-node @n))
      (doseq [n leader-selectors]
        (release-leader n))))

  (publish-all! [this]
    (let [{zk-ephemeral-nodes :ns-ephemeral-nodes
           leader-selectors :leader-selectors} @(.-zk-data this)]
    (doseq [{path :path node :node} zk-ephemeral-nodes]
      (let [new-node (zk/create-persistent-ephemeral-node (.-zk-conn this) path)]
        (reset! node new-node)))
    (doseq [n leader-selectors]
      (try-acquire-leader n))))

  (set-server-data! [this data]
    (let [serialized-data (utils/serialize-clj-to-bytes data)
          data-node (.-server-ephemeral-node @(.-zk-data this))]
      (zk/set-persistent-ephemeral-node-data data-node serialized-data)))

  (get-server-data [this]
    (let [data-node (.-server-ephemeral-node @(.-zk-data this))]
      (when-let [data-bytes (zk/get-persistent-ephemeral-node-data data-node)]
        (utils/deserialize-clj-from-bytes data-bytes)))))

(defn zookeeper-service-discovery [zk-addr options]
  (let [zk-conn (zk/connect zk-addr options)]
    (zk/register-error-handler zk-conn
                               (fn [msg e]
                                 (logging/warn e "Unhandled Error" msg)))
    (ZookeeperInfo. zk-conn (atom nil))))

(defn get-slacker-server-working-ip [zk-addrs]
  (auto-detect-ip (split zk-addrs #",")))
