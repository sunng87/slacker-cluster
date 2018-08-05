(ns slacker.server.cluster
  (:require [slacker.common :refer :all]
            [slacker.server]
            [slacker.serialization :refer :all]
            [slacker.utils :as utils]
            [slacker.zk :as zk]
            [clojure.tools.logging :as logging]
            [clojure.string :as string :refer [split]])
  (:import [java.net Socket InetSocketAddress]
           [org.apache.curator CuratorZookeeperClient]
           [org.apache.curator.framework.recipes.nodes PersistentNode]
           (io.netty.buffer ByteBuf)))

(declare ^{:dynamic true :private true} *zk-conn*)

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

(defn- select-leader [zk-root cluster-name ns-name server-node]
  (let [blocker (atom (promise))
        leader-path (utils/zk-path zk-root cluster-name "namespaces"
                                         ns-name "_leader")
        leader-mutex-path (utils/zk-path leader-path "mutex")]
    (create-node *zk-conn* leader-mutex-path
                 :persistent? true)
    (let [selector (zk/start-leader-election *zk-conn*
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

(defn- select-leaders [zk-root cluster-name nss server-node]
  (doall (map #(select-leader zk-root cluster-name % server-node) nss)))

(defn- release-leader [{blocker :blocker}]
  (deliver @blocker nil))

(defn- try-acquire-leader [{selector :selector blocker :blocker}]
  (reset! blocker (promise))
  (zk/requeue-leader-election selector))

(defn publish-cluster
  "publish server information to zookeeper as cluster for client"
  [cluster port ns-names funcs-map server-data]
  (let [cluster-name (cluster :name)
        zk-root (cluster :zk-root "/slacker/cluster/")
        server-node (str (or (cluster :node)
                             (auto-detect-ip (split (:zk cluster) #",")))
                         ":" port)
        server-path (utils/zk-path zk-root cluster-name "servers" server-node)
        ^ByteBuf server-data-buf (serialize :clj server-data)
        server-path-data (utils/bytes-from-buf server-data-buf)
        _ (.release server-data-buf)
        funcs (keys funcs-map)

        ns-path-fn (fn [p] (utils/zk-path zk-root cluster-name "namespaces" p server-node))
        ephemeral-ns-node-paths (map ns-path-fn ns-names)]

    ;; persistent nodes
    (create-node *zk-conn* (utils/zk-path zk-root cluster-name "servers")
                 :persistent? true)

    (doseq [nn ns-names]
      (create-node *zk-conn* (utils/zk-path zk-root cluster-name "namespaces" nn)
                   :persistent? true))

    (doseq [fname funcs]
      (let [^ByteBuf node-data (serialize :clj
                                          (select-keys
                                           (meta (funcs-map fname))
                                           [:name :doc :arglists]))]
        (create-node *zk-conn*
                     (utils/zk-path zk-root cluster-name "functions" fname)
                     :persistent? true
                     :data (utils/bytes-from-buf node-data))
        (.release node-data)))

    (let [server-ephemeral-node (do
                                  (try (zk/delete *zk-conn* server-path) (catch Exception _))
                                  (zk/create-persistent-ephemeral-node
                                   *zk-conn* server-path server-path-data))
          ns-ephemeral-nodes (doall (map #(do
                                            (try (zk/delete *zk-conn* %) (catch Exception _))
                                            {:path %
                                             :node (atom (zk/create-persistent-ephemeral-node *zk-conn* %))})
                                         ephemeral-ns-node-paths))
          leader-selectors (select-leaders zk-root cluster-name ns-names server-node)]
      [server-ephemeral-node ns-ephemeral-nodes leader-selectors])))

(defmacro ^:private with-zk
  "publish server information to specifized zookeeper for client"
  [zk-conn & body]
  `(binding [*zk-conn* ~zk-conn]
     ~@body))

(defrecord SlackerClusterServer [svr zk-conn zk-data])

(defn set-server-data!
  "Update server data for this server, clients will be notified"
  [slacker-server data]
  (let [^ByteBuf serialized-data (serialize :clj data)
        data-node (first (.-zk-data ^SlackerClusterServer slacker-server))]
    (zk/set-persistent-ephemeral-node-data data-node (utils/bytes-from-buf serialized-data))
    (.release serialized-data)))

(defn get-server-data
  "Fetch server data from zookeeper"
  [slacker-server]
  (let [data-node (first (.-zk-data ^SlackerClusterServer slacker-server))]
    (when-let [data-bytes (zk/get-persistent-ephemeral-node-data data-node)]
      (let [^ByteBuf data-buf (utils/buf-from-bytes data-bytes)
            data (deserialize :clj data-buf)]
        (.release data-buf)
        data))))

(serialize :clj {:a 1 :b 2})

(defn- extract-ns [fn-coll]
  (mapcat #(if (map? %) (keys %) [(ns-name %)]) fn-coll))

(defn unpublish-ns!
  "Unpublish a namespace from zookeeper, which means the service under the
  namespace on this server will be offline from client, while we still have
  time for remaining requests to finish. "
  [server ns-name]
  (let [{zk-data :zk-data} server
        [_ zk-ephemeral-nodes leader-selectors] zk-data]
    (doseq [[{path :path node :node} lead-sel]
            (partition 2 (interleave zk-ephemeral-nodes leader-selectors))]
      (when (string/index-of path (str "/namespaces/" ns-name "/"))
        (zk/uncreate-persistent-ephemeral-node @node)
        (release-leader lead-sel)))))

(defn unpublish-all!
  "Unpublish all namespaces on this server. This is helpful when you need
  graceful shutdown. The client needs some time to receive the offline event so
  in this period the server still works."
  [server]
  (let [{zk-data :zk-data} server
        [_ zk-ephemeral-nodes leader-selectors] zk-data]
    (doseq [{n :node} zk-ephemeral-nodes]
      (zk/uncreate-persistent-ephemeral-node @n))
    (doseq [n leader-selectors]
      (release-leader n))))

(defn publish-ns!
  "Publish a namespace to zookeeper. Typicially this is for republish a namespace after
  `unpublish-ns!` or `unpublish-all!`."
  [server ns-name]
  (let [{zk-data :zk-data zk-conn :zk-conn} server
        [_ zk-ephemeral-nodes leader-selectors] zk-data]
    (doseq [[{path :path node :node} lead-sel]
            (partition 2 (interleave zk-ephemeral-nodes leader-selectors))]
      (when (string/index-of path (str "/namespaces/" ns-name "/"))
        (let [new-node (with-zk zk-conn
                         (zk/create-persistent-ephemeral-node *zk-conn* path))]
          (reset! node new-node))
        (try-acquire-leader lead-sel)))))

(defn publish-all!
  "Re-publish all namespaces hosted by this server."
  [server]
  (let [{zk-data :zk-data zk-conn :zk-conn} server
        [_ zk-ephemeral-nodes leader-selectors] zk-data]
    (doseq [{path :path node :node} zk-ephemeral-nodes]
      (let [new-node (with-zk zk-conn
                       (zk/create-persistent-ephemeral-node *zk-conn* path))]
        (reset! node new-node)))
    (doseq [n leader-selectors]
      (try-acquire-leader n))))

(declare stop-slacker-server)
(defn- slacker-manager-api [server-ref]
  {"slacker.cluster.manager"
   {"offline" (fn [] (unpublish-all! @server-ref))
    "offline-ns" (fn [nsname] (unpublish-ns! @server-ref nsname))
    "online-ns" (fn [nsname] (publish-ns! @server-ref nsname))
    "online" (fn [] (publish-all! @server-ref))
    "set-server-data!" (fn [data] (set-server-data! @server-ref data))
    "server-data" (fn [] (get-server-data @server-ref))
    "shutdown" (fn [] (do (future (stop-slacker-server @server-ref)) nil))}})

(defn start-slacker-server
  "Start a slacker server to expose all public functions under
  a namespace. This function is enhanced for cluster support. You can
  supply a zookeeper instance and a cluster name to the :cluster option
  to register this server as a node of the cluster."
  [fn-coll port & options]
  (let [{:keys [cluster server-data manager]
         :as options-map} options
        user-fn-coll (if (vector? fn-coll) fn-coll [fn-coll])
        server-ref (when manager (atom nil))
        fn-coll (if manager
                  (conj user-fn-coll (slacker-manager-api server-ref))
                  user-fn-coll)
        server-backend (apply slacker.server/start-slacker-server
                              fn-coll port options)
        funcs (apply merge (map slacker.server/parse-funcs fn-coll))
        zk-conn (zk/connect (:zk cluster) options-map)
        zk-data (when-not (nil? cluster)
                  (with-zk zk-conn
                    ;; manager ns is not available on zk
                    (publish-cluster cluster port (extract-ns user-fn-coll)
                                     funcs server-data)))]
    (zk/register-error-handler zk-conn
                               (fn [msg e]
                                 (logging/warn e "Unhandled Error" msg)))

    (let [server (SlackerClusterServer. server-backend zk-conn zk-data)]
      (when server-ref
        (reset! server-ref server))
      server)))

(defn stop-slacker-server
  "Shutdown slacker server, gracefully."
  [server]
  (let [{svr :svr zk-conn :zk-conn zk-data :zk-data} server
        session-timeout (.. zk-conn
                            (getZookeeperClient)
                            (getZooKeeper)
                            (getSessionTimeout))
        [server-ephemeral-node ns-ephemeral-nodes leader-selectors] zk-data]
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
    (zk/close zk-conn)

    ;; wait a session timeout to make sure clients are notified
    (Thread/sleep session-timeout)
    (slacker.server/stop-slacker-server svr)))

(defn get-slacker-server-working-ip [zk-addrs]
  (auto-detect-ip (split zk-addrs #",")))
