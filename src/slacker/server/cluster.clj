(ns slacker.server.cluster
  (:require [slacker.common :refer :all]
            [slacker.server]
            [slacker.discovery.protocol :as dp]
            [slacker.discovery.zk :as dzk]
            [clojure.tools.logging :as logging]))

(defrecord SlackerClusterServer [server service-registry])

(defn set-server-data!
  "Update server data for this server, clients will be notified"
  [^SlackerClusterServer slacker-server data]
  (dp/set-server-data! (.-service-registry slacker-server) data))

(defn get-server-data
  "Fetch server data from zookeeper"
  [^SlackerClusterServer slacker-server]
  (dp/get-server-data (.-service-registry slacker-server)))

(defn- extract-ns [fn-coll]
  (mapcat #(if (map? %) (keys %) [(ns-name %)]) fn-coll))

(defn unpublish-ns!
  "Unpublish a namespace from zookeeper, which means the service under the
  namespace on this server will be offline from client, while we still have
  time for remaining requests to finish. "
  [^SlackerClusterServer server the-ns-name]
  (dp/unpublish-ns! (.-service-registry server) the-ns-name))

(defn unpublish-all!
  "Unpublish all namespaces on this server. This is helpful when you need
  graceful shutdown. The client needs some time to receive the offline event so
  in this period the server still works."
  [^SlackerClusterServer server]
  (dp/unpublish-all! (.-service-registry server)))

(defn publish-ns!
  "Publish a namespace to zookeeper. Typicially this is for republish a namespace after
  `unpublish-ns!` or `unpublish-all!`."
  [^SlackerClusterServer server the-ns-name]
  (dp/publish-ns! (.-service-registry server) the-ns-name))

(defn publish-all!
  "Re-publish all namespaces hosted by this server."
  [^SlackerClusterServer server]
  (dp/publish-all! (.-service-registry server)))

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

        zk-registry (when (:zk cluster)
                      (let [r (dzk/zookeeper-service-discovery (:zk cluster) options-map)]
                        (dp/init! r cluster port (extract-ns user-fn-coll)
                                  funcs server-data)
                        r))]
    (let [server (SlackerClusterServer. server-backend zk-registry)]
      (when server-ref
        (reset! server-ref server))
      server)))

(defn stop-slacker-server
  "Shutdown slacker server, gracefully."
  [slacker-server]
  (let [{server :server service-registry :service-registry} slacker-server
        session-timeout (when-let [zk-conn (:zk-conn service-registry)]
                          (.. zk-conn
                              (getZookeeperClient)
                              (getZooKeeper)
                              (getSessionTimeout)))]
    (dp/destroy! service-registry)
    ;; wait a session timeout to make sure clients are notified
    (Thread/sleep session-timeout)
    (slacker.server/stop-slacker-server server)))
