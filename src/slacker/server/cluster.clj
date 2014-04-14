(ns slacker.server.cluster
  (:require [zookeeper :as zk])
  (:use [slacker common serialization])
  (:use [clojure.string :only [split]])
  (:require [slacker.server])
  (:require [slacker.utils :as utils])
  (:require [clojure.tools.logging :as logging])
  (:import java.net.Socket))

(declare ^{:dynamic true} *zk-conn*)

(defn- auto-detect-ip
  "detect IP by connecting to zookeeper"
  [zk-addr]
  (let [zk-address (split zk-addr #":")
        zk-ip (first zk-address)
        zk-port (Integer/parseInt (second zk-address))
        socket (Socket. ^String ^Integer zk-ip zk-port)
        local-ip (.getHostAddress (.getLocalAddress socket))]
    (.close socket)
    local-ip))

(defn- create-node
  "get zk connector & node  :persistent?
   check whether exist already
   if not ,create & set node data with func metadata
   "
  [zk-conn node-name
   & {:keys [data persistent?]
      :or {data nil
           persistent? false}}]
  (if-not (zk/exists zk-conn node-name )
    (zk/create-all zk-conn node-name :persistent? persistent?))
  (if-not (nil? data)
    (zk/set-data zk-conn node-name data
                 (:version (zk/exists zk-conn node-name)))))

(defn publish-cluster
  "publish server information to zookeeper as cluster for client"
  [cluster port ns-names funcs-map]
  (let [cluster-name (cluster :name)
        zk-root (cluster :zk-root "/slacker/cluster/")
        server-node (str (or (cluster :node)
                             (auto-detect-ip (first (split (:zk cluster) #","))))
                         ":" port)
        funcs (keys funcs-map)]
    (create-node *zk-conn* (utils/zk-path zk-root cluster-name "servers")
                 :persistent? true)
    (create-node *zk-conn* (utils/zk-path zk-root
                                          cluster-name
                                          "servers"
                                          server-node ))
    (doseq [nn ns-names]
      (create-node *zk-conn* (utils/zk-path zk-root
                                            cluster-name
                                            "namespaces"
                                            nn)
                   :persistent? true)
      (create-node *zk-conn* (utils/zk-path zk-root
                                            cluster-name
                                            "namespaces"
                                            nn server-node)))
    (doseq [fname funcs]
      (create-node *zk-conn*
                   (utils/zk-path zk-root cluster-name "functions" fname)
                   :persistent? true
                   :data (serialize
                          :clj
                          (select-keys
                           (meta (funcs-map fname))
                           [:name :doc :arglists])
                          :bytes)))))

(defmacro with-zk
  "publish server information to specifized zookeeper for client"
  [zk-conn & body]
  `(binding [*zk-conn* ~zk-conn]
     ~@body))

(declare register-zk-data)
(defn- on-session-expired [cluster port nss fns]
  (fn [evt]
    (when (= (:keeper-state evt) :Expired)
      (logging/warn "Zookeeper session expired. Trying to reconnect.")
      (register-zk-data cluster port nss fns))))

(defn- register-zk-data [cluster port nss fns]
  (logging/info "Creating zookeeper nodes for slacker server.")
  (with-zk (zk/connect (:zk cluster)
                       :timeout-msec 3000
                       :watcher (on-session-expired cluster port nss fns))
    (publish-cluster cluster port
                     (map ns-name nss) fns)))

(defn start-slacker-server
  "Start a slacker server to expose all public functions under
  a namespace. This function is enhanced for cluster support. You can
  supply a zookeeper instance and a cluster name to the :cluster option
  to register this server as a node of the cluster."
  [exposed-ns port & options]
  (let [svr (apply slacker.server/start-slacker-server
                   exposed-ns
                   port
                   options)
        {:keys [cluster]} options
        exposed-ns (if (coll? exposed-ns) exposed-ns [exposed-ns])
        funcs (apply merge
                     (map slacker.server/ns-funcs exposed-ns))]
    (when-not (nil? cluster)
      (register-zk-data cluster port exposed-ns funcs))
    svr))
