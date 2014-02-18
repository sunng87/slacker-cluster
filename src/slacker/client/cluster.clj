(ns slacker.client.cluster
  (:require [zookeeper :as zk])
  (:require [slacker.client])
  (:require [slacker.utils :as utils])
  (:use [slacker.client.common])
  (:use [slacker.serialization])
  (:use [clojure.string :only [split]])
  (:require [clojure.tools.logging :as logging])
  (:use [slingshot.slingshot :only [throw+]]))

(defprotocol CoordinatorAwareClient
  (refresh-associated-servers [this ns])
  (refresh-all-servers [this])
  (get-connected-servers [this])
  (get-ns-mappings [this])
  (delete-ns-mapping [this fname]))

(defmacro defn-remote
  "cluster enabled defn-remote"
  [sc fname & options]
  (let [fname-str (str fname)
        remote-ns-declared (> (.indexOf fname-str "/") 0)
        {:keys [remote-ns] :or {remote-ns (ns-name *ns*)}} options
        remote-ns (if remote-ns-declared
                    (first (split fname-str #"/" 2))
                    remote-ns)]
    `(do
       (when (nil? ((get-ns-mappings ~sc) ~remote-ns))
         (refresh-associated-servers ~sc ~remote-ns))
       (slacker.client/defn-remote ~sc ~fname ~@options))))

(defn use-remote
  "cluster enabled use-remote"
  ([sc-sym] (use-remote sc-sym (ns-name *ns*)))
  ([sc-sym rns & options]
     (let [sc @(resolve sc-sym)]
       (do
         (when (nil? ((get-ns-mappings sc) (str rns)))
           (refresh-associated-servers sc (str rns)))
         (apply slacker.client/use-remote sc-sym rns options)))))

(defn- create-slackerc [connection-info & options]
  (apply slacker.client/slackerc connection-info options))

(defn- find-server [slacker-ns-servers ns-name grouping]
  (if-let [servers (@slacker-ns-servers ns-name)]
    (let [grouped-servers (grouping servers)
          selected-servers (case grouped-servers
                             :all servers
                             :random [(rand-nth servers)]
                             (if (sequential? grouped-servers)
                               grouped-servers
                               (vector grouped-servers)))]
      (if-not (empty? selected-servers)
        selected-servers
        (throw+ {:code :not-found})))
    (throw+ {:code :not-found})))

(defn- ns-callback [e sc nsname]
  (case (:event-type e)
    :NodeDeleted (delete-ns-mapping sc nsname)
    :NodeChildrenChanged (refresh-associated-servers sc nsname)
    nil))

(defn- clients-callback [e sc]
  (case (:event-type e)
    :NodeChildrenChanged (refresh-all-servers sc) ;;TODO
    nil))

(defn- meta-data-from-zk [zk-conn zk-root cluster-name fname]
  (let [fnode (utils/zk-path zk-root cluster-name "functions" fname)]
    (if-let [node-data (zk/data zk-conn fnode)]
      (deserialize :clj (:data node-data) :bytes))))

(deftype ClusterEnabledSlackerClient
    [cluster-name zk-conn
     slacker-clients slacker-ns-servers
     grouping grouping-results
     options]
  CoordinatorAwareClient
  (refresh-associated-servers [this nsname]
    (let [node-path (utils/zk-path (:zk-root options)
                                   cluster-name "namespaces" nsname)
          servers (zk/children zk-conn node-path
                               :watch? true)]
      ;; update servers for this namespace
      (swap! slacker-ns-servers assoc nsname servers)
      ;; establish connection if the server is not connected
      (doseq [s servers]
        (if-not (contains? @slacker-clients s)
          (let [sc (apply create-slackerc s (flatten (vec options)))]
            (logging/info (str "establishing connection to " s))
            (swap! slacker-clients assoc s sc))))
      servers))
  (refresh-all-servers [this]
    (let [node-path (utils/zk-path (:zk-root options) cluster-name "servers")
          servers (into #{} (zk/children zk-conn node-path :watch? true))]
      ;; close connection to offline servers, remove from slacker-clients
      (doseq [s (keys @slacker-clients)]
        (when-not (contains? servers s)
          (logging/info (str "closing connection of " s))
          (close (@slacker-clients s))
          (swap! slacker-clients dissoc s)))))
  (get-connected-servers [this]
    (keys @slacker-clients))
  (get-ns-mappings [this]
    @slacker-ns-servers)
  (delete-ns-mapping [this ns]
    (swap! slacker-ns-servers dissoc ns))

  SlackerClientProtocol
  (sync-call-remote [this ns-name func-name params]
    (let [fname (str ns-name "/" func-name)
          target-servers (find-server slacker-ns-servers ns-name
                                     (partial grouping ns-name func-name params))
          target-conns (map @slacker-clients target-servers)]
      (logging/debug (str "calling " ns-name "/"
                          func-name " on " target-servers))
      (let [call-results (doall (map #(sync-call-remote % ns-name func-name params)
                                     target-conns))]
        (case (grouping-results ns-name func-name params)
          :single (first call-results)
          :vector (vec call-results)
          :map (into {} (map vector target-servers call-results))))))
  (async-call-remote [this ns-name func-name params cb]
    (let [fname (str ns-name "/" func-name)
          target-servers (find-server slacker-ns-servers ns-name
                                     (partial grouping ns-name func-name params))
          target-conns (map @slacker-clients target-servers)]
      (logging/debug (str "calling " ns-name "/"
                          func-name " on " target-servers))
      (let [call-results (doall (map #(async-call-remote % ns-name func-name params cb)
                                     target-conns))]
        (case (grouping-results ns-name func-name params)
          :single (first call-results)
          :vector (vec call-results)
          :map (into {} (map vector target-servers call-results))))))
  (close [this]
    (zk/close zk-conn)
    (doseq [s (vals @slacker-clients)] (close s))
    (reset! slacker-clients {})
    (reset! slacker-ns-servers {}))
  (inspect [this cmd args]
    (case cmd
      :functions
      (let [nsname (or args "")
            ns-root (utils/zk-path (:zk-root options) cluster-name
                                   "functions" nsname)
            fnames (or (zk/children zk-conn ns-root) [])]
        (map #(str nsname "/" %) fnames))
      :meta (meta-data-from-zk zk-conn (:zk-root options)
                               cluster-name args))))

(defn- on-zk-events [e sc]
  (if (.endsWith ^String (:path e) "servers")
    ;; event on `servers` node
    (clients-callback e sc)
    ;; event on `namespaces` nodes
    (let [matcher (re-matches #"/.+/namespaces/?(.*)" (:path e))]
      (if-not (nil? matcher)
        (ns-callback e sc (second matcher))))))

(defn clustered-slackerc
  "create a cluster enalbed slacker client
  options:
  * zk-root: specify the root path in zookeeper
  * grouping: specify how the client select servers to call,
              this allows one or more servers to be called.
              possible values:
                * `:random` choose a server by random (default)
                * `:all` call function on all servers
                * `(fn [ns fname params servers])` specify a function to choose.
                   you can also return :random or :all in this function
  * grouping-results: when you call functions on multiple server, you can specify
                      how many results return for the call. possible values:
                      * `:single` returns only one value
                      * `:vector` returns values from all servers as a vector
                      * `:map` returns values from all servers as a map, server host:port as key
                      * `(fn [ns fname params])` a function that returns keywords above
                      Note that if you use :vector or :map, you will break default behavior of
                      the function"

  [cluster-name zk-server & {:keys [zk-root grouping grouping-results]
                             :or {zk-root "/slacker/cluster"
                                  grouping :random
                                  grouping-results :single}
                             :as options}]
  (let [zk-conn (zk/connect zk-server)
        slacker-clients (atom {})
        slacker-ns-servers (atom {})
        grouping (if-not (fn? grouping) (constantly grouping) grouping)
        grouping-results (if-not (fn? grouping-results)
                           (constantly grouping-results)
                           grouping-results)
        sc (ClusterEnabledSlackerClient.
            cluster-name zk-conn
            slacker-clients slacker-ns-servers
            grouping grouping-results
            (assoc options :zk-root zk-root))]
    (zk/register-watcher zk-conn (fn [e] (on-zk-events e sc)))
    ;; watch 'servers' node
    (zk/children zk-conn (utils/zk-path zk-root
                                        cluster-name
                                        "servers")
                 :watch? true)
    sc))
