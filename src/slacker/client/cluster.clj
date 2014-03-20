(ns slacker.client.cluster
  (:require [zookeeper :as zk])
  (:require [slacker.client])
  (:require [slacker.utils :as utils])
  (:use [slacker.client.common])
  (:use [slacker.serialization])
  (:use [clojure.string :only [split]])
  (:require [clojure.tools.logging :as logging])
  (:import [clojure.lang IDeref IPending IBlockingDeref]))

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
    `(slacker.client/defn-remote ~sc ~fname ~@options)))

(defn use-remote
  "cluster enabled use-remote"
  ([sc-sym] (use-remote sc-sym (ns-name *ns*)))
  ([sc-sym rns & options]
     (let [sc @(resolve sc-sym)]
       (apply slacker.client/use-remote sc-sym rns options))))

(defn- create-slackerc [connection-info & options]
  (apply slacker.client/slackerc connection-info options))

(defn- find-server [slacker-ns-servers ns-name grouping]
  (let [servers (@slacker-ns-servers ns-name)]
    (if-not (empty? servers)
      (let [grouped-servers (grouping servers)
            selected-servers (case grouped-servers
                               :all servers
                               :random [(rand-nth servers)]
                               :first [(first servers)]
                               (if (sequential? grouped-servers)
                                 grouped-servers
                                 (vector grouped-servers)))]
        (if-not (empty? selected-servers)
          selected-servers
          []))
      [])))

(defn- ns-callback [e sc nsname]
  (case (:event-type e)
    :NodeDeleted (delete-ns-mapping sc nsname)
    :NodeChildrenChanged (refresh-associated-servers sc nsname)
    nil))

(defn- clients-callback [e sc]
  (case (:event-type e)
    :NodeChildrenChanged (refresh-all-servers sc)
    nil))

(defn- meta-data-from-zk [zk-conn zk-root cluster-name fname]
  (let [fnode (utils/zk-path zk-root cluster-name "functions" fname)]
    (if-let [node-data (zk/data zk-conn fnode)]
      (deserialize :clj (:data node-data) :bytes))))

(defn- to-fn [f]
  (if-not (fn? f)
    (constantly f)
    f))

(defn group-call-results [grouping-results
                          grouping-exceptions
                          servers
                          call-results]
  (let [call-results (map #(assoc %1 :server %2) call-results servers)]
    (doseq [r (filter :cause call-results)]
      (logging/warn (str "error calling "
                         (:server r)
                         ". Error: "
                         (:cause r))))
    (cond
     ;; there's exception occured and we don't want to ignore
     (every? :cause call-results)
     {:cause {:code :failed
              :nested (map :cause call-results)}}

     (and
      (some :cause call-results)
      (= grouping-exceptions :any))
     {:cause {:code :failed
              :servers (map :server (filter :cause call-results))
              :nested (map :cause (filter :cause call-results))}}

     :else
     (let [valid-results (remove :cause call-results)]
       {:result (case (grouping-results)
                  :single (:result (first valid-results))
                  :vector (mapv :result valid-results)
                  :map (into {} (map #(vector (:server %) (:result %))
                                     valid-results)))}))))

(deftype GroupedPromise [grouping-fn promises]
  IDeref
  (deref [_]
    (let [call-results (mapv deref promises)]
      (grouping-fn call-results)))
  IBlockingDeref
  (deref [this timeout timeout-var]
    (let [time-start (System/currentTimeMillis)]
      (loop [prmss promises]
        (when (not-empty prmss)
          (deref (first prmss) timeout nil)
          (when (< (- (System/currentTimeMillis) time-start) timeout)
            (recur (rest prmss))))))
    (if (every? realized? promises)
      (deref this)
      timeout-var))
  IPending
  (isRealized [_]
    (every? realized? promises)))

(defn grouped-promise [grouping-fn promises]
  (GroupedPromise. grouping-fn promises))

(defn- parse-grouping-options [options call-options
                               ns-name func-name params]
  (vector
   (partial (to-fn (:grouping call-options (:grouping options)))
            ns-name func-name params)
   (partial (to-fn (:grouping-results call-options (:grouping-results options)))
            ns-name func-name params)
   (or (:grouping-exceptions call-options)
       (:grouping-exceptions options))))

(deftype ClusterEnabledSlackerClient
    [cluster-name zk-conn
     slacker-clients slacker-ns-servers
     options]
  CoordinatorAwareClient
  (refresh-associated-servers [this nsname]
    (let [node-path (utils/zk-path (:zk-root options)
                                   cluster-name "namespaces" nsname)
          servers (zk/children zk-conn node-path
                               :watch? true)
          servers (or servers [])]
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
          servers (into #{} (zk/children zk-conn node-path :watch? true))
          servers (or servers [])]
      ;; close connection to offline servers, remove from slacker-clients
      (doseq [s (keys @slacker-clients)]
        (when-not (contains? servers s)
          (logging/info (str "closing connection of " s))
          (slacker.client/close-slackerc (@slacker-clients s))
          (swap! slacker-clients dissoc s)))))
  (get-connected-servers [this]
    (keys @slacker-clients))
  (get-ns-mappings [this]
    @slacker-ns-servers)
  (delete-ns-mapping [this ns]
    (swap! slacker-ns-servers dissoc ns))

  SlackerClientProtocol
  (sync-call-remote [this ns-name func-name params call-options]
    (when (nil? ((get-ns-mappings this) ns-name))
      (refresh-associated-servers this ns-name))
    (let [[grouping* grouping-results* grouping-exceptions*]
          (parse-grouping-options options call-options
                                  ns-name func-name params)
          target-servers (find-server slacker-ns-servers ns-name grouping*)
          target-conns (map @slacker-clients target-servers)]
      (if (empty? target-conns)
        {:cause {:error :not-found}}
        (do
          (logging/debug (str "calling " ns-name "/"
                              func-name " on " target-servers))
          (let [call-results (pmap #(sync-call-remote @%
                                                      ns-name
                                                      func-name
                                                      params
                                                      call-options)
                                   target-conns)]
            (group-call-results grouping-results* grouping-exceptions*
                                target-servers call-results))))))

  (async-call-remote [this ns-name func-name params cb call-options]
    (when (nil? ((get-ns-mappings this) ns-name))
      (refresh-associated-servers this ns-name))
    (let [[grouping* grouping-results* grouping-exceptions*]
          (parse-grouping-options options call-options
                                  ns-name func-name params)
          target-servers (find-server slacker-ns-servers ns-name
                                     (partial grouping* ns-name func-name params))
          target-conns (map @slacker-clients target-servers)
          grouping-fn (partial group-call-results
                               grouping-results*
                               grouping-exceptions*
                               target-servers)
          cb-results (atom [])
          sys-cb (fn [result]
                   (when (= (count (swap! cb-results conj result))
                            (count target-servers))
                     (cb (grouping-fn @cb-results))))]
      (if (empty? target-conns)
        (doto (promise) (deliver {:cause {:error :not-found}}))
        (do
          (logging/debug (str "calling " ns-name "/"
                              func-name " on " target-servers))
          (let [call-prms (mapv
                           #(async-call-remote @%
                                               ns-name
                                               func-name
                                               params
                                               sys-cb
                                               call-options)
                           target-conns)]
            (grouped-promise grouping-fn call-prms))))))
  (close [this]
    (zk/close zk-conn)
    (doseq [s (vals @slacker-clients)]
      (slacker.client/close-slackerc s))
    (reset! slacker-clients {})
    (reset! slacker-ns-servers {}))
  (ping [this]
    (doseq [c (vals @slacker-clients)]
      (ping c)))
  (inspect [this cmd args]
    {:result
     (case cmd
       :functions
       (let [nsname (or args "")
             ns-root (utils/zk-path (:zk-root options) cluster-name
                                    "functions" nsname)
             fnames (or (zk/children zk-conn ns-root) [])]
         (map #(str nsname "/" %) fnames))
       :meta (meta-data-from-zk zk-conn (:zk-root options)
                                cluster-name args))}))

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
                * `:first` always choose the first server available
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
                      the function
  * grouping-exception: how to deal with the exceptions when calling functions
                        on multiple instance.
                        * `:all` the API throws exception when exception
                                   is thrown on every instance
                        * `:any` the API throws exception when any instance throws exception"

  [cluster-name zk-server & {:keys [zk-root grouping grouping-results
                                    grouping-exceptions ping-interval]
                             :or {zk-root "/slacker/cluster"
                                  grouping :random
                                  grouping-results :single
                                  grouping-exceptions :all
                                  ping-interval 0}
                             :as options}]
  (delay
   (let [zk-conn (zk/connect zk-server)
         slacker-clients (atom {})
         slacker-ns-servers (atom {})
         sc (ClusterEnabledSlackerClient.
             cluster-name zk-conn
             slacker-clients slacker-ns-servers
             (assoc options
               :zk-root zk-root
               :grouping grouping
               :grouping-results grouping-results
               :grouping-exceptions grouping-exceptions))]
     (zk/register-watcher zk-conn (fn [e] (on-zk-events e sc)))
     ;; watch 'servers' node
     (zk/children zk-conn
                  (utils/zk-path zk-root cluster-name "servers")
                  :watch? true)
     sc)))
