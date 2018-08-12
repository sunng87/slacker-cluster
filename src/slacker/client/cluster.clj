(ns slacker.client.cluster
  (:require [slacker.client]
            [slacker.client.common :refer :all]
            [slacker.client.weight :as weighted]
            [slacker.serialization :refer :all]
            [slacker.utils :as utils]
            [slacker.discovery.protocol :as ds]
            [slacker.discovery.zk.client :as zk]
            [manifold.deferred :as d]
            [clojure.string :refer [split]]
            [clojure.tools.logging :as logging])
  (:import [clojure.lang IDeref IPending IBlockingDeref]
           [slacker.client.common SlackerClient ]
           (io.netty.buffer ByteBuf)))

(def ^{:dynamic true
       :doc "cluster grouping function"}
  *grouping* nil)
(def ^{:dynamic true
       :doc "clustered call result merge function"}
  *grouping-results* nil)
(def ^{:dynamic true
       :doc "clustered exception option"}
  *grouping-exceptions* nil)

(declare try-update-server-data!
         find-least-in-flight-server
         find-weighted-server)

(defprotocol CoordinatorAwareClient
  (refresh-associated-servers [this ns])
  (refresh-all-servers [this])
  (get-connected-servers [this])
  (get-ns-mappings [this])
  (delete-ns-mapping [this fname])
  (server-data [this server-id]))

(defmacro defn-remote
  "cluster enabled defn-remote, with cluster options supported."
  [sc fname & options]
  ;; FIXME: these bindings are unused
  (let [fname-str (str fname)
        remote-ns-declared (> (.indexOf fname-str "/") 0)
        {:keys [remote-ns] :or {remote-ns (ns-name *ns*)}} options
        remote-ns (if remote-ns-declared
                    (first (split fname-str #"/" 2))
                    remote-ns)]
    `(slacker.client/defn-remote ~sc ~fname ~@options)))

(defn use-remote
  "cluster enabled use-remote"
  [& options]
  (apply slacker.client/use-remote options))

(defn- create-slackerc [connection-info & options]
  (apply slacker.client/slackerc connection-info options))

(defn- find-server [slacker-client slacker-ns-servers ns-name grouping]
  (let [servers (slacker-ns-servers ns-name)]
    (if-not (empty? servers)
      (let [grouped-servers (grouping slacker-client servers)
            selected-servers (case grouped-servers
                               :all servers
                               :random [(rand-nth servers)]
                               :first [(first servers)]
                               :leader [(first servers)]
                               :least-in-flight [(find-least-in-flight-server slacker-client servers)]
                               :weighted [(find-weighted-server slacker-client servers)]
                               (if (coll? grouped-servers)
                                 grouped-servers
                                 (vector grouped-servers)))]
        (if-not (empty? selected-servers)
          selected-servers
          []))
      [])))

(defn- to-fn [f]
  (if-not (fn? f)
    (constantly f)
    f))

(defn ^:no-doc group-call-results [grouping-results
                                   grouping-exceptions
                                   call-results]
  (let [req-info (dissoc (first call-results) :result :cause)
        {valid-results false failed-results true} (group-by #(contains? % :cause) call-results)
        grouping-exceptions-results (when (not-empty failed-results)
                                      (cond
                                        (fn? grouping-exceptions)
                                        (grouping-exceptions valid-results failed-results)

                                        (= grouping-exceptions :any)
                                        {:servers (map :server failed-results)
                                         :nested (map :cause failed-results)}

                                        (and (empty? valid-results)
                                             (= grouping-exceptions :all))
                                        {:nested (map :cause call-results)}))]
    (doseq [r failed-results]
      (logging/warn (str "error calling "
                         (:server r)
                         ". Error: "
                         (:cause r))))

    (if grouping-exceptions-results
      (assoc req-info :cause (merge {:code :failed}
                                    grouping-exceptions-results))
      (let [grouping-results-config (grouping-results)]
        (assoc req-info
          :result (case grouping-results-config
                    :nil nil
                    :single (:result (first valid-results))
                    :vector (mapv :result valid-results)
                    :map (into {} (map #(vector (:server %) (:result %))
                                       valid-results))
                    (if (fn? grouping-results-config)
                      (grouping-results-config valid-results)
                      (throw (ex-info "Unsupported grouping-results value"
                                      {:grouping-results grouping-results-config
                                       :results          valid-results})))))))))

(defn ^:no-doc grouped-deferreds [grouping-fn deferreds call-options callback]
  (let [post-group-fn (fn [call-results]
                        (->> call-results
                             ((:before-merge (:interceptors call-options) identity))
                             grouping-fn
                             ((:after-merge (:interceptors call-options) identity))))
        zipped-deferred (d/chain (apply d/zip deferreds) vec post-group-fn)]
    (when callback
      (d/chain zipped-deferred #(callback (:cause %) (:result %))))
    zipped-deferred))

(defn- parse-grouping-options [call-options ns-name func-name params]
  (vector
   (partial (to-fn (or *grouping*
                       (:grouping call-options)))
            ns-name func-name params)
   (partial (to-fn (or *grouping-results*
                       (:grouping-results call-options)))
            ns-name func-name params)
   (or *grouping-exceptions*
       (:grouping-exceptions call-options))))

(deftype ^:no-doc ClusterEnabledSlackerClient
         [discover slacker-clients options]
  CoordinatorAwareClient

  (refresh-associated-servers [this the-ns-name]
    (ds/fetch-ns-servers! (.-discover this) the-ns-name))

  (refresh-all-servers [this]
    (ds/fetch-all-servers! (.-discover this)))

  (get-connected-servers [this]
    (keys @slacker-clients))

  (get-ns-mappings [this]
    (ds/ns-server-mappings (.-discover this)))

  (server-data [this addr]
    (get (ds/get-server-data-cache (.-discover this)) addr))

  SlackerClientProtocol
  (sync-call-remote [this ns-name func-name params call-options]
    ;; synchronized refresh servers from zookeeper
    (when (nil? ((get-ns-mappings this) ns-name))
      (locking this
        (when (nil? ((get-ns-mappings this) ns-name))
          (refresh-associated-servers this ns-name))))

    (let [fname (str ns-name "/" func-name)
          call-options (merge options call-options)
          [grouping* grouping-results* grouping-exceptions*]
          (parse-grouping-options call-options
                                  ns-name func-name params)
          slacker-ns-servers (get-ns-mappings this)
          target-servers (find-server this slacker-ns-servers ns-name grouping*)
          target-conns (filter identity (map @slacker-clients target-servers))]
      (if (empty? target-conns)
        (if (contains? call-options :unavailable-value)
          {:result (:unavailable-value call-options)
           :fname fname}
          {:cause {:error :unavailable :servers target-servers}
           :fname fname})
        (do
          (logging/debug (str "calling " ns-name "/"
                              func-name " on " target-servers))
          (let [call-results (pmap #(assoc (sync-call-remote @%1
                                                             ns-name
                                                             func-name
                                                             params
                                                             call-options)
                                           :server %2)
                                   target-conns
                                   target-servers)]
            (->> call-results
                 ((:before-merge (:interceptors call-options) identity))
                 (group-call-results grouping-results* grouping-exceptions*)
                 ((:after-merge (:interceptors call-options) identity))))))))

  (async-call-remote [this ns-name func-name params cb call-options]
    ;; synchronized refresh servers from zookeeper
    (when (nil? ((get-ns-mappings this) ns-name))
      (locking this
        (when (nil? ((get-ns-mappings this) ns-name))
          (refresh-associated-servers this ns-name))))

    (let [fname (str ns-name "/" func-name)
          call-options (merge options call-options)
          [grouping* grouping-results* grouping-exceptions*]
          (parse-grouping-options call-options
                                  ns-name func-name params)
          slacker-ns-servers (get-ns-mappings this)
          target-servers (find-server this slacker-ns-servers ns-name
                                      (partial grouping* ns-name func-name params))
          target-conns (filter identity (map @slacker-clients target-servers))
          grouping-fn (partial group-call-results
                               grouping-results*
                               grouping-exceptions*)]
      (if (empty? target-conns)
        (doto (d/deferred)
          (d/success! (if (contains? call-options :unavailable-value)
                        {:result (:unavailable-value call-options)
                         :fname fname}
                        {:cause {:error :unavailable :servers target-servers}
                         :fname fname})))
        (do
          (logging/debug (str "calling " ns-name "/"
                              func-name " on " target-servers))
          (let [call-deferreds (mapv
                                #(d/chain (async-call-remote @%1
                                                             ns-name
                                                             func-name
                                                             params
                                                             nil
                                                             call-options)
                                          (fn [result] (assoc result :server %2)))
                                target-conns
                                target-servers)]
            (grouped-deferreds grouping-fn call-deferreds call-options cb))))))
  (close [this]
    (ds/destroy! (.-discover this))
    (doseq [s (vals @slacker-clients)]
      (slacker.client/close-slackerc s))
    (reset! slacker-clients {}))
  (ping [this]
    (doseq [c (vals @slacker-clients)]
      (ping c)))
  (inspect [this cmd args]
    {:result
     (case cmd
       :functions (ds/fetch-ns-functions discover args)
       :meta (ds/fetch-fn-metadata discover args))}))

(defn- ns-server-update-callback [cluster-slacker-client-ref the-ns-name servers]
  ;; establish connection if the server is not connected

  (let [slacker-clients (.-slacker-clients ^ClusterEnabledSlackerClient @cluster-slacker-client-ref)
        options (.-options ^ClusterEnabledSlackerClient @cluster-slacker-client-ref)]
    (doseq [s servers]
      (when-not (contains? @slacker-clients s)
        (let [sc (apply create-slackerc s (flatten (vec options)))
              data (ds/fetch-server-data (.-discover ^ClusterEnabledSlackerClient @cluster-slacker-client-ref) s)]
          (logging/info "establishing connection to " s "with data" data)
          (swap! slacker-clients assoc s sc))))))

(defn- servers-update-callback [cluster-slacker-client-ref servers]
  ;; close connection to offline servers, remove from slacker-clients
  (let [slacker-clients (.-slacker-clients ^ClusterEnabledSlackerClient @cluster-slacker-client-ref)]
    (doseq [s (keys @slacker-clients)]
      (when-not (contains? servers s)
        (logging/infof "closing connection of %s" s)
        (let [the-client (@slacker-clients s)]
          (swap! slacker-clients dissoc s)
          (slacker.client/close-slackerc the-client))))))

(defn- find-least-in-flight-server [^ClusterEnabledSlackerClient client servers]
  (->> servers
       (map #(if-let [sc (get @(.-slacker-clients client) %)]
               (let [pendings (pending-count @sc)]
                 (if (some? pendings) pendings Integer/MAX_VALUE))
               Integer/MAX_VALUE))
       (zipmap servers)
       (sort-by second)
       first
       first))

(defn- find-weighted-server [^ClusterEnabledSlackerClient slacker-client servers]
  (let [weights (map #(:weight (server-data slacker-client %) 1) servers)
        selected-index (weighted/select-weighted-item weights)]
    (nth servers selected-index)))


(defn clustered-slackerc
  "create a cluster enalbed slacker client options:

  * zk-root: specify the root path in zookeeper
  * factory: a client factory, default to non-ssl implementation
  * grouping: specify how the client select servers to call, this allows one or more servers to be called. Possible values:
      * `:first` always choose the first server available
      * `:random` choose a server by random (default)
      * `:leader` use current leader server
      * `:least-in-flight` use server with least pending requests
      * `:weighted` randomly select a server using `:weight` from server data as weight
      * `:all` call function on all servers
      * `(fn [ns fname params slacker-client servers])` specify a function to choose. You can also return :random or :all in this function
  * grouping-results: when you call functions on multiple server, you can specify how many results return for the call. Note that if you use :vector or :map, you will break default behavior of the function. Possible values:
      * `:nil` always returns nil
      * `:single` returns only one value
      * `:vector` returns values from all servers as a vector
      * `:map` returns values from all servers as a map, server host:port as key
      * `(fn [ns fname params])` a function that returns keywords above
  * grouping-exceptions: how to deal with the exceptions when calling functions  on multiple instance.
      * `:all` the API throws exception when exception is thrown on every instance
      * `:any` the API throws exception when any instance throws exception
      * `(fn [valid-result error-results])` a function to decide what exception to throw or never
  * server-data-change-handler: a function accepts the client object, server
    address and server data. Note that this function runs on
    event thread so never do blocking things within it. "

  [cluster-name zk-server & {:keys [zk-root grouping grouping-results
                                    grouping-exceptions ping-interval
                                    factory server-data-change-handler
                                    interceptors]
                             :or {zk-root "/slacker/cluster"
                                  grouping :random
                                  grouping-results :single
                                  grouping-exceptions :all
                                  ping-interval 0}
                             :as options}]
  (delay
   (let [options (assoc options
                        :zk-root zk-root
                        :grouping grouping
                        :grouping-results grouping-results
                        :grouping-exceptions grouping-exceptions
                        :interceptors interceptors)

         sc-ref (promise)

         ns-server-update-callback (partial ns-server-update-callback sc-ref)
         servers-update-callback (partial servers-update-callback sc-ref)
         server-data-change-handler (partial server-data-change-handler sc-ref)

         zk-discover (zk/zookeeper-discover zk-server cluster-name
                                        ns-server-update-callback
                                        servers-update-callback
                                        server-data-change-handler
                                        options)
         slacker-clients (atom {})

         the-client (ClusterEnabledSlackerClient. zk-discover slacker-clients options)]
     (deliver sc-ref the-client)
     the-client)))
