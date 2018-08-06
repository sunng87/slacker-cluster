(ns slacker.discovery.protocol)

(defprotocol SlackerServiceRegistry
  (init! [this cluster-info port ns-names funcs-map server-data])
  (destroy! [this])
  (publish-ns! [this the-ns-name])
  (publish-all! [this])
  (unpublish-ns! [this the-ns-name])
  (unpublish-all! [this])
  (set-server-data! [this data])
  (get-server-data [this]))

(defprotocol SlackerRegistryClient
  (fetch-ns-servers! [this the-ns-name])
  (fetch-all-servers! [this])
  (fetch-server-data [this server])
  (get-server-data-cache [this])
  (fetch-ns-functions [this the-ns-name])
  (fetch-fn-metadata [this fname])
  (ns-server-mappings [this])
  (destroy! [this]))
