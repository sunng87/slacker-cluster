(ns slacker.example.cluster-client
  (:require [slacker.common :refer :all]
            [slacker.client.cluster :refer :all]
            [slacker.client :refer [close-slackerc shutdown-slacker-client-factory]]
            [slacker.interceptor :as si]))

(def interceptor {:before-merge (fn [results] (println "Before merge:" results) results)
                  :after-merge (fn [result] (println "After merge:" result) result)})

(def sc (clustered-slackerc "example-cluster" "127.0.0.1:2181"))

(use-remote 'sc 'slacker.example.api)

;; def a remote function that returns async promise
(defn-remote sc async-timestamp
  :remote-name "timestamp"
  :remote-ns "slacker.example.api"
  :async? true)

;; def a remote function that calls on all provides in cluster, and
;; return a vector of results
(defn-remote sc all-timestamp
  :remote-name "timestamp"
  :remote-ns "slacker.example.api"
  :grouping :all
  :grouping-results :vector)

;; def a remote function that choose a provide with least pending
;; requests, and return a map of result, of which key is the address
;; of provider. Returns a promise, and a callback is registered and
;; will be called when result is ready.
(defn-remote sc async-timestamp-cb
  :remote-name "timestamp"
  :remote-ns "slacker.example.api"
  :grouping :least-in-flight
  :grouping-results :map
  :async? true
  :callback (fn [e r]
              (println "Callback:" r)))

;; You can access more then 1 remote namespace.
(defn-remote sc echo2
  :remote-ns "slacker.example.api2"
  :grouping :random)

(defn -main [& args]
  (dotimes [_ 100] (timestamp))

  (println (echo 23))
  (println (all-timestamp))
  (println "Async call timestamp:" @(async-timestamp))
  (async-timestamp-cb)
  (try (make-error) (catch Exception e (println "Expected exception:" (ex-data e))))
  (println (echo2 "Echo" "Some" "Data"))

  (close-slackerc sc)
  (shutdown-slacker-client-factory)
  (shutdown-agents))
