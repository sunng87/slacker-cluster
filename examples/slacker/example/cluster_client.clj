(ns slacker.example.cluster-client
  (:require [slacker.common :refer :all]
            [slacker.client.cluster :refer :all]
            [slacker.client :refer [close-slackerc shutdown-slacker-client-factory]]
            [slacker.interceptor :as si]))

(def interceptor {:before-merge (fn [results] (println "Before merge:" results) results)
                  :after-merge (fn [result] (println "After merge:" result) result)})

(def sc (clustered-slackerc "example-cluster" "127.0.0.1:2181"
                            :interceptors interceptor))

(use-remote 'sc 'slacker.example.api)
(defn-remote sc async-timestamp
  :remote-name "timestamp"
  :remote-ns "slacker.example.api"
  :async? true)
(defn-remote sc all-timestamp
  :remote-name "timestamp"
  :remote-ns "slacker.example.api"
  :grouping :all
  :grouping-results :vector)
(defn-remote sc async-timestamp
  :remote-name "timestamp"
  :remote-ns "slacker.example.api"
  :grouping :least-in-flight
  :grouping-results :map
  :async? true
  :callback (fn [e r]
              (println "++++" r)))
(defn-remote sc echo2
  :remote-ns "slacker.example.api2"
  :grouping :random)

(defn -main [& args]
  (dotimes [_ 100] (timestamp))

  (println (echo 23))
  (println (all-timestamp))
  (async-timestamp)
  (try (make-error) (catch Exception e (println "Expected exception:" (ex-data e))))
  (println (echo2 "Echo" "Some" "Data"))

  (close-slackerc sc)
  (shutdown-slacker-client-factory)
  (shutdown-agents))
