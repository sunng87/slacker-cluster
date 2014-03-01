(ns slacker.example.cluster-client
  (:use [slacker.common])
  (:use [slacker.client.cluster])
  (:use [slacker.client :only [close-slackerc]]))

(def sc (clustered-slackerc "example-cluster" "127.0.0.1:2181"))

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
  :grouping :all
  :grouping-results :map
  :async? true
  :callback (fn [e r]
              (println r)))

(defn -main [& args]
  (binding [*debug* true]
    (println (timestamp))
    (println (rand-ints 10))
    (println @(async-timestamp)))

  (dotimes [_ 100] (timestamp))

  (println (all-timestamp))
  (async-timestamp)

  (close-slackerc sc)
  (System/exit 0))
