(ns slacker.example.cluster-client
  (:use [slacker.common])
  (:use [slacker.client.cluster])
  (:use [slacker.client :only [close-slackerc shutdown-slacker-client-factory]]))

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
              (println "++++" r)))

(defn -main [& args]
  (dotimes [_ 100] (timestamp))

  (println (echo 23))
  (println (all-timestamp))
  (async-timestamp)
  (try (make-error) (catch Exception e (println "Expected exception:" (ex-data e))))

  (close-slackerc sc)
  (shutdown-slacker-client-factory)
  (shutdown-agents))
