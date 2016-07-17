(ns slacker.example.embedded-zk
  (:import [org.apache.curator.test TestingServer]))

(defn -main [& args]
  (println "Starting embedded Zookeeper on 2181.")
  (.. (TestingServer. 2181)
      (start))
  (println "Zookeeper Started."))
