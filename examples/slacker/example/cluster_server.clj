(ns slacker.example.cluster-server
  (:use [slacker.server.cluster])
  (:use [slacker.interceptor])
  (:require [slacker.example.api]))

(definterceptor log-function-calls
  :before (fn [req]
            (println (str "calling: " (:fname req)))
            req))

(defn -main [& args]
  (start-slacker-server [(the-ns 'slacker.example.api)
                         {"slacker.example.api2" {"echo2" (fn [& args] args)}}]
                        (if (first args)
                          (Integer/valueOf (first args))
                          (+ 10000 (rand-int 10000)))
                        :cluster {:zk "127.0.0.1:2181"
                                  :name "example-cluster"}
                        :server-data {:label :example}
                        :interceptors (interceptors [log-function-calls])
                        :zk-session-timeout 10000)
  (println "Slacker example server (cluster enabled) started."))
