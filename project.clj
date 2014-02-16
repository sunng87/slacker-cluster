(defproject slacker/slacker-cluster "0.10.0"
  :description "Cluster support for slacker"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [slacker "0.10.1"]
                 [zookeeper-clj "0.9.3"
                  :exclusions [jline junit]]
                 [org.clojure/tools.logging "0.2.4"]]
  :profiles {:dev {:source-paths ["examples"]
                   :dependencies [[codox "0.6.1"]]}
             :1.3 {:dependencies [org.clojure/clojure "1.3.0"]}}
  :warn-on-reflection true
  :aliases {"run-example-server" ["with-profile" "default,dev" "run" "-m" "slacker.example.cluster-server"]
            "run-example-client" ["with-profile" "default,dev" "run" "-m" "slacker.example.cluster-client"]})
