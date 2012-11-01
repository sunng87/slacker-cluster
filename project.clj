(defproject slacker/slacker-cluster "0.8.5"
  :description "Cluster support for slacker"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [slacker "0.8.5"]
                 [zookeeper-clj "0.9.2"
                  :exclusions [jline junit]]
                 [org.clojure/tools.logging "0.2.3"]]
  :profiles {:dev {:resource-paths ["examples"]
                   :dependencies [[codox "0.6.1"]]}
             :1.3 {:dependencies [org.clojure/clojure "1.3.0"]}}
  :warn-on-reflection true
  :aliases {"run-example-server" ["run" "-m" "slacker.example.cluster-server"]
            "run-example-client" ["run" "-m" "slacker.example.cluster-client"]})

