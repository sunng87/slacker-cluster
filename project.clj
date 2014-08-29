(defproject slacker/slacker-cluster "0.11.7-SNAPSHOT"
  :description "Cluster support for slacker"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :url "https://github.com/sunng87/slacker-cluster"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [slacker "0.11.5"]
                 [zookeeper-clj "0.9.3"]
                 [org.apache.zookeeper/zookeeper "3.4.5"
                  :exclusions [jline junit]]
                 [org.clojure/tools.logging "0.2.6"]]
  :profiles {:example {:source-paths ["examples"]}}
  :plugins [[codox "0.6.7"]]
  :global-vars {*warn-on-reflection* true}
  :aliases {"run-example-server" ["with-profile" "default,example" "run" "-m" "slacker.example.cluster-server"]
            "run-example-client" ["with-profile" "default,example" "run" "-m" "slacker.example.cluster-client"]})
