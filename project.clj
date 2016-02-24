(defproject slacker/slacker-cluster "0.14.0-SNAPSHOT"
  :description "Cluster support for slacker"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :url "https://github.com/sunng87/slacker-cluster"
  :dependencies [[org.apache.curator/curator-framework "2.9.0"
                  :exclusions [jline]]
                 [org.apache.curator/curator-recipes "2.9.0"]
                 [org.clojure/tools.logging "0.3.1"]]
  :profiles {:example {:source-paths ["examples"]}
             :dev {:dependencies [[org.clojure/clojure "1.8.0"]
                                  [slacker "0.14.0-SNAPSHOT"]
                                  [log4j "1.2.17"]
                                  [org.slf4j/slf4j-log4j12 "1.7.12"]]}
             :clojure15 {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :clojure16 {:dependencies [[org.clojure/clojure "1.6.0"]]}
             :clojure17 {:dependencies [[org.clojure/clojure "1.7.0"]]}
             :clojure18 {:dependencies [[org.clojure/clojure "1.8.0"]]}}
  :plugins [[codox "0.8.15"]]
  :global-vars {*warn-on-reflection* true}
  :aliases {"run-example-server" ["with-profile" "default,dev,clojure17,example" "run" "-m" "slacker.example.cluster-server"]
            "run-example-client" ["with-profile" "default,dev,clojure17,example" "run" "-m" "slacker.example.cluster-client"]
            "test-all" ["with-profile" "default,clojure15:default,clojure16" "test"]}
  :deploy-repositories {"releases" :clojars}
  :jvm-opts ["-Xmx256m"])
