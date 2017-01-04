(defproject slacker/slacker-cluster "0.14.3-SNAPSHOT"
  :description "Cluster support for slacker"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :url "https://github.com/sunng87/slacker-cluster"
  :dependencies [[org.apache.curator/curator-framework "2.11.1"
                  :exclusions [jline]]
                 [org.apache.curator/curator-recipes "2.11.1"]
                 [org.clojure/tools.logging "0.3.1"]]
  :profiles {:example {:source-paths ["examples"]
                       :dependencies [[org.apache.curator/curator-test "2.11.1"]]}
             :dev {:dependencies [[org.clojure/clojure "1.8.0"]
                                  [slacker "0.14.2"]
                                  [log4j "1.2.17"]
                                  [org.slf4j/slf4j-log4j12 "1.7.21"]]}
             :clojure17 {:dependencies [[org.clojure/clojure "1.7.0"]]}
             :clojure18 {:dependencies [[org.clojure/clojure "1.8.0"]]}
             :clojure19 {:dependencies [[org.clojure/clojure "1.9.0-alpha10"]]}}
  :plugins [[lein-codox "0.9.5"]]
  :global-vars {*warn-on-reflection* true}
  :aliases {"run-example-server" ["with-profile" "default,dev,clojure18,example" "run" "-m" "slacker.example.cluster-server"]
            "run-example-client" ["with-profile" "default,dev,clojure18,example" "run" "-m" "slacker.example.cluster-client"]
            "run-example-zk" ["with-profile" "default,dev,clojure18,example" "run" "-m" "slacker.example.embedded-zk"]
            "test-all" ["with-profile" "default,dev,clojure17:default,dev,clojure18" "test"]}
  :deploy-repositories {"releases" :clojars}
  :jvm-opts ["-Xmx256m"]
  :codox {:output-path "target/codox"
          :source-uri "https://github.com/sunng87/slacker/blob/master/{filepath}#L{line}"
          :metadata {:doc/format :markdown}})
