(defproject slacker/slacker-cluster "0.16.0-SNAPSHOT"
  :description "Cluster support for slacker"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :url "https://github.com/sunng87/slacker-cluster"
  :dependencies [[org.apache.curator/curator-framework "4.0.0"
                  :exclusions [jline]]
                 [org.apache.curator/curator-recipes "4.0.0"]
                 [manifold "0.1.6"]
                 [org.clojure/tools.logging "0.4.0"]]
  :profiles {:example {:source-paths ["examples"]
                       :dependencies [[org.apache.curator/curator-test "4.0.0"]]}
             :dev {:dependencies [[org.clojure/clojure "1.9.0"]
                                  [slacker "0.16.0-SNAPSHOT"]
                                  [log4j "1.2.17"]
                                  [org.slf4j/slf4j-log4j12 "1.7.25"]]}
             :clojure18 {:dependencies [[org.clojure/clojure "1.8.0"]]}
             :clojure19 {:dependencies [[org.clojure/clojure "1.9.0"]]}}
  :plugins [[lein-codox "0.9.5"]]
  :global-vars {*warn-on-reflection* true}
  :aliases {"run-example-server" ["with-profile" "default,dev,clojure18,example" "run" "-m" "slacker.example.cluster-server"]
            "run-example-client" ["with-profile" "default,dev,clojure18,example" "run" "-m" "slacker.example.cluster-client"]
            "run-example-zk" ["with-profile" "default,dev,clojure18,example" "run" "-m" "slacker.example.embedded-zk"]
            "test-all" ["with-profile" "default,dev,clojure18:default,dev,clojure19" "test"]}
  :deploy-repositories {"releases" :clojars}
  :jvm-opts ["-Xmx256m"]
  :codox {:output-path "target/codox"
          :source-uri "https://github.com/sunng87/slacker/blob/master/{filepath}#L{line}"
          :metadata {:doc/format :markdown}})
