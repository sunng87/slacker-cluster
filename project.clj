(defproject slacker/slacker-cluster "0.12.1-SNAPSHOT"
  :description "Cluster support for slacker"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :url "https://github.com/sunng87/slacker-cluster"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [slacker "0.12.1-SNAPSHOT"]
                 [org.apache.curator/curator-framework "2.6.0"
                  :exclusions [jline]]
                 [org.apache.curator/curator-recipes "2.6.0"]
                 [org.clojure/tools.logging "0.3.0"]]
  :profiles {:example {:source-paths ["examples"]}}
  :plugins [[codox "0.6.7"]]
  :global-vars {*warn-on-reflection* true}
  :aliases {"run-example-server" ["with-profile" "default,example" "run" "-m" "slacker.example.cluster-server"]
            "run-example-client" ["with-profile" "default,example" "run" "-m" "slacker.example.cluster-client"]}
  :lein-release {:scm :git
                 :deploy-via :shell
                 :shell ["lein" "deploy" "clojars"]})
