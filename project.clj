(defproject slacker/slacker-cluster "0.8.2"
  :description "Cluster support for slacker"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [slacker "0.8.2"]
                 [zookeeper-clj "0.9.2"
                  :exclusions [jline junit]]
                 [org.clojure/tools.logging "0.2.3"]]
  :dev-dependencies [[codox "0.6.1"]]
  :extra-classpath-dirs ["examples"]
  :run-aliases {:cluster-server "slacker.example.cluster-server"
                :cluster-client "slacker.example.cluster-client"})

