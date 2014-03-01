(ns slacker.test.server.cluster
  (:use [clojure.test] )
  (:use [slacker.server cluster])
  (:use [slacker.utils :only [zk-path]]))

(deftest test-zk-path
  (is (= "/tom/cat/path" (zk-path "tom" "cat" "path"))))
