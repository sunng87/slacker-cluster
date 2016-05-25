(ns slacker.server.cluster-test
  (:require [clojure.test :refer :all]
            [slacker.server.cluster :refer :all]
            [slacker.utils :refer [zk-path]]))

(deftest test-zk-path
  (is (= "/tom/cat/path" (zk-path "tom" "cat" "path"))))
