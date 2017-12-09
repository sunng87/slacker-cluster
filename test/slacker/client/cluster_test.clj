(ns slacker.client.cluster-test
  (:require [clojure.test :refer :all]
            [slacker.client.common :refer :all]
            [slacker.client.cluster :refer :all]
            [slacker.serialization :refer :all]
            [slacker.utils :refer [zk-path]]
            [slacker.zk :as zk])
  (:import [slacker.client.cluster ClusterEnabledSlackerClient]))

(deftest sync-call-test
  (testing "unavialble-value"
    (let [client (ClusterEnabledSlackerClient. "dummy-cluster"
                                               nil (atom {}) (atom {}) {})
          d-ns "dummy-ns"
          unavailable-value "N/A"]
      (with-redefs [zk-path (constantly "dummy-path")
                    zk/children (constantly ["a" "b"])
                    zk/data (constantly (.getBytes "dummy-dataq" "utf-8"))]
        (is (= (sync-call-remote client d-ns "dummy-fn" []
                                 {:grouping (constantly [])
                                  :unavailable-value unavailable-value})
               {:result unavailable-value :fname "dummy-ns/dummy-fn"}))
        (is (= (sync-call-remote client d-ns "dummy-fn" []
                                 {:grouping (constantly [])})
               {:cause {:error :unavailable :servers []}
                :fname "dummy-ns/dummy-fn"}))))))
