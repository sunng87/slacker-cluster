(ns slacker.client.cluster-test
  (:require [clojure.test :refer :all]
            [slacker.client.common :refer :all]
            [slacker.client.cluster :refer :all]
            [slacker.discovery.protocol :as ds]
            [slacker.serialization :refer :all]
            [slacker.utils :refer [zk-path]]
            [slacker.zk :as zk])
  (:import [slacker.client.cluster ClusterEnabledSlackerClient]))

(deftest test-group-call-results
  (is (:cause (group-call-results (constantly :vector)
                                  :all
                                  [{:cause {:error true}}
                                   {:cause {:error true}}])))
  (let [r (group-call-results (constantly :vector)
                              :all
                              [{:cause {:error true}}
                               {}])]
    (is (not (:cause r))
        (= 1 (count (:result r)))))
  (is (:cause (group-call-results (constantly :vector)
                                  :any
                                  [{:cause {:error true}}
                                   {:result 1}])))
  (is (count
       (:result
        (group-call-results (constantly :vector)
                            :any
                            [{:result 1}
                             {:result 2}]))))
  (let [r (group-call-results (constantly :map)
                              :any
                              [{:result 1 :server "1"}
                               {:result 2 :server "2"}])]
    (is (= 1 (-> r :result (get "1")))))
  (let [grf (fn []
              (fn [results]
                (reduce + (map :result results))))
        r (group-call-results grf
                              :all
                              [{:result 1}
                               {:result 2}])]
    (is (= 3 (:result r)))))


(deftest sync-call-test
  (testing "unavialble-value"
    (let [d-ns "dummy-ns"
          unavailable-value "N/A"]
      (with-redefs [zk/connect (constantly nil)
                    zk-path (constantly "dummy-path")
                    zk/register-watcher (constantly nil)
                    zk/register-error-handler (constantly nil)
                    zk/children (constantly ["a" "b"])
                    zk/data (constantly (.getBytes "dummy-dataq" "utf-8"))]
        (let [client @(clustered-slackerc "dummy-cluster" "127.0.0.1")]
          (is (= (sync-call-remote client d-ns "dummy-fn" []
                                 {:grouping (constantly [])
                                  :unavailable-value unavailable-value})
                 {:result unavailable-value :fname "dummy-ns/dummy-fn"}))
          (is (= (sync-call-remote client d-ns "dummy-fn" []
                                 {:grouping (constantly [])})
               {:cause {:error :unavailable :servers []}
                :fname "dummy-ns/dummy-fn"})))))))
