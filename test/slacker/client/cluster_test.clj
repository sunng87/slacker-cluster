(ns slacker.client.cluster-test
  (:require [clojure.test :refer :all]
            [slacker.client.common :refer :all]
            [slacker.client.cluster :refer :all]
            [slacker.serialization :refer :all]
            [slacker.utils :refer [zk-path]]
            [slacker.zk :as zk])
  (:import [slacker.client.cluster ClusterEnabledSlackerClient]))

(deftest test-group-promise []
  (let [prmss (take 5 (repeatedly promise))
        gprm (grouped-promise identity prmss nil)]
    (dorun (map #(deliver % true) prmss))
    (is (every? true? @gprm)))
  (let [prmss (take 5 (repeatedly promise))
        gprm (grouped-promise identity prmss nil)]
    (is (= 1 (deref gprm 2 1))))
  (let [prmss (take 5 (repeatedly promise))
        gprm (grouped-promise identity prmss nil)]
    (future
      (dorun (map #(do
                     (deliver % true)
                     (Thread/sleep 500)) prmss)))
    (is (every? true? (deref gprm 3000 [false])))))

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
