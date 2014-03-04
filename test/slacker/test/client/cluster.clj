(ns slacker.test.client.cluster
  (:use [clojure.test])
  (:use [slacker.client common cluster])
  (:use [slacker.serialization])
  (:use [slacker.utils :only [zk-path]]))

(deftest test-group-promise []
  (let [prmss (take 5 (repeatedly promise))
        gprm (grouped-promise identity prmss)]
    (dorun (map #(deliver % true) prmss))
    (is (every? true? @gprm)))
  (let [prmss (take 5 (repeatedly promise))
        gprm (grouped-promise identity prmss)]
    (is (= 1 (deref gprm 2 1))))
  (let [prmss (take 5 (repeatedly promise))
        gprm (grouped-promise identity prmss)]
    (future
      (dorun (map #(do
                     (deliver % true)
                     (Thread/sleep 500)) prmss)))
    (is (every? true? (deref gprm 3000 [false])))))

(deftest test-group-call-results
  (is (:cause (group-call-results (constantly :vector)
                                  :all
                                  ["1" "2"]
                                  [{:cause {:error true}}
                                   {:cause {:error true}}])))
  (let [r (group-call-results (constantly :vector)
                              :all
                              ["1" "2"]
                              [{:cause {:error true}}
                               {}])]
    (is (not (:cause r))
        (= 1 (count (:result r)))))
  (is (:cause (group-call-results (constantly :vector)
                                  :any
                                  ["1" "2"]
                                  [{:cause {:error true}}
                                   {:result 1}])))
  (is (count
       (:result
        (group-call-results (constantly :vector)
                            :any
                            ["1" "2"]
                            [{:result 1}
                             {:result 2}]))))
  (let [r (group-call-results (constantly :map)
                              :any
                              ["1" "2"]
                              [{:result 1}
                               {:result 2}])]
    (is (= 1 (-> r :result (get "1"))))))
