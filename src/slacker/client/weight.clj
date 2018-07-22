(ns slacker.client.weight)

(defn select-weighted-item
  "Given a vector of weight, return the n as selected item"
  [weights]
  (let [weight-sum (reduce + weights)]
    (if (zero? weight-sum)
      (rand-int (count weights))
      (let [normalized-weight-range (map #(/ %1 weight-sum) weights)
            total-items (count weights)
            ;; r is (0, 1]
            r (- 1 (rand))]
        ;; we will pick the first
        (loop [re r values normalized-weight-range]
          (let [v (first values)]
            (if (> re v)
              (recur (- re v) (rest values))
              (- total-items (count values)))))))))
