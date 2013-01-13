(ns slacker.utils
  (:refer-clojure :exclude [replace])
  (:use [clojure.string :only [join replace]]))


(defn zk-path
  "concat a list of string to zookeeper path"
  [zk-root & nodes]
  (let [path (str zk-root (join "/" nodes))]
    (if (= (.charAt ^String path 0) \/)
      path
      (str "/" path))))

(defn escape-zkpath [fname]
  (replace fname "/" "_slash_"))

(defn unescape-zkpath [fname]
  (replace fname "_slash_" "/"))

