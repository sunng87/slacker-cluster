(ns ^:no-doc slacker.utils
  (:refer-clojure :exclude [replace])
  (:require [clojure.string :refer [join replace]])
  (:import [io.netty.buffer ByteBuf Unpooled ByteBufUtil]))


(defn zk-path
  "concat a list of string to zookeeper path"
  [zk-root & nodes]
  (let [zk-root (if (.endsWith ^String zk-root "/")
                  zk-root (str zk-root "/"))
        path (str zk-root (join "/" nodes))]
    (if (= (.charAt ^String path 0) \/)
      path
      (str "/" path))))

(defn escape-zkpath [fname]
  (replace fname "/" "_slash_"))

(defn unescape-zkpath [fname]
  (replace fname "_slash_" "/"))

(defn meta-path? [^String name]
  (.startsWith name "_"))

(defn buf-from-bytes [bytes]
  (Unpooled/wrappedBuffer ^bytes bytes))

(defn bytes-from-buf [buf]
  (ByteBufUtil/getBytes ^ByteBuf buf))

(defmacro with-buf [buf-binding & body]
  (let [buf (first buf-binding)]
    `(let ~buf-binding
       (try
         ~@body
         (finally
           (.release ~buf))))))
