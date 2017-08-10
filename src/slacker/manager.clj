(ns slacker.manager
  (:require [slacker.server.cluster :as ss]))

(defn slacker-manager-api [server]
  {"slacker.cluster.manager"
   {"offline" (fn [] (ss/unpublish-all! server))
    "offline-ns" (fn [nsname] (ss/unpublish-ns! server nsname))
    "online-ns" (fn [nsname] (ss/publish-ns! server nsname))
    "online" (fn [] (ss/publish-all! server))
    "set-server-data!" (fn [data] (ss/set-server-data! server data))
    "server-data" (fn [] (ss/get-server-data server))
    "shutdown" (fn [] (ss/stop-slacker-server server))}})
