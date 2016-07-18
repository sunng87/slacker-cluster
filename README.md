# Slacker Cluster

![slacker](http://i.imgur.com/Jd02f.png)

This is the cluster module for
[slacker](https://github.com/sunng87/slacker), a clojure RPC
framework.

[![Build Status](https://travis-ci.org/sunng87/slacker-cluster.png?branch=master)](https://travis-ci.org/sunng87/slacker-cluster)
[![Clojars](https://img.shields.io/clojars/v/slacker/slacker-cluster.svg)](https://clojars.org/slacker/slacker-cluster)
[![License](https://img.shields.io/badge/license-eclipse-blue.svg)](https://github.com/sunng87/slacker-cluster/blob/master/LICENSE)

Base on service discovery by ZooKeeper, slacker has a solution for
high availability and load balancing. You can have several slacker
servers in a cluster serving namespaces of functions. The
cluster-enabled slacker client will choose one (or several, based on your
grouping function) of these servers to call. Once a server is added
to or removed from the cluster, the client will receive a notification
from zookeeper and establish/destroy the connection to the server.

To create such a slacker cluster, you have to deploy at least one zookeeper
instance in your topology.

## Leiningen

![latest version on clojars](https://clojars.org/slacker/slacker-cluster/latest-version.svg)

* Stable: 0.13.x
* Development: 0.14.0-SNAPSHOT

## Cluster Enabled Slacker Server

On the server side, using the server starter function from
`slacker.server.cluster`, add an option `:cluster` and provide your
cluster information.

``` clojure
(use 'slacker.server.cluster)
(start-slacker-server ...
                      :cluster {:name "cluster-name"
                                :zk "127.0.0.1:2181"})
```

Cluster information here:

* `:name` the cluster name (*required*)
* `:zk` zookeeper server address (*required*)
* `:node` server IP (*optional*, if you don't provide the server IP
  here, slacker will try to detect server IP by connecting to zk,
  on which we assume that your slacker server are bound on the same
  network with zookeeper)
* `:server-data` allows you to set some small amount, server specific
  data that will be available to client grouping function to choose
  server.

## Cluster Enabled Slacker Client

On the client side, you have to specify the zookeeper address instead
of a particular slacker server. Use the `clustered-slackerc`:

``` clojure
(use 'slacker.client.cluster)
(def sc (clustered-slackerc "cluster-name" "127.0.0.1:2181"))
(use-remote 'sc 'slapi)
```

*Important*: You should make sure to use the `use-remote` and
`defn-remote` from `slacker.client.cluster` instead of
`slacker.client`.

## Client Grouping

New in 0.10.1. By default, slacker cluster client randomly pick a
server to run your invocation. But at some situation, you may like to
override this behavior, for example, run a function on all servers.

The grouping option gives you total control of this behavior.

```clojure
(defn my-grouping [ns-name fn-name params slacker-client servers]
  ...)

(clustered-slackerc "cluster-name" "127.0.0.1:2181" :grouping my-grouping)
```

The first three arguments represent your current invocation. And you
can return one or more servers based on the information and your
business logic. Server specific data is available from `(server-data
client "server-addr")`.

You can also return constant value:

* `:all` call on all servers available
* `:random` pick a random server to call
* `:leader` always pick the leader node in cluster (new in 0.12)

### Grouping results

When you are using :grouping function, you might get multiple return
values from different servers. A new option `:grouping-results` is to
define how many values to return for the call. Possible values are:

* `:single` default, only return a single value, behavior same as the
  function call
* `:vector` returns values from different servers as a vector
* `:map` returns values from different servers as a map, with server's
  host:port as key
* `(fn [ns-name fn-name params])` a function that returns values
  above. You can use different grouping-results policy according to
  function you call.

Note that `:vector` and `:map` would break default behavior of the
function call, and change return type.

### Grouping exceptions

You can decide whether the client will throw an exception when some of
remote functions throwing exceptions.

* `:all` default, only throw exception when all remote functions return error
* `:any` will throw exception when any remote server caused an error

If you choose `:all` but some of servers failed to return, we will
only return results from servers succeeded.

### Grouping at function level

You can also define `:grouping` and `:grouping-results` function at
`defn-remote` level.

```clojure
(defn-remote sc slacker.example.api/timestamp
  :grouping :all
  :grouping-results :single)
```

If there is no server in the cluster provides the function, you can
add `:unavailable-value` option on `defn-remote` for return value on
this situation.

## Examples

There is a cluster example in the source code. To run the server,
start a zookeeper on your machine (127.0.0.1:2181)

Start server instance:

    lein run-example-server 2104

Open a new terminal, start another server instance:

    lein run-example-server 2105

On another terminal, you can run the example client:

    lein run-example-client

By checking logs, you can trace the calls on each server instance.

## License

Copyright (C) 2011-2014 Sun Ning

Distributed under the Eclipse Public License, the same as Clojure.
