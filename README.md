# Slacker Cluster

![slacker](http://i.imgur.com/Jd02f.png)

With help from ZooKeeper, slacker has a solution for high
availability and load balancing. You can have several slacker servers
in a cluster serving functions. And the clustered slacker client will
randomly select one of these server to invoke. Once a server is added
to or removed from the cluster, the client will automatically
establish/destroy the connection to it. 

To create such a slacker cluster, you have to deploy at least one zookeeper
instance in your system.

## Leiningen

`[slacker/slacker-cluster "0.8.5"]`

## Cluster Enabled Slacker Server

On the server side, using the server starter function from 
`slacker.server.cluster`, add an option `:cluster` and provide some 
information.

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

## Cluster Enabled Slacker Client

On the client side, you have to specify the zookeeper address instead
of a particular slacker server. Use the `clustered-slackerc`:

``` clojure
(use 'slacker.client.cluster)
(def sc (clustered-slackerc "cluster-name" "127.0.0.1:2181"))
(use-remote 'sc 'slapi)
```

*Important*: You should make sure to use the `use-remote` and `defn-remote` from
`slacker.client.cluster` instead of `slacker.client`.

## Examples

There is a cluster example in the source code. To run the server,
start a zookeeper on your machine (127.0.0.1:2181)

Start server instance:

    lein2 run-example-server 2104

Open a new terminal, start another server instance:

    lein2 run-example-server 2105

On another terminal, you can run the example client:

    lein2 run-example-client

By checking logs, you can trace the calls on each server instance.

## Contributors

* [lbt05](https://github.com/lbt05)
* [johnchapin](https://github.com/johnchapin)

## License

Copyright (C) 2011-2012 Sun Ning

Distributed under the Eclipse Public License, the same as Clojure.

