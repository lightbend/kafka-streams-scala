# Http layer for Interactive Queries in Kafka Streams

Kafka Streams' stateful streaming creates and uses local state information in the node where the application is running. If the application runs in a distributed mode on multiple nodes, then each node contains the respective state information. Kafka Streams does not publish any unifying API that allows you to query across all the nodes for the state information. However it has a set of infrastructure components that can be used to implement a query service based on your favorite end points.

Interactive Queries were introduced on version `0.10.1` and the main goal is stated as follows:

> This feature allows you to treat the stream processing layer as a lightweight embedded database and, more concretely, to directly query the latest state of your stream processing application, without needing to materialize that state to external databases or external storage first.

However Kafka Streams documentation also makes it clear that the query layer for the global state of your application does not come out of the box.

> Kafka Streams provides all the required functionality for interactively querying your application’s state out of the box, with but one exception: if you want to expose your application’s full state via interactive queries, then – for reasons we explain further down below – it is your responsibility to add an appropriate RPC layer to your application that allows application instances to communicate over the network. If, however, you only need to let your application instances access their own local state, then you do not need to add such an RPC layer at all.

The goal of this small library is to offer such a query layer based on [akka-http](https://doc.akka.io/docs/akka-http/current/scala/http/).

## The Library

The library is organized around 3 main packages containing the following:

1. `http`: The main end point implementations including a class `InteractiveQueryHttpService` that provides methods for starting and stopping the http service. The other classes provided are `HttpRequester` that handles the request, does some validations and forwards the request to `KeyValueFetcher` that invokes the actual service for fetching the state information.
2. `services`: This layer interacts with the underlying Kafka Streams APIs to fetch data from the local state. The 2 classes in this layer are (a) `MetadataService` that uses Kafka Streams API to fetch the metadata for the state and (b) `LocalStateStoreQueryService` that does the actual query for the state.
3. `serializers`: A bunch of serializers useful for application development that helps you serialize your model structures.

## Distributed Query

If the pplication is run in distributed mode across multiple physical nodes, local state information are spread across all the nodes. The `http` services that the library offers can handle this nd provide with a unified view of the global application state. 

Consider the following scenario:

1. The application is deployed in 3 nodes with IPs, **ip1**, **ip2** and **ip3**. Assuming the application uses this library, the http services run on the port **7070** in each of the nodes. 
2. The user queries for some information from `http://ip1:7070/<path>/<to>/<key>`. 

It may so happen that the `<key>` that she is looking for may not reside in host **ip1**. The query service handles such situation by interacting with the `MetadataService` as follows:

1. User queries from host **ip1**
2. Check `MetadataService` to get information about the `key` that the user is looking for
3. If the metadata for the key indicates that the data is part of the local state in **ip1**, then we are done. Return the query result.
4. Otherwise, get the host information from the metadata where this state resides.
5. Query the appropriate node by reissuing the http request and get the state information.

## Handling Rebalancing of Partitions 

It may so happen that when the user does the query, Kafka Streams may be doing a  partition rebalancing when states may migrate from one store (node) to another. During such situation Kafka Streams throws `InvalidStateStoreException`. 

Migration is typically done when new instances of the application come up or Kafka Streams does a rebalancing. The library handles such situation through a retry semantics. The query API will continuw to retry till the rebalancing is complete or the retry count is exhausted.
