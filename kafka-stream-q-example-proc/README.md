## Example implementation of http based Interactive Query Service

The current implementation demonstrates the following usages in Kafka Streams along with an http based interactive query service:

1. Data ingestion
2. Data transformation using Kafka Streams Procedure based implementation
3. Implementing a custom state store (based on bloom filter)
4. Managing local state with custom state store
5. Interactive query service with http end points 

The implementation is based on the [Clarknet dataset](http://ita.ee.lbl.gov/html/contrib/ClarkNet-HTTP.html), which has to be downloaded in a local folder.

## Build and run locally

1. Download the above dataset and have it in a local folder
2. Update `resources/application-proc.conf` entry for `directorytowatch` with the folder name
4. Ensure Kafka and Zookeeper are running
5. Ensure topics are created. You can use the following script.

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic logerr-proc
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic server-log-proc
```

Now run the application as follows:

```bash
$ sbt
> clean
> compile
> proc
```

This will start the application. Now you can query on the global state using `curl`:

```bash
$ curl localhost:7071/weblog/access/check/world.std.com
true
$ curl localhost:7071/weblog/access/check/world.stx.co
false
```

## Run in Distributed Mode

@todo