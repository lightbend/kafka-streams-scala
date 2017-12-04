## Example implementation of http based interactive query service

The implementation is absed on the [Clarknet dataset](http://ita.ee.lbl.gov/html/contrib/ClarkNet-HTTP.html), which has to be downloaded in a local folder.

## Build and run locally

1. Download the above dataset and have it in a local folder
2. Update `resources/application-dsl.conf` entry for `directorytowatch` with the folder name
3. Ensure that the `schemaregistryurl` entry in `resources/application-dsl.conf` is commented out  
4. Ensure Kafka and Zookeeper are running
5. Ensure topics are created. You can use the following script.

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic logerr-dsl
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic server-log-dsl
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic processed-log
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic summary-access-log
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic windowed-summary-access-log
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic summary-payload-log
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic windowed-summary-payload-log
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic avro-topic
```

Now run the application as follows:

```bash
$ sbt
> clean
> compile
> dsl
```

This will start the application. Now you can query on the global state using `curl`:

```bash
$ curl http://localhost:7070/weblog/access/world.std.com
$ curl http://localhost:7070/weblog/bytes/world.std.com
```

## Run in Distributed Mode

@todo