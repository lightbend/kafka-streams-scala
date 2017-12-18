## Example implementation of HTTP based Interactive Query Service

The current implementation demonstrates the following usages in Kafka Streams along with an HTTP based interactive query service:

1. Data ingestion
2. Data transformation using Kafka Streams Procedure based implementation
3. Implementing a custom state store (based on bloom filter)
4. Managing local state with custom state store
5. Interactive query service with HTTP end points

The implementation is based on the [ClarkNet dataset](http://ita.ee.lbl.gov/html/contrib/ClarkNet-HTTP.html), which has to be downloaded in a local folder.

## Build and Run Locally

To run the application, do the following steps.

### Build the Libraries

You'll need to build the Scala API library, `kafka-scala-s`, and the interactive queries library, `kafka-scala-q`. Change to each of those directories and run the SBT command `sbt -mem 1500 publishLocal`, which compiles the code, creates archives, and "publishes" them to your local _ivy2_ repository. Note that Scala 2.12.4 and Kafka 1.0.0 are used.

### Start ZooKeeper and Kafka

Start ZooKeeper and Kafka, if not already running. You can download Kafka 1.0.0 for Scala 2.12 [here](https://kafka.apache.org/documentation/#quickstart), then follow the [Quick Start](https://kafka.apache.org/documentation/#quickstart) instructions for running ZooKeeper and Kafka, steps 1 and 2.

### Download the ClarkNet dataset

Download the [ClarkNet dataset](http://ita.ee.lbl.gov/html/contrib/ClarkNet-HTTP.html) and put it in a convenient local folder.

### Configure the Application Properties

Copy `src/main/resources/application-proc.conf.template` to  `src/main/resources/application-proc.conf`.

Edit `src/main/resources/application-proc.conf` and set the entry for `directorytowatch` to match the folder name where you installed the ClarkNet dataset.

### Create the Kafka Topics

Create the topics using the `kafka-topics.sh` command that comes with the Kafka distribution. We'll refer to the directory where you installed Kafka as `$KAFKA_HOME`. Run the following commands:

```bash
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic logerr-proc
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic server-log-proc
```

### Run the Application!

Now run the application as follows:

```bash
$ sbt -mem 1500
> clean
> compile
> proc
```

This will start the application. Now you can query on the global state using `curl`:

```bash
$ curl http://localhost:7071/weblog/access/check/world.std.com
true
$ curl http://localhost:7071/weblog/access/check/world.stx.co
false
```

## Run in Distributed Mode

@todo
