## Example Implementation of HTTP-based Interactive Query Service

This example demonstrates the following features in Kafka Streams along with an HTTP based interactive query service:

1. Data ingestion
2. Data transformation using a Kafka Streams DSL-based implementation
3. Managing local state with key-value stores
4. Interactive query service with HTTP end points

The implementation is based on the [ClarkNet dataset](http://ita.ee.lbl.gov/html/contrib/ClarkNet-HTTP.html), which has to be downloaded in a local folder.

## Build and Run Locally

To run the application, do the following steps.

### Build the Libraries

You'll need to build the Scala API library, `kafka-scala-s`, and the interactive queries library, `kafka-scala-q`. Change to each of those directories and run the SBT command `sbt publishLocal`, which compiles the code, creates archives, and "publishes" them to your local _ivy2_ repository. Note that Scala 2.12.4 and Kafka 1.0.0 are used.

### Start ZooKeeper and Kafka

Start ZooKeeper and Kafka, if not already running. You can download Kafka 1.0.0 for Scala 2.12 [here](https://kafka.apache.org/documentation/#quickstart), then follow the [Quick Start](https://kafka.apache.org/documentation/#quickstart) instructions for running ZooKeeper and Kafka, steps 1 and 2.

### Download the ClarkNet dataset

Download the [ClarkNet dataset](http://ita.ee.lbl.gov/html/contrib/ClarkNet-HTTP.html) and put it in a convenient local folder.

### Configure the Application Properties

Copy `src/main/resources/application-dsl.conf.template` to  `src/main/resources/application-dsl.conf`.

Edit `src/main/resources/application-dsl.conf` and set the entry for `directorytowatch` to match the folder name where you installed the ClarkNet dataset.

### Create the Kafka Topics

Create the topics using the `kafka-topics.sh` command that comes with the Kafka distribution. We'll refer to the directory where you installed Kafka as `$KAFKA_HOME`. Run the following commands:

```bash
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic logerr-dsl
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic server-log-dsl
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic processed-log
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic summary-access-log
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic windowed-summary-access-log
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic summary-payload-log
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic windowed-summary-payload-log
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic avro-topic
```

### Run the Application!

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
$ ## Fetch the number of accesses made to the host world.std.com as per the
$ ## downloaded data file
$
$ curl http://localhost:7070/weblog/bytes/world.std.com
$ ## Fetch the number of bytes in the reply for queries to the host 
$ ## world.std.com as per the downloaded data file
```

## Run in Distributed Mode

The http query layer is designed to work even when your application runs in the distributed mode. Running your Kafka Streams application in the distributed mode means that all the instances must have the same application id.

Here are the steps that you need to follow to run the application in distributed mode. We assume here you are running both the instances in the same node with different port numbers. It's fairly easy to scale this on different nodes.

### Step 1: Build and configure for distribution

```bash
$ sbt
$ dslPackage/universal:packageZipTarball
```

This creates a distribution under a folder `<project home>/build`.

```bash
$ pwd
<project home>
$ cd build/dsl/target/universal
$ ls
dslpackage-0.0.1.tgz
$ tar xvfz dslpackage-0.0.1.tgz
## unpack the distribution
$ cd dslpackage-0.0.1
$ ls
bin	   conf	lib
$ cd conf
$ ls
application.conf	logback.xml
## change the above 2 files based on your requirements.
$ cd ..
$ pwd
<...>/dslpackage-0.0.1
```

### Step 2: Run the first instance of the application
Ensure the following:

1. Zookeeper and Kafka are running
2. All topics mentioned above are created
3. The folder mentioned in `directoyToWatch` in `application.conf` has the data file

```bash
$ pwd
<...>/dslpackage-0.0.1
$ bin/dslpackage
```

This starts the single instance of the application. After some time you will see data printed in the console regarding the host access information as present from the data file.

In the log file, created under `<...>/dslpackage-0.0.1/logs`, check if the REST service has started and note the host and port details. It should be something like `localhost:7070` (the default setting in `application.conf`).

### Step 3: Run the second instance of the application

If you decide to run multiple instances of the application you may choose to split the dataset into 2 parts and keep them in different folders. Also you need to copy the current distribution in some other folder and start the seocnd instance from there, since you need to run it with changed settings in `application.conf`. Say we want to copy in a folder named `clarknet-2`.

```bash
$ cp <project home>/build/dsl/target/universal/dslpackage-0.0.1.tgz clarknet-2
$ cd clarknet-2
$ tar xvfz dslpackage-0.0.1.tgz
## unpack the distribution
$ cd dslpackage-0.0.1
$ ls
bin	   conf	lib
$ cd conf
$ ls
application.conf	logback.xml
## change the above 2 files based on your requirements.
$ cd ..
$ pwd
<...>/dslpackage-0.0.1
```

The following settings need to be changed in `application.conf` before you can run the second instance:

1. `dcos.kafka.statestoredir` - This is the folder where the local state information gets persisted by Kafka streams. This has to be different for every new instance set up.
2. `dcos.kafka.loader.directorytowatch` - The data folder because we would like to ingest different data for the 2 instances.
3. `dcos.http.interface` and `dcos.http.port` - The REST service endpoints. If the node is not different then it can be `localhost` for both.

```bash
$ pwd
<...>/dslpackage-0.0.1
$ bin/dslpackage
```

This will start the second instance. Check the log file to verify that the REST endpoints are properly started.

### Step 4: Do query

The idea of a distributed interactive query interface is to allow the user to query for *all* keys using *any* of the end points where the REST service are running. Assume that the 2 instances are running at `localhost:7070` and `localhost:7071`. 

Here are a few examples:

```bash
## world.std.com was loaded by the first instance of the app
## Query using the end points corresponding to the first instance gives correct result
$ curl localhost:7070/weblog/access/world.std.com
14

## we get correct result even if we query using the end points of of the second instance
$ curl localhost:7071/weblog/access/world.std.com
14

## ppp19.glas.apc.org was loaded by the second instance of the app
## Query using the end points corresponding to the first instance also gives correct result
$ curl localhost:7070/weblog/access/ppp19.glas.apc.org
17
```