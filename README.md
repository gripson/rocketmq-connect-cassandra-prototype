# Rocketmq Connect Cassandra  
This project is used to receive and send messages between RocketMQ and Cassandra, includes:

* A [Source Connector]() that extracts data from Cassandra and provides the data to RocketMQ.
* An [Sink Connector]() that consumes messages from RocketMQ and writes data to Cassandra (WIP).

## Prerequisites (Minimum)
1. 64bit JDK 1.8+/OpenJDK1.8+;
2. Maven 3.2.x+;
3. A running RocketMQ;
4. A rocketMQ Runtime;
5. Two Cassandra clusters;

**Notes: Reference of  [RocketMQ](https://rocketmq-1.gitbook.io/rocketmq-connector/quick-start/qian-qi-zhun-bei/dan-ji-huan-jing) and [RocketMQ Runtime](https://rocketmq-1.gitbook.io/rocketmq-connector/quick-start/runtime-qs) installation.
## Build
```
mvn clean install -Dmaven.test.skip=true
```
## Installation
1. Copy connector to pluginPaths
```
# Enter the directory where rocketmq-connect-cassandra is located
$ cd {rocketmq-connect-cassandra directory}/target
# Copy the jar package(only jar with all dependencies) to the connector plugin directory, 
# here assume /usr/local/connector-plugins/ is the pluginPaths
$ cp rocketmq-connect-cassandra-prototype-1.0-SNAPSHOT-jar-with-dependencies.jar /usr/local/connector-plugins/
```
## Configuration
Modify Runtime startup file. 
rocketmq-connector-cassandra uses DataStax Java Driver for Apache Cassandra® . Since the driver includes Netty which is not compatible with a version of Netty in Runtime classpath, the most direct way is to disable conflict function.
More details refer to [DataStax documentation](https://docs.datastax.com/en/developer/java-driver-dse/1.4/faq/#what-is-netty-s-native-epoll-transport-and-how-do-i-enable-or-disable-it)
```
echo "run rumtime worker"
cd target/distribution/ && java -Dcom.datastax.driver.FORCE_NIO=true -cp .:./conf/:./lib/* org.apache.rocketmq.connect.runtime.ConnectStartup -c conf/connect.conf
```
## QuickStart
Creating Topic using mqadmin of RocketMQ
```
$ sh mqadmin updateTopic -b xxx.xxx.xxx.xxx:10911 -t {topic}
```
**Note :
* 1 Here the topic is the name of the table that needs to be transferred
* 2 Do not need to manually create the topic if the RocketMQ broker is configured with the `autoCreateTopicEnable=true` parameter

Starting RocketMQ Runtime
```
$ sh ./run_worker.sh
```
Launch template of Source Connector 
```

HTTP GET Request

http://(your worker ip):(port)/connectors/(connector name)?config={"connector-class":"org.apache.rocketmq.connect.cassandra.source.CassandraSourceConnector",
"contactPoints":"(Cassandra IPs)","port":"(ports)","username":"(Cassandra username)",
"password":"(connector password)","mode":"(mode option)","keyspace":"(keyspace name)","table":"(table name)",
"source-record-converter":"org.apache.rocketmq.connect.runtime.converter.JsonConverter"}
```

Example
```

http://localhost:8081/connectors/CassandraConnectorSource?config={"connector-class":"org.apache.rocketmq.connect.cassandra.source.CassandraSourceConnector",
"contactPoints":"127.0.0.1","port":"9042","username":"Cassandra",
"password":"Cassandra","mode":"bulk","keyspace":"demo","table":"orders",
"source-record-converter":"org.apache.rocketmq.connect.runtime.converter.JsonConverter"}
```

Source connector configuration instructions (Standalone )

| key         | nullable | default   | description                                  |
| ------------- | -------- | ---------------- | ----------------------------------- |
| connector.class   |  false| |The Java class for the connector                         |
| contactPoints         | false    |         | Contact points (hosts) in Cassandra cluster                      |
| port              | false    |         | Cassandra Port for the native Java driver. (e.g.9042)                         |
| username              | false    |         | Cassandra username                |
| password                | false    |        | Cassandra password               |
| mode        | false    |   Bulk     | Source connector pulls data mode       |
| keyspace|    false| |Cassandra Keyspace the tables to write belong to |
| table|           false| |Cassandra table |
| source-record-converter | false    |         | Full class name of the impl of the converter used to convert SourceDataEntry to byte[] |


## CQL 3.8 Types Supported (WIP)
Refer Documentation of [ DataStax Java Driver 3.8](https://docs.datastax.com/en/developer/java-driver/3.8/manual/)
and [OpenMessaging API](https://github.com/openmessaging/openmessaging-connect/blob/master/connector/src/main/java/io/openmessaging/connector/api/data/FieldType.java)

|CQL3 data type| Getter name|	Java type	|See also| Field Type defined by Openmessaging |
|----|----|----|----|----|
|ascii|	getString|	java.lang.String| |STRING|	
|bigint|	getLong|	long|	|INT64|
|*blob|	getByteBuffer|	java.nio.ByteBuffer	
|boolean|	getBoolean|	boolean	| | BOOLEAN|
|counter|	getLong	|long|	|INT64|
|*date|	getLocalDate|	java.time.LocalDate|Temporal types | DATETIME|
|decimal|	getBigDecimal|	java.math.BigDecimal| |FLOAT64|	
|double|	getDouble|	double	| |FLOAT64|
|float|	getFloat|	float	| | FLOAT32|
|inet|	getInetAddress|	java.net.InetAddress| |STRING|	
|int|	getInt	|int	| | INT32 |
|*list|	getList	|java.util.List	| | ARRAY |
|map|	getMap	|java.util.Map	| | MAP   |
|*set|	getSet	|java.util.Set	| | ARRAY |
|smallint|	getShort|	short	| |INT32|
|text|	getString|	java.lang.String| |STRING|	
|*time|	getLocalTime|	java.time.LocalTime	|Temporal types |DATETIME|
|*timestamp|	getInstant	|java.time.Instant	|Temporal types |DATETIME|
|timeuuid|	getUuid	|java.util.UUID|	|STRING|
|tinyint|	getByte	|byte | | BYTES|	
|*tuple|	getTupleValue|	TupleValue|	Tuples
|*user-defined types	|getUDTValue |UDTValue	|User-defined types
|uuid	|getUuid|	java.util.UUID | |STRING|	
|varchar|	getString|	java.lang.String| |STRING|	 
|varint|	getBigInteger|	java.math.BigInteger | |BIG_INTEGER|	