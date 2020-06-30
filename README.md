# Rocketmq Connect Cassandra  
This project is used to receive and send messages between RocketMQ and Cassandra, includes:

* A [Source Connector]() that extracts data from Cassandra and provides the data to RocketMQ.
* An [Sink Connector]() that consumes messages from RocketMQ and writes data to Cassandra (WIP).

## Environment requirements
1. 64bit JDK 1.8+/OpenJDK1.8+;
2. Maven 3.2.x+;
3. Two running RocketMQ clusters;
4. RocketMQ Runtime

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
## Running Examples
Launch template
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

Source connector configuration instructions

| key         | nullable | default   | description                                  |
| ------------- | -------- | ---------------- | ----------------------------------- |
| connector.class   |  false| |The Java class for the connector                         |
| contactPoints         | false    |         | Cassandra IPS                       |
| port              | false    |         | Cassandra port                         |
| username              | false    |         | Cassandra username                |
| password                | false    |        | Cassandra password               |
| mode        | false    |   Bulk     | Source connector pulls data mode:       |
| keyspace|    false| |Cassandra keyspace |
| table|           false| |Cassandra table |
| source-record-converter | false    |         | Full class name of the impl of the converter used to convert SourceDataEntry to byte[] |


## CQL Types Supported (WIP)
|CQL3 data type| Getter name|	Java type	|See also|
|----|----|----|----|
|ascii|	getString|	java.lang.String|	
|bigint|	getLong	long|	
|blob|	getByteBuffer|	java.nio.ByteBuffer	
|boolean|	getBoolean|	boolean	
|counter|	getLong	long|	
|date|	getLocalDate|	java.time.LocalDate|	Temporal types
|decimal|	getBigDecimal|	java.math.BigDecimal	
|double|	getDouble|	double	
|duration|	getCqlDuration|	CqlDuration	|Temporal types
|float|	getFloat|	float	
|inet|	getInetAddress|	java.net.InetAddress	
|int|	getInt	|int	
|list|	getList	|java.util.List	
|map|	getMap	|java.util.Map	
|set|	getSet	|java.util.Set	
|smallint|	getShort|	short	
|text|	getString|	java.lang.String	
|time|	getLocalTime|	java.time.LocalTime	|Temporal types
|timestamp|	getInstant	|java.time.Instant	|Temporal types
|timeuuid|	getUuid	|java.util.UUID	
|tinyint|	getByte	|byte	
|tuple|	getTupleValue|	TupleValue	Tuples
|user-defined types	|getUDTValue |UDTValue	|User-defined types
|uuid	|getUuid|	java.util.UUID	
|varchar|	getString|	java.lang.String	
|varint|	getBigInteger|	java.math.BigInteger	