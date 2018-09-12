### 5.5.1　MySQL 数据库连接 ###
Cassandra 还没有使用 Spark SQL，不过它会返回由 CassandraRow 对象组成的 RDD，这些对象有一部分方法与 Spark SQL 的 Row 对象的方法相同。  

Spark 的 Cassandra 连接器目前只能在 Java 和 Scala 中使用。
#### Maven 依赖 ####
[https://github.com/datastax/spark-cassandra-connector](https://github.com/datastax/spark-cassandra-connector)  
[Java 连接文档](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/7_java_api.md)
``` 
<dependency> <!-- Cassandra -->
    <groupId>com.datastax.spark</groupId>
    <artifactId>spark-cassandra-connector</artifactId>
    <version>${spark.cassandra.version}</version>
</dependency>
<dependency>
    <groupId>com.datastax.spark</groupId>
    <artifactId>spark-cassandra-connector-java_2.11</artifactId>
    <version>1.6.0-M1</version>
</dependency>
```
#### 连接 ####
Java
``` 
SparkConf conf = new SparkConf(true).set("spark.cassandra.connection.host", DataBaseUtil.CASSANDRA_HOSTNAME);
JavaSparkContext sc = new JavaSparkContext(DataBaseUtil.SPARK_MASTER, DataBaseUtil.SPARK_APPNAME,conf);
```
Scala
``` 
val conf = new SparkConf(true).set("spark.cassandra.connection.host", DataBaseUtil.CASSANDRA_HOSTNAME)
val sc = new SparkContext(DataBaseUtil.SPARK_MASTER, DataBaseUtil.SPARK_APPNAME, conf)
```
#### 保存 ####
在 Scala 中保存数据到 Cassandra
``` 
val rdd = sc.parallelize(List(Seq("moremagic", 1)))
rdd.saveToCassandra("test" , "kv", SomeColumns("key", "value"))
```
#### 代码 ####
-   [Sacla](S52Cassandra.scala)
-   [Java](J52Cassandra.java)
#### 说明 ####
Cassandra 连接器要读取一个作业属性来决定连接到哪个集群。  
我们把 spark.cassandra.connection.host 设置为指向 Cassandra 集群。  

如果有用户名和密码的话，则需要分别设置 spark.cassandra.auth.username 和 spark.cassandra.auth.password 。  
假定你只有一个 Cassandra 集群要连接，可以在创建 SparkContext 时就把这些都设好。

通过在 cassandraTable() 的调用中加上 where 子句，可以限制查询的数据。  
例如 sc.cassandraTable(...).where("key=?", "panda") 。