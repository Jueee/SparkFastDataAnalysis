### 9.4　JDBC/ODBC服务器 ###
Spark SQL 也提供 JDBC 连接支持，这对于让商业智能（BI）工具连接到 Spark 集群上以及在多用户间共享一个集群的场景都非常有用。  

JDBC 服务器作为一个独立的 Spark 驱动器程序运行，可以在多用户之间共享。  
任意一个客户端都可以在内存中缓存数据表，对表进行查询。  
集群的资源以及缓存数据都在所有用户之间共享。

启动 JDBC 服务器：
``` 
spark-2.3.1-bin-hadoop2.7$ ./sbin/start-thriftserver.sh --master sparkMaster
```
使用 Beeline 连接 JDBC 服务器：
``` 
spark-2.3.1-bin-hadoop2.7$ ./bin/beeline -u jdbc:hive2://localhost:10000
Connected to: Spark SQL (version 2.3.1)
Driver: Hive JDBC (version 1.2.1.spark2)
Transaction isolation: TRANSACTION_REPEATABLE_READ
Beeline version 1.2.1.spark2 by Apache Hive
0: jdbc:hive2://localhost:10000> show tables;
+-----------+------------+--------------+--+
| database  | tableName  | isTemporary  |
+-----------+------------+--------------+--+
+-----------+------------+--------------+--+
No rows selected (0.674 seconds)
```
#### 使用Beeline ####
[Hive 语言手册](https://cwiki.apache.org/confluence/display/Hive/LanguageManual)  

如果你想要缓存数据表，使用 CACHE TABLE  tableName 语句。  
缓存之后你可以使用 UNCACHE TABLE  tableName 命令取消对表的缓存。需要注意的是，之前也提到过，缓存的表会在这个 JDBC 服务器上的所有客户端之间共享。

#### 长生命周期的表与查询 ####
使用 Spark SQL 的 JDBC 服务器的优点之一就是我们可以在多个不同程序之间共享缓存下来的数据表。  
JDBC Thrift 服务器是一个单驱动器程序，这就使得共享成为了可能。 

