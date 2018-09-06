### 5.4.1　Apache Hive ###
Apache Hive 是 Hadoop 上的一种常见的结构化数据源。  
Hive 可以在 HDFS 内或者在其他存储系统上存储多种格式的表。  
这些格式从普通文本到列式存储格式，应有尽有。  
SparkSQL 可以读取 Hive 支持的任何表。


要把 Spark SQL 连接到已有的 Hive 上，需要提供 Hive 的配置文件。  
需要将 hive-site.xml 文件复制到 Spark 的 ./conf/ 目录下。  
这样做好之后，再创建出 HiveContext 对象，也就是 Spark SQL 的入口，然后你就可以使用 Hive 查询语言（HQL）来对你的表进行查询，并以由行组成的 RDD 的形式拿到返回数据。
#### 代码 ####
-   [Python](P41HiveSQL.py)
-   [Sacla](S41HiveSQL.scala)
-   [Java](J41HiveSQL.java)