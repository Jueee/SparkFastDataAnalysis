### 第 9 章	Spark SQL ###
Spark SQL 是 Spark 用来操作结构化和半结构化数据的接口。  

结构化数据是指任何有结构信息的数据。  
所谓结构信息，就是每条记录共用的已知的字段集合。  
当数据符合这样的条件时，Spark SQL 就会使得针对这些数据的读取和查询变得更加简单高效。  

Spark SQL 提供了以下三大功能：
1.  Spark SQL 可以从各种结构化数据源（例如 JSON、Hive、Parquet 等）中读取数据。
2.  Spark SQL 不仅支持在 Spark 程序内使用 SQL 语句进行数据查询，也支持从类似商业
    智能软件 Tableau 这样的外部工具中通过标准数据库连接器（JDBC/ODBC）连接 Spark
    SQL 进行查询。
3.  当在 Spark 程序内使用 Spark SQL 时，Spark SQL 支持 SQL 与常规的 Python/Java/Scala
    代码高度整合，包括连接 RDD 与 SQL 表、公开的自定义 SQL 函数接口等。  
    
为了实现这些功能，Spark SQL 提供了一种特殊的 RDD，叫作 SchemaRDD。   
SchemaRDD 是存放 Row 对象的 RDD，每个 Row 对象代表一行记录。
#### 本章目录 ####
1.	[连接 Spark SQL](C1连接SparkSQL.md)    
2.	[在应用中使用 Spark SQL](C2在应用中使用SparkSQL.md)    
2.1	初始化 Spark SQL    
2.2	基本查询示例    
2.3	SchemaRDD    
2.4	缓存   
3.	[读取和存储数据](C3读取和存储数据.md)    
3.1	Apache Hive    
3.2	Parquet   
3.3	JSON    
3.4	基于 RDD    
4.	[JDBC/ODBC 服务器](C4JDBC服务器.md)    
4.1	使用 Beeline    
4.2	长生命周期的表与查询    
5.	[用户自定义函数](C5用户自定义函数.md)    
5.1	Spark SQL UDF    
5.2	Hive UDF    
6.	[Spark SQL 性能](C6SparkSQL性能.md)    
#### 本章总结 ####    
-   Spark 利用 Spark SQL 进行结构化和半结构化数据处理的方式。