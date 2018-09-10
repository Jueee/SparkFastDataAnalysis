### 5.5.1　MySQL 数据库连接 ###
JdbcRDD 接收这样几个参数：

1.  要提供一个用于对数据库创建连接的函数。  
这个函数让每个节点在连接必要的配置后创建自己读取数据的连接。

2.  要提供一个可以读取一定范围内数据的查询，以及查询参数中 lowerBound 和 upperBound 的值。  
这些参数可以让 Spark 在不同机器上查询不同范围的数据，这样就不会因尝试在一个节点上读取所有数据而遭遇性能瓶颈。 

3.  这个函数的最后一个参数是一个可以将输出结果从 java.sql.ResultSet 转为对操作数据有用的格式的函数。  
如果这个参数空缺，Spark 会自动将每行结果转为一个对象数组。


和其他的数据源一样，在使用 JdbcRDD 时，需要确保你的数据库可以应付 Spark 并行读取的负载。  
如果你想要离线查询数据而不使用在线数据库，可以使用数据库的导出功能，将数据导出为文本文件。
#### 参数 ####
``` 
class JdbcRDD[T](
    sc : org.apache.spark.SparkContext, 
    getConnection : scala.Function0[java.sql.Connection], 
    sql : scala.Predef.String, 
    lowerBound : scala.Long, 
    upperBound : scala.Long, 
    numPartitions : scala.Int, 
    mapRow : scala.Function1[java.sql.ResultSet, T] = {})
(implicit evidence$1 : scala.reflect.ClassTag[T]) extends org.apache.spark.rdd.RDD[T] with org.apache.spark.internal.Logging {}
```
-   getConnection  
返回一个已经打开的结构化数据库连接，JdbcRDD会自动维护关闭。
-   sql  
查询语句，此查询语句必须包含两处占位符?来作为分割数据库ResulSet的参数。
-   lowerBound  
第一占位符
-   upperBound  
第二占位符
-   numPartitions  
partition的个数。
-   mapRow  
转换函数，将返回的ResultSet转成RDD需用的单行数据，此处可以选择Array或其他，也可以是自定义的case class。  
默认的是将ResultSet 转换成一个Object数组。

例如，给出lowebound 1，upperbound 20， numpartitions 2，则查询分别为(1, 10)与(11, 20)。

【局限性】
-   要带有两个？的占位符，而这两个占位符是给参数lowerBound和参数upperBound定义where语句的边界的。
-   参数lowerBound和参数upperBound都是Long类型的。

参照JdbcRDD的源代码，用户也可以写出符合自己需求的JdbcRDD。
#### 代码 ####
-   [Python](P51MySQL.py)
-   [Sacla](S51MySQL.scala)
-   [Java](J51MySQL.java)
#### Python 命令行操作 ####
``` 
bin$ ./pyspark 
Python 2.7.9 (default, Jun 29 2016, 13:08:31) 
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.3.1
      /_/

Using Python version 2.7.9 (default, Jun 29 2016 13:08:31)
SparkSession available as 'spark'.
>>> sqlContext
<pyspark.sql.context.SQLContext object at 0x7f76a2a40cd0>
>>> dataframe_mysql = sqlContext.read.format("jdbc").options( url="jdbc:mysql://IP:HOST/information_schema",driver = "com.mysql.jdbc.Driver",dbtable = "TABLES",user="USER", password="PASSWORD").load()
>>> dataframe_mysql.show()
```
若提示：java.lang.ClassNotFoundException: com.mysql.jdbc.Driver
``` 
bin$ ./pyspark --packages mysql:mysql-connector-java:5.1.38
```
会自动下载引用相关 Jar 包。