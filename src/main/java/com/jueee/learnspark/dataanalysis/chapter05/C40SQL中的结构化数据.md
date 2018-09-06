### 5.4　Spark SQL中的结构化数据 ###
Spark SQL 是在 Spark 1.0 中新加入 Spark 的组件，并快速成为了 Spark 中较受欢迎的操作结构化和半结构化数据的方式。  
结构化数据指的是有结构信息的数据——也就是所有的数据记录都具有一致字段结构的集合。  
Spark SQL 支持多种结构化数据源作为输入，而且由于 Spark SQL 知道数据的结构信息，它还可以从这些数据源中只读出所需字段。  


在各种情况下，我们把一条 SQL 查询给 Spark SQL，让它对一个数据源执行查询（选出一些字段或者对字段使用一些函数），然后得到由 Row 对象组成的 RDD，每个 Row 对象表示一条记录。  
-   在 Java 和 Scala 中， Row 对象的访问是基于下标的。  
每个 Row 都有一个 get() 方法，会返回一个一般类型让我们可以进行类型转换。  
另外还有针对常见基本类型的专用 get() 方法（例如 getFloat() 、 getInt() 、 getLong() 、 getString() 、 getShort() 、getBoolean() 等）。
-   在 Python 中，可以使用 row[column_number] 以及 row.column_name 来访问元素。
#### Hive ####
[Apache Hive](C41HiveSQL.md)
#### JSON ####
[JSON](C42JsonSQL.md)