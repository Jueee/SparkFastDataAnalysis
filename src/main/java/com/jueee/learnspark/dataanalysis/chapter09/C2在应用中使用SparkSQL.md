### 9.2　在应用中使用Spark SQL ###
Spark SQL 最强大之处就是可以在 Spark 应用内使用。  

要以这种方式使用 Spark SQL，需要基于已有的 SparkContext 创建出一个 HiveContext。  
（如果使用的是去除了 Hive 支持的 Spark 版本，则创建出 SQLContext）  
这个上下文环境提供了对 Spark SQL 的数据进行查询和交互的额外函数。  
使用 HiveContext 可以创建出表示结构化数据的 SchemaRDD，并且使用 SQL 或是类似 map() 的普通 RDD 操作来操作这些 SchemaRDD。  

#### 代码 ####
-   [Python](P2SparkSqlApp.py)
-   [Scala](S2SparkSqlApp.scala)
-   [Java](J2SparkSqlApp.java)

#### Dataset\<Row> ####
读取数据和执行查询都会返回 Dataset\<Row>。  
从内部机理来看，Dataset\<Row> 是一个由 Row 对象组成的 RDD，附带包含每列数据类型的结构信息。

#### 使用 Row 对象 ####
Row 对象本质就是一个定长的字段数组。   
  
在 Scala/Java 中，Row 对象有一系列 getter 方法，可以通过下标获取每个字段的值。  
标准的取值方法 get （或Scala 中的 apply ），读入一个列的序号然后返回一个 Object 类型（或 Scala 中的 Any 类型）的对象，然后由我们把对象转为正确的类型。  

#### 缓存 ####
为了确保使用更节约内存的表示方式进行缓存而不是储存整个对象，应当使用专门的 hiveCtx.cacheTable("tableName") 方法。  

当缓存数据表时，Spark SQL 使用一种列式存储格式在内存中表示数据。  
这些缓存下来的表只会在驱动器程序的生命周期里保留在内存中，所以如果驱动器进程退出，就需要重新缓存数据。  




