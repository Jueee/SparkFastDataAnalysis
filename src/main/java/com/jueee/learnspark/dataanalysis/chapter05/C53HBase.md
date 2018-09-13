### 5.5.3　HBase ###
由 于 org.apache.hadoop.hbase.mapreduce.TableInputFormat 类的实现，Spark 可以通过 Hadoop 输入格式访问 HBase。  
这个输入格式会返回键值对数据：
-   键的类型为 org.apache.hadoop.hbase.io.ImmutableBytesWritable 
-   值的类型为 org.apache.hadoop.hbase.client.Result

要将 Spark 用于 HBase，你需要使用正确的输入格式调用 SparkContext.newAPIHadoopRDD 。

TableInputFormat 包含多个可以用来优化对 HBase 的读取的设置项，比如将扫描限制到一部分列中，以及限制扫描的时间范围。
#### 代码 ####
-   [Sacla](S53HBase.scala)
-   [Java](J53HBase.java)