### 5.2.4　SequenceFile ###
SequenceFile 是由没有相对关系结构的键值对文件组成的常用 Hadoop 格式。  
SequenceFile 文件有同步标记，Spark 可以用它来定位到文件中的某个点，然后再与记录的边界对齐。  
这可以让 Spark 使用多个节点高效地并行读取 SequenceFile 文件。  
#### 代码 ####
-   [Python](P24SequenceFile.py)
-   [Scala](S24SequenceFile.scala)
-   [Java](J24SequenceFile.java)
#### 读取SequenceFile ####
Spark 有专门用来读取 SequenceFile 的接口。  
在 SparkContext 中，可以调用 sequenceFile(path, keyClass, valueClass, minPartitions) 。
#### 保存SequenceFile ####
-   Scala：创建一个由可以写出到 SequenceFile 的类型构成的 PairRDD  
直接调用 saveSequenceFile(path) 保存你的 PairRDD ，它会帮你写出数据。  
-   在 Java 中保存 SequenceFile 要稍微复杂一些，因为 JavaPairRDD 上没有 saveAsSequenceFile() 方法。

