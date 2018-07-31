### 第 3 章	RDD 编程 ###
介绍 Spark 对数据的核心抽象——弹性分布式数据集（Resilient Distributed Dataset，简称 RDD）。  
RDD 其实就是分布式的元素集合。  
在 Spark 中，对数据的所有操作不外乎创建 RDD、转化已有 RDD 以及调用 RDD 操作进行求值。  
而在这一切背后，Spark 会自动将 RDD 中的数据分发到集群上，并将操作并行化执行。  
#### 本章目录 ####
1.	[RDD 基础](C1RDD基础.md)    
2.	[创建 RDD](C2创建RDD.md)    
3.	[RDD 操作](C3RDD操作.md)    
3.1	转化操作    
3.2	行动操作    
3.3	惰性求值    
4.	[向 Spark 传递函数](C4向Spark传递函数.md)    
4.1	[Python](P4PassFunctions.py)    
4.2	[Scala](S4PassFunctions.scala)    
4.3	[Java](J4PassFunctions.java)    
5.	常见的转化操作和行动操作    
5.1	[基本 RDD](C51基本RDD.md)    
5.2	[在不同 RDD 类型间转换](C52不同RDD类型转换.md)    
6.	[持久化 ( 缓存 )](C6持久化(缓存).md)    
#### 本章总结 ####    
-   介绍了 RDD 运行模型。
-   介绍了 RDD 的许多常见操作。