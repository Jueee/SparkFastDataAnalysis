### 2.2　Spark中Python和Scala的shell ###
Spark shell 可用来与分布式存储在许多机器的内存或者硬盘上的数据进行交互，并且处理过程的分发由 Spark 自动控制完成。  

#### RDD ####
在 Spark 中，我们通过对分布式数据集的操作来表达我们的计算意图，这些计算会自动地在集群上并行进行。  
这样的数据集被称为弹性分布式数据集（resilient distributed dataset），简称 RDD。  
RDD 是 Spark 对分布式数据和计算的基本抽象。
#### Python 版本的 Spark shell ####
<pre>bin/pyspark</pre>
[Python 行数统计](P2PythonShell.py)
<pre>
>>> lines = sc.textFile("pyspark.cmd")
>>> lines.count()
25
>>> lines.first()
u'@echo off'
</pre>
#### Scala 版本的 Spark shell ####
<pre>bin/spark-shell</pre>
[Scala 行数统计](S2ScalaShell.scala)
<pre>
scala> var lines = sc.textFile("derby.log")
18/07/19 10:39:57 WARN SizeEstimator: Failed to check whether UseCompressedOops is set; assuming yes 
lines: org.apache.spark.rdd.RDD[String] = derby.log MapPartitionsRDD[1] at textFile at <console>:24

scala> lines.count()
[Stage 0:>                                                          (0 + 0) / 2]
[Stage 0:=============================>                             (1 + 1) / 2]

res0: Long = 76

scala> lines.first()
res1: String = Thu Jul 19 10:39:10 CST 2018 Thread[main,5,main] Ignored duplicate property derby.
</pre>