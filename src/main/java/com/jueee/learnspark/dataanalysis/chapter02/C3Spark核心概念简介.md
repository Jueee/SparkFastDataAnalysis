### 2.3　Spark核心概念简介 ###
-   从上层来看，每个 Spark 应用都由一个驱动器程序（driver program）来发起集群上的各种并行操作。  
驱动器程序包含应用的 main 函数，并且定义了集群上的分布式数据集，还对这些分布式数据集应用了相关操作。
-   驱动器程序通过一个 SparkContext 对象来访问 Spark。这个对象代表对计算集群的一个连接。  
shell 启动时已经自动创建了一个 SparkContext 对象，是一个叫作 sc 的变量。
-   驱动器程序一般要管理多个执行器（executor）节点。
#### Python 版本筛选的例子 ####
<pre>bin/pyspark</pre>
[Python 版本筛选]（
<pre>
>>> lines = sc.textFile("pyspark")
>>> pythonLines = lines.filter(lambda line: "Python" in line)
>>> pythonLines.count()
6
>>> pythonLines.first()
u'# to use IPython and set PYSPARK_DRIVER_PYTHON_OPTS to pass options when starting the Python driver'
</pre>
#### Scala 版本筛选的例子 ####
<pre>bin/spark-shell</pre>
[Scala 版本筛选](S3CoreConcepts.scala)
<pre>
scala> val lines = sc.textFile("spark-shell")
lines: org.apache.spark.rdd.RDD[String] = spark-shell MapPartitionsRDD[1] at textFile at <console>:24

scala> val scalaLines = lines.filter(line => line.contains("scala"))
scalaLines: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[2] at filter at <console>:25

scala> scalaLines.count()
res2: Long = 4

scala> scalaLines.first()
res3: String = # SPARK-4161: scala does not assume use of the java classpath,
</pre>
#### 向 Spark 传递函数 ####
[Java 版本](J3CoreConcepts.java)  
[Scala 版本](S3CoreConcepts.scala)  
[Python 版本](P3CoreConcepts.py)
