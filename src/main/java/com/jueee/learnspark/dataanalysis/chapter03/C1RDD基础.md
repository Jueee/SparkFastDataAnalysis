### 3.1　RDD基础 ###
-   Spark 中的 RDD 就是一个不可变的分布式对象集合。
-   每个 RDD 都被分为多个分区，这些分区运行在集群中的不同节点上。
-   RDD 可以包含 Python、Java、Scala 中任意类型的对象，甚至可以包含用户自定义的对象。
#### 创建 RDD ####
用户可以使用两种方法创建 RDD：
-   读取一个外部数据集
-   在驱动器程序里分发驱动器程序中的对象集合（比如 list 和 set）  

在 Python 中使用 textFile() 创建一个字符串的 RDD
```
>>> lines = sc.textFile('pyspark')
```
#### RDD 操作 ####
RDD 支持两种类型的操作：
-   转化操作（transformation）  
转化操作会由一个 RDD 生成一个新的 RDD。  
例如，根据谓词匹配情况筛选数据就是一个常见的转化操作。
```
调用转化操作 filter()：
>>> pythonLines = lines.filter(lambda line: "Python" in line)
```
-   行动操作（action）  
行动操作会对 RDD 计算出一个结果，并把结果返回到驱动器程序中，或把结果存储到外部存储系统（如 HDFS）中。  
```
调用 first() 行动操作：
>>> pythonLines.first()
u'# to use IPython and set PYSPARK_DRIVER_PYTHON_OPTS to pass options when starting the Python driver'
```

转化操作和行动操作的区别在于 Spark 计算 RDD 的方式不同。  
虽然你可以在任何时候定义新的 RDD，但 Spark 只会惰性计算这些 RDD。  
它们只有第一次在一个行动操作中用到时，才会真正计算。  
###  RDD 持久化到内存中 #### 
默认情况下，Spark 的 RDD 会在你每次对它们进行行动操作时重新计算。   
如果想在多个行动操作中重用同一个 RDD，可以使用 RDD.persist() 让 Spark 把这个 RDD 缓存下来。  
```
>>> pythonLines.persist
<bound method PipelinedRDD.persist of PythonRDD[4] at RDD at PythonRDD.scala:49>
>>> pythonLines.first()
u'# to use IPython and set PYSPARK_DRIVER_PYTHON_OPTS to pass options when starting the Python driver'
>>> pythonLines.count()
6
```
#### 总结 ####
总的来说，每个 Spark 程序或 shell 会话都按如下方式工作。
1.  从外部数据创建出输入 RDD。
2.  使用诸如 filter() 这样的转化操作对 RDD 进行转化，以定义新的 RDD。
3.  告诉 Spark 对需要被重用的中间结果 RDD 执行 persist() 操作。
4.  使用行动操作（例如 count() 和 first() 等）来触发一次并行计算，Spark 会对计算进行优化后再执行。