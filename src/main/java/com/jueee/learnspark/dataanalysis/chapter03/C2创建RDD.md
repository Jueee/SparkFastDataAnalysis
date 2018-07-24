### 创建RDD
创建 RDD 最简单的方式就是把程序中一个已有的集合传给 SparkContext 的 parallelize() 方法

#### parallelize() 方法
Python:
```
>>> lines = sc.parallelize(["pandas", "i like pandas"])
>>> lines.first()
'pandas'
```
Scala:
```
scala> val lines = sc.parallelize(List("pandas", "i like pandas"))
lines: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[0] at parallelize at <console>:24
   
scala> lines.first()
res0: String = pandas
```
Java:
```
JavaRDD<String> lines = sc.parallelize(Arrays.asList("pandas","i like pandas"));
System.out.println(lines.first());
```
#### textFile() 方法
Python:
``` 
>>> lines = sc.textFile('pyspark')
>>> lines.count()
77
```
Scala:
``` 
scala> val lines = sc.textFile("spark-shell")
lines: org.apache.spark.rdd.RDD[String] = spark-shell MapPartitionsRDD[2] at textFile at <console>:24

scala> lines.count()
res1: Long = 95
```
Java:
``` 
JavaRDD<String> lines = sc.textFile("README.md");
System.out.println(lines.count());
```