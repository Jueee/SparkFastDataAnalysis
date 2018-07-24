### 3.3　RDD操作
RDD 支持两种操作：
-   转化操作：(返回的是 RDD)  
返回一个新的 RDD 的操作，比如 map() 和 filter() 。  
-   行动操作：(返回的是其他的数据类型)  
向驱动器程序返回结果或把结果写入外部系统的操作，会触发实际的计算，比如 count() 和 first() 。
#### 3.3.1　转化操作
RDD 的转化操作是返回新 RDD 的操作。  
  
Python 实现 filter() 转化操作：  
``` 
>>> lines = sc.textFile('pyspark')
>>> pythonLines = lines.filter(lambda line: "Python" in line)
>>> pythonLines
PythonRDD[16] at RDD at PythonRDD.scala:49
>>> sparkLines = lines.filter(lambda line: "spark" in line)
>>> allLines = pythonLines.union(sparkLines)
>>> allLines.count()
15
```
Scala 实现 filter() 转化操作： 
``` 
scala> val lines = sc.textFile("spark-shell")
lines: org.apache.spark.rdd.RDD[String] = spark-shell MapPartitionsRDD[4] at textFile at <console>:24

scala> val scalalines = lines.filter(line => line.contains("scala"))
scalalines: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[5] at filter at <console>:25
```
Java 实现 filter() 转化操作：
``` 
JavaRDD<String> lines = sc.textFile("README.md");
JavaRDD<String> javaRDD = lines.filter(
        new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("Java");
            }
        }
);
```
``` 
JavaRDD<String> javaRDD = lines.filter(line -> line.contains("Java"));
```

注意， filter() 操作不会改变已有的 inputRDD 中的数据。  
实际上，该操作会返回一个全新的 RDD。
#### 3.3.2　行动操作
行动操作会把最终求得的结果返回到驱动器程序，或者写入外部存储系统中。  
由于行动操作需要生成实际的输出，它们会强制执行那些求值必须用到的 RDD 的转化操作。  

【注】
-   用 count() 来返回计数结果，用 take() 来收集RDD 中的一些元素。 
-   RDD 还有一个 collect() 函数，可以用来获取整个 RDD 中的数据。  
如果你的程序把 RDD 筛选到一个很小的规模，并且你想在本地处理这些数据时，就可以使用它。
-   可以使用 saveAsTextFile() 、 saveAsSequenceFile() ，或者任意的其他行动操作来把 RDD 的数据内容以各种自带的格式保存起来。
-   每当我们调用一个新的行动操作时，整个 RDD 都会从头开始计算。  
要避免这种低效的行为，用户可以将中间结果持久化。
       
在 Python 中使用行动操作进行计数  
``` 
pythonLines = lines.filter(lambda line: "Python" in line)
for line in pythonLines.take(pythonLines.count()):
    print(line)
```
在 Scala 中使用行动操作进行计数
``` 
val scalalines = lines.filter(line => line.contains("Scala"))
scalalines.take(scalalines.count().toInt).foreach(println)
```
在 Java 中使用行动操作进行计数
``` 
JavaRDD<String> javaRDD = lines.filter(line -> line.contains("Java"));
for (String line:javaRDD.take((int) javaRDD.count())){
    System.out.println(line);
}
或
javaRDD.take((int) javaRDD.count()).forEach(System.out::println);
javaRDD.collect().forEach(System.out::println);
```
####3.3.3　惰性求值
RDD 的转化操作都是惰性求值的。这意味着在被调用行动操作之前 Spark 不会开始计算。  
