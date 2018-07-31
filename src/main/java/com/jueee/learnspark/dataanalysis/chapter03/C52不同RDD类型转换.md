### 3.5.2　在不同RDD类型间转换 ###
有些函数只能用于特定类型的 RDD，比如 mean() 和 variance() 只能用在数值 RDD 上，而 join() 只能用在键值对 RDD 上。  
要访问这些附加功能，必须要确保获得了正确的专用 RDD 类。  
#### Scala ####
在 Scala 中，将 RDD 转为有特定函数的 RDD（比如在 RDD[Double] 上进行数值操作）是由隐式转换来自动处理的。  
我们需要加上 import org.apache.spark.SparkContext._ 来使用这些隐式转换。  
隐式转换可以隐式地将一个 RDD 转为各种封装类，比如 DoubleRDDFunctions（数值数据的 RDD）和 PairRDDFunctions （键值对 RDD）。  

隐式转换虽然强大，但是会让阅读代码的人感到困惑。
#### Java ####
在 Java 中，各种 RDD 的特殊类型间的转换更为明确。  
Java 中有两个专门的类 JavaDoubleRDD 和 JavaPairRDD ，来处理特殊类型的 RDD，这两个类还针对这些类型提供了额外的函数。  

要构建出这些特殊类型的 RDD，需要使用特殊版本的类来替代一般使用的 Function 类。  
如果要从 T 类型的 RDD 创建出一个 DoubleRDD ，我们就应当在映射操作中使用 DoubleFunction<T> 来替代 Function<T, Double> 。

当需要一个 DoubleRDD 时，我们应当调用 mapToDouble() 来替代 map() ，跟其他所有函数所遵循的模式一样。  
[Java 创建 DoubleRDD](J52DoubleRDD.java)
#### Python ####
Python 的 API 结构与 Java 和 Scala 有所不同。  
在 Python 中，所有的函数都实现在基本的 RDD 类中，但如果操作对应的 RDD 数据类型不正确，就会导致运行时错误。