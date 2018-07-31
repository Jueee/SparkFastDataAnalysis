### 4.2　创建Pair RDD ###
在 Spark 中有很多种创建 pair RDD 的方式：
-   很多存储键值对的数据格式会在读取时直接返回由其键值对数据组成的 pair RDD。
-   当需要把一个普通的 RDD 转为 pair RDD 时，可以调用 map() 函数来实现，传递的函数需要返回键值对。
#### 在Python中创建Pair RDD ####
[在 Python 中使用第一个单词作为键创建出一个 pair RDD](P2CreatePairRDD.py)  
在 Python 中，为了让提取键之后的数据能够在函数中使用，需要返回一个由二元组组成的 RDD。
#### 在Scala中创建Pair RDD ####
[在 Scala 中使用第一个单词作为键创建出一个 pair RDD](S2CreatePairRDD.scala)
在 Scala 中，为了让提取键之后的数据能够在函数中使用，同样需要返回二元组。  
隐式转换可以让二元组 RDD 支持附加的键值对函数。
#### 在 Java 中创建Pair RDD ####
[在 Java 中使用第一个单词作为键创建出一个 pair RDD](J2CreatePairRDD.java)
-   Java 没有自带的二元组类型，因此 Spark 的 Java API 让用户使用 scala.Tuple2 类来创建二元组。
-   Java 用户可以通过 new Tuple2(elem1, elem2) 来创建一个新的二元组，并且可以通过 ._1() 和 ._2() 方法访问其中的元素。
-   Java 用户还需要调用专门的 Spark 函数来创建 pair RDD。  
例如，要使用 mapToPair() 函数来代替基础版的 map() 函数。
#### 内存创建 ####
-   使用 Scala 和 Python 从一个内存中的数据集创建 pair RDD 时，只需要对这个由二元组组成的集合调用 SparkContext.parallelize() 方法。
-   使用 Java 从内存数据集创建 pair RDD的话，则需要使用 SparkContext.parallelizePairs() 。