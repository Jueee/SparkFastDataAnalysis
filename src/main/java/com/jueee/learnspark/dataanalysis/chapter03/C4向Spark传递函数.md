### 3.4　向Spark传递函数 ###
Spark 的大部分转化操作和一部分行动操作，都需要依赖用户传递的函数来计算。   
在我们支持的三种主要语言中，向 Spark 传递函数的方式略有区别。
#### [3.4.1　Python](P4PassFunctions.py) ####
传递函数时需要小心的一点是，Python 会在你不经意间把函数所在的对象也序列化传出去。
#### [3.4.2　Scala](S4PassFunctions.scala) ####
如果在 Scala 中出现了 NotSerializableException ，通常问题就在于我们传递了一个不可序列化的类中的函数或字段。  
记住，传递局部可序列化变量或顶级对象中的函数始终是安全的。
-   所传递的函数及其引用的数据需要是可序列化的（实现了 Java 的 Serializable 接口）。
-   与 Python 类似，传递一个对象的方法或者字段时，会包含对整个对象的引用。
#### [3.4.3　Java](J4PassFunctions.java) ####
在 Java 中，函数需要作为实现了 Spark 的 org.apache.spark.api.java.function 包中的任一函数接口的对象来传递。  
-   Function<T, R>   
接收一个输入值并返回一个输出值，用于类似 map() 和 filter() 等操作中
-   Function2<T1, T2, R>  
接收两个输入值并返回一个输出值，用于类似 aggregate() 和 fold() 等操作中
-   FlatMapFunction<T, R>  
接收一个输入值并返回任意个输出，用于类似 flatMap() 这样的操作中



