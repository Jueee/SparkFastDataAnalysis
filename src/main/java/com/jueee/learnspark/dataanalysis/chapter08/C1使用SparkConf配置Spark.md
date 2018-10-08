### 8.1　使用SparkConf配置Spark ###
对 Spark 进行性能调优，通常就是修改 Spark 应用的运行时配置选项。  
Spark 中最主要的配置机制是通过 SparkConf 类对 Spark 进行配置。
#### 代码 ####
使用 SparkConf 创建一个应用：
-   [Python](P1SparkConf.py)
-   [Scala](S1SparkConf.scala)
-   [Java](J1SparkConf.java)

SparkConf 实例包含用户要重载的配置选项的键值对。  
Spark 中的每个配置选项都是基于字符串形式的键值对。  
要使用创建出来的 SparkConf 对象，可以调用 set() 方法来添加配置项的设置，然后把这个对象传给 SparkContext 的构造方法。

#### 动态设置配置项 ####
Spark 允许通过 spark-submit 工具动态设置配置项。  
当应用被 spark-submit 脚本启动时，脚本会把这些配置项设置到运行环境中。  
当一个新的 SparkConf 被创建出来时，这些环境变量会被检测出来并且自动配好。  

在运行时使用标记设置配置项的值：
``` 
$ bin/spark-submit \
    --class com.example.MyApp \
    --master local[4] \
    --name "My Spark App" \
    --conf spark.ui.port=36000 \
    myApp.jar
```

#### 从文件中读取配置项 ####
spark-submit 也支持从文件中读取配置项的值。  
这对于设置一些与环境相关的配置项比较有用，方便不同用户共享这些配置（比如默认的 Spark 主节点）。
``` 
$ bin/spark-submit \
    --class com.example.MyApp \
    --properties-file my-config.conf \
    myApp.jar
    
## Contents of my-config.conf ##
spark.master local[4]
spark.app.name "My Spark App"
spark.ui.port 36000
```

#### 配置优先级 ####
1.  优先级最高的是在用户代码中显式调用 set() 方法设置的选项。
2.  其次是通过 spark-submit 传递的参数。
3.  再次是写在配置文件中的值。
4.  最后是系统的默认值。



