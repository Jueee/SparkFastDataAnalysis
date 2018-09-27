### 7.6.2　Hadoop YARN ###
YARN 是在 Hadoop 2.0 中引入的集群管理器，它可以让多种数据处理框架运行在一个共享的资源池上，并且通常安装在与 Hadoop 文件系统（简称 HDFS）相同的物理节点上。  

在 Spark 里使用 YARN 很简单：你只需要设置指向你的 Hadoop 配置目录的环境变量，然后使用 spark-submit 向一个特殊的主节点 URL 提交作业即可。  

找到你的 Hadoop 的配置目录，并把它设为环境变量 HADOOP_CONF_DIR 。  
这个目录包含 yarn-site.xml 和其他配置文件；如果你把 Hadoop 装到 HADOOP_HOME 中，那么这个目录通常位于 HADOOP_HOME/conf 中，否则可能位于系统目录 /etc/hadoop/conf 中。  

提交应用：
``` 
export HADOOP_CONF_DIR="..."
spark-submit --master yarn yourapp
```

#### 配置资源用量 ####
当在 YARN 上运行时，根据你在 spark-submit 或 spark-shell 等脚本的 --num-executors 标记中设置的值，Spark 应用会使用固定数量的执行器节点。  

