### 7.6.3　Apache Mesos ###
Apache Mesos 是一个通用集群管理器，既可以运行分析型工作负载又可以运行长期运行的服务（比如网页服务或者键值对存储）。  

要在 Mesos 上使用 Spark，需要把一个 mesos:// 的URI 传给 spark-submit ：
``` 
spark-submit --master mesos://masternode:5050 yourapp
```

在运行多个主节点时，你可以使用 ZooKeeper 来为 Mesos 集群选出一个主节点。  
在这种情况下，应该使用 mesos://zk:// 的 URI 来指向一个 ZooKeeper 节点列表。  
``` 
mesos://zk://node1:2181/mesos,node2:2181/mesos,node3:2181/mesos
```
#### Mesos调度模式 ####
Mesos 提供了两种模式来在一个集群内的执行器进程间共享资：
-   “细粒度”模式（默认）  
执行器进程占用的 CPU 核心数会在它们执行任务时动态变化，因此一台运行了多个执行器进程的机器可以动态共享 CPU 资源。  
-   “粗粒度”模式  
Spark 提前为每个执行器进程分配固定数量的 CPU 数目，并且在应用结束前绝不释放这些资源，哪怕执行器进程当前不在运行任务。  
可以通过向 spark-submit 传递<b>--conf spark.mesos.coarse=true</b> 来打开粗粒度模式。

#### 配置资源用量 ####
可以通过 spark-submit 的两个参数来控制运行在 Mesos 上的资源用量：
-   --executor-memory  
用来设置每个执行器进程的内存，后者则用来设置应
-   --total-executor-cores  
用占用的核心数（所有执行器节点占用的总数）的最大值。 

如果你不设置 --total-executor-cores 参数，Mesos 会尝试使用集群中所有可用的核心。


