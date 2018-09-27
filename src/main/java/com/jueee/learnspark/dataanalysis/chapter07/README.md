### 第 7 章	在集群上运行 Spark ###
介绍如何在集群上运行 Spark。  

Spark 可以在各种各样的集群管理器（Hadoop YARN、Apache Mesos，还有 Spark 自带的独立集群管理器）上运行，所以 Spark 应用既能够适应专用集群，又能用于共享的云计算环境。  
我们会对各种使用情况下的优缺点和配置方法进行探讨。  
同时，也会讨论 Spark 应用在调度、部署、配置等各方面的一些细节。
#### 本章目录 ####
1.	简介    
2.	[Spark 运行时架构](C2Spark运行时架构.md)    
2.1	驱动器节点   
2.2	执行器节点    
2.3	集群管理器    
2.4	启动一个程序    
2.5	小结    
3.	[使用 spark-submit 部署应用](C3使用spark-submit部署应用.md)    
4.	[打包代码与依赖](C4打包代码与依赖.md)    
4.1	使用 Maven 构建的用 Java 编写的 Spark 应用    
4.2	使用 sbt 构建的用 Scala 编写的 Spark 应用    
4.3	依赖冲突    
5.	[Spark 应用内与应用间调度](C5Spark应用内与应用间调度.md)    
6.	集群管理器    
6.1	[独立集群管理器](C61独立集群管理器.md)    
6.2	[Hadoop YARN](C62HadoopYARN.md)    
6.3	[Apache Mesos](C63ApacheMesos.md)    
6.4	[Amazon EC2](C64AmazonEC2.md)    
7.	[选择合适的集群管理器]()    
#### 本章总结 ####    
-   描述了 Spark 应用的运行时架构  
它是由一个驱动器节点和一系列分布式执行器节点组成的。
-   构建、打包 Spark 应用并向集群提交执行。
-   常见的 Spark 部署环境。