### 5.2.6　Hadoop输入输出格式 ###

#### newAPIHadoopFile 读取 ####
[Scala示例](S26HadoopFormats.scala)  
newAPIHadoopFile 接收一个路径以及三个类：
1.  第一个类是“格式”类，代表输入格式。  
相似的函数 hadoopFile() 则用于使用旧的 API 实现的 Hadoop 输入格式。
2.  第二个类是键的类。
3.  最后一个类是值的类。   
4.  如果需要设定额外的 Hadoop 配置属性，也可以传入一个 conf 对象。

KeyValueTextInputFormat 是最简单的 Hadoop 输入格式之一，可以用于从文本文件中读取键值对数据。  
每一行都会被独立处理，键和值之间用制表符隔开。 
#### 自定义 Hadoop 输入格式来读取 JSON 数据 ####
Twitter 的 Elephant Bird 包支持很多种数据格式，包括 JSON、Lucene、Protocol Buffer 相关的格式等。  
[https://github.com/twitter/elephant-bird](https://github.com/twitter/elephant-bird)
``` 
<dependency>
    <groupId>com.twitter.elephantbird</groupId>
    <artifactId>elephant-bird-core</artifactId>
    <version>4.8</version>
</dependency>
```
#### 保存Hadoop输出格式 ####
[Java示例](J26HadoopFormats.java)    
#### Protocol buffer ####
-   [官网](https://developers.google.com/protocol-buffers/)
-   [Git Code](https://github.com/protocolbuffers/protobuf)
-   [Git Java Code](https://github.com/protocolbuffers/protobuf/tree/master/java)
``` 
<dependency>
  <groupId>com.google.protobuf</groupId>
  <artifactId>protobuf-java</artifactId>
  <version>3.6.1</version>
</dependency>
```