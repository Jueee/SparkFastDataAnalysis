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
PB 是结构化数据，它要求字段和类型都要明确定义。它们是经过优化的，编解码速度快，而且占用空间也很小。
-   [官网](https://developers.google.com/protocol-buffers/)
-   [Git Code](https://github.com/protocolbuffers/protobuf)
-   [Git Java Code](https://github.com/protocolbuffers/protobuf/tree/master/java)

PB 由可选字段、必需字段、重复字段三种字段组成。  
在解析时，可选字段的缺失不会导致解析失败，而必需字段的缺失则会导致数据解析失败。  
因此，在往 PB 定义中添加新字段时，最好将新字段设为可选字段，毕竟不是所有人都会同时更新到新版本。  

``` 
<dependency>
  <groupId>com.google.protobuf</groupId>
  <artifactId>protobuf-java</artifactId>
  <version>3.6.1</version>
</dependency>
```
``` 
<dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-netty</artifactId>
    <version>${grpc.version}</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-protobuf</artifactId>
    <version>${grpc.version}</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-stub</artifactId>
    <version>${grpc.version}</version>
    <scope>provided</scope>
</dependency>
```
``` 
<plugin>
    <groupId>org.xolstice.maven.plugins</groupId>
    <artifactId>protobuf-maven-plugin</artifactId>
    <version>0.5.1</version>
    <configuration>
        <protocArtifact>com.google.protobuf:protoc:${protobuf.version}:exe:${os.detected.classifier}</protocArtifact>
        <pluginId>grpc-java</pluginId>
        <pluginArtifact>io.grpc:protoc-gen-grpc-java:${grpc.version}}:exe:${os.detected.classifier}</pluginArtifact>
    </configuration>
    <executions>
        <execution>
            <goals>
                <goal>compile</goal>
                <goal>compile-custom</goal>
            </goals>
        </execution>
    </executions>
</plugin>
```