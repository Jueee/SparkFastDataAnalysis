### 5.5.4　Elasticsearch ###
Elasticsearch 是一个开源的、基于 Lucene 的搜索系统。  

Elasticsearch 连接器会忽略我们提供的路径信息，而依赖于在 SparkContext 中设置的配置项。  
Elasticsearch 的 OutputFormat 连接器也没有用到 Spark 所封装的类型，所以我们使用 saveAsHadoopDataSet 来代替，这意味着我们需要手动设置更多属性。  

就输出而言，Elasticsearch 可以进行映射推断，但是偶尔会推断出不正确的数据类型。  
因此如果你要存储字符串以外的数据类型，最好明确指定类型映射。  
[参考文档](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-put-mapping.html)  
#### Elasticsearch-Hadoop ####
Spark 可以使用 Elasticsearch-Hadoop 从 Elasticsearch 中读写数据。  
[Elasticsearch-Hadoop](https://github.com/elastic/elasticsearch-hadoop)
``` 
<dependency>
  <groupId>org.elasticsearch</groupId>
  <artifactId>elasticsearch-hadoop</artifactId>
  <version>6.2.1</version>
</dependency>
```
#### 代码 ####
-   [Sacla](S54Elasticsearch.scala)
-   [Java](J54Elasticsearch.java)

保存报错：
``` 
18/09/14 20:03:14 ERROR Utils: Aborting task
org.elasticsearch.hadoop.EsHadoopException: Could not write all entries for bulk operation [3/3]. Error sample (first [5] error messages):
	failed to parse
	failed to parse
	failed to parse
```