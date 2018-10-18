### 9.1　连接Spark SQL ###
带有 Hive 支持的 Spark SQL 的 Maven 索引：
``` 
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-hive_2.11</artifactId>
    <version>${spark.jar.version}</version>
    <scope>compile</scope>
</dependency>
```

若要把 Spark SQL 连接到一个部署好的 Hive 上，你必须把 hive-site.xml 复制到 Spark 的配置文件目录中（$SPARK_HOME/conf）。  

即使没有部署好 Hive，Spark SQL 也可以运行。  
需要注意的是，如果你没有部署好 Hive，Spark SQL 会在当前的工作目录中创建出自己的 Hive 元数据仓库，叫作 metastore_db 。