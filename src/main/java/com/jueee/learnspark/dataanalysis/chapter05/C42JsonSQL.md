### 5.4.2　JSON ###
如果你有记录间结构一致的 JSON 数据，Spark SQL 也可以自动推断出它们的结构信息，并将这些数据读取为记录，这样就可以使得提取字段的操作变得很简单。  
要读取 JSON 数据，首先需要和使用 Hive 一样创建一个 HiveContext 。  
（不过在这种情况下我们不需要安装好 Hive，也就是说你也不需要 hive-site.xml 文件。）  
然后使用 HiveContext.jsonFile 方法来从整个文件中获取由 Row 对象组成的 RDD。
#### 代码 ####
-   [Python](P42JsonSQL.py)
-   [Sacla](S42JsonSQL.scala)
-   [Java](J42JsonSQL.java)