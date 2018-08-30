### 5.2.3　逗号分隔值与制表符分隔值 ###
逗号分隔值（CSV）文件每行都有固定数目的字段，字段间用逗号隔开（在制表符分隔值文件，即 TSV 文件中用制表符隔开）。  
CSV 文件和 TSV 文件有时支持的标准并不一致，主要是在处理换行符、转义字符、非 ASCII 字符、非整数值等方面。  
CSV 原生并不支持嵌套字段，所以需要手动组合和分解特定的字段。  
#### 使用库 ####
-   在 Python 我们会使用自带的 csv 库
-   在 Scala 和 Java 中则使用 opencsv 库
[http://opencsv.sourceforge.net/](http://opencsv.sourceforge.net/)
```
  <dependency>
     <groupId>com.opencsv</groupId>
     <artifactId>opencsv</artifactId>
     <version>4.2</version>
  </dependency>
```
#### 代码 ####
-   [Python](P23CSV.py)
-   [Scala](S23CSV.scala)
-   [Java](J23CSV.java)
#### 读取CSV ####
读取 CSV/TSV 数据和读取 JSON 数据相似，都需要先把文件当作普通文本文件来读取数
据，再对数据进行处理。
-   如果恰好你的 CSV 的所有数据字段均没有包含换行符，你也可以使用 textFile() 读取并解析数据
-   如果在字段中嵌有换行符，就需要完整读入每个文件，然后解析各段
#### 保存CSV ####
写出 CSV/TSV 数据相当简单，同样可以通过重用输出编码器来加速。  
由于在 CSV 中我们不会在每条记录中输出字段名，因此为了使输出保持一致，需要创建一种映射关系。  
一种简单做法是写一个函数，用于将各字段转为指定顺序的数组。  
