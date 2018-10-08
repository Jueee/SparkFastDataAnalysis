from pyspark import SparkConf,SparkContext

conf = SparkConf()
conf.set("spark.app.name", "My Spark App")
conf.set("spark.master", "local[4]")
conf.set("spark.ui.port", "36000") # 重载默认端口配置
print(conf)

# 使用这个配置对象创建一个SparkContext
sc = SparkContext("")
print(sc)