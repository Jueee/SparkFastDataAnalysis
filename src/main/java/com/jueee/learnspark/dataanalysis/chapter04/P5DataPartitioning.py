
# Python 自定义分区方式
import urllib.parse

def hash_domain(url):
    return hash(urllib.parse.urlparse(url).netloc)




# 使用
from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("MY App")
sc = SparkContext(conf = conf)
lines = sc.textFile("README.md")
lines.partitionBy(20,hash_domain)   # 创建20个分区