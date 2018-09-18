from pyspark import SparkConf,SparkContext

# 对每个键对应的元素分别计数
def countByKey(pairs):
    result = pairs.countByKey()
    for v in result:
        print(v,result[v])

# 将结果以映射表的形式返回，以便查询
def collectAsMap(pairs):
    result = pairs.collectAsMap()
    for v in result:
        print(v,result[v])

# 返回给定键对应的所有值
def lookup(pairs):
    result = pairs.lookup(3)
    for v in result:
        print(v)

if __name__ == '__main__':
    conf = SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    sc = SparkContext(conf = conf)
    pairs = sc.parallelize( [(1, 2), (3, 4), (3, 6)])
    countByKey(pairs)
    collectAsMap(pairs)
    lookup(pairs)