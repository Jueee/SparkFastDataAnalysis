from pyspark import SparkConf,SparkContext

# 使用 reduceByKey() 和 mapValues() 计算每个键对应的平均值
def reduceByKey(sc):
    pairs = sc.parallelize({('panda', 0), ('pink', 3), ('pirate', 3), ('panda', 1), ('pink', 4)})
    values = pairs.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
    for value in values.collect():
        print(value)

if __name__ == '__main__':
    conf = SparkConf().setMaster("local").setAppName("MY App")
    sc = SparkContext(conf = conf)
    reduceByKey(sc)