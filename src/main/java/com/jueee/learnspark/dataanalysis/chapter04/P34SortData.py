from pyspark import SparkConf,SparkContext


if __name__ == '__main__':
    conf = SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    sc = SparkContext(conf = conf)
    pairs = sc.parallelize( [(1, 2), (4, 4), (23, 6)])

    for v in pairs.collect():
        print(v)

    print()

    sortpairs = pairs.sortByKey(ascending=True, numPartitions=None, keyfunc=lambda x:str(x))
    for v in sortpairs.collect():
        print(v)