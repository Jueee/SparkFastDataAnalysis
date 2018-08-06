from pyspark import SparkConf,SparkContext

# 使用 reduceByKey() 和 mapValues() 计算每个键对应的平均值
def reduceByKey(sc):
    pairs = sc.parallelize({('panda', 0), ('pink', 3), ('pirate', 3), ('panda', 1), ('pink', 4)})
    values = pairs.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
    for value in values.collect():
        print(value)
    averages = values.map(lambda x:(x[0],x[1][0]/x[1][1]))
    for average in averages.collect():
        print(average)

# 实现单词计数
# 使用 flatMap() 来生成以单词为键、以数字 1 为值的 pair RDD
def wordcount(sc):
    rdd = sc.textFile("README.md")
    words = rdd.flatMap(lambda x:x.split(" "))
    result = words.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
    for v in result.collect():
        print(v)

def wordcountByCountByValue(sc):
    rdd = sc.textFile("README.md")
    words = rdd.flatMap(lambda x:x.split(" ")).countByValue()
    for v in words:
        print(v)

# 使用 combineByKey() 求每个键对应的平均值
def combineByKey(sc):
    print("---combineByKey---")
    pairs = sc.parallelize([('panda', 0), ('pink', 3), ('pirate', 3), ('panda', 1), ('pink', 4)])
    sumCount = pairs.combineByKey(
        (lambda x:(x,1)),
        (lambda x,y:(x[0] + y, x[1] + 1)),
        (lambda x,y:(x[0] + y[0], x[1] + y[1]))
    )
    for v in sumCount.collect():
        print(v)
    result = sumCount.map(lambda x: (x[0],x[1][0]/x[1][1])).collectAsMap()
    for v in result:
        print(v)

if __name__ == '__main__':
    conf = SparkConf().setMaster("local").setAppName("MY App")
    sc = SparkContext(conf = conf)
    reduceByKey(sc)
    wordcount(sc)
    wordcountByCountByValue(sc)
    combineByKey(sc)
