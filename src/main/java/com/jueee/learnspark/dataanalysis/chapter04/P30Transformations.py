from pyspark import SparkConf,SparkContext
from pyspark import StorageLevel


def transByOne(sc):
    pairs = sc.parallelize( {(1, 2), (3, 4), (3, 6)})
    for value in pairs.collect():
        print(value)
    pairs.persist(storageLevel=StorageLevel.DISK_ONLY)
    #  合并具有相同键的值
    #  [(1, 2), (3, 10)]
    print(pairs.reduceByKey(lambda x,y: x+y).collect())

    #  对具有相同键的值进行分组
    #  [(1, <pyspark.resultiterable.ResultIterable object at 0x00000000076E7860>), (3, <pyspark.resultiterable.ResultIterable object at 0x00000000076E7898>)]
    print(pairs.groupByKey().collect())

    #  对 pair RDD 中的每个值应用一个函数而不改变键
    #  [(1, 3), (3, 5), (3, 7)]
    print(pairs.mapValues(lambda x:x+1).collect())

    #  对 pair RDD 中的每个值应用一个返回迭代器的函数
    #  然后对返回的每个元素都生成一个对应原键的键值对记录。
    #  通常用于符号化
    #  [(1, 2), (1, 3), (1, 4), (1, 5), (1, 6), (1, 7), (3, 4), (3, 5), (3, 6), (3, 7), (3, 6), (3, 7)]
    print(pairs.flatMapValues(lambda x:range(x,8)).collect())

    #  返回一个仅包含键的 RDD
    #  [1, 3, 3]
    print(pairs.keys().collect())

    #  返回一个仅包含值的 RDD
    #  [2, 4, 6]
    print(pairs.values().collect())

    #  返回一个根据键排序的 RDD
    #  [(1, 2), (3, 4), (3, 6)]
    print(pairs.sortByKey().collect())

def transByTwo(sc):
    rdd = sc.parallelize(  {(1, 2), (3, 4), (3, 6)})
    other = sc.parallelize( {(3, 9)})
    rdd.persist(storageLevel=StorageLevel.DISK_ONLY)
    other.persist(storageLevel=StorageLevel.DISK_ONLY)
    # 删掉 RDD 中键与 other RDD 中的键相同的元素
    # [(1, 2)]
    print(rdd.subtractByKey(other).collect())

    # 对两个 RDD 进行内连接
    # [(3, (4, 9)), (3, (6, 9))]
    print(rdd.join(other).collect())

    # 对两个 RDD 进行连接操作，确保第一个 RDD 的键必须存在（右外连接）
    # [(3, (4, 9)), (3, (6, 9))]
    print(rdd.rightOuterJoin(other).collect())

    # 对两个 RDD 进行连接操作，确保第二个 RDD 的键必须存在（左外连接）
    # [(1, (2, None)), (3, (4, 9)), (3, (6, 9))]
    print(rdd.leftOuterJoin(other).collect())

    # 将两个 RDD 中拥有相同键的数据分组到一起
    # [(1, (<pyspark.resultiterable.ResultIterable object at 0x0000000006E26588>, <pyspark.resultiterable.ResultIterable object at 0x0000000006EDDCC0>)), (3, (<pyspark.resultiterable.ResultIterable object at 0x0000000006E266D8>, <pyspark.resultiterable.ResultIterable object at 0x0000000006EDDBE0>))]
    print(rdd.cogroup(other).collect())

def filterPair(sc):
    lines = sc.textFile("README.md")
    pairs = lines.map(lambda x:(x.split(" ")[0],x))
    ttt = pairs.filter(lambda t:len(t[1])<20)
    for value in ttt.collect():
        print(value)


if __name__ == '__main__':
    conf = SparkConf().setMaster("local").setAppName("MY App")
    sc = SparkContext(conf = conf)
    transByOne(sc)
    transByTwo(sc)
    filterPair(sc)
