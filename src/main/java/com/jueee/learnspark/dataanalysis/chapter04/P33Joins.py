from pyspark import SparkConf,SparkContext


if __name__ == '__main__':
    conf = SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    sc = SparkContext(conf = conf)
    
    storeAddress = sc.parallelize([
        ("Ritual", "1026 Valencia St"),("Philz", "748 Van Ness Ave"),
        ("Philz", "3101 24th St"),("Starbucks", "Seattle")
    ])

    storeRating = sc.parallelize([
        ("Ritual", 4.9),("Philz",4.8),("Jueee",4.2)
    ])

    print("--- join ---")
    joinResult = storeAddress.join(storeRating)
    for v in joinResult.collect():
        print(v)

    print("--- leftOuterJoin ---")
    leftOuterJoinResult = storeAddress.leftOuterJoin(storeRating)
    for v in leftOuterJoinResult.collect():
        print(v)

    print("--- rightOuterJoin ---")
    rightOuterJoinResult = storeAddress.rightOuterJoin(storeRating)
    for v in rightOuterJoinResult.collect():
        print(v)