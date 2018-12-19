from pyspark.mllib.recommendation import ALS,Rating
from pyspark import SparkContext

# 协同过滤与推荐

# 转换用户评分数据格式
def get_rating(str):
    arr = str.split('\t')
    user_id = int(arr[0])
    movie_id = int(arr[1])
    user_rating = float(arr[2])
    return Rating(user_id, movie_id, user_rating)

#计算MSE
def getMSE(ratings, model):
    usersProducts = ratings.map(lambda rating:(rating.user, rating.product))
    # predictAll方法以对(int, int)形式的rdd作为参数，这点与scala不同，scala直接用predict
    predictions = model.predictAll(usersProducts).map(lambda rating:((rating.user, rating.product), rating.rating))
    ratesAndPreds = ratings.map(lambda rating:((rating.user, rating.product), rating.rating))
    joins = ratesAndPreds.join(predictions)
    # mean() 元素平均值
    return joins.map(lambda o:(o[1][0] - o[1][1]) * (o[1][0] - o[1][1])).mean()

sc = SparkContext('local')
lines = sc.textFile(r"C:\Codes\GitHub\SparkFastDataAnalysis\src\main\resources\data\chapter11\u.data")
ratings = lines.map(get_rating)
print("ratings length:%s" % len(ratings.collect()))

# 分割数据，一部分用于训练，一部分用于测试
(training, test) = ratings.randomSplit([0.6, 0.4],11)
print("training length:%s" %  len(training.collect()))
print("test length:%s" % len(test.collect()))

rank = 10
numIterations = 3
model = ALS.train(training, rank, numIterations, 0.01)
print(model)
MSE = getMSE(training, model)
print("Mean Squared Error = %s" % MSE) #训练数据的MSE

MSE1 = getMSE(test, model)
print("Mean Squared Error1 = %s" % MSE1) #测试数据的MSE


"""
ratings length:100000
training length:59778
test length:40222
Mean Squared Error = 0.48225962870311656
Mean Squared Error1 = 1.2922615156720993
"""