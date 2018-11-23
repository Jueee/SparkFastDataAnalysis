from pyspark import SparkConf,SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import LogisticRegressionWithSGD

outputPath = r'E:\code\Java\workspace48\SparkFastDataAnalysis\src\main\resources\data\chapter11'

SPAM_FILE = outputPath+r'\spam.txt'
NORMAL_FILE = outputPath+r'\normal.txt'

conf = SparkConf().setMaster("local").setAppName("MY App")
sc = SparkContext(conf = conf)
spam = sc.textFile(SPAM_FILE)
normal = sc.textFile(NORMAL_FILE)

# 创建一个HashingTF实例来把邮件文本映射为包含10000个特征的向量
tf = HashingTF(numFeatures = 10000)
# 各邮件都被切分为单词，每个单词被映射为一个特征
spamFeatures = spam.map(lambda email: tf.transform(email.split(" ")))
normalFeatures = normal.map(lambda email: tf.transform(email.split(" ")))

# 创建LabeledPoint数据集分别存放阳性（垃圾邮件）和阴性（正常邮件）的例子
positiveExamples = spamFeatures.map(lambda features: LabeledPoint(1, features))
negativeExamples = normalFeatures.map(lambda features: LabeledPoint(0, features))
trainingData = positiveExamples.union(negativeExamples)
trainingData.cache() # 因为逻辑回归是迭代算法，所以缓存训练数据RDD
# 使用SGD算法运行逻辑回归
model = LogisticRegressionWithSGD.train(trainingData)
# 以阳性（垃圾邮件）和阴性（正常邮件）的例子分别进行测试。首先使用
# 一样的HashingTF特征来得到特征向量，然后对该向量应用得到的模型
posTest = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
negTest = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))
print("Prediction for positive test example: %g" % model.predict(posTest))
print("Prediction for negative test example: %g" % model.predict(negTest))
