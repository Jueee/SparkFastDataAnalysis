from pyspark.mllib.feature import HashingTF, IDF
from pyspark import SparkContext

sentence = "hello hello world"
words = sentence.split()        # 将句子切分为一串单词
tf = HashingTF(10000)           # 创建一个向量，其尺寸S = 10,000
aa = tf.transform(words)
print(aa)

# 将若干文本文件读取为TF向量
sc = SparkContext('local')
rdd = sc.wholeTextFiles('P51FeatureExtraction.py').map(lambda text:text[1].split())
tfVectors = tf.transform(rdd)   # 对整个RDD进行转化操作
for v in tfVectors.collect():
    print(v)



# 在 Python 中使用 TF-IDF
idf = IDF()
idfModel = idf.fit(tfVectors)
tfIdVectors = idfModel.transform(tfVectors)
print(tfIdVectors)
for v in tfIdVectors.collect():
    print(v)



