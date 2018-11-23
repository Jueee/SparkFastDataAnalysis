package com.jueee.learnspark.dataanalysis.chapter11

import java.io.File

import com.jueee.learnspark.dataanalysis.util.{DataBaseUtil, FilesUtilByJava}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object S3SpamClassification {

  var FILE_PATH = FilesUtilByJava.getDataPath + File.separator + "chapter11" + File.separator
  var SPAM_FILE = FILE_PATH + "spam.txt"
  var NORMAL_FILE = FILE_PATH + "normal.txt"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)
    val spam = sc.textFile(SPAM_FILE)
    val normal = sc.textFile(NORMAL_FILE)

    // 创建一个HashingTF实例来把邮件文本映射为包含10000个特征的向量
    val tf = new HashingTF(10000)

    // 创建LabeledPoint数据集分别存放阳性（垃圾邮件）和阴性（正常邮件）的例子
    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))

    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))
    val trainingData = positiveExamples.union(negativeExamples)
    trainingData.cache() // 因为逻辑回归是迭代算法，所以缓存训练数据RDD

    // 使用SGD算法运行逻辑回归
    val model = new LogisticRegressionWithSGD().run(trainingData)

    // 以阳性（垃圾邮件）和阴性（正常邮件）的例子分别进行测试
    val posTest = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
    val negTest = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))

    println("Prediction for positive example: " + model.predict(posTest))
    println("Prediction for negative example: " + model.predict(negTest))
  }
}
