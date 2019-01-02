package com.jueee.learnspark.dataanalysis.chapter11

import java.io.File

import com.jueee.learnspark.dataanalysis.chapter09.J2SparkSqlApp
import com.jueee.learnspark.dataanalysis.util.{DataBaseUtil, FilesUtilByJava}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object S7PipelineAPI {

  var FILE_PATH = FilesUtilByJava.getDataPath + File.separator + "chapter11" + File.separator
  var SPAM_FILE = FILE_PATH + "spam.txt"
  var NORMAL_FILE = FILE_PATH + "normal.txt"


  // 用来表示文档的类，会被转入SchemaRDD中
  case class LabeledDocument(id: Long, text: String, label: Double)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)

    // （读取LabeledDocument的RDD）
    val sqlContext = new SQLContext(sc)
    val documents = sqlContext.jsonFile(SPAM_FILE)

    // 配置该机器学习流水线中的三个步骤：分词、词频计数、逻辑回归；每个步骤
    // 会输出SchemaRDD的一个列，并作为下一个步骤的输入列
    val tokenizer = new Tokenizer() // 把各邮件切分为单词
      .setInputCol("text")
      .setOutputCol("words")
    val tf = new HashingTF() // 将邮件中的单词映射为包含10000个特征的向量
      .setNumFeatures(10000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression() // 默认使用"features"作为输入列
    val pipeline = new Pipeline().setStages(Array(tokenizer, tf, lr))

    // 使用流水线对训练文档进行拟合
    val model = pipeline.fit(documents)

    // 或者，不使用上面的参数只对训练集进行一次拟合，也可以
    // 通过交叉验证对一批参数进行网格搜索，来找到最佳的模型
    val paramMaps = new ParamGridBuilder()
      .addGrid(tf.numFeatures, Array(10000, 20000))
      .addGrid(lr.maxIter, Array(100, 200))
      .build() // 构建参数的所有组合
    val eval = new BinaryClassificationEvaluator()
    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEstimatorParamMaps(paramMaps)
      .setEvaluator(eval)
    val bestModel = cv.fit(documents)
  }

}
