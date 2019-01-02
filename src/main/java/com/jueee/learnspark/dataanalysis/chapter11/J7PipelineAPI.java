package com.jueee.learnspark.dataanalysis.chapter11;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import com.jueee.learnspark.dataanalysis.util.FilesUtilByJava;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.File;
import java.util.Arrays;

public class J7PipelineAPI {

    public static String FILE_PATH = FilesUtilByJava.getDataPath() + File.separator +"chapter11" + File.separator;
    public static String SPAM_FILE = FILE_PATH + "spam.txt";
    public static String NORMAL_FILE = FILE_PATH + "normal.txt";


    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);


        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> documents = sqlContext.jsonFile(SPAM_FILE);

        // 配置该机器学习流水线中的三个步骤：分词、词频计数、逻辑回归；每个步骤
        // 会输出SchemaRDD的一个列，并作为下一个步骤的输入列

        // 把各邮件切分为单词
        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("words");
        HashingTF tf = new HashingTF(10000); // 将邮件中的单词映射为包含10000个特征的向量
        LogisticRegression lr = new LogisticRegression();// 默认使用"features"作为输入列
        Pipeline pipeline = new Pipeline();
    }
}
