package com.jueee.learnspark.dataanalysis.chapter11;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import com.jueee.learnspark.dataanalysis.util.FilesUtilByJava;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.File;
import java.util.Arrays;

public class J3SpamClassification {

    public static String FILE_PATH = FilesUtilByJava.getDataPath() + File.separator +"chapter11" + File.separator;

    public static String SPAM_FILE = FILE_PATH + "spam.txt";

    public static String NORMAL_FILE = FILE_PATH + "normal.txt";

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> spam = sc.textFile(SPAM_FILE);
        JavaRDD<String> normal = sc.textFile(NORMAL_FILE);

        // 创建一个HashingTF实例来把邮件文本映射为包含10000个特征的向量
        final HashingTF tf = new HashingTF(10000);

        // 创建LabeledPoint数据集分别存放阳性（垃圾邮件）和阴性（正常邮件）的例子
        JavaRDD<LabeledPoint> posExamples = spam.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String email) throws Exception {
                return new LabeledPoint(1, tf.transform(Arrays.asList(email.split(" "))));
            }
        });
        JavaRDD<LabeledPoint> negExamples = normal.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String email) throws Exception {
                return new LabeledPoint(0, tf.transform(Arrays.asList(email.split(" "))));
            }
        });

        JavaRDD<LabeledPoint> trainData = posExamples.union(negExamples);
        trainData.cache();  // 因为逻辑回归是迭代算法，所以缓存训练数据RDD

        // 使用SGD算法运行逻辑回归
        LogisticRegressionModel model = new LogisticRegressionWithSGD().run(trainData.rdd());

        // 以阳性（垃圾邮件）和阴性（正常邮件）的例子分别进行测试
        Vector posTest = tf.transform(Arrays.asList("O M G GET cheap stuff by sending money to ...".split(" ")));
        Vector negTest = tf.transform(Arrays.asList("Hi Dad, I started studying Spark the other ...".split(" ")));
        Vector negTest2 = tf.transform(Arrays.asList("Our Studio. 343 Industries is home to the Halo Universe, and to a team of talented people dedicated to creating incredible science-fiction experiences.".split(" ")));

        System.out.println("Prediction for positive example: " + model.predict(posTest));
        System.out.println("Prediction for negative example: " + model.predict(negTest));
        System.out.println("Prediction for negative example: " + model.predict(negTest2));
    }
}
