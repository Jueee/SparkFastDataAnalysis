package com.jueee.learnspark.dataanalysis.chapter11;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.Arrays;
import java.util.List;

/**
 * 朴素贝叶斯
 * 朴素贝叶斯需要非负特征值!!!
 */
public class J53NaiveBayes {


    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);
        LabeledPoint point1 = LabeledPoint.apply(1.0, Vectors.dense(3.0,5.0, 1.0));
        LabeledPoint point2 = LabeledPoint.apply(0.0, Vectors.dense(2.0,0.0, 1.0));
        List<LabeledPoint> labeledPoints = Arrays.asList(point1, point2);
        JavaRDD<LabeledPoint> points = sc.parallelize(labeledPoints);
        NaiveBayes lr = new NaiveBayes();
        NaiveBayesModel model = lr.run(points.rdd());
        Vector denseVec = Vectors.dense(1.5,2.0, 3.0);
        System.out.printf("predict: %s, theta length: %s\n", model.predict(denseVec), model.theta().length);
        // predict: 0.0, theta length: 2
    }
}
