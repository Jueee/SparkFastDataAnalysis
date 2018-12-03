package com.jueee.learnspark.dataanalysis.chapter11;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.Arrays;
import java.util.List;

/**
 * 逻辑回归
 */
public class J53LogisticRegression {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);
        LabeledPoint point1 = LabeledPoint.apply(1.0, Vectors.dense(-2.0,5.0, 1.0));
        LabeledPoint point2 = LabeledPoint.apply(0.0, Vectors.dense(2.0,0.0, 1.0));
        List<LabeledPoint> labeledPoints = Arrays.asList(point1, point2);
        JavaRDD<LabeledPoint> points = sc.parallelize(labeledPoints);
        LogisticRegressionWithLBFGS lr = new LogisticRegressionWithLBFGS();
        lr.setIntercept(true);
        LogisticRegressionModel model = lr.run(points.rdd());
        System.out.printf("weights: %s, intercept: %s\n", model.weights(), model.intercept());
        // weights: [-5.648693355757891,3.0491285907000156,0.0], intercept: -7.349130469531492
    }
}
