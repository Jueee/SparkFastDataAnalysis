package com.jueee.learnspark.dataanalysis.chapter11;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.Arrays;
import java.util.List;

/**
 * 支持向量机
 */
public class J53SupportVectorMachines {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);
        LabeledPoint point1 = LabeledPoint.apply(1.0, Vectors.dense(-2.0,5.0, 1.0));
        LabeledPoint point2 = LabeledPoint.apply(0.0, Vectors.dense(2.0,0.0, 1.0));
        List<LabeledPoint> labeledPoints = Arrays.asList(point1, point2);
        JavaRDD<LabeledPoint> points = sc.parallelize(labeledPoints);
        SVMWithSGD lr = new SVMWithSGD();
        lr.setIntercept(true);
        SVMModel model = lr.run(points.rdd());
        System.out.printf("weights: %s, intercept: %s\n", model.weights(), model.intercept());
        // weights: [-0.8385291954371556,0.0,-0.4192645977185778], intercept: 0.4108793057642061
    }
}
