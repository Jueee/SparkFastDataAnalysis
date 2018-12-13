package com.jueee.learnspark.dataanalysis.chapter11;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 决策树
 */
public class J53DecisionTrees {


    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);
        LabeledPoint point1 = LabeledPoint.apply(1.0, Vectors.dense(3.0,5.0, 1.0));
        LabeledPoint point2 = LabeledPoint.apply(0.0, Vectors.dense(2.0,0.0, 1.0));
        List<LabeledPoint> labeledPoints = Arrays.asList(point1, point2);
        JavaRDD<LabeledPoint> points = sc.parallelize(labeledPoints);

        //设置决策树参数，训练模型
        Integer numClasses = 3;
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
        String impurity = "gini";
        Integer maxDepth = 5;
        Integer maxBins = 32;
        DecisionTreeModel decisionTreeModel = DecisionTree.trainClassifier(points, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins);
        System.out.println("决策树模型："+decisionTreeModel.toDebugString());

        //未处理数据，带入模型处理
        Vector denseVec = Vectors.dense(3.5,2.0, 3.0);
        double predict = decisionTreeModel.predict(denseVec);
        System.out.printf("predict: %s\n", predict);

        /**
         * 决策树模型：DecisionTreeModel classifier of depth 1 with 3 nodes
         *   If (feature 0 <= 2.5)
         *    Predict: 0.0
         *   Else (feature 0 > 2.5)
         *    Predict: 1.0
         *
         *  predict: 1.0
         */
    }
}
