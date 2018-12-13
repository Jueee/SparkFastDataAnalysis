package com.jueee.learnspark.dataanalysis.chapter11;


import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 随机森林
 */
public class J53RandomForests {


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
        Integer numTrees = 3;
        String featureSubsetStrategy = "auto";
        String impurity = "gini";
        Integer maxDepth = 5;
        Integer maxBins = 32;
        Integer seed = 5;
        // def trainClassifier(input : org.apache.spark.api.java.JavaRDD[org.apache.spark.mllib.regression.LabeledPoint], numClasses : scala.Int, categoricalFeaturesInfo : java.util.Map[java.lang.Integer, java.lang.Integer], numTrees : scala.Int, featureSubsetStrategy : scala.Predef.String, impurity : scala.Predef.String, maxDepth : scala.Int, maxBins : scala.Int, seed : scala.Int) : org.apache.spark.mllib.tree.model.RandomForestModel = { /* compiled code */ }
        RandomForestModel randomForestModel = RandomForest.trainClassifier(points, numClasses, categoricalFeaturesInfo,numTrees,featureSubsetStrategy, impurity, maxDepth, maxBins, seed);
        System.out.println("随机森林模型："+randomForestModel.toDebugString());

        //未处理数据，带入模型处理
        Vector denseVec = Vectors.dense(3.5,2.0, 3.0);
        double predict = randomForestModel.predict(denseVec);
        System.out.printf("predict: %s\n", predict);

        /**
         * 随机森林模型：TreeEnsembleModel classifier with 3 trees
         *
         *   Tree 0:
         *     Predict: 1.0
         *   Tree 1:
         *     If (feature 1 <= 2.5)
         *      Predict: 0.0
         *     Else (feature 1 > 2.5)
         *      Predict: 1.0
         *   Tree 2:
         *     Predict: 0.0
         *
         * predict: 0.0
         */
    }
}
