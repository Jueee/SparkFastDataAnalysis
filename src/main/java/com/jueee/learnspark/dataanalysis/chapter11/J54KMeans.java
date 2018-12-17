package com.jueee.learnspark.dataanalysis.chapter11;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.Arrays;
import java.util.List;

/**
 * 聚类   -  K-Means 算法
 */
public class J54KMeans {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);
        Vector vector1 = Vectors.dense(-2.0, 5.0, 1.0);
        Vector vector2 = Vectors.dense(2.0,0.0, 1.0);
        List<Vector> labeledPoints = Arrays.asList(vector1, vector2);
        JavaRDD<Vector> points = sc.parallelize(labeledPoints);
        int numClusters = 3;
        int numIterations = 20;
        KMeansModel kMeansModel = KMeans.train(points.rdd(), numClusters, numIterations);
        System.out.println("K-Means 聚类模型："+kMeansModel);

        System.out.println("kMeansModel centers:");
        for (Vector center: kMeansModel.clusterCenters()) {
            System.out.println(" " + center);
        }
        double cost = kMeansModel.computeCost(points.rdd());
        System.out.println("Cost: " + cost);

        //未处理数据，带入模型处理
        Vector denseVec = Vectors.dense(3.5,2.0, 3.0);
        double predict = kMeansModel.predict(denseVec);
        System.out.printf("predict: %s\n", predict);

        /**
         * K-Means 聚类模型：org.apache.spark.mllib.clustering.KMeansModel@d96250
         * kMeansModel centers:
         *  [-2.0,5.0,1.0]
         *  [2.0,0.0,1.0]
         * Cost: 0.0
         * predict: 1.0
         */
    }
}
