package com.jueee.learnspark.dataanalysis.chapter11;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.rdd.RDD;

import java.util.Arrays;
import java.util.List;

public class J56PrincipalComponentAnalysis {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);
        Vector vector1 = Vectors.dense(-2.0, 5.0, 1.0);
        Vector vector2 = Vectors.dense(2.0,0.0, 1.0);
        List<Vector> labeledPoints = Arrays.asList(vector1, vector2);
        JavaRDD<Vector> points = sc.parallelize(labeledPoints);

        RowMatrix mat = new RowMatrix(points.rdd());
        Matrix pc = mat.computePrincipalComponents(2);

        // 将点投影到低维空间中
        RDD<Vector> projected = mat.multiply(pc).rows();
        System.out.println(projected.collect());

        // 在投影出的二维数据上训练k-means模型
        KMeansModel kMeansModel = KMeans.train(projected, 3, 20);
        System.out.println("K-Means 聚类模型："+kMeansModel);

        System.out.println("kMeansModel centers:");
        for (Vector center: kMeansModel.clusterCenters()) {
            System.out.println(" " + center);
        }

        /**
         * K-Means 聚类模型：org.apache.spark.mllib.clustering.KMeansModel@89079a
         * kMeansModel centers:
         *  [-1.2493900951088484,1.0]
         *  [5.153734142324,1.0]
         */
    }
}
