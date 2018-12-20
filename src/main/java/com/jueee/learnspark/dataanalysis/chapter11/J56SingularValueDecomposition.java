package com.jueee.learnspark.dataanalysis.chapter11;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import java.util.Arrays;
import java.util.List;

/**
 * 奇异值分解
 */
public class J56SingularValueDecomposition {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);
        Vector vector1 = Vectors.dense(-2.0, 5.0, 1.0);
        Vector vector2 = Vectors.dense(2.0,0.0, 1.0);
        List<Vector> labeledPoints = Arrays.asList(vector1, vector2);
        JavaRDD<Vector> points = sc.parallelize(labeledPoints);

        RowMatrix mat = new RowMatrix(points.rdd());
        SingularValueDecomposition<RowMatrix, Matrix> svd = mat.computeSVD(3, true, 1.0);

        // U是一个分布式RowMatrix
        System.out.println("---U是一个分布式RowMatrix---");
        RowMatrix u = svd.U();
        System.out.println(u);

        // 奇异值用一个局部稠密向量表示
        System.out.println("---奇异值用一个局部稠密向量表示---");
        Vector s = svd.s();
        System.out.println(s);

        // V是一个局部稠密矩阵
        System.out.println("---V是一个局部稠密矩阵---");
        Matrix v = svd.V();
        System.out.println(v);

        /**
         * ---U是一个分布式RowMatrix---
         * org.apache.spark.mllib.linalg.distributed.RowMatrix@6aa968
         * ---奇异值用一个局部稠密向量表示---
         * [5.509533567570166]
         * ---V是一个局部稠密矩阵---
         * -0.4031460160351149
         * 0.9012313115913181
         * 0.15891951661896966
         */
    }
}
