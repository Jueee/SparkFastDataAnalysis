package com.jueee.learnspark.dataanalysis.chapter11;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * 用 Java 创建向量
 */
public class J4CreateVectors {

    public static void main(String[] args){
        Vector denseVec1 = Vectors.dense(1.0,2.0, 3.0);
        Vector denseVec2 = Vectors.dense(new double[]{1.0, 2.0, 3.0});

        Vector sparseVec1 = Vectors.sparse(5, new int[]{0,2}, new double[]{1.0, 2.0});

        System.out.println(denseVec1.size());
        System.out.println(denseVec2.size());
        System.out.println(sparseVec1.size());
    }
}
