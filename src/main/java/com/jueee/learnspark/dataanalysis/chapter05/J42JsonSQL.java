package com.jueee.learnspark.dataanalysis.chapter05;

import com.jueee.learnspark.dataanalysis.util.FilesUtilByJava;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import java.io.File;

public class J42JsonSQL {

    static String inputFile = FilesUtilByJava.getDataPath() + File.separator + "chapter05" + File.separator + "FileFormats" + File.separator + "json.txt";

    /**
     * 在 Java 中使用 Spark SQL 读取 JSON 数据
     * @param args
     */
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);

        HiveContext hiveContext = new HiveContext(sc);
        Dataset<Row> rowDataset = hiveContext.jsonFile(inputFile);
        rowDataset.registerTempTable("test");
        Dataset<Row> rows = hiveContext.sql("select date,city,data.ganmao from test");
        Row firstRow = rows.first();
        System.out.println(firstRow.getString(0));
        rows.show();
    }
}
