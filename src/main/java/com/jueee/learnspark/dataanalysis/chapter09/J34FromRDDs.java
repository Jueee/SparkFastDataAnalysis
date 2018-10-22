package com.jueee.learnspark.dataanalysis.chapter09;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import java.util.ArrayList;

public class J34FromRDDs {



    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext hiveCtx = new HiveContext(sc);
        System.out.println(hiveCtx);

        ArrayList<HappyPerson> peopleList = new ArrayList<HappyPerson>();
        peopleList.add(new HappyPerson("holden1", "coffee1"));
        peopleList.add(new HappyPerson("holden2", "coffee2"));
        JavaRDD<HappyPerson> happyPeopleRDD = sc.parallelize(peopleList);
        Dataset<Row> happyPeopleSchemaRDD = hiveCtx.applySchema(happyPeopleRDD,
                HappyPerson.class);
        happyPeopleSchemaRDD.registerTempTable("happy_people");
        happyPeopleSchemaRDD.show();
    }
}
