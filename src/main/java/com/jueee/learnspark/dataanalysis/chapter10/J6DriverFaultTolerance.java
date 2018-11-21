package com.jueee.learnspark.dataanalysis.chapter10;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;

public class J6DriverFaultTolerance {



    public static String checkpointDir = "";

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
//        JavaStreamingContextFactory fact = new JavaStreamingContextFactory() {
//            public JavaStreamingContext call() {
//                JavaSparkContext sc = new JavaSparkContext(conf);
//                // 以1秒作为批次大小创建StreamingContext
//                JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(1));
//                jssc.checkpoint(checkpointDir);
//                return jssc;
//            }};
//        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(checkpointDir, fact);

    }
}
