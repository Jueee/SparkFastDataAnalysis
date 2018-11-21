package com.jueee.learnspark.dataanalysis.chapter10;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

public class J5ApacheFlume {

    public static String receiverHostname = "";

    public static Integer receiverPort = 0;

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        // 从SparkConf创建StreamingContext并指定10秒钟的批处理大小
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaDStream<SparkFlumeEvent> events = FlumeUtils.createStream(jssc, receiverHostname, receiverPort);
        events.print();
    }
}
