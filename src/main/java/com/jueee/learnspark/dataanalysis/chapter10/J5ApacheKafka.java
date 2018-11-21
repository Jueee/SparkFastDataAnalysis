package com.jueee.learnspark.dataanalysis.chapter10;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.Map;

public class J5ApacheKafka {

    public static String zkQuorum = "";

    public static String group = "";

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        // 从SparkConf创建StreamingContext并指定10秒钟的批处理大小
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        Map<String, Integer> topics = new HashMap<String, Integer>();
        topics.put("pandas", 1);
        topics.put("logs", 1);
        JavaPairDStream<String, String> input = KafkaUtils.createStream(jssc, zkQuorum, group, topics);
        input.print();
    }
}
