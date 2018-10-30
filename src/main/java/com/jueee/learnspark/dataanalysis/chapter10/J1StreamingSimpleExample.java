package com.jueee.learnspark.dataanalysis.chapter10;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class J1StreamingSimpleExample {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);

        // 从SparkConf创建StreamingContext并指定10秒钟的批处理大小
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // 连接到本地机器7777端口上后，使用收到的数据创建DStream
        JavaDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        lines.print();

        // 从DStream中筛选出包含字符串"error"的行
        JavaDStream<String> errorLines = lines.filter(line -> line.contains("error"));
        // 打印出有"error"的行
        errorLines.print();

        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                stringJavaRDD.collect().forEach(System.out::println);
            }
        });


        try {
            // 启动流计算环境StreamingContext并等待它"完成"
            jssc.start();
            // 等待作业完成
            jssc.awaitTermination();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
