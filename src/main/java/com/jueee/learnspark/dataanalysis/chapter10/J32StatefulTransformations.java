package com.jueee.learnspark.dataanalysis.chapter10;

import com.jueee.learnspark.dataanalysis.common.ApacheAccessLog;
import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class J32StatefulTransformations {

    public static final class ParseFromLogLine implements Function<String, ApacheAccessLog> {
        @Override
        public ApacheAccessLog call(String line) {
            return ApacheAccessLog.parseFromLogLine(line);
        }
    }

    static class ExtractIp implements PairFunction<ApacheAccessLog, String, Long> {
        @Override
        public Tuple2<String, Long> call(ApacheAccessLog entry) {
            return new Tuple2(entry.getIpAddress(), 1L);
        }
    }
    static class AddLongs implements Function2<Long, Long, Long> {
        @Override
        public Long call(Long v1, Long v2) throws Exception {
            return v1 + v2;
        }
    }
    static class SubtractLongs implements Function2<Long, Long, Long> {
        @Override
        public Long call(Long v1, Long v2) {
            return v1 - v2;
        }
    }

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);

        // 从SparkConf创建StreamingContext并指定10秒钟的批处理大小
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // 连接到本地机器7777端口上后，使用收到的数据创建DStream
        JavaDStream<String> logData = jssc.socketTextStream("localhost", 9999);
        logData.print();

        JavaDStream<ApacheAccessLog> accessLogsDStream = logData.map(new ParseFromLogLine());



    }

    /**
     * 每个 IP 地址的访问量计数
     * @param accessLogsDStream
     */
    public static void example1(JavaDStream<ApacheAccessLog> accessLogsDStream){
        JavaPairDStream<String, Long> ipAddressPairDStream = accessLogsDStream.mapToPair(new ExtractIp());
        JavaPairDStream<String, Long> ipCountDStream = ipAddressPairDStream.
                reduceByKeyAndWindow(
                        new AddLongs(), // 加上新进入窗口的批次中的元素
                        new SubtractLongs(), // 移除离开窗口的老批次中的元素
                        Durations.seconds(30), // 窗口时长
                        Durations.seconds(10)); // 滑动步长
        ipCountDStream.print();
    }

    /**
     * 窗口计数操作
     * @param accessLogsDStream
     */
    public static void example2(JavaDStream<ApacheAccessLog> accessLogsDStream){
        JavaDStream<String> ip = accessLogsDStream.map(
                new Function<ApacheAccessLog, String>() {
                    public String call(ApacheAccessLog entry) {
                        return entry.getIpAddress();
                    }});

        JavaDStream<Long> requestCount = accessLogsDStream.countByWindow(
                Durations.seconds(30), Durations.seconds(10));
        requestCount.print();

        JavaPairDStream<String, Long> ipAddressRequestCount = ip.countByValueAndWindow(
                Durations.seconds(30), Durations.seconds(10));
        ipAddressRequestCount.print();
    }
}
