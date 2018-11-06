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

public class J31StatelessTransformations {

    // 假设ApacheAccessingLog是用来从Apache日志中解析条目的工具类
    static final class IpTuple implements PairFunction<ApacheAccessLog, String, Long> {
        public Tuple2<String, Long> call(ApacheAccessLog log) {
            return new Tuple2<>(log.getIpAddress(), 1L);
        }
    }

    public static final class ParseFromLogLine implements Function<String, ApacheAccessLog> {
        @Override
        public ApacheAccessLog call(String line) {
            return ApacheAccessLog.parseFromLogLine(line);
        }
    }


    public static final class LongSumReducer implements Function2<Long, Long, Long> {
        @Override
        public Long call(Long a, Long b) {
            return  a + b;
        }
    };


    public static final class IpContentTuple implements PairFunction<ApacheAccessLog, String, Long> {
        @Override
        public Tuple2<String, Long> call(ApacheAccessLog log) {
            return new Tuple2<>(log.getIpAddress(), log.getContentSize());
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
        JavaPairDStream<String, Long> ip = accessLogsDStream.mapToPair(new IpTuple());
        JavaPairDStream<String, Long> ipCountsDStream = ip.reduceByKey(new LongSumReducer());
        ipCountsDStream.print();

        JavaPairDStream<String, Long> ipBytesDStream = accessLogsDStream.mapToPair(new IpContentTuple());
        JavaPairDStream<String, Long> ipBytesSumDStream = ipBytesDStream.reduceByKey(new LongSumReducer());
        ipCountsDStream.join(ipBytesSumDStream).print();

    }
}
