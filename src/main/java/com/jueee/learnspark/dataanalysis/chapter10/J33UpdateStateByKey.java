package com.jueee.learnspark.dataanalysis.chapter10;

import com.jueee.learnspark.dataanalysis.common.ApacheAccessLog;
import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

public class J33UpdateStateByKey {

    static class UpdateRunningSum implements Function2<List<Long>, Optional<Long>, Optional<Long>> {
        public Optional<Long> call(List<Long> nums, Optional<Long> current) {
            long sum = current.orElse(0L);
            return Optional.of(sum + nums.size());
        }
    };

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);

        // 从SparkConf创建StreamingContext并指定10秒钟的批处理大小
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // 连接到本地机器7777端口上后，使用收到的数据创建DStream
        JavaDStream<String> logData = jssc.socketTextStream("localhost", 9999);
        logData.print();

        JavaDStream<ApacheAccessLog> accessLogsDStream = logData.map(new J32StatefulTransformations.ParseFromLogLine());

        /**
         * 使用 updateStateByKey() 运行响应代码的计数
         */
        JavaPairDStream<Integer, Long> responseCodeCountDStream = accessLogsDStream.mapToPair(
                new PairFunction<ApacheAccessLog, Integer, Long>() {
                    public Tuple2<Integer, Long> call(ApacheAccessLog log) {
                        return new Tuple2(log.getResponseCode(), 1L);
                    }})
                .updateStateByKey(new UpdateRunningSum());
        responseCodeCountDStream.print();
    }
}
