package com.jueee.learnspark.dataanalysis.chapter10;

import com.jueee.learnspark.dataanalysis.common.ApacheAccessLog;
import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class J4SaveSequenceFile {


    class OutFormat extends SequenceFileOutputFormat<Text, LongWritable> {};

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);

        // 从SparkConf创建StreamingContext并指定10秒钟的批处理大小
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // 连接到本地机器7777端口上后，使用收到的数据创建DStream
        JavaDStream<String> logData = jssc.socketTextStream("localhost", 9999);
        logData.print();

        JavaDStream<ApacheAccessLog> accessLogsDStream = logData.map(new J32StatefulTransformations.ParseFromLogLine());

        JavaPairDStream<String, Long> ipAddressRequestCount = J32StatefulTransformations.example2(accessLogsDStream);
        ipAddressRequestCount.print();

        // 在 Java 中将 DStream 保存为 SequenceFile
        JavaPairDStream<Text, LongWritable> writableDStream = ipAddressRequestCount.mapToPair(new PairFunction<Tuple2<String, Long>, Text, LongWritable>() {
            @Override
            public Tuple2<Text, LongWritable> call(Tuple2<String, Long> e) throws Exception {
                return new Tuple2(new Text(e._1()), new LongWritable(e._2()));
            }
        });
        writableDStream.saveAsHadoopFiles("outputDir", "txt", Text.class, LongWritable.class, OutFormat.class);
    }
}
