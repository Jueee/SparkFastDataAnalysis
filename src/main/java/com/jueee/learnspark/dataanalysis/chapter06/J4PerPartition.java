package com.jueee.learnspark.dataanalysis.chapter06;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class J4PerPartition {

    static Tuple2<String, S3BroadcastVariables.CallLog[]> fetchResultFromRequest(ObjectMapper mapper,
                                                                                 Tuple2<String, ContentExchange> signExchange) {
        String sign = signExchange._1();
        ContentExchange exchange = signExchange._2();
        return new Tuple2(sign, S3BroadcastVariables.readExchangeCallLog(mapper, exchange));
    }

    public static void main(String[] args){

        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> validCallSigns = J3BroadcastVariables.getValidCallSigns(sc);

        // 使用mapPartitions重用配置工作
        JavaPairRDD<String, S3BroadcastVariables.CallLog[]> contactsContactLists =
                validCallSigns.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, S3BroadcastVariables.CallLog[]>() {
                    @Override
                    public Iterator<Tuple2<String, S3BroadcastVariables.CallLog[]>> call(Iterator<String> input) throws Exception {
                        ArrayList<Tuple2<String, S3BroadcastVariables.CallLog[]>> callsignLogs = new ArrayList<>();
                        ArrayList<Tuple2<String, ContentExchange>> requests = new ArrayList<>();
                        ObjectMapper mapper = S3BroadcastVariables.createMapper();
                        HttpClient client = new HttpClient();
                        try {
                            client.start();
                            while (input.hasNext()){
                                requests.add(S3BroadcastVariables.createExchangeForSign(client,input.next()));
                            }
                            for (Tuple2<String, ContentExchange> signExchange:requests){
                                callsignLogs.add(fetchResultFromRequest(mapper, signExchange));
                            }
                        } catch (Exception e){
                            e.printStackTrace();
                        }
                        return callsignLogs.iterator();
                    }
                });
        System.out.println(StringUtils.join(contactsContactLists.collect(), ","));
    }
}
