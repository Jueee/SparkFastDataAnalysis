package com.jueee.learnspark.dataanalysis.chapter06;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class J3BroadcastVariables {

    static String[] loadCallSignTable() throws FileNotFoundException {
        Scanner callSignTbl = new Scanner(new File("./files/callsign_tbl_sorted"));
        ArrayList<String> callSignList = new ArrayList<String>();
        while (callSignTbl.hasNextLine()) {
            callSignList.add(callSignTbl.nextLine());
        }
        return callSignList.toArray(new String[0]);
    }

    static String lookupCountry(String callSign, String[] table) {
        Integer pos = java.util.Arrays.binarySearch(table, callSign);
        if (pos < 0) {
            pos = -pos-1;
        }
        return table[pos].split(",")[1];
    }

    public static class SumInts implements Function2<Integer, Integer, Integer> {
        public Integer call(Integer x, Integer y) {
            return x + y;
        }
    }

    public static JavaRDD<String> getValidCallSigns(JavaSparkContext sc){
        JavaRDD<String> rdd = sc.textFile("README.md");
        final Accumulator<Integer> blankLines = sc.accumulator(0);
        JavaRDD<String> callSigns = rdd.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String line) throws Exception {
                        if (line.equals("")) {
                            blankLines.add(1);
                        }
                        return Arrays.asList(line.split(" ")).iterator();
                    }
                }
        );
        final Accumulator<Integer> validSignCount = sc.accumulator(0);
        final Accumulator<Integer> invalidSignCount = sc.accumulator(0);
        JavaRDD<String> validCallSigns = callSigns.filter(
                new Function<String, Boolean>(){ public Boolean call(String callSign) {
                    Pattern p = Pattern.compile("\\A\\d?\\p{Alpha}{1,2}\\d{1,4}\\p{Alpha}{1,3}\\Z");
                    Matcher m = p.matcher(callSign);
                    boolean b = m.matches();
                    if (b) {
                        validSignCount.add(1);
                    } else {
                        invalidSignCount.add(1);
                    }
                    return b;
                }
                });
        return validCallSigns;
    }

    public static void main(String[] args){

        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> validCallSigns = getValidCallSigns(sc);

        JavaPairRDD<String, Integer> contactCounts = validCallSigns.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String callSign) {
                        return new Tuple2(callSign, 1);
                    }}).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
                return x + y;
            }});
        try {
            final Broadcast<String[]> signPrefixes = sc.broadcast(loadCallSignTable());
            JavaPairRDD<String,Integer> countryContactCounts = contactCounts.mapToPair(
                    new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                        @Override
                        public Tuple2<String, Integer> call(Tuple2<String, Integer> callSignCount) throws Exception {
                            String sign = callSignCount._1();
                            String country = lookupCountry(sign, signPrefixes.value());
                            return new Tuple2(country, callSignCount._2());
                        }
                    }
            ).reduceByKey(new SumInts());

            System.out.println(countryContactCounts.count());
        }catch (Exception e){
            e.printStackTrace();
        }


    }
}
