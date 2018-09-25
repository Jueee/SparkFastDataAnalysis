package com.jueee.learnspark.dataanalysis.chapter06;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;

import java.util.Arrays;

public class J6NumericRDD {


    public static void main(String[] args){

        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("123","231","312"));

        JavaDoubleRDD distanceDoubles = rdd.mapToDouble(
                new DoubleFunction<String>() {
                    @Override
                    public double call(String s) throws Exception {
                        return Double.valueOf(s);
                    }
                }
        );

        final StatCounter stats = distanceDoubles.stats();

        // 标准差
        final Double stddev = stats.stdev();
        System.out.println(stddev);
        // 元素的平均值
        final Double mean = stats.mean();
        System.out.println(mean);

        JavaDoubleRDD reasonableDistance = distanceDoubles.filter(
                new Function<Double, Boolean>() {
                    @Override
                    public Boolean call(Double x) throws Exception {
                        return (Math.abs(x-mean) < 3 * stddev);
                    }
                }
        );
        System.out.println(StringUtils.join(reasonableDistance.collect(), ","));
    }
}
