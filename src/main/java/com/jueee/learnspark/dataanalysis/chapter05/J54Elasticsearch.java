package com.jueee.learnspark.dataanalysis.chapter05;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import com.jueee.learnspark.dataanalysis.util.FilesUtilByJava;
import com.jueee.learnspark.dataanalysis.util.StringsUtilByJava;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import scala.Tuple2;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class J54Elasticsearch {

    static String filePath = FilesUtilByJava.getDataPath() + File.separator + "chapter05" + File.separator + "FileFormats";
    static String fileName = filePath + File.separator + "save_by_scala.sequence" + File.separator + "part-00000";

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        saveElasticsearch(sc);
        readElasticsearch(sc);
    }

    /**
     * 查询Elasticsearch
     * @param sc
     */
    public static void readElasticsearch(JavaSparkContext sc){
        StringsUtilByJava.printFinish();
        JobConf jobConf = new JobConf(sc.hadoopConfiguration());
        jobConf.set(ConfigurationOptions.ES_RESOURCE_READ, DataBaseUtil.ES_RESOURCE_READ);
        jobConf.set(ConfigurationOptions.ES_NODES, DataBaseUtil.ES_NODES);
        JavaPairRDD<Object, MapWritable> currentTweets = sc.hadoopRDD(jobConf, EsInputFormat.class, Object.class, MapWritable.class);
        JavaRDD<Map<String, String>> map = currentTweets.map(new Function<Tuple2<Object, MapWritable>, Map<String, String>>() {
            @Override
            public Map<String, String> call(Tuple2<Object, MapWritable> value) {
                Map<String, String> map = new HashMap<String, String>();
                MapWritable mapWritable = value._2();
                for (Writable writable : mapWritable.keySet()) {
                    map.put(writable.toString(), mapWritable.get(writable).toString());
                }
                return map;
            }
        });
        System.out.println(map.count());
        map.collect().forEach(System.out::println);
    }

    public static void saveElasticsearch(JavaSparkContext sc){
        StringsUtilByJava.printFinish();
        JavaPairRDD<Text, IntWritable> pairs = sc.sequenceFile(fileName, Text.class, IntWritable.class);

        JobConf jobConf = new JobConf(sc.hadoopConfiguration());
        jobConf.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat");
        jobConf.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat");
        jobConf.setOutputCommitter(FileOutputCommitter.class);
        jobConf.set(ConfigurationOptions.ES_RESOURCE_WRITE, DataBaseUtil.ES_RESOURCE_WRITE);
        jobConf.set(ConfigurationOptions.ES_NODES, DataBaseUtil.ES_NODES);
        jobConf.set(ConfigurationOptions.ES_BATCH_SIZE_ENTRIES, "1");
        FileOutputFormat.setOutputPath(jobConf, new Path("-"));

        pairs.saveAsHadoopDataset(jobConf);
    }
}
