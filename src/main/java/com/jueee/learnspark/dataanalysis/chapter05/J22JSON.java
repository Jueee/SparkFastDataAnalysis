package com.jueee.learnspark.dataanalysis.chapter05;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jueee.learnspark.dataanalysis.util.FilesUtilByJava;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

public class J22JSON {

    @JsonIgnoreProperties(ignoreUnknown = true)     // 忽略类中不存在的字段
    public static class Person implements java.io.Serializable {
        public String date;
        public String city;
        public String message;
        public int status;
        public int count;
    }

    public static class ParseJson implements FlatMapFunction<Iterator<String>, Person>{
        @Override
        public Iterator<Person> call(Iterator<String> lines) throws Exception {
            ArrayList<Person> people = new ArrayList<Person>();
            ObjectMapper mapper = new ObjectMapper();
            while (lines.hasNext()){
                String line = lines.next();
                try {
                    people.add(mapper.readValue(line, Person.class));
                } catch (Exception e){
                    // 跳过失败的数据
                }
            }
            return people.iterator();
        }
    }

    public void readTest1(JavaRDD<String> input){
        JavaRDD<Person> result = input.mapPartitions(new ParseJson());
        result.collect().forEach(person -> {
            System.out.println(person + "-" + person.city + "-" + person.count + "-" + person.date + "-" + person.message + "-" + person.status);
        });
    }

    public static void main(String[] args){
        J22JSON test = new J22JSON();

        String inputFile = FilesUtilByJava.getDataPath() + File.separator + "chapter05" + File.separator + "FileFormats" + File.separator + "json.txt";
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordcount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile(inputFile);
        test.readTest1(input);
    }
}
