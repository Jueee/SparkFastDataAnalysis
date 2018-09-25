package com.jueee.learnspark.dataanalysis.chapter06;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class J5Piping {

    public static void main(String[] args){

        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        String distScript = "./src/R/finddistance.R";
        sc.addFile(distScript);

        /**
        JavaRDD<String> pipeInputs = contactsContactLists.values()
                .map(new VerifyCallLogs()).flatMap(
                        new FlatMapFunction<CallLog[], String>() {
                            public Iterable<String> call(CallLog[] calls) {
                                ArrayList<String> latLons = new ArrayList<String>();
                                for (S3BroadcastVariables.CallLog call: calls) {
                                    latLons.add(call.mylat + "," + call.mylong +
                                            "," + call.contactlat + "," + call.contactlong);
                                }
                                return latLons;
                            }
                        });
        JavaRDD<String> distances = pipeInputs.pipe(SparkFiles.get(distScriptName));
        System.out.println(StringUtils.join(distances.collect(), ","));

         **/
    }
}
