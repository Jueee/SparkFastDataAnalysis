package com.jueee.learnspark.dataanalysis.chapter05;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Available columns are: [key, bootstrapped, broadcast_address, cluster_name, cql_version, data_center, gossip_generation, host_id, listen_address, native_protocol_version, partitioner, rack, release_version, rpc_address, schema_version, thrift_version, tokens, truncated_at]
 */
public class J52Cassandra {

    public static void readTableData(JavaSparkContext sc){
        CassandraTableScanJavaRDD<CassandraRow> data =  CassandraJavaUtil.javaFunctions(sc).cassandraTable("system", "local");

        JavaRDD<String> host_id = data.map(row -> row.getString("host_id"));
        host_id.collect().forEach(System.out::println);

        data.collect().forEach(row -> {
            System.out.println(row.fieldNames());
            System.out.println(row.columnValues());
        });

    }

    public static void main(String[] args){
        SparkConf conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", DataBaseUtil.CASSANDRA_HOSTNAME)
                .set("spark.cassandra.auth.username", DataBaseUtil.CASSANDRA_USERNAME)
                .set("spark.cassandra.auth.password", DataBaseUtil.CASSANDRA_PASSWORD);
        JavaSparkContext sc = new JavaSparkContext(DataBaseUtil.SPARK_MASTER, DataBaseUtil.SPARK_APPNAME, conf);
        System.out.println(sc);

        readTableData(sc);
    }
}
