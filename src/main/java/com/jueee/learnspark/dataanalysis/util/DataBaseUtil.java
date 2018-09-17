package com.jueee.learnspark.dataanalysis.util;

public class DataBaseUtil {

    final public static String SPARK_MASTER = "local";

    final public static String SPARK_APPNAME = "test";

    final public static String MYSQL_DRIVER = "com.mysql.jdbc.Driver";

    final public static String MYSQL_CONNECTION_ALL = "jdbc:mysql://localhost:3306/test?user=root&password=jue";

    final public static String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/test";

    final public static String MYSQL_CONNECTION_NAME = "root";

    final public static String MYSQL_CONNECTION_PASSWORD = "jue";

    final public static String MYSQL_TEST_SQL = "SELECT * FROM `stat` ";

    final public static String CASSANDRA_HOSTNAME = "127.0.0.1";

    final public static String CASSANDRA_USERNAME = "hzweiyongqiang";

    final public static String CASSANDRA_PASSWORD = "postgres";

    final public static String ZOOKERPER_HOSTNAME = "zookerper.host1,zookerper.host2,zookerper.host3";

    final public static String HBASE_PRODUCT = "/hbase_product";

    final public static String ES_NODES = "elasticsearch.host";

    final public static String ES_RESOURCE_READ = "test_mailindex";

    final public static String ES_RESOURCE_WRITE = "test/mail";
}
