package com.jueee.learnspark.dataanalysis.chapter05;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import com.jueee.learnspark.dataanalysis.util.StringsUtilByJava;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.JdbcRDD;
import scala.reflect.ClassManifestFactory$;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class J51MySQL {


    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(J51MySQL.class);

    static class DbConnection extends AbstractFunction0<Connection> implements Serializable {

        private String driverClassName;
        private String connectionUrl;
        private String userName;
        private String password;

        public DbConnection(String driverClassName, String connectionUrl, String userName, String password) {
            this.driverClassName = driverClassName;
            this.connectionUrl = connectionUrl;
            this.userName = userName;
            this.password = password;
        }

        @Override
        public Connection apply() {
            try {
                Class.forName(driverClassName);
            } catch (ClassNotFoundException e) {
                LOGGER.error("Failed to load driver class", e);
            }

            Properties properties = new Properties();
            properties.setProperty("user", userName);
            properties.setProperty("password", password);

            Connection connection = null;
            try {
                connection = DriverManager.getConnection(connectionUrl, properties);
            } catch (SQLException e) {
                LOGGER.error("Connection failed", e);
            }

            return connection;
        }
    }

    static class MapResult extends AbstractFunction1<ResultSet, Object[]> implements Serializable {
        @Override
        public Object[] apply(ResultSet row) {
            return JdbcRDD.resultSetToObjectArray(row);
        }
    }

    public static void selectMySQLbyJava7(JavaSparkContext sc,DbConnection dbConnection){
        StringsUtilByJava.printFinish();
        JdbcRDD<Object[]> jdbcRDD = new JdbcRDD(sc.sc(), dbConnection, DataBaseUtil.MYSQL_TEST_SQL + "where id >= ? and id <= ?",
                1, 40, 2, new MapResult(), ClassManifestFactory$.MODULE$.fromClass(Object[].class));
        JavaRDD<Object[]> javaRDD = JavaRDD.fromRDD(jdbcRDD, null);
        List<String> infoList = javaRDD.map(new Function<Object[], String>() {
            @Override
            public String call(Object[] record) {
                return record[0] + "\t" + record[1] + "\t" + record[2] + "\t" + record[3];
            }
        }).collect();
        for (String info: infoList){
            System.out.println(info);
        }
    }

    public static void selectMySQLbyJava8(JavaSparkContext sc,DbConnection dbConnection){
        StringsUtilByJava.printFinish();
        JdbcRDD<Object[]> jdbcRDD = new JdbcRDD(sc.sc(), dbConnection, DataBaseUtil.MYSQL_TEST_SQL + "where id >= ? and id <= ?",
                1, 40, 2, new MapResult(), ClassManifestFactory$.MODULE$.fromClass(Object[].class));
        JavaRDD<Object[]> javaRDD = JavaRDD.fromRDD(jdbcRDD, null);
        List<String> infoList = javaRDD.map(record -> record[0] + "\t" + record[1] + "\t" + record[2] + "\t" + record[3]).collect();
        infoList.forEach(System.out::println);
    }

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        DbConnection dbConnection = new DbConnection(DataBaseUtil.MYSQL_DRIVER, DataBaseUtil.MYSQL_CONNECTION_URL, DataBaseUtil.MYSQL_CONNECTION_NAME, DataBaseUtil.MYSQL_CONNECTION_PASSWORD);

        selectMySQLbyJava7(sc, dbConnection);
        selectMySQLbyJava8(sc, dbConnection);

    }
}
