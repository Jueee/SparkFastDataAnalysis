package com.jueee.learnspark.dataanalysis.chapter11;

import com.jueee.learnspark.dataanalysis.util.DataBaseUtil;
import com.jueee.learnspark.dataanalysis.util.FilesUtilByJava;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * 协同过滤与推荐
 *
 * 报错：18/12/18 19:26:32 ERROR Executor: Exception in task 0.0 in stage 25.0 (TID 21)
 * java.lang.StackOverflowError
 * 	at java.io.ObjectInputStream$BlockDataInputStream.readByte(ObjectInputStream.java:2792)
 *
 * 	解决：减少迭代次数，numIterations 从 20 减少为 5.
 *
 */
public class J55Recommendation implements Serializable {


    private static final Pattern TAB = Pattern.compile("\t");

    public MatrixFactorizationModel buildModel(JavaRDD<Rating> rdd) { //训练模型
        int rank = 10;
        int numIterations = 5;
        MatrixFactorizationModel model = ALS.train(rdd.rdd(), rank, numIterations, 0.01);
        return model;
    }

    public JavaRDD<Rating>[] splitData() { //分割数据，一部分用于训练，一部分用于测试
        SparkConf sparkConf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile(FilesUtilByJava.getDataPath() + File.separator + "chapter11" + File.separator + "u.data");
        JavaRDD<Rating> ratings = lines.map(line -> {
            String[] tok = TAB.split(line);
            int x = Integer.parseInt(tok[0]);
            int y = Integer.parseInt(tok[1]);
            double rating = Double.parseDouble(tok[2]);
            return new Rating(x, y, rating);
        });
        JavaRDD<Rating>[] splits = ratings.randomSplit(new double[]{0.6, 0.4}, 11L);
        return splits;
    }

    public static void main(String[] args) {
        J55Recommendation cf = new J55Recommendation();
        JavaRDD<Rating>[] splits = cf.splitData();
        MatrixFactorizationModel model = cf.buildModel(splits[0]);
        System.out.println("model:"+model.formatVersion());
        Double MSE = cf.getMSE(splits[0], model);
        System.out.println("Mean Squared Error = " + MSE); //训练数据的MSE
        Double MSE1 = cf.getMSE(splits[1], model);
        System.out.println("Mean Squared Error1 = " + MSE1); //测试数据的MSE
    }

    public Double getMSE(JavaRDD<Rating> ratings, MatrixFactorizationModel model) { //计算MSE
        JavaPairRDD usersProducts = ratings.mapToPair(rating -> new Tuple2<>(rating.user(), rating.product()));
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = model.predict(usersProducts.rdd())
                .toJavaRDD()
                .mapToPair(new PairFunction<Rating, Tuple2<Integer, Integer>, Double>() {
                    @Override
                    public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating rating) throws Exception {
                        return new Tuple2<>(new Tuple2<>(rating.user(), rating.product()), rating.rating());
                    }
                });

        JavaPairRDD<Tuple2<Integer, Integer>, Double> ratesAndPreds = ratings
                .mapToPair(new PairFunction<Rating, Tuple2<Integer, Integer>, Double>() {
                    @Override
                    public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating rating) throws Exception {
                        return new Tuple2<>(new Tuple2<>(rating.user(), rating.product()), rating.rating());
                    }
                });
        JavaPairRDD joins = ratesAndPreds.join(predictions);

        return joins.mapToDouble(new DoubleFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>() {
            @Override
            public double call(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o) throws Exception {
                double err = o._2()._1() - o._2()._2();
                return err * err;
            }
        }).mean();
    }

    /**
     * 运行结果：
     * model:1.0
     * Mean Squared Error = 0.41770412286361863
     * Mean Squared Error1 = 1.2770432331078498
     */
}
