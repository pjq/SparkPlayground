package me.pjq;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;

/**
 * Created by pengjianqing on 4/10/15.
 */
public class WordCount implements Serializable {
    public void count(String filePath) {
        SparkConf sparkConf = new SparkConf().setAppName("WordCount").setMaster("local");

        JavaSparkContext spark = new JavaSparkContext(sparkConf);
        JavaRDD<String> file = spark.textFile(filePath).cache();
        JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        JavaRDD<String> wordsFilter =  words.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return !s.equalsIgnoreCase("");
            }
        });

        JavaPairRDD<String, Integer> pairs = wordsFilter.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaRDD<Tuple2<Integer, String>> countsSwap = counts.map(new Function<Tuple2<String, Integer>, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        });

        JavaPairRDD<Integer, String> countSwap2 = JavaPairRDD.fromJavaRDD(countsSwap).sortByKey(false);

        String saved = filePath + "_sparkresult"+new Date();
        File saveFile = new File(saved);
        if (saveFile.exists()){
            saveFile.delete();
        }


        countSwap2.saveAsTextFile(saved);

        spark.stop();
        spark.close();
    }

}
