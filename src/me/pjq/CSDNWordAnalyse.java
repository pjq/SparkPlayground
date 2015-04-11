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
import java.util.Date;

/**
 * Created by pengjianqing on 4/10/15.
 */
public class CSDNWordAnalyse implements Serializable {
    private int count = 0;

    public void count(String filePath, String appName) {
        SparkConf sparkConf = new SparkConf().setAppName(appName);

        JavaSparkContext spark = new JavaSparkContext(sparkConf);
        JavaRDD<String> file = spark.textFile(filePath).cache();

        doWork(file, filePath);

        spark.stop();
    }

    private void sleep() {
        try {
//            log("sleep count = " + (count++));
//            Thread.sleep(10);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void doWork(JavaRDD<String> file, String filePath) {
        JavaRDD<Account> account = file.flatMap(new FlatMapFunction<String, Account>() {
            @Override
            public Iterable<Account> call(String s) throws Exception {
                Account account = new Account(s);
                return Arrays.asList(account);
            }
        }).cache();


        analyse(account, filePath, true);
        analyse(account, filePath, false);
    }

    private void analyse(JavaRDD<Account> account, String filePath, final boolean password) {
        JavaPairRDD<String, Integer> accountPair = account.mapToPair(new PairFunction<Account, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Account account) throws Exception {
                if (password) {
                    return new Tuple2<String, Integer>(account.password, 1);
                } else {
                    return new Tuple2<String, Integer>(account.getEmailDomain(), 1);
                }
            }
        });

        String saved = "./" + filePath + "_sparkresult" + new Date().toLocaleString();
        if (password) {
            saved += "password";
        } else {
            saved += "mail";
        }

        File saveFile = new File(saved);
        if (saveFile.exists()) {
            File[] files = saveFile.listFiles();
            for (File item : files) {
                item.delete();
            }

            saveFile.delete();
        }

        save(reduceByKey(accountPair), saved);
    }

    private void save(JavaPairRDD<Integer, String> pairRDD, String path) {
        log("save to " + path);
        pairRDD.saveAsTextFile(path);
    }

    private JavaPairRDD<Integer, String> reduceByKey(JavaPairRDD<String, Integer> pairRDD) {
        JavaPairRDD<String, Integer> counts = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                sleep();
                return integer + integer2;
            }
        });

        JavaRDD<Tuple2<Integer, String>> countsSwap = counts.map(new Function<Tuple2<String, Integer>, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                sleep();
                return stringIntegerTuple2.swap();
            }
        });

        JavaPairRDD<Integer, String> countSwap2 = JavaPairRDD.fromJavaRDD(countsSwap).sortByKey(false);

        return countSwap2;
    }

    public static final void log(String msg) {
        System.out.println(msg);
    }

    private static class Account {
        String username;
        String password;
        String email;

        //zhzhyljp # olbamboo # zhzhyljp@126.com
        public Account(String s) {
            String[] strings = s.split("#");
            if (strings.length == 3) {
                username = strings[0];
                password = strings[1];
                email = strings[2];
            }
        }

        @Override
        public String toString() {
            return "username=" + username + ", password=" + password + ", email=" + email;
        }

        public String getEmailDomain() {
            if (null == email) {
                return "EMAILDOMAIN";
            }

            String[] mails = email.split("@");
            if (mails.length == 2) {
                return mails[1];
            }

            return email;
        }
    }
}
